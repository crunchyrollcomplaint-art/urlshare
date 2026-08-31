import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import PurePath
from typing import Any, Optional
from urllib.parse import quote

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("tg-cpanel-channel-bot")

BOT_TOKEN = os.environ["BOT_TOKEN"]
STREAM_SECRET = os.environ["STREAM_SECRET"].encode("utf-8")
CPANEL_WATCH_URL = os.getenv("CPANEL_WATCH_URL", "https://chalchitra.site/e/").rstrip("?")
PUBLIC_BOT_URL = os.getenv("PUBLIC_BOT_URL", "").rstrip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
TOKEN_TTL_SECONDS = int(os.getenv("TOKEN_TTL_SECONDS", "2592000"))
TARGET_CHANNEL_ID = int(os.environ["TARGET_CHANNEL_ID"])
try:
    PRIVATE_LINK_CHANNEL_ID = int(os.environ["PRIVATE_LINK_CHANNEL_ID"])
except KeyError as exc:
    raise RuntimeError("PRIVATE_LINK_CHANNEL_ID is required for the private website-link index channel") from exc
except ValueError as exc:
    raise RuntimeError("PRIVATE_LINK_CHANNEL_ID must be a valid integer Telegram chat ID") from exc
if PRIVATE_LINK_CHANNEL_ID == TARGET_CHANNEL_ID:
    raise RuntimeError("PRIVATE_LINK_CHANNEL_ID must be different from TARGET_CHANNEL_ID")
CPANEL_INGEST_URL = os.environ["CPANEL_INGEST_URL"]
CPANEL_INGEST_SECRET = os.environ["CPANEL_INGEST_SECRET"]
CPANEL_MEDIA_URL = os.getenv("CPANEL_MEDIA_URL", "https://chalchitra.site/chitraengine/media").rstrip("?")
COPY_MAX_ATTEMPTS = max(1, int(os.getenv("COPY_MAX_ATTEMPTS", "3")))
COPY_RETRY_DELAY_SECONDS = max(0.0, float(os.getenv("COPY_RETRY_DELAY_SECONDS", "2")))

app = FastAPI(title="Telegram cPanel Direct Stream Bot", version="4.0.0")

UNSUPPORTED_EXTENSIONS = {".ts", ".avi", ".flv", ".wmv", ".vob", ".mpg", ".mpeg", ".m2ts", ".3gp"}
SUPPORTED_EXTENSIONS = {".mp4", ".mkv", ".webm", ".mov", ".m4v"}
UNSUPPORTED_MIMES = {"video/mp2t", "video/mpeg", "video/x-msvideo", "video/x-flv", "video/x-ms-wmv"}
QUALITY_RE = re.compile(r"(?<![A-Za-z0-9])(2160|1440|1080|720|576|480|360|240|144)\s*[pP](?=$|[\s._-])")
EPISODE_RE = re.compile(r"(?:episode|ep|e)[\s:#._-]*(\d+)(?=$|[\s._-])", re.IGNORECASE)
SEASON_RE = re.compile(r"(?:season|s)[\s:#._-]*(\d+)(?=$|[\s._-]|(?:e|ep|episode)[\s:#._-]*\d)", re.IGNORECASE)
SEASON_EPISODE_RE = re.compile(
    r"(?<![A-Za-z0-9])(?:s\s*)?(\d{1,2})\s*[x×]\s*(?:e(?:p|pisode)?[\s:#._-]*)?(\d{1,3})(?=$|[^0-9])",
    re.IGNORECASE,
)

LANGUAGE_PATTERNS = [
    ("hindi", "Hindi"),
    ("english", "English"),
    ("tamil", "Tamil"),
    ("telugu", "Telugu"),
    ("malayalam", "Malayalam"),
    ("bengali", "Bengali"),
    ("japanese", "Japanese"),
]


@dataclass
class Candidate:
    source_chat_id: int
    source_message_id: int
    file_name: str
    mime_type: str
    file_size: int
    caption: str
    season: str
    episode: str
    quality: str
    audio_language: str
    channel_message_id: Optional[int] = None
    channel_file_id: str = ""
    channel_file_unique_id: str = ""
    source_file_id: str = ""
    source_file_unique_id: str = ""

    @property
    def episode_key(self) -> tuple[str, str]:
        return self.season, self.episode

    @property
    def source_key(self) -> tuple[str, str, str, str]:
        return self.season, self.episode, self.quality, self.audio_language


@dataclass
class ChatState:
    bulk_prefix: Optional[str] = None
    bulk_series_title: str = ""
    bulk_items: list[Candidate] = field(default_factory=list)
    pending_bulk: dict[tuple[int, int], tuple[Candidate, str]] = field(default_factory=dict)
    invalid_replacements: dict[tuple[str, str], str] = field(default_factory=dict)
    pending_single: Optional[Candidate] = None
    processing: bool = False
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


states: dict[int, ChatState] = {}
link_index_messages: dict[str, int] = {}
link_index_entries: dict[str, dict[str, str]] = {}


def state_for(chat_id: int) -> ChatState:
    if chat_id not in states:
        states[chat_id] = ChatState()
    return states[chat_id]


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def sign_payload(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    encoded = b64url(raw)
    signature = hmac.new(STREAM_SECRET, encoded.encode("ascii"), hashlib.sha256).digest()
    return f"{encoded}.{b64url(signature)}"


def make_channel_token(channel_message_id: int, file_size: int, file_name: str, mime: str) -> str:
    now = int(time.time())
    return sign_payload(
        {
            "v": 2,
            "c": TARGET_CHANNEL_ID,
            "m": channel_message_id,
            "f": "",
            "s": file_size,
            "n": file_name[:180],
            "t": mime[:100] or "video/mp4",
            "iat": now,
            "exp": now + TOKEN_TTL_SECONDS,
        }
    )


def media_name(media: dict[str, Any]) -> str:
    return str(media.get("file_name") or "video")


def extension_of(name: str) -> str:
    return PurePath(name.lower()).suffix


def unsupported_reason(media: dict[str, Any], file_name: str) -> Optional[str]:
    mime = str(media.get("mime_type") or "").lower()
    ext = extension_of(file_name)
    if ext in UNSUPPORTED_EXTENSIONS:
        return f"{ext} file"
    if mime in UNSUPPORTED_MIMES:
        return f"{mime}"
    return None


def is_video_media(media: dict[str, Any], file_name: str) -> bool:
    mime = str(media.get("mime_type") or "").lower()
    ext = extension_of(file_name)
    return mime.startswith("video/") or ext in SUPPORTED_EXTENSIONS or ext in UNSUPPORTED_EXTENSIONS


def detect_metadata(caption: str) -> tuple[str, str, str, str]:
    text = caption or ""
    season_episode_match = SEASON_EPISODE_RE.search(text)
    if season_episode_match:
        season = f"S{int(season_episode_match.group(1)):02d}"
        episode = str(int(season_episode_match.group(2))).zfill(2)
    else:
        season_match = SEASON_RE.search(text)
        season = f"S{int(season_match.group(1)):02d}" if season_match else "S01"
        episode_match = EPISODE_RE.search(text)
        if not episode_match:
            raise ValueError("Episode number detect nahi hua. Description mein Episode 1, Ep1 ya E1 format hona chahiye.")
        episode = str(int(episode_match.group(1))).zfill(2)
    quality_match = QUALITY_RE.search(text)
    quality = f"{quality_match.group(1)}p" if quality_match else ""
    if not quality:
        upper = text.upper()
        if re.search(r"(?<![A-Za-z0-9])2K(?=$|[\s._-])", upper):
            quality = "2k"
        elif re.search(r"(?<![A-Za-z0-9])4K(?=$|[\s._-])", upper):
            quality = "4k"
    if not quality:
        raise ValueError("Quality detect nahi hui. Description mein 720p, 1080p, 4K ya similar quality hona chahiye.")

    lower = text.lower()
    if "dual audio" in lower or "multi audio" in lower:
        audio = "Dual Audio"
    else:
        found = [normalized for marker, normalized in LANGUAGE_PATTERNS if re.search(rf"\b{re.escape(marker)}\b", lower)]
        audio = "Dual Audio" if len(found) >= 2 else (found[0] if found else "Unknown")
    return season, episode, quality, audio


def detect_metadata_with_fallback(caption: str, file_name: str) -> tuple[str, str, str, str]:
    if caption.strip():
        try:
            return detect_metadata(caption)
        except ValueError:
            pass
    return detect_metadata(file_name)


def validate_prefix(prefix: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9_-]{1,120}", prefix))


def validate_slug(slug: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._~-]{0,49}", slug))


def help_text() -> str:
    return (
        "🎬 Telegram → GDPlayer direct stream\n\n"
        "/bulk PREFIX- SERIES TITLE\n"
        "Example: /bulk Naruto- Naruto\n"
        "Videos collect karo; links tabhi banenge jab /done bhejoge.\n\n"
        "/done\n"
        "Bulk queue process karke episode links banata hai.\n\n"
        "Single video ke liye video bhejo, phir custom slug bhejo.\n"
        "Example slug: JJK-S01-Ep-10\n\n"
        "Caption me Season, Episode aur Quality hona chahiye.\n"
        "Render video bytes handle nahi karta; stream Telegram → cPanel → viewer hota hai."
    )


class TelegramAPIError(RuntimeError):
    def __init__(
        self,
        method: str,
        *,
        http_status: Optional[int] = None,
        error_code: Optional[int] = None,
        description: str = "unknown error",
        retry_after: Optional[float] = None,
        retryable: bool = False,
    ) -> None:
        self.method = method
        self.http_status = http_status
        self.error_code = error_code
        self.description = description
        self.retry_after = retry_after
        self.retryable = retryable
        parts = []
        if http_status is not None:
            parts.append(f"HTTP {http_status}")
        if error_code is not None:
            parts.append(f"Telegram error {error_code}")
        parts.append(description)
        if retry_after is not None:
            parts.append(f"retry_after={retry_after:g}")
        super().__init__(": ".join(parts))


def telegram_error_from_response(method: str, response: httpx.Response, body: Any) -> TelegramAPIError:
    body = body if isinstance(body, dict) else {}
    parameters = body.get("parameters") if isinstance(body.get("parameters"), dict) else {}
    retry_after = parameters.get("retry_after")
    try:
        retry_after = float(retry_after) if retry_after is not None else None
    except (TypeError, ValueError):
        retry_after = None
    error_code = body.get("error_code")
    try:
        error_code = int(error_code) if error_code is not None else None
    except (TypeError, ValueError):
        error_code = None
    description = str(body.get("description") or response.text[:500] or "unknown error")
    retryable = response.status_code == 429 or response.status_code >= 500 or error_code in {429}
    return TelegramAPIError(
        method,
        http_status=response.status_code,
        error_code=error_code,
        description=description,
        retry_after=retry_after,
        retryable=retryable,
    )


async def telegram_api(method: str, data: dict[str, Any]) -> dict[str, Any]:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    try:
        async with httpx.AsyncClient(timeout=60) as http:
            response = await http.post(url, json=data)
    except httpx.TimeoutException as exc:
        raise TelegramAPIError(method, description=f"network timeout: {exc or 'request timed out'}", retryable=True) from exc
    except httpx.RequestError as exc:
        raise TelegramAPIError(method, description=f"network error: {exc}", retryable=True) from exc

    try:
        body = response.json()
    except ValueError:
        body = {}
    if response.status_code >= 400 or not isinstance(body, dict) or not body.get("ok"):
        raise telegram_error_from_response(method, response, body)
    return body["result"]


async def send_bot_message(chat_id: int, text: str) -> dict[str, Any]:
    return await telegram_api("sendMessage", {"chat_id": chat_id, "text": text, "disable_web_page_preview": True})


async def copy_to_database_channel(source_chat_id: int, source_message_id: int) -> dict[str, Any]:
    return await telegram_api(
        "copyMessage",
        {"chat_id": TARGET_CHANNEL_ID, "from_chat_id": source_chat_id, "message_id": source_message_id},
    )


async def update_private_link_index(
    series_key: str,
    series_title: str,
    slug: str,
    season: str,
    episode: str,
    website_url: str,
    post_ids: list[int],
) -> None:
    entries = link_index_entries.setdefault(series_key, {})
    ids = ", ".join(str(message_id) for message_id in post_ids)
    entries[slug] = (
        f"• {season} E{episode}\n"
        f"➥ ᴡᴇʙ ʟɪɴᴋ: {website_url}\n"
        f"➤ ᴘᴏꜱᴛ ɪᴅ: {ids}"
    )
    heading = series_title or series_key.split("|", 1)[-1]
    text = f"➤ {heading}\n\n" + "\n\n".join(entries.values())
    message_id = link_index_messages.get(series_key)
    if message_id is None:
        sent = await telegram_api(
            "sendMessage",
            {"chat_id": PRIVATE_LINK_CHANNEL_ID, "text": text, "disable_web_page_preview": True},
        )
        link_index_messages[series_key] = int(sent["message_id"])
        return
    try:
        await telegram_api(
            "editMessageText",
            {"chat_id": PRIVATE_LINK_CHANNEL_ID, "message_id": message_id, "text": text, "disable_web_page_preview": True},
        )
    except TelegramAPIError as exc:
        if exc.http_status == 400 and "message to edit not found" in exc.description.lower():
            sent = await telegram_api(
                "sendMessage",
                {"chat_id": PRIVATE_LINK_CHANNEL_ID, "text": text, "disable_web_page_preview": True},
            )
            link_index_messages[series_key] = int(sent["message_id"])
            return
        raise


async def save_gdplayer_metadata(record: dict[str, Any], sources: Optional[list[dict[str, Any]]] = None) -> dict[str, Any]:
    payload = dict(record)
    if sources:
        payload["sources"] = sources
    headers = {"X-CPANEL-INGEST-SECRET": CPANEL_INGEST_SECRET}
    async with httpx.AsyncClient(timeout=60) as http:
        response = await http.post(CPANEL_INGEST_URL, json=payload, headers=headers)
        response_text = response.text[:1000].replace("\n", " ").strip()
        if response.status_code >= 400:
            detail = response_text or "empty response"
            raise RuntimeError(f"cPanel ingest HTTP {response.status_code}: {detail[:500]}")
        if not response_text:
            raise RuntimeError(f"cPanel ingest returned empty HTTP {response.status_code} response")
        try:
            body = response.json()
        except ValueError:
            raise RuntimeError(f"cPanel ingest returned non-JSON HTTP {response.status_code}: {response_text[:500]}")
    if not body.get("ok"):
        raise RuntimeError(body.get("error", "cPanel metadata save failed"))
    return body


def candidate_from_message(chat_id: int, message: dict[str, Any]) -> tuple[Optional[Candidate], Optional[str]]:
    media = message.get("video") or message.get("document")
    if not media:
        return None, "Video bhejo. Sirf video/document media supported hai."
    file_name = media_name(media)
    if not is_video_media(media, file_name):
        return None, "Yeh video file nahi lag rahi. MP4 ya supported video document bhejo."
    reason = unsupported_reason(media, file_name)
    caption = str(message.get("caption") or "")
    try:
        season, episode, quality, audio = detect_metadata_with_fallback(caption, file_name)
    except ValueError as exc:
        return None, str(exc) + " Video queue mein add nahi ki gayi."
    candidate = Candidate(
        source_chat_id=chat_id,
        source_message_id=int(message["message_id"]),
        file_name=file_name,
        mime_type=str(media.get("mime_type") or "video/mp4"),
        file_size=int(media.get("file_size") or 0),
        caption=caption,
        season=season,
        episode=episode,
        quality=quality,
        audio_language=audio,
        source_file_id=str(media.get("file_id") or ""),
        source_file_unique_id=str(media.get("file_unique_id") or ""),
    )
    if reason:
        return candidate, (
            "❌ Unsupported video format\n\n"
            f"Detected: {season} / Episode {episode}\n"
            f"Format: {reason}\n\n"
            f"Ye video format supported nahi hai. Please Episode {episode} ka supported-format replacement bhejo."
        )
    return candidate, None


def copy_candidate_metadata(media: dict[str, Any], candidate: Candidate) -> None:
    # Bot API copyMessage returns only the new message_id. Preserve the original
    # Telegram identifiers; the channel message_id remains the authoritative stream key.
    candidate.channel_file_id = str(media.get("file_id") or candidate.source_file_id)
    candidate.channel_file_unique_id = str(media.get("file_unique_id") or candidate.source_file_unique_id)


def stream_fields(candidate: Candidate) -> dict[str, Any]:
    if not candidate.channel_message_id:
        raise RuntimeError("channel message missing")
    token = make_channel_token(candidate.channel_message_id, candidate.file_size, candidate.file_name, candidate.mime_type)
    stream_url = f"{CPANEL_MEDIA_URL}?token={quote(token, safe='')}"
    watch_url = f"{CPANEL_WATCH_URL}?token={quote(token, safe='')}"
    return {"stream_url": stream_url, "watch_url": watch_url}


def source_record(candidate: Candidate, slug: str, series_title: str = "") -> dict[str, Any]:
    fields = stream_fields(candidate)
    title = candidate.file_name[:1000]
    if series_title:
        title = f"{candidate.season} Ep {candidate.episode} • {series_title}"[:1000]
    return {
        "slug": slug,
        "episode_slug": slug,
        "season": candidate.season,
        "episode": candidate.episode,
        "quality": candidate.quality,
        "audio_language": candidate.audio_language,
        "channel_id": TARGET_CHANNEL_ID,
        "channel_message_id": candidate.channel_message_id,
        "source_chat_id": candidate.source_chat_id,
        "source_message_id": candidate.source_message_id,
        "title": title,
        "file_id": candidate.channel_file_id or candidate.source_file_id,
        "file_unique_id": candidate.channel_file_unique_id or candidate.source_file_unique_id,
        "file_name": candidate.file_name[:180],
        "mime_type": candidate.mime_type[:100] or "video/mp4",
        "file_size": candidate.file_size,
        "caption": candidate.caption,
        "stream_url": fields["stream_url"],
    }


_copy_channel_lock = asyncio.Lock()


async def copy_candidate(candidate: Candidate) -> None:
    if candidate.channel_message_id:
        return
    async with _copy_channel_lock:
        if candidate.channel_message_id:
            return
        for attempt in range(1, COPY_MAX_ATTEMPTS + 1):
            logger.info(
                "Copy attempt %s/%s: source_chat_id=%s source_message_id=%s",
                attempt, COPY_MAX_ATTEMPTS, candidate.source_chat_id, candidate.source_message_id,
            )
            try:
                copied = await copy_to_database_channel(candidate.source_chat_id, candidate.source_message_id)
                candidate.channel_message_id = int(copied["message_id"])
                copied_media = copied.get("video") or copied.get("document") or {}
                copy_candidate_metadata(copied_media, candidate)
                logger.info(
                    "Copy succeeded: source_message_id=%s -> channel_message_id=%s",
                    candidate.source_message_id, candidate.channel_message_id,
                )
                return
            except TelegramAPIError as exc:
                logger.warning("Copy attempt %s/%s failed: %s", attempt, COPY_MAX_ATTEMPTS, exc)
                if not exc.retryable or attempt >= COPY_MAX_ATTEMPTS:
                    raise
                delay = exc.retry_after if exc.retry_after is not None else COPY_RETRY_DELAY_SECONDS * attempt
                logger.info("Waiting %.2f seconds before retrying copy", delay)
                await asyncio.sleep(delay)
            except (httpx.TimeoutException, httpx.RequestError) as exc:
                logger.warning("Copy attempt %s/%s failed: network error: %s", attempt, COPY_MAX_ATTEMPTS, exc)
                if attempt >= COPY_MAX_ATTEMPTS:
                    raise
                delay = COPY_RETRY_DELAY_SECONDS * attempt
                logger.info("Waiting %.2f seconds before retrying copy", delay)
                await asyncio.sleep(delay)


async def handle_single_slug(chat_id: int, slug: str) -> None:
    state = state_for(chat_id)
    async with state.lock:
        candidate = state.pending_single
        if candidate is None:
            await send_bot_message(chat_id, "Pehle video bhejo, phir custom slug bhejo.\n\n" + help_text())
            return
        if not validate_slug(slug):
            await send_bot_message(chat_id, "❌ Slug invalid hai. Sirf letters, numbers, hyphen, underscore, dot ya tilde use karo.")
            return
        state.pending_single = None
    try:
        await copy_candidate(candidate)
        record = source_record(candidate, slug)
        saved = await save_gdplayer_metadata(record)
        player_url = saved.get("slug_url") or saved.get("embed_url") or f"{CPANEL_WATCH_URL}?token={quote(stream_fields(candidate)['watch_url'].split('token=', 1)[-1], safe='')}"
        await send_bot_message(chat_id, f"✅ Video ready\n\n🔗 {player_url}")
    except Exception as exc:
        logger.exception("Single video processing failed")
        async with state.lock:
            if state.pending_single is None:
                state.pending_single = candidate
        detail = str(exc).replace("\n", " ")[:500]
        await send_bot_message(chat_id, f"❌ Video save nahi ho paya.\nReason: {detail}\n\nSame video ke liye slug dobara bhej sakte ho.")


async def handle_bulk_media(chat_id: int, message: dict[str, Any]) -> None:
    state = state_for(chat_id)
    candidate, error = candidate_from_message(chat_id, message)
    if error:
        await send_bot_message(chat_id, error)
        if candidate is not None and error.startswith("❌ Unsupported video format"):
            state.invalid_replacements[candidate.episode_key] = "replacement pending"
        return

    assert candidate is not None
    async with state.lock:
        if state.processing or not state.bulk_prefix:
            await send_bot_message(chat_id, "Bulk mode active nahi hai. Single video ke liye video bhejo; bulk ke liye /bulk PREFIX- bhejo.")
            return
        pending_key = (candidate.source_chat_id, candidate.source_message_id)
        pending_entry = state.pending_bulk.pop(pending_key, None)
        if pending_entry is not None:
            # Reuse the original candidate so a previously successful copy can never be duplicated.
            candidate = pending_entry[0]
        if any(item.source_key == candidate.source_key for item in state.bulk_items):
            if pending_entry is not None and not candidate.channel_message_id:
                state.pending_bulk[pending_key] = pending_entry
            await send_bot_message(chat_id, f"❌ Is episode ki ye quality already save hai: {candidate.season}-Ep-{candidate.episode}-{candidate.quality}. Duplicate source ignore kiya gaya.")
            return
    try:
        await copy_candidate(candidate)
    except Exception as exc:
        detail = str(exc).replace("\n", " ")[:500]
        logger.error("Bulk copy failed for episode %s: %s", candidate.episode, detail)
        async with state.lock:
            state.pending_bulk[(candidate.source_chat_id, candidate.source_message_id)] = (candidate, detail)
        await send_bot_message(
            chat_id,
            f"❌ Episode {candidate.episode} private channel copy nahi ho paya.\n"
            f"Reason: {detail}\n\nVideo pending retry queue me rakha gaya hai; doosre videos process hote rahenge.",
        )
        return
    async with state.lock:
        replacement = candidate.episode_key in state.invalid_replacements
        state.bulk_items.append(candidate)
        state.pending_bulk.pop((candidate.source_chat_id, candidate.source_message_id), None)
        state.invalid_replacements.pop(candidate.episode_key, None)
        prefix = state.bulk_prefix
    await send_bot_message(
        chat_id,
        (
            f"✅ Replacement accepted.\nDetected: {candidate.season} / Episode {candidate.episode} / {candidate.quality} / {candidate.audio_language}\n"
            f"Episode {candidate.episode} queue mein update ho gaya."
            if replacement else
            f"✅ Video queue mein add ho gaya.\nDetected: {candidate.season} / Episode {candidate.episode} / {candidate.quality} / {candidate.audio_language}\n\n"
            "Aur videos bhejo. Sab complete hone ke baad /done bhejo."
        ),
    )


async def process_bulk(
    chat_id: int,
    prefix: str,
    items: list[Candidate],
    invalid: dict[tuple[str, str], str],
    series_title: str = "",
) -> None:
    grouped: dict[str, list[Candidate]] = {}
    for item in items:
        slug = f"{prefix}{item.season}-Ep-{item.episode}"
        grouped.setdefault(slug, []).append(item)
    complete_links: list[str] = []
    for slug, candidates in grouped.items():
        candidates.sort(key=lambda item: (item.quality, item.audio_language))
        primary = candidates[0]
        copied_candidates = list(candidates)
        try:
            sources = [source_record(item, slug, series_title) for item in candidates]
            saved = await save_gdplayer_metadata(sources[0], sources=sources)
            website_url = saved.get("slug_url") or saved.get("embed_url") or f"{CPANEL_WATCH_URL}{slug}"
            complete_links.append(website_url)
            await send_bot_message(
                chat_id,
                "✅ Episode ready\n"
                f"Season: {primary.season}\nEpisode: {primary.episode}\n"
                f"Sources: {', '.join(item.quality + ' • ' + item.audio_language for item in candidates)}\n\n"
                f"🔗 {website_url}",
            )
            try:
                await update_private_link_index(
                    series_key=f"{prefix}|{series_title}",
                    series_title=series_title,
                    slug=slug,
                    season=primary.season,
                    episode=primary.episode,
                    website_url=website_url,
                    post_ids=[item.channel_message_id for item in copied_candidates if item.channel_message_id],
                )
            except Exception as index_exc:
                logger.exception("Private link index update failed for %s", slug)
                detail = str(index_exc).replace("\n", " ")[:500]
                await send_bot_message(chat_id, f"⚠️ Episode {slug} save ho gaya, lekin private link index update nahi ho paya.\nReason: {detail}")
        except Exception as exc:
            logger.exception("Bulk episode ingest failed for %s", slug)
            detail = str(exc).replace("\n", " ")[:500]
            await send_bot_message(chat_id, f"❌ Episode {slug} save nahi ho paya.\nReason: {detail}")
    if invalid:
        missing = ", ".join(f"{season}/Episode {episode}" for season, episode in sorted(invalid))
        await send_bot_message(chat_id, f"⚠️ Replacement ke bina incomplete episodes: {missing}")
    await send_bot_message(
        chat_id,
        f"✅ Bulk complete ho gaya.\nTotal valid videos: {len(items)}\nTotal episodes: {len(grouped)}\nLinks generated: {len(complete_links)}",
    )


async def finish_bulk(chat_id: int) -> None:
    state = state_for(chat_id)
    async with state.lock:
        if not state.bulk_prefix:
            await send_bot_message(chat_id, "Bulk mode active nahi hai. Pehle /bulk PREFIX- bhejo.")
            return
        if state.processing:
            await send_bot_message(chat_id, "Bulk processing already chal rahi hai.")
            return
        prefix = state.bulk_prefix
        series_title = state.bulk_series_title
        items = list(state.bulk_items)
        invalid = dict(state.invalid_replacements)
        pending = dict(state.pending_bulk)
        state.processing = True
        state.bulk_prefix = None
        state.bulk_series_title = ""
        state.bulk_items = []
        state.invalid_replacements = {}
    await send_bot_message(chat_id, "⏳ Bulk processing start ho gayi hai. Queued videos process ho rahe hain.")
    try:
        await process_bulk(chat_id, prefix, items, invalid, series_title)
        if pending:
            pending_lines = "\n".join(
                f"• Episode {candidate.episode}: {reason}"
                for candidate, reason in pending.values()
            )
            await send_bot_message(
                chat_id,
                "⚠️ Videos pending retry queue me preserve kiye gaye:\n" + pending_lines,
            )
    finally:
        state.processing = False


async def process_update(update: dict[str, Any]) -> None:
    message = update.get("message") or update.get("edited_message")
    if not message:
        return
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    message_id = message.get("message_id")
    if not isinstance(chat_id, int) or not isinstance(message_id, int):
        return
    text = str(message.get("text") or "").strip()
    if text.startswith("/start"):
        await send_bot_message(chat_id, help_text())
        return
    if text.startswith("/help"):
        await send_bot_message(chat_id, help_text())
        return
    if text.startswith("/bulk") or text.startswith("/nulk"):
        bulk_args = text[5:].strip()
        prefix, _, series_title = bulk_args.partition(" ")
        series_title = series_title.strip()
        if not validate_prefix(prefix):
            await send_bot_message(chat_id, "Usage: /bulk PREFIX- SERIES TITLE\nExample: /bulk Naruto- Naruto")
            return
        state = state_for(chat_id)
        async with state.lock:
            state.bulk_prefix = prefix
            state.bulk_series_title = series_title
            state.bulk_items = []
            state.invalid_replacements = {}
            state.pending_single = None
            state.processing = False
        title_note = f"\nSeries title: {series_title}" if series_title else ""
        await send_bot_message(chat_id, f"✅ Bulk mode active: {prefix}{title_note}\nVideos bhejo. Links ke liye end me /done bhejo.")
        return
    if text.startswith("/done"):
        asyncio.create_task(finish_bulk(chat_id))
        return
    if text and not message.get("video") and not message.get("document"):
        state = state_for(chat_id)
        if state.pending_single is not None:
            await handle_single_slug(chat_id, text)
        else:
            await send_bot_message(chat_id, "Command samajh nahi aaya. /help likho.")
        return
    if message.get("video") or message.get("document"):
        state = state_for(chat_id)
        if state.bulk_prefix:
            await handle_bulk_media(chat_id, message)
            return
        candidate, error = candidate_from_message(chat_id, message)
        if error:
            await send_bot_message(chat_id, error)
            return
        assert candidate is not None
        async with state.lock:
            state.pending_single = candidate
        await send_bot_message(chat_id, "🔗 Is video ke liye custom slug bhejo:")
        return
    await send_bot_message(chat_id, "Video bhejo ya /help likho.")


async def set_telegram_webhook_once() -> bool:
    if not PUBLIC_BOT_URL:
        return True
    data: dict[str, Any] = {"url": f"{PUBLIC_BOT_URL}/telegram/webhook", "allowed_updates": ["message"]}
    if WEBHOOK_SECRET:
        data["secret_token"] = WEBHOOK_SECRET
    try:
        await telegram_api("setWebhook", data)
        return True
    except Exception as exc:
        logger.error("Webhook registration failed: %s", exc)
        return False


async def webhook_retry_loop() -> None:
    delay = 5
    while True:
        if await set_telegram_webhook_once():
            return
        await asyncio.sleep(delay)
        delay = min(delay * 2, 300)


@app.on_event("startup")
async def startup() -> None:
    app.state.webhook_task: Optional[asyncio.Task] = None
    if PUBLIC_BOT_URL:
        app.state.webhook_task = asyncio.create_task(webhook_retry_loop())


@app.on_event("shutdown")
async def shutdown() -> None:
    task = getattr(app.state, "webhook_task", None)
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"ok": True, "service": "telegram-cpanel-direct-stream-bot", "media_relay": False, "conversion": False}


@app.post("/telegram/webhook")
async def telegram_webhook(
    request: Request,
    x_telegram_bot_api_secret_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    if WEBHOOK_SECRET and not hmac.compare_digest(x_telegram_bot_api_secret_token or "", WEBHOOK_SECRET):
        raise HTTPException(status_code=401, detail="Invalid webhook secret")
    update = await request.json()
    asyncio.create_task(process_update(update))
    return JSONResponse({"ok": True})
