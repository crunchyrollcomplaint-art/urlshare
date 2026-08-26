import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import re
import time
from typing import Any, Optional
from urllib.parse import quote

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("tg-cpanel-channel-bot")

BOT_TOKEN = os.environ["BOT_TOKEN"]
STREAM_SECRET = os.environ["STREAM_SECRET"].encode("utf-8")
CPANEL_WATCH_URL = os.environ["CPANEL_WATCH_URL"].rstrip("?")
PUBLIC_BOT_URL = os.getenv("PUBLIC_BOT_URL", "").rstrip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
TOKEN_TTL_SECONDS = int(os.getenv("TOKEN_TTL_SECONDS", "2592000"))
TARGET_CHANNEL_ID = int(os.environ["TARGET_CHANNEL_ID"])
CPANEL_INGEST_URL = os.environ["CPANEL_INGEST_URL"]
CPANEL_INGEST_SECRET = os.environ["CPANEL_INGEST_SECRET"]
CPANEL_MEDIA_URL = os.getenv("CPANEL_MEDIA_URL", "https://chalchitra.site/tgstreamnode/media").rstrip("?")

app = FastAPI(title="Telegram cPanel Channel Bot", version="4.0.0")

# These locks serialize updates for one chat. They never contain media bytes.
CHAT_LOCKS: dict[int, asyncio.Lock] = {}


class SlugConflict(RuntimeError):
    pass


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def sign_payload(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    encoded = b64url(raw)
    signature = hmac.new(STREAM_SECRET, encoded.encode("ascii"), hashlib.sha256).digest()
    return f"{encoded}.{b64url(signature)}"


async def telegram_api(method: str, data: dict[str, Any]) -> dict[str, Any]:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    async with httpx.AsyncClient(timeout=60) as http:
        response = await http.post(url, json=data)
        response.raise_for_status()
        body = response.json()
    if not body.get("ok"):
        raise RuntimeError(f"Telegram {method} failed: {body.get('description', 'unknown error')}")
    return body["result"]


async def send_bot_message(chat_id: int, text: str) -> None:
    await telegram_api(
        "sendMessage",
        {"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
    )


async def copy_to_database_channel(source_chat_id: int, source_message_id: int) -> dict[str, Any]:
    # Telegram performs this copy internally; Render never downloads or relays media bytes.
    return await telegram_api(
        "copyMessage",
        {
            "chat_id": TARGET_CHANNEL_ID,
            "from_chat_id": source_chat_id,
            "message_id": source_message_id,
            # Empty replacement caption removes the original caption on the channel copy.
            "caption": "",
        },
    )


async def session_request(
    action: str,
    chat_id: int,
    mode: str = "",
    prefix: str = "",
    pending: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Use the protected cPanel endpoint for small session state only, never media bytes."""
    payload: dict[str, Any] = {
        "action": "session",
        "session_action": action,
        "chat_id": chat_id,
    }
    if action == "set":
        payload.update({"mode": mode, "prefix": prefix, "pending": pending})
    headers = {"X-CPANEL-INGEST-SECRET": CPANEL_INGEST_SECRET}
    async with httpx.AsyncClient(timeout=30) as http:
        response = await http.post(CPANEL_INGEST_URL, json=payload, headers=headers)
        response.raise_for_status()
        body = response.json()
    if not body.get("ok"):
        raise RuntimeError(body.get("error", "Session state request failed"))
    return body.get("session") or {}


def parse_command(text: str) -> tuple[Optional[str], str]:
    parts = text.strip().split(maxsplit=1)
    if not parts or not parts[0].startswith("/"):
        return None, ""
    command = parts[0].split("@", 1)[0].lower()
    return command, parts[1].strip() if len(parts) > 1 else ""


def valid_slug(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,49}", value))


def valid_prefix(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,39}", value))


def extract_video_payload(message: dict[str, Any]) -> Optional[dict[str, Any]]:
    media = message.get("video") or message.get("document")
    if not isinstance(media, dict):
        return None

    mime = str(media.get("mime_type") or "")
    file_name = str(media.get("file_name") or "video")
    is_video = mime.startswith("video/") or file_name.lower().endswith(
        (".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v")
    )
    if not is_video:
        return None

    return {
        "source_chat_id": int(message["chat"]["id"]),
        "source_message_id": int(message["message_id"]),
        "file_id": str(media.get("file_id") or ""),
        "file_unique_id": str(media.get("file_unique_id") or ""),
        "file_name": file_name,
        "mime_type": mime or "video/mp4",
        "file_size": int(media.get("file_size") or 0),
        "caption": str(message.get("caption") or ""),
    }


def detect_season(caption: str) -> str:
    patterns = (
        r"\bS\s*[-:]?\s*(\d+)\b",
        r"\bSeason\s*[-:]?\s*(\d+)\b",
        r"الموسم\s*[-:]?\s*(\d+)",
    )
    for pattern in patterns:
        match = re.search(pattern, caption, flags=re.IGNORECASE)
        if match:
            return "S" + str(int(match.group(1))).zfill(2)
    return "S01"


def detect_episode(caption: str) -> str:
    match = re.search(r"\b(?:Episode|Ep|E)\s*[-:]?\s*(\d+)\b", caption, flags=re.IGNORECASE)
    if match:
        return str(int(match.group(1))).zfill(2)
    return "01"


def detect_quality(caption: str) -> str:
    match = re.search(r"\b(2160|1440|1080|720|576|480|360|240)\s*[pP]\b", caption)
    if match:
        return f"{match.group(1)}p"
    if re.search(r"\b4k\b", caption, flags=re.IGNORECASE):
        return "2160p"
    if re.search(r"\b2k\b", caption, flags=re.IGNORECASE):
        return "1440p"
    return "unknown"


def make_bulk_slug(prefix: str, caption: str, source_message_id: int) -> str:
    base = f"{prefix}{detect_season(caption)}-Ep-{detect_episode(caption)}-{detect_quality(caption)}"
    return base[:50] or f"telegram-{source_message_id}"


def unique_retry_slug(slug: str, source_message_id: int) -> str:
    suffix = f"-{source_message_id}"
    return (slug[: max(1, 50 - len(suffix))] + suffix)[:50]


def make_channel_token(channel_message_id: int, file_size: int, file_name: str, mime: str) -> str:
    now = int(time.time())
    payload = {
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
    return sign_payload(payload)


async def save_gdplayer_metadata(record: dict[str, Any]) -> dict[str, Any]:
    headers = {"X-CPANEL-INGEST-SECRET": CPANEL_INGEST_SECRET}
    async with httpx.AsyncClient(timeout=30) as http:
        response = await http.post(CPANEL_INGEST_URL, json=record, headers=headers)
        try:
            body = response.json()
        except ValueError:
            body = {}
        if response.status_code == 409 or "slug" in str(body.get("error", "")).lower():
            raise SlugConflict(body.get("error", "Slug already exists"))
        response.raise_for_status()
    if not body.get("ok"):
        raise RuntimeError(body.get("error", "cPanel metadata save failed"))
    return body


async def process_captured_video(chat_id: int, video: dict[str, Any], slug: str, allow_slug_retry: bool) -> None:
    copied = await copy_to_database_channel(video["source_chat_id"], video["source_message_id"])
    channel_message_id = int(copied["message_id"])
    copied_media = copied.get("video") or copied.get("document") or {}
    file_id = str(copied_media.get("file_id") or video["file_id"])
    file_unique_id = str(copied_media.get("file_unique_id") or video["file_unique_id"])
    file_name = str(copied_media.get("file_name") or video["file_name"])
    mime = str(copied_media.get("mime_type") or video["mime_type"] or "video/mp4")
    file_size = int(copied_media.get("file_size") or video["file_size"] or 0)
    caption = video["caption"]
    title = file_name[:1000]
    token = make_channel_token(channel_message_id, file_size, file_name, mime)
    stream_url = f"{CPANEL_MEDIA_URL}?token={quote(token, safe='')}"

    candidates = [slug]
    if allow_slug_retry:
        candidates.append(unique_retry_slug(slug, video["source_message_id"]))

    saved: Optional[dict[str, Any]] = None
    final_slug = slug
    for candidate in candidates:
        record = {
            "slug": candidate,
            "channel_id": TARGET_CHANNEL_ID,
            "channel_message_id": channel_message_id,
            "source_chat_id": video["source_chat_id"],
            "source_message_id": video["source_message_id"],
            "title": title,
            "file_id": file_id,
            "file_unique_id": file_unique_id,
            "file_name": file_name[:180],
            "mime_type": mime[:100] or "video/mp4",
            "file_size": file_size,
            "caption": caption,
            "stream_url": stream_url,
        }
        try:
            saved = await save_gdplayer_metadata(record)
            final_slug = candidate
            break
        except SlugConflict:
            if candidate == candidates[-1]:
                raise

    if not saved:
        raise RuntimeError("Metadata save did not return a result")

    size_text = f"{file_size / (1024 * 1024):.1f} MB" if file_size else "unknown size"
    player_url = str(saved.get("embed_url") or "")
    if not player_url:
        raise RuntimeError("cPanel did not return GDPlayer link")
    await send_bot_message(
        chat_id,
        "✅ Video channel mein save ho gaya.\n\n"
        f"Size: {size_text}\n"
        "▶️ GDPlayer link:\n"
        f"{player_url}\n\n"
        "Render sirf copy command, metadata aur link handle karta hai. Video cPanel se Telegram channel ke through stream hoga.",
    )
    logger.info("Processed chat=%s channel_message=%s slug=%s", chat_id, channel_message_id, final_slug)


async def process_update(update: dict[str, Any]) -> None:
    message = update.get("message") or update.get("edited_message")
    if not message:
        return
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    source_message_id = message.get("message_id")
    if not isinstance(chat_id, int) or not isinstance(source_message_id, int):
        return

    lock = CHAT_LOCKS.setdefault(chat_id, asyncio.Lock())
    async with lock:
        try:
            text = str(message.get("text") or "").strip()
            command, argument = parse_command(text) if text else (None, "")

            if command == "/set":
                await session_request("set", chat_id, mode="set_awaiting_video")
                await send_bot_message(chat_id, "Video bhejo.")
                return

            if command == "/bulk":
                if not valid_prefix(argument):
                    await send_bot_message(chat_id, "Format: /bulk JJK-")
                    return
                await session_request("set", chat_id, mode="bulk", prefix=argument)
                await send_bot_message(
                    chat_id,
                    "Bulk mode active hai.\n\n"
                    f"Base prefix: {argument}\n"
                    "Season, episode aur quality description se automatically detect hogi.\n"
                    "Videos bhejte raho. Bulk mode end karne ke liye /done bhejo.",
                )
                return

            if command == "/done":
                await session_request("clear", chat_id)
                await send_bot_message(chat_id, "✅ Bulk mode complete ho gaya.")
                return

            state = await session_request("get", chat_id)
            mode = str(state.get("mode") or "")
            pending = state.get("pending") if isinstance(state.get("pending"), dict) else None
            prefix = str(state.get("prefix") or "")

            if text and mode == "set_awaiting_slug":
                if not pending:
                    await session_request("set", chat_id, mode="set_awaiting_video")
                    await send_bot_message(chat_id, "Video bhejo.")
                    return
                if not valid_slug(text):
                    await send_bot_message(chat_id, "Slug invalid hai. Sirf letters, numbers, - aur _ use karo (max 50).")
                    return
                await process_captured_video(chat_id, pending, text, allow_slug_retry=False)
                await session_request("clear", chat_id)
                return

            video = extract_video_payload(message)
            if video:
                if mode == "set_awaiting_video":
                    await session_request("set", chat_id, mode="set_awaiting_slug", pending=video)
                    await send_bot_message(chat_id, "Ab slug bhejo. Example: JJK-S01-Ep-01-720p")
                    return
                if mode == "set_awaiting_slug":
                    await send_bot_message(chat_id, "Pehle is video ka slug bhejo.")
                    return
                if mode == "bulk" and prefix:
                    slug = make_bulk_slug(prefix, video["caption"], video["source_message_id"])
                    await process_captured_video(chat_id, video, slug, allow_slug_retry=True)
                    return
                await send_bot_message(chat_id, "Pehle /set ya /bulk PREFIX bhejo.")
                return

            if mode == "set_awaiting_video":
                await send_bot_message(chat_id, "Video bhejo.")
            elif not command and mode == "":
                await send_bot_message(chat_id, "Pehle /set ya /bulk PREFIX bhejo.")
        except Exception as exc:
            logger.exception("Update processing failed: %s", exc)
            await send_bot_message(chat_id, "Video process nahi ho paya. cPanel/Telegram setup check karo.")


async def set_telegram_webhook_once() -> bool:
    if not PUBLIC_BOT_URL:
        return True
    data: dict[str, Any] = {
        "url": f"{PUBLIC_BOT_URL}/telegram/webhook",
        "allowed_updates": ["message"],
    }
    if WEBHOOK_SECRET:
        data["secret_token"] = WEBHOOK_SECRET
    try:
        await telegram_api("setWebhook", data)
        logger.info("Telegram webhook configured")
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
    return {"ok": True, "service": "telegram-cpanel-channel-bot", "media_relay": False, "workflow": "set-bulk-done"}


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
