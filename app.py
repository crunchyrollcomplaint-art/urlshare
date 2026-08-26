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

app = FastAPI(title="Telegram cPanel Channel Bot", version="5.0.0")
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
    # Telegram performs the copy internally. Render never downloads or relays media bytes.
    # No caption override is sent: the original caption stays unchanged in the channel.
    return await telegram_api(
        "copyMessage",
        {
            "chat_id": TARGET_CHANNEL_ID,
            "from_chat_id": source_chat_id,
            "message_id": source_message_id,
        },
    )


async def session_request(
    action: str,
    chat_id: int,
    mode: str = "",
    prefix: str = "",
    queue: Optional[list[dict[str, Any]]] = None,
    pending: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "action": "session",
        "session_action": action,
        "chat_id": chat_id,
    }
    if action == "set":
        payload.update({"mode": mode, "prefix": prefix, "queue": queue or [], "pending": pending})
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
    return parts[0].split("@", 1)[0].lower(), parts[1].strip() if len(parts) > 1 else ""


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
    for pattern in (
        r"\bS\s*[-:]?\s*(\d+)\b",
        r"\bSeason\s*[-:]?\s*(\d+)\b",
        r"الموسم\s*[-:]?\s*(\d+)",
    ):
        match = re.search(pattern, caption, flags=re.IGNORECASE)
        if match:
            return "S" + str(int(match.group(1))).zfill(2)
    return "S01"


def detect_episode(caption: str) -> str:
    match = re.search(r"\b(?:Episode|Ep|E)\s*[-:]?\s*(\d+)\b", caption, flags=re.IGNORECASE)
    return str(int(match.group(1))).zfill(2) if match else "01"


def detect_quality(value: str) -> str:
    match = re.search(r"\b(2160|1440|1080|720|576|480|360|240)\s*[pP]\b", value)
    if match:
        return f"{match.group(1)}p"
    if re.search(r"\b4k\b", value, flags=re.IGNORECASE):
        return "2160p"
    if re.search(r"\b2k\b", value, flags=re.IGNORECASE):
        return "1440p"
    return "unknown"


def quality_size(quality: str) -> int:
    match = re.fullmatch(r"(\d+)p", quality, flags=re.IGNORECASE)
    return int(match.group(1)) if match else 0


def make_bulk_slug(prefix: str, caption: str) -> str:
    normalized_prefix = prefix if prefix.endswith("-") else prefix + "-"
    return f"{normalized_prefix}{detect_season(caption)}-Ep-{detect_episode(caption)}"[:50]


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


def build_record(video: dict[str, Any], copied: dict[str, Any], slug: str, quality: str) -> dict[str, Any]:
    copied_media = copied.get("video") or copied.get("document") or {}
    channel_message_id = int(copied["message_id"])
    file_name = str(copied_media.get("file_name") or video["file_name"])
    mime = str(copied_media.get("mime_type") or video["mime_type"] or "video/mp4")
    file_size = int(copied_media.get("file_size") or video["file_size"] or 0)
    token = make_channel_token(channel_message_id, file_size, file_name, mime)
    return {
        "slug": slug,
        "quality": quality,
        "quality_size": quality_size(quality),
        "channel_id": TARGET_CHANNEL_ID,
        "channel_message_id": channel_message_id,
        "source_chat_id": video["source_chat_id"],
        "source_message_id": video["source_message_id"],
        "title": file_name[:1000],
        "file_id": str(copied_media.get("file_id") or video["file_id"]),
        "file_unique_id": str(copied_media.get("file_unique_id") or video["file_unique_id"]),
        "file_name": file_name[:180],
        "mime_type": mime[:100] or "video/mp4",
        "file_size": file_size,
        "caption": video["caption"],
        "stream_url": f"{CPANEL_MEDIA_URL}?token={quote(token, safe='')}",
    }


async def save_gdplayer_metadata(record: dict[str, Any]) -> dict[str, Any]:
    headers = {"X-CPANEL-INGEST-SECRET": CPANEL_INGEST_SECRET}
    async with httpx.AsyncClient(timeout=30) as http:
        response = await http.post(CPANEL_INGEST_URL, json=record, headers=headers)
        try:
            body = response.json()
        except ValueError:
            body = {}
        if response.status_code == 409:
            raise SlugConflict(body.get("error", "Slug or quality already exists"))
        response.raise_for_status()
    if not body.get("ok"):
        raise RuntimeError(body.get("error", "cPanel metadata save failed"))
    return body


async def process_single(chat_id: int, record: dict[str, Any]) -> dict[str, Any]:
    saved = await save_gdplayer_metadata(record)
    size = record["file_size"]
    size_text = f"{size / (1024 * 1024):.1f} MB" if size else "unknown size"
    player_url = str(saved.get("embed_url") or "")
    await send_bot_message(
        chat_id,
        "✅ Video channel mein save ho gaya.\n\n"
        f"Size: {size_text}\n"
        "▶️ GDPlayer link:\n"
        f"{player_url}\n\n"
        "Render sirf copy command, metadata aur link handle karta hai. Video cPanel se Telegram channel ke through stream hoga.",
    )
    return saved


async def collect_bulk_video(chat_id: int, state: dict[str, Any], video: dict[str, Any]) -> None:
    copied = await copy_to_database_channel(video["source_chat_id"], video["source_message_id"])
    quality = detect_quality(video["caption"])
    record = build_record(video, copied, make_bulk_slug(str(state.get("prefix") or ""), video["caption"]), quality)
    queue = list(state.get("queue") or [])
    queue.append(record)
    await session_request("set", chat_id, mode="bulk_collecting", prefix=str(state.get("prefix") or ""), queue=queue)
    season = detect_season(video["caption"])
    episode = detect_episode(video["caption"])
    await send_bot_message(chat_id, f"✅ Queue mein add ho gaya: {season} / Episode {episode} / {quality}")


async def finish_bulk(chat_id: int, state: dict[str, Any]) -> None:
    prefix = str(state.get("prefix") or "")
    queue = list(state.get("queue") or [])
    await session_request("set", chat_id, mode="bulk_processing", prefix=prefix, queue=queue)
    await send_bot_message(chat_id, "⏳ Bulk processing start ho gayi hai. Queued videos process ho rahe hain.")

    episodes: dict[str, dict[str, Any]] = {}
    remaining = queue[:]
    try:
        for record in queue:
            saved = await save_gdplayer_metadata(record)
            item = episodes.setdefault(record["slug"], {"url": saved.get("embed_url", ""), "qualities": set()})
            item["url"] = saved.get("embed_url", item["url"])
            item["qualities"].add(record["quality"])
            remaining.pop(0)
            await session_request("set", chat_id, mode="bulk_processing", prefix=prefix, queue=remaining)
    except Exception:
        await session_request("set", chat_id, mode="bulk_collecting", prefix=prefix, queue=remaining)
        raise

    for slug, item in episodes.items():
        qualities = sorted(item["qualities"], key=lambda q: quality_size(q) or 99999)
        season_match = re.search(r"(S\d+)", slug)
        episode_match = re.search(r"-Ep-(\d+)", slug)
        await send_bot_message(
            chat_id,
            "✅ Episode ready\n\n"
            f"Season: {season_match.group(1) if season_match else 'S01'}\n"
            f"Episode: {episode_match.group(1) if episode_match else '01'}\n"
            f"Qualities: {', '.join(qualities)}\n\n"
            f"🔗 {item['url']}",
        )

    await session_request("clear", chat_id)
    await send_bot_message(
        chat_id,
        "✅ Bulk complete ho gaya.\n"
        f"Total videos: {len(queue)}\n"
        f"Total episodes: {len(episodes)}\n"
        f"Links generated: {len(episodes)}",
    )


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

            if command in {"/start", "/help"}:
                await send_bot_message(
                    chat_id,
                    "Telegram video bot ready hai.\n\n"
                    "/set - single video ke liye\n"
                    "/bulk PREFIX - multiple qualities/episodes ke liye\n"
                    "/done - bulk collection band karke processing start karne ke liye",
                )
                return

            if command == "/set":
                await session_request("set", chat_id, mode="set_awaiting_video")
                await send_bot_message(chat_id, "Video bhejo.")
                return

            if command == "/bulk":
                if not valid_prefix(argument):
                    await send_bot_message(chat_id, "Format: /bulk JJK-")
                    return
                await session_request("set", chat_id, mode="bulk_collecting", prefix=argument, queue=[])
                await send_bot_message(
                    chat_id,
                    "Bulk mode active hai.\n\n"
                    f"Base prefix: {argument}\n"
                    "Season, episode aur quality description se automatically detect hogi.\n"
                    "Videos bhejte raho. Bulk mode end karne ke liye /done bhejo.",
                )
                return

            state = await session_request("get", chat_id)
            mode = str(state.get("mode") or "")

            if command == "/done":
                if mode in {"bulk_collecting", "bulk_processing"}:
                    await finish_bulk(chat_id, state)
                else:
                    await session_request("clear", chat_id)
                    await send_bot_message(chat_id, "✅ Bulk mode complete ho gaya.")
                return

            pending = state.get("pending") if isinstance(state.get("pending"), dict) else None
            if text and mode == "set_awaiting_slug":
                if not pending:
                    await session_request("set", chat_id, mode="set_awaiting_video")
                    await send_bot_message(chat_id, "Video bhejo.")
                    return
                if not valid_slug(text):
                    await send_bot_message(chat_id, "Slug invalid hai. Sirf letters, numbers, - aur _ use karo (max 50).")
                    return
                quality = detect_quality(pending.get("caption", ""))
                copied = await copy_to_database_channel(pending["source_chat_id"], pending["source_message_id"])
                record = build_record(pending, copied, text, "Original" if quality == "unknown" else quality)
                await process_single(chat_id, record)
                await session_request("clear", chat_id)
                return

            video = extract_video_payload(message)
            if video:
                if mode == "set_awaiting_video":
                    # Keep only Telegram message metadata until the manual slug arrives.
                    await session_request("set", chat_id, mode="set_awaiting_slug", pending=video)
                    await send_bot_message(chat_id, "Ab slug bhejo. Example: JJK-S01-Ep-01-720p")
                    return
                if mode == "set_awaiting_slug":
                    await send_bot_message(chat_id, "Pehle is video ka slug bhejo.")
                    return
                if mode == "bulk_collecting":
                    await collect_bulk_video(chat_id, state, video)
                    return
                await send_bot_message(chat_id, "Pehle /set ya /bulk PREFIX bhejo.")
                return

            if mode == "set_awaiting_video":
                await send_bot_message(chat_id, "Video bhejo.")
            elif mode == "bulk_collecting":
                await send_bot_message(chat_id, "Bulk mode active hai. Video bhejo ya /done bhejo.")
            elif not command:
                await send_bot_message(chat_id, "Pehle /set ya /bulk PREFIX bhejo.")
        except Exception as exc:
            logger.exception("Update processing failed: %s", exc)
            await send_bot_message(chat_id, "Video process nahi ho paya. cPanel/Telegram setup check karo.")


async def set_telegram_webhook_once() -> bool:
    if not PUBLIC_BOT_URL:
        return True
    data: dict[str, Any] = {"url": f"{PUBLIC_BOT_URL}/telegram/webhook", "allowed_updates": ["message"]}
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
    return {"ok": True, "service": "telegram-cpanel-channel-bot", "media_relay": False, "workflow": "set-bulk-done-multi-quality"}


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
