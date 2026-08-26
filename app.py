import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
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

app = FastAPI(title="Telegram cPanel Channel Bot", version="3.0.0")


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
    # Telegram performs this copy internally; Render never downloads the media bytes.
    return await telegram_api(
        "copyMessage",
        {
            "chat_id": TARGET_CHANNEL_ID,
            "from_chat_id": source_chat_id,
            "message_id": source_message_id,
        },
    )


def make_slug(value: str, fallback_id: int) -> str:
    cleaned = "".join(
        ch if ("A" <= ch <= "Z" or "a" <= ch <= "z" or "0" <= ch <= "9" or ch in "_-") else "-"
        for ch in value
    ).strip("-")
    cleaned = "-".join(part for part in cleaned.split("-") if part)
    suffix = f"-{fallback_id}"
    if not cleaned:
        return f"telegram{suffix}"[:50]
    prefix = cleaned[:50 - len(suffix)].rstrip("-")
    return (prefix + suffix)[:50]


async def save_gdplayer_metadata(record: dict[str, Any]) -> dict[str, Any]:
    headers = {"X-CPANEL-INGEST-SECRET": CPANEL_INGEST_SECRET}
    async with httpx.AsyncClient(timeout=30) as http:
        response = await http.post(CPANEL_INGEST_URL, json=record, headers=headers)
        response.raise_for_status()
        body = response.json()
    if not body.get("ok"):
        raise RuntimeError(body.get("error", "cPanel metadata save failed"))
    return body


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


async def process_update(update: dict[str, Any]) -> None:
    message = update.get("message") or update.get("edited_message")
    if not message:
        return
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    source_message_id = message.get("message_id")
    if not isinstance(chat_id, int) or not isinstance(source_message_id, int):
        return

    media = message.get("video") or message.get("document")
    if not media:
        await send_bot_message(chat_id, "Video bhejo. Sirf video/document media supported hai.")
        return

    mime = str(media.get("mime_type") or "")
    file_name = str(media.get("file_name") or "video")
    is_video = mime.startswith("video/") or file_name.lower().endswith(
        (".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v")
    )
    if not is_video:
        await send_bot_message(chat_id, "Yeh video file nahi lag rahi. MP4 ya video document bhejo.")
        return

    file_size = int(media.get("file_size") or 0)
    try:
        copied = await copy_to_database_channel(chat_id, source_message_id)
        channel_message_id = int(copied["message_id"])
        copied_media = copied.get("video") or copied.get("document") or media
        channel_file_id = str(copied_media.get("file_id") or media.get("file_id") or "")
        channel_file_unique_id = str(copied_media.get("file_unique_id") or media.get("file_unique_id") or "")
        file_name = str(copied_media.get("file_name") or media.get("file_name") or file_name)
        mime = str(copied_media.get("mime_type") or media.get("mime_type") or mime)
        file_size = int(copied_media.get("file_size") or media.get("file_size") or file_size or 0)
        caption = str(message.get("caption") or copied.get("caption") or "")
        title = file_name[:1000]
        slug = make_slug(title.rsplit(".", 1)[0], channel_message_id)
        token = make_channel_token(channel_message_id, file_size, file_name, mime)
        watch_url = f"{CPANEL_WATCH_URL}?token={quote(token, safe='')}"
        stream_url = f"{CPANEL_MEDIA_URL}?token={quote(token, safe='')}"
        record = {
            "slug": slug,
            "channel_id": TARGET_CHANNEL_ID,
            "channel_message_id": channel_message_id,
            "source_chat_id": chat_id,
            "source_message_id": source_message_id,
            "title": title,
            "file_id": channel_file_id,
            "file_unique_id": channel_file_unique_id,
            "file_name": file_name[:180],
            "mime_type": mime[:100] or "video/mp4",
            "file_size": file_size,
            "caption": caption,
            "stream_url": stream_url,
        }
        saved = await save_gdplayer_metadata(record)
        size_text = f"{file_size / (1024 * 1024):.1f} MB" if file_size else "unknown size"
        player_url = saved.get("embed_url") or watch_url
        await send_bot_message(
            chat_id,
            "✅ Video channel mein save ho gaya.\n\n"
            f"Size: {size_text}\n"
            "▶️ GDPlayer link:\n"
            f"{player_url}\n\n"
            "Render sirf copy command, metadata aur link handle karta hai. Video cPanel se Telegram channel ke through stream hoga.",
        )
    except Exception as exc:
        logger.exception("Channel video processing failed: %s", exc)
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
    return {"ok": True, "service": "telegram-cpanel-channel-bot", "media_relay": False}


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
