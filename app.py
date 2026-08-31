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
logger = logging.getLogger("tg-cpanel-metadata-bot")

BOT_TOKEN = os.environ["BOT_TOKEN"]
STREAM_SECRET = os.environ["STREAM_SECRET"].encode("utf-8")
CPANEL_WATCH_URL = os.environ["CPANEL_WATCH_URL"].rstrip("?")
PUBLIC_BOT_URL = os.getenv("PUBLIC_BOT_URL", "").rstrip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
TOKEN_TTL_SECONDS = int(os.getenv("TOKEN_TTL_SECONDS", "2592000"))

app = FastAPI(title="Telegram cPanel Metadata Bot", version="2.0.0")


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def sign_payload(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    encoded = b64url(raw)
    signature = hmac.new(STREAM_SECRET, encoded.encode("ascii"), hashlib.sha256).digest()
    return f"{encoded}.{b64url(signature)}"


async def send_bot_message(chat_id: int, text: str) -> None:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=30) as http:
        response = await http.post(url, json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True})
        response.raise_for_status()


async def set_telegram_webhook_once() -> bool:
    if not PUBLIC_BOT_URL:
        return True
    webhook_url = f"{PUBLIC_BOT_URL}/telegram/webhook"
    api_url = f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook"
    data: dict[str, Any] = {"url": webhook_url, "allowed_updates": ["message"]}
    if WEBHOOK_SECRET:
        data["secret_token"] = WEBHOOK_SECRET
    try:
        async with httpx.AsyncClient(timeout=30) as http:
            response = await http.post(api_url, json=data)
            response.raise_for_status()
        logger.info("Telegram webhook configured at %s", webhook_url)
        return True
    except httpx.HTTPStatusError as exc:
        logger.error("Telegram webhook registration failed: HTTP %s - %s", exc.response.status_code, exc.response.text[:500])
    except Exception as exc:
        logger.error("Telegram webhook registration failed: %s", exc)
    return False


async def webhook_retry_loop() -> None:
    delay = 5
    while True:
        if await set_telegram_webhook_once():
            return
        logger.warning("Webhook will be retried in %s seconds", delay)
        await __import__("asyncio").sleep(delay)
        delay = min(delay * 2, 300)


async def process_update(update: dict[str, Any]) -> None:
    message = update.get("message") or update.get("edited_message")
    if not message:
        return
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    message_id = message.get("message_id")
    if not isinstance(chat_id, int) or not isinstance(message_id, int):
        return

    media = message.get("video") or message.get("document")
    if not media:
        await send_bot_message(chat_id, "Video bhejo. Abhi test MVP mein sirf video/document media supported hai.")
        return

    mime = str(media.get("mime_type") or "")
    file_name = str(media.get("file_name") or "video")
    is_video = mime.startswith("video/") or file_name.lower().endswith((".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v"))
    if not is_video:
        await send_bot_message(chat_id, "Yeh video file nahi lag rahi. MP4 ya video document bhejkar test karo.")
        return

    file_id = str(media.get("file_id") or "")
    if not file_id:
        await send_bot_message(chat_id, "Telegram file reference nahi mila. Is file ko dobara video/document ke roop mein bhejo.")
        return

    file_size = int(media.get("file_size") or 0)
    now = int(time.time())
    payload = {
        "v": 2,
        "c": chat_id,
        "m": message_id,
        "f": file_id,
        "s": file_size,
        "n": file_name[:180],
        "t": mime[:100] or "video/mp4",
        "iat": now,
        "exp": now + TOKEN_TTL_SECONDS,
    }
    token = sign_payload(payload)
    watch_url = f"{CPANEL_WATCH_URL}?token={quote(token, safe='')}"
    size_text = f"{file_size / (1024 * 1024):.1f} MB" if file_size else "unknown size"
    text = (
        "✅ Video receive ho gaya.\n\n"
        f"Size: {size_text}\n"
        "▶️ Stream link:\n"
        f"{watch_url}\n\n"
        "Render sirf link/reference banata hai. Video cPanel se direct Telegram origin par stream hoga."
    )
    await send_bot_message(chat_id, text)


@app.on_event("startup")
async def startup() -> None:
    import asyncio
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
        except __import__("asyncio").CancelledError:
            pass


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"ok": True, "service": "telegram-cpanel-metadata-bot", "media_relay": False}


@app.post("/telegram/webhook")
async def telegram_webhook(request: Request, x_telegram_bot_api_secret_token: Optional[str] = Header(default=None)) -> JSONResponse:
    if WEBHOOK_SECRET and not hmac.compare_digest(x_telegram_bot_api_secret_token or "", WEBHOOK_SECRET):
        raise HTTPException(status_code=401, detail="Invalid webhook secret")
    update = await request.json()
    import asyncio
    asyncio.create_task(process_update(update))
    return JSONResponse({"ok": True})
