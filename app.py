import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import time
from typing import Any, AsyncIterator, Optional
from urllib.parse import quote

import httpx
from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from telethon import TelegramClient
from telethon.sessions import StringSession

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("tg-cpanel-stream-mvp")

BOT_TOKEN = os.environ["BOT_TOKEN"]
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
STREAM_SECRET = os.environ["STREAM_SECRET"].encode("utf-8")
CPANEL_WATCH_URL = os.environ["CPANEL_WATCH_URL"].rstrip("?")
PUBLIC_BOT_URL = os.getenv("PUBLIC_BOT_URL", "").rstrip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
TOKEN_TTL_SECONDS = int(os.getenv("TOKEN_TTL_SECONDS", "2592000"))
REQUEST_SIZE = int(os.getenv("TELEGRAM_REQUEST_SIZE", str(512 * 1024)))

app = FastAPI(title="Telegram cPanel Streaming MVP", version="1.0.0")
client = TelegramClient(StringSession(), API_ID, API_HASH)
startup_lock = asyncio.Lock()
started = False


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def sign_payload(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    encoded = b64url(raw)
    signature = hmac.new(STREAM_SECRET, encoded.encode("ascii"), hashlib.sha256).digest()
    return f"{encoded}.{b64url(signature)}"


def verify_token(token: str) -> dict[str, Any]:
    try:
        encoded, signature = token.split(".", 1)
        expected = b64url(hmac.new(STREAM_SECRET, encoded.encode("ascii"), hashlib.sha256).digest())
        if not hmac.compare_digest(signature, expected):
            raise ValueError("bad signature")
        raw = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
        payload = json.loads(raw.decode("utf-8"))
        if payload.get("v") != 1:
            raise ValueError("unsupported token version")
        if int(payload.get("exp", 0)) < int(time.time()):
            raise ValueError("expired token")
        if not isinstance(payload.get("c"), int) or not isinstance(payload.get("m"), int):
            raise ValueError("invalid message reference")
        return payload
    except Exception as exc:
        raise HTTPException(status_code=404, detail="Invalid or expired stream link") from exc


def media_details(message: Any) -> tuple[int, str, str]:
    file_obj = getattr(message, "file", None)
    if file_obj is None or not getattr(file_obj, "size", None):
        raise HTTPException(status_code=415, detail="Telegram message does not contain streamable media")
    size = int(file_obj.size)
    mime = getattr(file_obj, "mime_type", None) or "video/mp4"
    name = getattr(file_obj, "name", None) or "video"
    return size, mime, name


def parse_range(value: Optional[str], size: int) -> tuple[int, int, bool]:
    if not value:
        return 0, size - 1, False
    if not value.startswith("bytes=") or "," in value:
        raise HTTPException(status_code=416, detail="Only one byte range is supported")
    spec = value[6:].strip()
    if "-" not in spec:
        raise HTTPException(status_code=416, detail="Invalid byte range")
    left, right = spec.split("-", 1)
    try:
        if left == "":
            suffix = int(right)
            if suffix <= 0:
                raise ValueError
            start = max(0, size - suffix)
            end = size - 1
        else:
            start = int(left)
            end = int(right) if right else size - 1
            if start < 0 or start >= size or end < start:
                raise ValueError
            end = min(end, size - 1)
    except ValueError as exc:
        raise HTTPException(status_code=416, detail="Invalid byte range") from exc
    return start, end, True


async def send_bot_message(chat_id: int, text: str) -> None:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=30) as http:
        response = await http.post(url, json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True})
        response.raise_for_status()


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

    file_size = int(media.get("file_size") or 0)
    payload = {
        "v": 1,
        "c": chat_id,
        "m": message_id,
        "s": file_size,
        "n": file_name[:180],
        "t": mime[:100] or "video/mp4",
        "iat": int(time.time()),
        "exp": int(time.time()) + TOKEN_TTL_SECONDS,
    }
    token = sign_payload(payload)
    watch_url = f"{CPANEL_WATCH_URL}?token={quote(token, safe='')}"
    size_text = f"{file_size / (1024 * 1024):.1f} MB" if file_size else "unknown size"
    text = (
        "✅ Video receive ho gaya.\n\n"
        f"Size: {size_text}\n"
        "▶️ Stream link:\n"
        f"{watch_url}\n\n"
        "Test MVP: video cPanel par store nahi hota; Telegram se live stream hota hai."
    )
    await send_bot_message(chat_id, text)


@app.on_event("startup")
async def startup() -> None:
    global started
    async with startup_lock:
        if started:
            return
        await client.start(bot_token=BOT_TOKEN)
        if PUBLIC_BOT_URL:
            webhook_url = f"{PUBLIC_BOT_URL}/telegram/webhook"
            set_webhook_url = f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook"
            data: dict[str, Any] = {"url": webhook_url, "allowed_updates": ["message"]}
            if WEBHOOK_SECRET:
                data["secret_token"] = WEBHOOK_SECRET
            async with httpx.AsyncClient(timeout=30) as http:
                response = await http.post(set_webhook_url, json=data)
                response.raise_for_status()
            logger.info("Telegram webhook configured at %s", webhook_url)
        started = True


@app.on_event("shutdown")
async def shutdown() -> None:
    await client.disconnect()


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"ok": True, "service": "telegram-cpanel-stream-mvp", "started": started}


@app.post("/telegram/webhook")
async def telegram_webhook(request: Request, x_telegram_bot_api_secret_token: Optional[str] = Header(default=None)) -> JSONResponse:
    if WEBHOOK_SECRET and not hmac.compare_digest(x_telegram_bot_api_secret_token or "", WEBHOOK_SECRET):
        raise HTTPException(status_code=401, detail="Invalid webhook secret")
    update = await request.json()
    asyncio.create_task(process_update(update))
    return JSONResponse({"ok": True})


@app.api_route("/media/{token}", methods=["GET", "HEAD"])
async def media(token: str, request: Request) -> Response:
    payload = verify_token(token)
    chat_id = int(payload["c"])
    message_id = int(payload["m"])
    message = await client.get_messages(chat_id, ids=message_id)
    if not message:
        raise HTTPException(status_code=404, detail="Telegram message no longer exists")
    size, mime, name = media_details(message)

    try:
        start, end, partial = parse_range(request.headers.get("range"), size)
    except HTTPException as exc:
        if exc.status_code == 416:
            return Response(status_code=416, headers={"Content-Range": f"bytes */{size}"})
        raise

    length = end - start + 1
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Type": mime,
        "Content-Length": str(length),
        "Content-Disposition": f'inline; filename="{name.replace(chr(34), "")}"',
        "Cache-Control": "private, no-store",
        "X-Content-Type-Options": "nosniff",
    }
    if partial:
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"

    if request.method == "HEAD":
        return Response(status_code=206 if partial else 200, headers=headers)

    if not message.media:
        raise HTTPException(status_code=415, detail="No media available")

    async def body() -> AsyncIterator[bytes]:
        remaining = length
        async for chunk in client.iter_download(message.media, offset=start, limit=length, request_size=REQUEST_SIZE):
            if not chunk:
                continue
            if len(chunk) > remaining:
                chunk = chunk[:remaining]
            remaining -= len(chunk)
            yield chunk
            if remaining <= 0:
                break

    return StreamingResponse(body(), status_code=206 if partial else 200, headers=headers, media_type=mime)
