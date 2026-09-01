import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import quote

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("tg-cpanel-metadata-bot")
BOT_TOKEN = os.environ["BOT_TOKEN"]
STREAM_SECRET = os.environ["STREAM_SECRET"].encode()
CPANEL_WATCH_URL = os.environ["CPANEL_WATCH_URL"].rstrip("?")
PUBLIC_BOT_URL = os.getenv("PUBLIC_BOT_URL", "").rstrip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
TOKEN_TTL_SECONDS = int(os.getenv("TOKEN_TTL_SECONDS", "2592000"))
CPANEL_INGEST_URL = os.getenv("CPANEL_INGEST_URL", "").rstrip("/")
INGEST_SECRET = os.getenv("INGEST_SECRET", "").encode()
CPANEL_MEDIA_BASE = os.getenv("CPANEL_MEDIA_BASE", CPANEL_WATCH_URL.replace("/watch.php", "/streamx")).rstrip("/")
app = FastAPI(title="Telegram cPanel Metadata Bot", version="3.1.0")

@dataclass
class QueuedVideo:
    chat_id: int
    message_id: int
    file_id: str
    file_size: int
    filename: str
    caption: str
    mime: str

bulk_sessions: dict[int, dict[str, Any]] = {}
pending_slugs: dict[int, str] = {}

def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")

def sign_payload(payload: dict[str, Any]) -> str:
    encoded = b64url(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode())
    return encoded + "." + b64url(hmac.new(STREAM_SECRET, encoded.encode(), hashlib.sha256).digest())

def make_stream_url(item: QueuedVideo) -> str:
    now = int(time.time())
    payload = {"v": 2, "c": item.chat_id, "m": item.message_id, "f": item.file_id, "s": item.file_size, "n": item.filename, "t": item.mime or "video/mp4", "iat": now, "exp": now + TOKEN_TTL_SECONDS}
    return f"{CPANEL_MEDIA_BASE}/media?token={quote(sign_payload(payload), safe='')}"

def public_watch_url(item: QueuedVideo) -> str:
    now = int(time.time())
    payload = {"v": 2, "c": item.chat_id, "m": item.message_id, "f": item.file_id, "s": item.file_size, "n": item.filename, "t": item.mime or "video/mp4", "iat": now, "exp": now + TOKEN_TTL_SECONDS}
    return f"{CPANEL_WATCH_URL}?token={quote(sign_payload(payload), safe='')}"

async def send_bot_message(chat_id: int, text: str) -> None:
    async with httpx.AsyncClient(timeout=30) as http:
        r = await http.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True})
        r.raise_for_status()

async def sync_cpanel(item: QueuedVideo, media_url: str, slug: str, season: int, episode: int, quality: str, audio: str) -> tuple[bool, str]:
    if not CPANEL_INGEST_URL or not INGEST_SECRET:
        return True, "not-configured"
    data = {"slug": slug, "title": slug, "season": season, "episode": episode, "quality": quality, "audio_language": audio, "telegram_chat_id": item.chat_id, "telegram_message_id": item.message_id, "telegram_file_id": item.file_id, "original_filename": item.filename, "original_caption": item.caption, "mime_type": item.mime or "video/mp4", "file_size": item.file_size, "signed_stream_url": media_url}
    body = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
    stamp = str(int(time.time()))
    signature = hmac.new(INGEST_SECRET, (stamp + "." + body).encode(), hashlib.sha256).hexdigest()
    async with httpx.AsyncClient(timeout=30) as http:
        r = await http.post(CPANEL_INGEST_URL, content=body.encode(), headers={"Content-Type": "application/json", "X-Ingest-Timestamp": stamp, "X-Ingest-Signature": signature})
        r.raise_for_status()
        result = r.json()
    return bool(result.get("ok")), str(result.get("error", ""))

def safe_slug(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9_-]+", "-", value.strip())
    return value.strip("-")[:150]

def detect(caption: str) -> tuple[Optional[int], Optional[int], Optional[str], str]:
    text = caption or ""
    season_match = re.search(r"(?:season\s*[:#-]?\s*|\bS\s*[-:]?\s*)(\d+)\b", text, re.I)
    season = int(season_match.group(1)) if season_match else 1
    episode_match = re.search(r"(?:episode|ep|e)\s*[:#-]?\s*(\d+)\b", text, re.I)
    episode = int(episode_match.group(1)) if episode_match else None
    quality = None
    for pattern in (r"(?:web[- ]?dl|hdrip|hevc)\s*(\d{3,4})\s*p?", r"\b(2160|1440|1080|720|576|480|360|240|144)\s*p?\b", r"\b(2k|4k)\b"):
        match = re.search(pattern, text, re.I)
        if match:
            quality = match.group(1).lower() + ("p" if match.group(1).isdigit() else "")
            break
    audio = "Unknown"
    for label, pattern in [("Dual Audio", r"(?:hindi.*english|english.*hindi|dual\s+audio|multi\s+audio)"), ("Hindi", r"hindi(?:\s+dub)?"), ("English", r"english(?:\s+dub)?"), ("Tamil", r"tamil(?:\s+dub)?"), ("Telugu", r"telugu(?:\s+dub)?"), ("Malayalam", r"malayalam(?:\s+dub)?"), ("Bengali", r"bengali(?:\s+dub)?"), ("Japanese", r"japanese(?:\s+dub)?")]:
        if re.search(pattern, text, re.I):
            audio = label
            break
    return season, episode, quality, audio

def item_from_message(message: dict[str, Any]) -> Optional[QueuedVideo]:
    media = message.get("video") or message.get("document")
    if not media:
        return None
    mime = str(media.get("mime_type") or "")
    filename = str(media.get("file_name") or "video")
    if not (mime.startswith("video/") or filename.lower().endswith((".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v"))):
        return None
    return QueuedVideo(int(message["chat"]["id"]), int(message["message_id"]), str(media.get("file_id") or ""), int(media.get("file_size") or 0), filename, str(message.get("caption") or ""), mime or "video/mp4")

async def publish_one(item: QueuedVideo, slug: str) -> tuple[bool, str]:
    season, episode, quality, audio = detect(item.caption)
    if episode is None:
        return False, "episode"
    if quality is None:
        return False, "quality"
    media_url = make_stream_url(item)
    ok, _ = await sync_cpanel(item, media_url, slug, season, episode, quality, audio)
    return ok, f"{slug}|{quality}|{audio}"

async def finish_bulk(chat_id: int, session: dict[str, Any]) -> None:
    await send_bot_message(chat_id, "⏳ Bulk processing started. Queued videos are being processed.")
    results: list[str] = []
    errors: list[str] = []
    for item in session["queue"]:
        season, episode, quality, audio = detect(item.caption)
        if episode is None:
            errors.append("Episode number not detected for one video.")
            continue
        if quality is None:
            errors.append("Quality not detected for one video.")
            continue
        slug = f"{session['prefix']}S{season:02d}-Ep-{episode:02d}" if episode < 100 else f"{session['prefix']}S{season:02d}-Ep-{episode}"
        ok, result = await publish_one(item, slug)
        if ok:
            results.append(result)
    grouped: dict[str, list[str]] = {}
    for result in results:
        slug, quality, audio = result.split("|", 2)
        grouped.setdefault(slug, []).append(f"{quality} • {audio}")
    for slug, sources in grouped.items():
        await send_bot_message(chat_id, "✅ Episode ready\n\n" + slug + "\n" + "\n".join(sorted(set(sources))) + f"\n\n🔗 {CPANEL_WATCH_URL.split('/watch.php')[0]}/e/{quote(slug)}")
    for error in errors:
        await send_bot_message(chat_id, error)
    await send_bot_message(chat_id, f"✅ Bulk complete.\n\nTotal videos: {len(session['queue'])}\nTotal episodes: {len(grouped)}\nLinks generated: {len(grouped)}")

async def process_update(update: dict[str, Any]) -> None:
    message = update.get("message") or update.get("edited_message")
    if not message or not isinstance(message.get("chat", {}).get("id"), int):
        return
    chat_id = int(message["chat"]["id"])
    text = str(message.get("text") or "").strip()
    if text.lower().startswith("/bulk"):
        prefix = safe_slug(text[5:].strip())
        if not prefix:
            await send_bot_message(chat_id, "Usage: /bulk CUSTOM_PREFIX")
            return
        bulk_sessions[chat_id] = {"prefix": prefix, "queue": []}
        pending_slugs.pop(chat_id, None)
        await send_bot_message(chat_id, f"✅ Bulk mode active. Prefix: {prefix}\nVideos bhejo; finish ke liye /done bhejo.")
        return
    if text.lower() == "/done":
        session = bulk_sessions.pop(chat_id, None)
        if not session:
            await send_bot_message(chat_id, "Koi active bulk session nahi hai.")
            return
        await finish_bulk(chat_id, session)
        return
    if text.lower().startswith("/set"):
        slug = safe_slug(text[4:].strip())
        if not slug:
            await send_bot_message(chat_id, "Usage: /set CUSTOM_SLUG")
            return
        pending_slugs[chat_id] = slug
        await send_bot_message(chat_id, f"✅ Single-video slug set: {slug}\nAb video bhejo.")
        return
    if chat_id in pending_slugs and text and not text.startswith("/") and not (message.get("video") or message.get("document")):
        slug = safe_slug(text)
        if slug:
            pending_slugs[chat_id] = slug
            await send_bot_message(chat_id, f"✅ Single-video slug set: {slug}\nAb video bhejo.")
        return
    item = item_from_message(message)
    if not item:
        if text.startswith("/"):
            return
        await send_bot_message(chat_id, "Video bhejo. Video/document media supported hai.")
        return
    if not item.file_id:
        await send_bot_message(chat_id, "Telegram file reference nahi mila.")
        return
    if chat_id in bulk_sessions:
        season, episode, quality, audio = detect(item.caption)
        if episode is None:
            await send_bot_message(chat_id, "Episode number not detected. Video queue mein add nahi hua.")
            return
        if quality is None:
            await send_bot_message(chat_id, "Quality not detected. Video queue mein add nahi hua.")
            return
        bulk_sessions[chat_id]["queue"].append(item)
        await send_bot_message(chat_id, f"✅ Video added to queue.\nDetected: S{season:02d} / Episode {episode} / {quality} / {audio}\n\nSend more videos. When finished, send /done.")
        return
    slug = pending_slugs.pop(chat_id, None)
    if not slug:
        await send_bot_message(chat_id, "Is single video ke liye pehle custom slug bhejo. Example: /set qwefibqefo")
        return
    season, episode, quality, audio = detect(item.caption)
    if episode is None:
        await send_bot_message(chat_id, "Episode number not detected. Caption mein Episode 1, Ep1 ya E1 format do.")
        return
    if quality is None:
        await send_bot_message(chat_id, "Quality not detected. Caption mein 720p, 1080p, 4K ya similar do.")
        return
    media_url = make_stream_url(item)
    ok, error = await sync_cpanel(item, media_url, slug, season, episode, quality, audio)
    if not ok:
        await send_bot_message(chat_id, "Video link bana, lekin catalog database sync fail hua. Render logs check karo.")
        logger.error("cPanel ingest failed: %s", error)
        return
    await send_bot_message(chat_id, "✅ Video saved.\n\nEmbed URL:\nhttps://chalchitra.site/e/{quote(slug)}\n\nIs URL ko direct open ya iframe mein use kar sakte ho. Video cPanel se direct stream hoga; Render media relay nahi karta.")

@app.on_event("startup")
async def startup() -> None:
    app.state.webhook_task = asyncio.create_task(webhook_retry_loop()) if PUBLIC_BOT_URL else None

async def webhook_retry_loop() -> None:
    delay = 5
    while True:
        if await set_telegram_webhook_once():
            return
        await asyncio.sleep(delay)
        delay = min(delay * 2, 300)

async def set_telegram_webhook_once() -> bool:
    if not PUBLIC_BOT_URL:
        return True
    data = {"url": PUBLIC_BOT_URL + "/telegram/webhook", "allowed_updates": ["message"]}
    if WEBHOOK_SECRET:
        data["secret_token"] = WEBHOOK_SECRET
    try:
        async with httpx.AsyncClient(timeout=30) as http:
            r = await http.post(f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook", json=data)
            r.raise_for_status()
        return True
    except Exception as exc:
        logger.error("Webhook registration failed: %s", exc)
        return False

@app.get("/health")
async def health() -> dict[str, Any]:
    return {"ok": True, "service": "telegram-cpanel-metadata-bot", "media_relay": False}

@app.post("/telegram/webhook")
async def telegram_webhook(request: Request, x_telegram_bot_api_secret_token: Optional[str] = Header(default=None)) -> JSONResponse:
    if WEBHOOK_SECRET and not hmac.compare_digest(x_telegram_bot_api_secret_token or "", WEBHOOK_SECRET):
        raise HTTPException(status_code=401, detail="Invalid webhook secret")
    asyncio.create_task(process_update(await request.json()))
    return JSONResponse({"ok": True})

@app.on_event("shutdown")
async def shutdown() -> None:
    task = getattr(app.state, "webhook_task", None)
    if task and not task.done():
        task.cancel()
