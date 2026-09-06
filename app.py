import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import re
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
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
CPANEL_INSPECT_URL = os.getenv("CPANEL_INSPECT_URL", CPANEL_MEDIA_BASE + "/inspect")
INSPECT_SECRET = os.getenv("INSPECT_SECRET", os.getenv("INGEST_SECRET", "")).encode()
FFPROBE_BIN = os.getenv("FFPROBE_BIN", "ffprobe")
TELEGRAM_RANGE_CHUNK_BYTES = int(os.getenv("TELEGRAM_RANGE_CHUNK_BYTES", str(8 * 1024 * 1024)))
TELEGRAM_RANGE_PROBE_BYTES = int(os.getenv("TELEGRAM_RANGE_PROBE_BYTES", str(64 * 1024 * 1024)))
app = FastAPI(title="Telegram cPanel Metadata Bot", version="3.2.0")

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
bulkm_sessions: dict[int, dict[str, Any]] = {}
bulkm_pending_metadata: dict[int, tuple[int, str]] = {}
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


async def lookup_cpanel_metadata(item: QueuedVideo, quality: str) -> Optional[dict[str, list[dict[str, Any]]]]:
    if not CPANEL_INGEST_URL or not INGEST_SECRET:
        return None
    data = {"action": "metadata_lookup", "telegram_chat_id": item.chat_id, "telegram_message_id": item.message_id, "quality": quality}
    body = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
    stamp = str(int(time.time()))
    signature = hmac.new(INGEST_SECRET, (stamp + "." + body).encode(), hashlib.sha256).hexdigest()
    async with httpx.AsyncClient(timeout=30) as http:
        response = await http.post(CPANEL_INGEST_URL, content=body.encode(), headers={"Content-Type": "application/json", "X-Ingest-Timestamp": stamp, "X-Ingest-Signature": signature})
        if response.status_code == 404:
            return None
        response.raise_for_status()
        result = response.json()
    if not result.get("ok") or not result.get("found"):
        return None
    return {"audio_tracks": result.get("audio_tracks") or [], "subtitle_tracks": result.get("subtitle_tracks") or []}


async def sync_cpanel(item: QueuedVideo, media_url: str, slug: str, season: int, episode: int, quality: str, audio: str, *, workflow: str = "bulk", audio_tracks: Optional[list[dict[str, Any]]] = None, subtitle_tracks: Optional[list[dict[str, Any]]] = None) -> tuple[bool, str]:
    if not CPANEL_INGEST_URL or not INGEST_SECRET:
        return True, "not-configured"
    data = {"slug": slug, "title": slug, "season": season, "episode": episode, "quality": quality, "audio_language": audio, "workflow": workflow, "audio_tracks": audio_tracks or [], "subtitle_tracks": subtitle_tracks or [], "telegram_chat_id": item.chat_id, "telegram_message_id": item.message_id, "telegram_file_id": item.file_id, "original_filename": item.filename, "original_caption": item.caption, "mime_type": item.mime or "video/mp4", "file_size": item.file_size, "signed_stream_url": media_url}
    body = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
    stamp = str(int(time.time()))
    signature = hmac.new(INGEST_SECRET, (stamp + "." + body).encode(), hashlib.sha256).hexdigest()
    async with httpx.AsyncClient(timeout=60) as http:
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


# Existing /bulk pipeline: intentionally kept separate and behavior-compatible.
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


async def telegram_file_url(file_id: str) -> str:
    async with httpx.AsyncClient(timeout=30) as http:
        response = await http.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile", params={"file_id": file_id})
        response.raise_for_status()
        data = response.json()
    if not data.get("ok") or not data.get("result", {}).get("file_path"):
        raise RuntimeError("Telegram getFile did not return a file path")
    return f"https://api.telegram.org/file/bot{BOT_TOKEN}/{data['result']['file_path']}"


async def fetch_range(http: httpx.AsyncClient, url: str, start: int, end: int) -> bytes:
    response = await http.get(url, headers={"Range": f"bytes={start}-{end}"}, timeout=120)
    if response.status_code != 206:
        raise RuntimeError(f"Telegram media endpoint does not support byte ranges (HTTP {response.status_code})")
    content = response.content
    expected = end - start + 1
    if len(content) != expected:
        raise RuntimeError(f"Short media range: expected {expected}, got {len(content)}")
    return content


async def inspect_via_cpanel(item: QueuedVideo) -> Optional[dict[str, list[dict[str, Any]]]]:
    if not CPANEL_INSPECT_URL or not INSPECT_SECRET:
        return None
    body = json.dumps({"chat_id": item.chat_id, "message_id": item.message_id, "file_id": item.file_id, "filename": item.filename, "mime": item.mime, "chunk_bytes": TELEGRAM_RANGE_CHUNK_BYTES, "probe_bytes": TELEGRAM_RANGE_PROBE_BYTES}, separators=(",", ":"), ensure_ascii=False)
    stamp = str(int(time.time()))
    signature = hmac.new(INSPECT_SECRET, (stamp + "." + body).encode(), hashlib.sha256).hexdigest()
    async with httpx.AsyncClient(timeout=300) as http:
        response = await http.post(CPANEL_INSPECT_URL, content=body.encode(), headers={"Content-Type": "application/json", "X-Inspect-Timestamp": stamp, "X-Inspect-Signature": signature})
        response.raise_for_status()
        result = response.json()
    if not result.get("ok"):
        raise RuntimeError("cPanel metadata inspection failed")
    return {"audio_tracks": result.get("audio_tracks") or [], "subtitle_tracks": result.get("subtitle_tracks") or []}


async def inspect_media_tracks(item: QueuedVideo) -> dict[str, list[dict[str, Any]]]:
    """Prefer cPanel MTProto sparse inspection; Bot API range inspection is only a fallback."""
    remote = await inspect_via_cpanel(item)
    if remote is not None:
        return remote
    if item.file_size <= 0:
        raise RuntimeError("Telegram did not provide a file size")
    url = await telegram_file_url(item.file_id)
    probe_bytes = min(TELEGRAM_RANGE_PROBE_BYTES, item.file_size)
    ranges = [(0, min(TELEGRAM_RANGE_CHUNK_BYTES, item.file_size) - 1)]
    if item.file_size > probe_bytes:
        ranges.append((max(0, item.file_size - probe_bytes), item.file_size - 1))
    with tempfile.NamedTemporaryFile(prefix="bulkm-probe-", suffix=Path(item.filename).suffix or ".bin") as handle:
        handle.truncate(item.file_size)
        async with httpx.AsyncClient() as http:
            for start, end in ranges:
                chunk = await fetch_range(http, url, start, end)
                handle.seek(start)
                handle.write(chunk)
        handle.flush()
        command = [FFPROBE_BIN, "-v", "error", "-show_streams", "-of", "json", str(handle.name)]
        process = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            raise RuntimeError("FFprobe failed: " + stderr.decode(errors="replace")[-500:])
    parsed = json.loads(stdout.decode("utf-8"))
    audio_tracks: list[dict[str, Any]] = []
    subtitle_tracks: list[dict[str, Any]] = []
    audio_number = subtitle_number = 0
    for stream in parsed.get("streams", []):
        codec_type = stream.get("codec_type")
        if codec_type not in {"audio", "subtitle"}:
            continue
        tags = stream.get("tags") or {}
        language = str(tags.get("language") or "und")
        title = str(tags.get("title") or tags.get("handler_name") or "")
        if codec_type == "audio":
            audio_number += 1
            audio_tracks.append({"index": int(stream.get("index", -1)), "language": language, "title": title or f"Audio Track {audio_number}", "codec": str(stream.get("codec_name") or "unknown"), "channels": stream.get("channels"), "channel_layout": stream.get("channel_layout"), "sample_rate": stream.get("sample_rate"), "disposition": stream.get("disposition") or {}})
        else:
            subtitle_number += 1
            subtitle_tracks.append({"index": int(stream.get("index", -1)), "language": language, "title": title or f"Subtitle Track {subtitle_number}", "codec": str(stream.get("codec_name") or stream.get("codec_long_name") or "unknown"), "disposition": stream.get("disposition") or {}})
    return {"audio_tracks": audio_tracks, "subtitle_tracks": subtitle_tracks}


async def finish_bulkm(chat_id: int, session: dict[str, Any]) -> None:
    await send_bot_message(chat_id, "⏳ Bulkm processing started. Inspecting queued media ranges and saving all files.")
    successes = 0
    errors: list[str] = []
    for item in session["queue"]:
        try:
            episode = int(item["episode"])
            quality = item["quality"]
            video = item["video"]
            cache_key = f"{video.chat_id}:{video.message_id}:{quality}"
            metadata = session.setdefault("metadata_cache", {}).get(cache_key)
            if metadata is None:
                metadata = await lookup_cpanel_metadata(video, quality)
            if metadata is None:
                metadata = await inspect_media_tracks(video)
                session["metadata_cache"][cache_key] = metadata
            audio_tracks = metadata["audio_tracks"]
            subtitle_tracks = metadata["subtitle_tracks"]
            audio_label = ", ".join(str(track["title"]) for track in audio_tracks) or "No audio tracks"
            slug = f"{session['prefix']}Ep-{episode:02d}" if episode < 100 else f"{session['prefix']}Ep-{episode}"
            ok, error = await sync_cpanel(video, make_stream_url(video), slug, session["season"], episode, quality, audio_label, workflow="bulkm", audio_tracks=audio_tracks, subtitle_tracks=subtitle_tracks)
            if not ok:
                raise RuntimeError(error or "catalog sync failed")
            successes += 1
            player_base = CPANEL_WATCH_URL.split('/watch.php')[0].rstrip('/')
            player_url = f"{player_base}/e/{quote(slug)}"
            await send_bot_message(chat_id, f"✅ Saved S{session['season']:02d} Episode {episode} ({quality})\nAudio tracks: {len(audio_tracks)}\nSubtitle tracks: {len(subtitle_tracks)}\n\n🔗 Player URL:\n{player_url}")
        except Exception as exc:
            logger.exception("Bulkm item failed for chat=%s message=%s", chat_id, getattr(item.get("video"), "message_id", "unknown"))
            errors.append(f"Episode {item.get('episode', '?')} ({item.get('quality', '?')}): {str(exc)[:180]}")
    if errors:
        await send_bot_message(chat_id, "⚠️ Some Bulkm files failed:\n" + "\n".join(errors))
    await send_bot_message(chat_id, f"✅ Bulkm complete.\n\nQueued: {len(session['queue'])}\nSaved: {successes}\nFailed: {len(errors)}")


def parse_bulkm_header(text: str) -> tuple[Optional[int], Optional[str]]:
    episode_match = re.search(r"(?:episode|ep|e)\s*[:#-]?\s*(\d+)\b", text or "", re.I)
    quality_match = re.search(r"\b(2160|1440|1080|720|576|480|360|240|144)\s*p\b|\b(2k|4k)\b", text or "", re.I)
    episode = int(episode_match.group(1)) if episode_match else None
    quality = (quality_match.group(1) or quality_match.group(2)).lower() if quality_match else None
    if quality and quality.isdigit():
        quality += "p"
    return episode, quality


async def process_update(update: dict[str, Any]) -> None:
    message = update.get("message") or update.get("edited_message")
    if not message or not isinstance(message.get("chat", {}).get("id"), int):
        return
    chat_id = int(message["chat"]["id"])
    text = str(message.get("text") or "").strip()
    if text.lower().startswith("/bulkm"):
        prefix = safe_slug(text[6:].strip()) or ""
        bulkm_sessions[chat_id] = {"prefix": prefix, "season": 1, "queue": [], "metadata_cache": {}}
        bulkm_pending_metadata.pop(chat_id, None)
        bulk_sessions.pop(chat_id, None)
        pending_slugs.pop(chat_id, None)
        await send_bot_message(chat_id, "✅ Bulkm mode active. Send each video with a caption such as `Episode 01 | 1080p`; finish with /done.")
        return
    if chat_id in bulkm_sessions and text.lower() == "/done":
        session = bulkm_sessions.pop(chat_id)
        bulkm_pending_metadata.pop(chat_id, None)
        await finish_bulkm(chat_id, session)
        return
    if chat_id in bulkm_sessions and text and not text.startswith("/") and not (message.get("video") or message.get("document")):
        episode, quality = parse_bulkm_header(text)
        if episode is not None and quality is not None:
            bulkm_pending_metadata[chat_id] = (episode, quality)
            await send_bot_message(chat_id, f"✅ Bulkm metadata set for next file: Episode {episode} / {quality}.")
        else:
            await send_bot_message(chat_id, "Bulkm metadata format: Episode 01 | 1080p")
        return
    # Existing /bulk command and session handling remains below this point.
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
    if chat_id in bulkm_sessions:
        episode, quality = parse_bulkm_header(item.caption)
        if episode is None or quality is None:
            pending = bulkm_pending_metadata.pop(chat_id, None)
            if pending:
                episode, quality = pending
        if episode is None or quality is None:
            await send_bot_message(chat_id, "Bulkm caption/message mein manual Episode aur quality do, example: Episode 01 | 1080p")
            return
        bulkm_sessions[chat_id]["queue"].append({"video": item, "episode": episode, "quality": quality})
        await send_bot_message(chat_id, f"✅ Bulkm video queued. Episode {episode} / {quality}. Audio/subtitles will be detected from the media. Send more files or /done.")
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
    await send_bot_message(chat_id, f"✅ Video saved.\n\nEmbed URL:\nhttps://chalchitra.site/e/{quote(slug)}\n\nIs URL ko direct open ya iframe mein use kar sakte ho. Video cPanel se direct stream hoga; Render media relay nahi karta.")


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
    return {"ok": True, "service": "telegram-cpanel-metadata-bot", "media_relay": False, "bulkm": True}


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
