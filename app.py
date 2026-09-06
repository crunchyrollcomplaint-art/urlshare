const express = require("express");
const crypto = require("crypto");
const { spawn } = require("child_process");
const fs = require("fs");
const fsp = require("fs/promises");
const os = require("os");
const path = require("path");
const { TelegramClient } = require("telegram");
const { StringSession } = require("telegram/sessions");
const bigInt = require("big-integer");

const PORT = Number(process.env.PORT || 3000);
const API_ID = Number(process.env.API_ID || 0);
const API_HASH = process.env.API_HASH || "";
const BOT_TOKEN = process.env.BOT_TOKEN || "";
const STREAM_SECRET = process.env.STREAM_SECRET || "";
const REQUEST_SIZE = Number(process.env.TELEGRAM_REQUEST_SIZE || 524288);
const MAX_TOKEN_LENGTH = Number(process.env.MAX_TOKEN_LENGTH || 8192);
const FFMPEG_BIN = process.env.FFMPEG_BIN || "ffmpeg";
const FFPROBE_BIN = process.env.FFPROBE_BIN || "ffprobe";
const INSPECT_SECRET = process.env.INSPECT_SECRET || process.env.INGEST_SECRET || "";

if (!API_ID || !API_HASH || !BOT_TOKEN || !STREAM_SECRET) {
  throw new Error("API_ID, API_HASH, BOT_TOKEN and STREAM_SECRET are required");
}

const app = express();
app.disable("x-powered-by");

const telegram = new TelegramClient(
  new StringSession(process.env.TELEGRAM_SESSION || ""),
  API_ID,
  API_HASH,
  { connectionRetries: 5 }
);

let telegramReady;

async function ensureTelegram() {
  if (!telegramReady) {
    telegramReady = telegram.start({
      botAuthToken: BOT_TOKEN,
      onError: (error) => console.error("Telegram client login error:", error.message)
    });
  }
  await telegramReady;
}

function base64UrlDecode(value) {
  const padded = value + "=".repeat((4 - (value.length % 4)) % 4);
  return Buffer.from(padded.replace(/-/g, "+").replace(/_/g, "/"), "base64");
}

function base64UrlEncode(buffer) {
  return buffer.toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function decodeToken(token) {
  if (!token || token.length > MAX_TOKEN_LENGTH) throw new Error("invalid token");
  const separator = token.indexOf(".");
  if (separator <= 0) throw new Error("invalid token");
  const encoded = token.slice(0, separator);
  const signature = token.slice(separator + 1);
  const expected = base64UrlEncode(crypto.createHmac("sha256", STREAM_SECRET).update(encoded, "ascii").digest());
  const a = Buffer.from(signature);
  const b = Buffer.from(expected);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) throw new Error("bad signature");
  const payload = JSON.parse(base64UrlDecode(encoded).toString("utf8"));
  if (payload.v !== 2 || !Number.isInteger(payload.c) || !Number.isInteger(payload.m)) throw new Error("invalid payload");
  if (!Number.isInteger(payload.exp) || payload.exp < Math.floor(Date.now() / 1000)) throw new Error("expired token");
  return payload;
}

function parseRange(value, size) {
  if (!value) return { start: 0, end: size - 1, partial: false };
  if (!value.startsWith("bytes=") || value.includes(",")) throw new Error("invalid range");
  const spec = value.slice(6).trim();
  const dash = spec.indexOf("-");
  if (dash < 0) throw new Error("invalid range");
  const left = spec.slice(0, dash);
  const right = spec.slice(dash + 1);
  let start;
  let end;
  if (left === "") {
    const suffix = Number(right);
    if (!Number.isInteger(suffix) || suffix <= 0) throw new Error("invalid range");
    start = Math.max(0, size - suffix);
    end = size - 1;
  } else {
    start = Number(left);
    end = right === "" ? size - 1 : Number(right);
    if (!Number.isInteger(start) || !Number.isInteger(end) || start < 0 || end < start || start >= size) throw new Error("invalid range");
    end = Math.min(end, size - 1);
  }
  return { start, end, partial: true };
}

async function getTelegramMessage(payload) {
  await ensureTelegram();
  const result = await telegram.getMessages(payload.c, { ids: payload.m });
  if (Array.isArray(result)) return result[0];
  if (result && Array.isArray(result.messages)) return result.messages[0];
  return result;
}

function mediaInfo(message, payload) {
  const media = message && message.media;
  const document = media && media.document;
  const rawSize = document && document.size;
  const size = Number(rawSize && typeof rawSize === "object" && rawSize.toString ? rawSize.toString() : rawSize);
  if (!document || !Number.isSafeInteger(size) || size <= 0) throw new Error("message has no streamable document");
  return {
    media,
    size,
    mime: String(payload.t || document.mimeType || "video/mp4"),
    name: String(payload.n || "video").replace(/[\"\r\n]/g, "").slice(0, 180) || "video"
  };
}

async function streamRange(media, start, length, response) {
  const iterator = telegram.iterDownload({ file: media, offset: bigInt(start), limit: length, requestSize: REQUEST_SIZE });
  for await (const chunk of iterator) {
    if (response.destroyed) return;
    const buffer = Buffer.from(chunk);
    if (!response.write(buffer)) await new Promise((resolve) => response.once("drain", resolve));
  }
  if (!response.destroyed) response.end();
}

async function pipeFullMedia(media, size, writable) {
  const iterator = telegram.iterDownload({ file: media, offset: bigInt(0), limit: size, requestSize: REQUEST_SIZE });
  for await (const chunk of iterator) {
    if (writable.destroyed) return;
    if (!writable.write(Buffer.from(chunk))) await new Promise((resolve, reject) => { writable.once("drain", resolve); writable.once("error", reject); });
  }
  writable.end();
}

function safeStreamIndex(value) {
  const index = Number(value);
  if (!Number.isInteger(index) || index < 0 || index > 1000) throw new Error("invalid stream index");
  return index;
}

function verifyInspectSignature(request, body) {
  if (!INSPECT_SECRET) throw new Error("inspection secret is not configured");
  const timestamp = String(request.headers["x-inspect-timestamp"] || "");
  const signature = String(request.headers["x-inspect-signature"] || "");
  if (!/^\d+$/.test(timestamp) || Math.abs(Date.now() / 1000 - Number(timestamp)) > 300) throw new Error("invalid inspection timestamp");
  const expected = crypto.createHmac("sha256", INSPECT_SECRET).update(timestamp + "." + body).digest("hex");
  if (signature.length !== expected.length || !crypto.timingSafeEqual(Buffer.from(signature), Buffer.from(expected))) throw new Error("invalid inspection signature");
}

async function readRequestBody(request) {
  const chunks = [];
  for await (const chunk of request) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks).toString("utf8");
}

async function writeSparseRange(handle, media, start, length) {
  const iterator = telegram.iterDownload({ file: media, offset: bigInt(start), limit: length, requestSize: REQUEST_SIZE });
  let position = start;
  for await (const chunk of iterator) {
    const buffer = Buffer.from(chunk);
    await handle.write(buffer, 0, buffer.length, position);
    position += buffer.length;
  }
}

function runProbe(filePath) {
  return new Promise((resolve, reject) => {
    const child = spawn(FFPROBE_BIN, ["-v", "error", "-show_streams", "-of", "json", filePath], { stdio: ["ignore", "pipe", "pipe"] });
    const out = []; const err = [];
    child.stdout.on("data", (chunk) => out.push(chunk));
    child.stderr.on("data", (chunk) => err.push(chunk));
    child.on("error", reject);
    child.on("close", (code) => code === 0 ? resolve(JSON.parse(Buffer.concat(out).toString("utf8"))) : reject(new Error("FFprobe failed: " + Buffer.concat(err).toString("utf8").slice(-500))));
  });
}

async function inspectMetadata(request, response) {
  let tempDir;
  try {
    const body = await readRequestBody(request);
    verifyInspectSignature(request, body);
    const input = JSON.parse(body);
    if (!Number.isInteger(input.chat_id) || !Number.isInteger(input.message_id)) throw new Error("invalid Telegram source identity");
    const message = await getTelegramMessage({ c: input.chat_id, m: input.message_id });
    const info = mediaInfo(message, { t: input.mime || "video/mp4", n: input.filename || "video" });
    const chunk = Math.max(1024 * 1024, Math.min(Number(input.chunk_bytes) || 8 * 1024 * 1024, 64 * 1024 * 1024));
    const probe = Math.max(chunk, Math.min(Number(input.probe_bytes) || 64 * 1024 * 1024, 256 * 1024 * 1024));
    const ranges = [{ start: 0, length: Math.min(chunk, info.size) }];
    if (info.size > probe) ranges.push({ start: info.size - probe, length: probe });
    tempDir = await fsp.mkdtemp(path.join(os.tmpdir(), "bulkm-inspect-"));
    const filePath = path.join(tempDir, "media" + path.extname(info.name || ".bin"));
    const handle = await fsp.open(filePath, "w+");
    await handle.truncate(info.size);
    for (const range of ranges) await writeSparseRange(handle, info.media, range.start, range.length);
    await handle.close();
    const parsed = await runProbe(filePath);
    const audio_tracks = []; const subtitle_tracks = []; let audioNumber = 0; let subtitleNumber = 0;
    for (const stream of parsed.streams || []) {
      if (stream.codec_type !== "audio" && stream.codec_type !== "subtitle") continue;
      const tags = stream.tags || {};
      const language = String(tags.language || "und");
      const title = String(tags.title || tags.handler_name || "");
      if (stream.codec_type === "audio") {
        audioNumber += 1;
        audio_tracks.push({ index: Number(stream.index), language, title: title || `Audio Track ${audioNumber}`, codec: String(stream.codec_name || "unknown"), channels: stream.channels ?? null, channel_layout: stream.channel_layout || null, sample_rate: stream.sample_rate || null, disposition: stream.disposition || {} });
      } else {
        subtitleNumber += 1;
        subtitle_tracks.push({ index: Number(stream.index), language, title: title || `Subtitle Track ${subtitleNumber}`, codec: String(stream.codec_name || stream.codec_long_name || "unknown"), disposition: stream.disposition || {} });
      }
    }
    response.json({ ok: true, audio_tracks, subtitle_tracks });
  } catch (error) {
    console.error("Metadata inspection error:", error && error.stack ? error.stack : error);
    return sendError(response, 400, "Unable to inspect media metadata");
  } finally {
    if (tempDir) await fsp.rm(tempDir, { recursive: true, force: true }).catch(() => {});
  }
}

function sendError(response, status, message) {
  if (response.headersSent) return response.destroy();
  return response.status(status).json({ ok: false, error: message });
}

function health(_request, response) {
  response.json({ ok: true, service: "cPanel-node-telegram-media", render_media_relay: false, track_delivery: true });
}

async function media(request, response) {
  try {
    const payload = decodeToken(String(request.query.token || ""));
    const message = await getTelegramMessage(payload);
    const info = mediaInfo(message, payload);
    const range = parseRange(request.headers.range, info.size);
    const length = range.end - range.start + 1;
    const headers = { "Accept-Ranges": "bytes", "Content-Type": info.mime, "Content-Length": String(length), "Content-Disposition": `inline; filename="${info.name}"`, "Cache-Control": "private, no-store", "X-Content-Type-Options": "nosniff", "X-Accel-Buffering": "no" };
    if (range.partial) headers["Content-Range"] = `bytes ${range.start}-${range.end}/${info.size}`;
    response.status(range.partial ? 206 : 200).set(headers);
    if (request.method === "HEAD") return response.end();
    await streamRange(info.media, range.start, length, response);
  } catch (error) {
    console.error("Direct Telegram media error:", error && error.stack ? error.stack : error);
    return sendError(response, 502, "Unable to stream Telegram media");
  }
}

async function trackVariant(request, response) {
  let child;
  try {
    const payload = decodeToken(String(request.query.token || ""));
    const message = await getTelegramMessage(payload);
    const info = mediaInfo(message, payload);
    const audioIndex = safeStreamIndex(request.query.audio);
    if (request.method === "HEAD") return response.status(200).set({ "Content-Type": "video/mp4", "Cache-Control": "private, no-store" }).end();
    response.status(200).set({ "Content-Type": "video/mp4", "Cache-Control": "private, no-store", "X-Content-Type-Options": "nosniff", "X-Accel-Buffering": "no" });
    child = spawn(FFMPEG_BIN, ["-hide_banner", "-loglevel", "error", "-i", "pipe:0", "-map", "0:v:0", "-map", `0:${audioIndex}`, "-c:v", "copy", "-c:a", "aac", "-movflags", "frag_keyframe+empty_moov+default_base_moof", "-f", "mp4", "pipe:1"], { stdio: ["pipe", "pipe", "pipe"] });
    child.stdout.on("data", (chunk) => { if (!response.destroyed && !response.write(chunk)) child.stdout.pause(); });
    response.on("drain", () => child.stdout.resume());
    child.stderr.on("data", (chunk) => console.error("FFmpeg audio variant:", chunk.toString().trim()));
    child.on("close", (code) => { if (!response.destroyed) response.end(); if (code !== 0) console.error("FFmpeg audio variant exited", code); });
    request.on("close", () => { if (child && !child.killed) child.kill("SIGTERM"); });
    await pipeFullMedia(info.media, info.size, child.stdin);
  } catch (error) {
    if (child && !child.killed) child.kill("SIGTERM");
    console.error("Audio variant error:", error && error.stack ? error.stack : error);
    return sendError(response, 502, "Unable to deliver selected audio track");
  }
}

async function subtitleTrack(request, response) {
  let child;
  try {
    const payload = decodeToken(String(request.query.token || ""));
    const message = await getTelegramMessage(payload);
    const info = mediaInfo(message, payload);
    const subtitleIndex = safeStreamIndex(request.query.subtitle);
    if (request.method === "HEAD") return response.status(200).set({ "Content-Type": "text/vtt", "Cache-Control": "private, no-store" }).end();
    response.status(200).set({ "Content-Type": "text/vtt; charset=utf-8", "Cache-Control": "private, no-store", "X-Content-Type-Options": "nosniff" });
    child = spawn(FFMPEG_BIN, ["-hide_banner", "-loglevel", "error", "-i", "pipe:0", "-map", `0:${subtitleIndex}`, "-c:s", "webvtt", "-f", "webvtt", "pipe:1"], { stdio: ["pipe", "pipe", "pipe"] });
    child.stdout.on("data", (chunk) => { if (!response.destroyed) response.write(chunk); });
    child.stderr.on("data", (chunk) => console.error("FFmpeg subtitle track:", chunk.toString().trim()));
    child.on("close", (code) => { if (!response.destroyed) response.end(); if (code !== 0) console.error("FFmpeg subtitle track exited", code); });
    request.on("close", () => { if (child && !child.killed) child.kill("SIGTERM"); });
    await pipeFullMedia(info.media, info.size, child.stdin);
  } catch (error) {
    if (child && !child.killed) child.kill("SIGTERM");
    console.error("Subtitle track error:", error && error.stack ? error.stack : error);
    return sendError(response, 502, "Unable to deliver selected subtitle track");
  }
}

app.get("/health", health);
app.get("/tgstreamnode/health", health);
app.get("/tgstream/health", health);
app.get("/streamx/health", health);
app.get("/media", media);
app.head("/media", media);
app.get("/tgstreamnode/media", media);
app.head("/tgstreamnode/media", media);
app.get("/tgstream/media", media);
app.head("/tgstream/media", media);
app.get("/streamx/media", media);
app.head("/streamx/media", media);
app.get("/media/variant", trackVariant);
app.head("/media/variant", trackVariant);
app.get("/streamx/variant", trackVariant);
app.head("/streamx/variant", trackVariant);
app.post("/inspect", inspectMetadata);
app.post("/streamx/inspect", inspectMetadata);
app.get("/media/subtitle", subtitleTrack);
app.head("/media/subtitle", subtitleTrack);
app.get("/streamx/subtitle", subtitleTrack);
app.head("/streamx/subtitle", subtitleTrack);

app.listen(PORT, "0.0.0.0", () => console.log(`cPanel Node media service listening on port ${PORT}`));
