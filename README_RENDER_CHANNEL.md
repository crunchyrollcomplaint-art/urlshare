# Final Render channel bot deployment

This package is separate from the currently working DM MVP. Deploy it only when the cPanel GDPlayer integration has been prepared.

## Render files

Place `app.py`, `requirements.txt`, and `render.yaml` at the repository root. The start command is:

```text
uvicorn app:app --host 0.0.0.0 --port $PORT
```

## Environment variables

```text
BOT_TOKEN=<same BotFather token>
STREAM_SECRET=<same value used by cPanel Node>
CPANEL_WATCH_URL=https://chalchitra.site/watch.php
PUBLIC_BOT_URL=https://your-service.onrender.com
WEBHOOK_SECRET=<Telegram webhook secret>
TOKEN_TTL_SECONDS=31536000
TARGET_CHANNEL_ID=-1003956038160
CPANEL_INGEST_URL=https://chalchitra.site/telegram_ingest.php
CPANEL_INGEST_SECRET=<same value configured in cPanel telegram_ingest_config.php>
CPANEL_MEDIA_URL=https://chalchitra.site/tgstreamnode/media
```

Do not add `API_ID`, `API_HASH`, or `TELEGRAM_REQUEST_SIZE` to this final Render package. They belong to the cPanel Node app. Old extra variables are harmless but unnecessary.

## Required Telegram permission

The bot must be an administrator in the target channel with permission to post messages. The source DM message must be accessible to the bot. Telegram's `copyMessage` operation copies the message inside Telegram; this application does not call `getFile` or download the media on Render.

## Result

After a successful upload, the bot sends a GDPlayer database URL. The cPanel ingest endpoint stores the source as a GDPlayer `direct` record, with `host_id` pointing to the signed cPanel media URL.
