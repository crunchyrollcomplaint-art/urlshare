# Render Telegram Metadata Bot

Yeh service **sirf Telegram webhook aur signed watch-link generation** ke liye hai. Video bytes Render se pass nahi hote. Direct media streaming cPanel ke Python App se hogi.

## Render settings

```text
Service Type: Web Service
Root Directory: blank, agar files repository root mein hain
Build Command: pip install -r requirements.txt
Start Command: uvicorn app:app --host 0.0.0.0 --port $PORT
Health Check Path: /health
```

## Render Environment Variables

| Key | Value |
|---|---|
| `BOT_TOKEN` | @BotFather ka bot token |
| `STREAM_SECRET` | cPanel direct media app mein bhi exactly same value |
| `CPANEL_WATCH_URL` | `https://chalchitra.site/watch.php` |
| `PUBLIC_BOT_URL` | `https://urlshare-gl1t.onrender.com` |
| `WEBHOOK_SECRET` | Letters/numbers/underscore/hyphen wala random secret |
| `TOKEN_TTL_SECONDS` | `2592000` |

`API_ID` aur `API_HASH` ab Render par required nahi hain. Yeh cPanel ke direct Telegram media app mein set honge. Purane variables Render mein reh sakte hain, lekin media traffic unse nahi chalega.

## Deploy

GitHub par latest files push karo aur Render mein **Manual Deploy → Deploy latest commit** select karo. Deploy ke baad check karo:

```text
https://urlshare-gl1t.onrender.com/health
```

Expected response:

```json
{"ok":true,"service":"telegram-cpanel-metadata-bot","media_relay":false}
```

Webhook registration background retry mein hoti hai, isliye temporary Telegram error par service crash nahi karegi. Successful setup ke baad logs mein `Telegram webhook configured` dikhna chahiye.

## Important

Render bot ko Telegram se message metadata milega, jisme chat ID aur message ID hota hai. Bot isi reference ko signed token mein encode karke cPanel watch URL bhejega. Render `/media` endpoint intentionally nahi rakhta, taaki video playback Render bandwidth use na kare.

Do not commit `.env` or real credentials to GitHub. `.env.example` sirf key names ka template hai.
