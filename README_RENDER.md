# Render Telegram Bot Package

Yeh folder sirf Telegram bot ke liye hai. Is service ko Render par **Web Service** ke roop mein deploy karo, Background Worker ke roop mein nahi, kyunki Telegram webhook ko public HTTPS endpoint chahiye.

## Render settings

```text
Root Directory: render_bot
Build Command: pip install -r requirements.txt
Start Command: uvicorn app:app --host 0.0.0.0 --port $PORT
Health Check Path: /health
```

Agar repository mein sirf is ZIP ka content upload karte ho, to Root Directory blank rakhkar same build/start commands use kar sakte ho.

## Required Environment Variables

| Key | Value |
|---|---|
| `BOT_TOKEN` | @BotFather se mila bot token |
| `API_ID` | my.telegram.org ka numeric API ID |
| `API_HASH` | Telegram API hash |
| `STREAM_SECRET` | 32+ characters ka random secret |
| `CPANEL_WATCH_URL` | `https://YOUR-DOMAIN.com/watch.php` |
| `PUBLIC_BOT_URL` | Tumhara Render service URL, jaise `https://your-service.onrender.com` (without `/health` or `/telegram/webhook`) |
| `WEBHOOK_SECRET` | 32+ characters ka random secret |
| `TOKEN_TTL_SECONDS` | `2592000` |
| `TELEGRAM_REQUEST_SIZE` | `524288` |

Secrets GitHub mein commit mat karna. Render Dashboard ke Environment section mein add karna.

## Deploy ke baad

Webhook registration ab background retry mein hoti hai, isliye Telegram ka temporary 400 response Render app ko crash nahi karega. Code update ke baad GitHub par push karke Render se redeploy karo, phir environment mein `PUBLIC_BOT_URL=https://your-service.onrender.com` set rakho.

Browser mein yeh open karo:

```text
https://YOUR-RENDER-SERVICE.onrender.com/health
```

Expected response mein `"ok": true` aur `"started": true` hona chahiye. Uske baad bot ko 100–200 MB ka actual MP4 bhejo.

Bot video ko Render disk par save nahi karta. Telegram message reference se signed watch link banata hai. Large media stream ke waqt bytes Render memory se cPanel ko forward hote hain, isliye Render media bandwidth bhi use hogi.

## Security

`BOT_TOKEN`, `API_HASH`, `STREAM_SECRET` aur `WEBHOOK_SECRET` kabhi chat, GitHub ya browser code mein expose mat karna. Token expire hone par link kaam nahi karega.
