import os

os.environ["BOT_TOKEN"] = "123456:TEST"
os.environ["STREAM_SECRET"] = "metadata-test-secret"
os.environ["CPANEL_WATCH_URL"] = "https://chalchitra.site/watch.php"
os.environ["PUBLIC_BOT_URL"] = ""
os.environ["WEBHOOK_SECRET"] = "webhook-test"

from app import sign_payload

payload = {"v": 2, "c": 100, "m": 200, "f": "telegram-file-id", "exp": 4102444800}
token = sign_payload(payload)
assert token.count(".") == 1
assert "telegram-file-id" not in token
print("metadata-only Render bot test passed")
