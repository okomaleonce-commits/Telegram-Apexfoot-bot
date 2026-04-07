import os
import requests
from flask import Flask, jsonify

app = Flask(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

@app.route("/")
def home():
    return "Apex Football Bot is running."

@app.route("/ping")
def ping():
    return jsonify({
        "status": "ok",
        "message": "pong"
    })

@app.route("/send-test")
def send_test():
    if not BOT_TOKEN:
        return jsonify({
            "status": "error",
            "message": "BOT_TOKEN is missing"
        }), 500

    if not CHAT_ID:
        return jsonify({
            "status": "error",
            "message": "CHAT_ID is missing"
        }), 500

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": "Test OK depuis Render vers Telegram."
    }

    response = requests.post(url, json=payload, timeout=15)

    try:
        telegram_data = response.json()
    except Exception:
        return jsonify({
            "status": "error",
            "message": "Invalid response from Telegram",
            "raw_response": response.text
        }), 500

    return jsonify({
        "status": "ok",
        "telegram_response": telegram_data
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
