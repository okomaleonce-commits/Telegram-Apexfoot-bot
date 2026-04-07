import os
import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")


def send_telegram_message(text: str):
    if not BOT_TOKEN:
        return {
            "status": "error",
            "message": "BOT_TOKEN is missing"
        }, 500

    if not CHAT_ID:
        return {
            "status": "error",
            "message": "CHAT_ID is missing"
        }, 500

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text
    }

    try:
        response = requests.post(url, json=payload, timeout=15)
        telegram_data = response.json()
    except Exception as e:
        return {
            "status": "error",
            "message": "Telegram request failed",
            "details": str(e)
        }, 500

    if not response.ok or not telegram_data.get("ok"):
        return {
            "status": "error",
            "message": "Telegram API returned an error",
            "telegram_response": telegram_data
        }, 500

    return {
        "status": "ok",
        "telegram_response": telegram_data
    }, 200


@app.route("/")
def home():
    return "Apex Football Bot is running."


@app.route("/ping")
def ping():
    return jsonify({
        "status": "ok",
        "message": "pong"
    })


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "bot_token_present": bool(BOT_TOKEN),
        "chat_id_present": bool(CHAT_ID)
    })


@app.route("/send-test")
def send_test():
    data, status_code = send_telegram_message("Test OK depuis Render vers Telegram.")
    return jsonify(data), status_code


@app.route("/send-custom")
def send_custom():
    text = request.args.get("text", "").strip()

    if not text:
        return jsonify({
            "status": "error",
            "message": "Missing 'text' query parameter"
        }), 400

    data, status_code = send_telegram_message(text)
    return jsonify(data), status_code


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
