import os
from datetime import datetime

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
API_KEY = os.environ.get("API_KEY")

API_FOOTBALL_BASE_URL = "https://v3.football.api-sports.io"


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


def call_api_football(endpoint: str, params=None):
    if not API_KEY:
        return {
            "status": "error",
            "message": "API_KEY is missing"
        }, 500

    headers = {
        "x-apisports-key": API_KEY
    }

    url = f"{API_FOOTBALL_BASE_URL}/{endpoint}"

    try:
        response = requests.get(url, headers=headers, params=params or {}, timeout=20)
        data = response.json()
    except Exception as e:
        return {
            "status": "error",
            "message": "API-Football request failed",
            "details": str(e)
        }, 500

    if not response.ok:
        return {
            "status": "error",
            "message": "API-Football returned an HTTP error",
            "http_status": response.status_code,
            "api_response": data
        }, 500

    return {
        "status": "ok",
        "data": data
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
        "chat_id_present": bool(CHAT_ID),
        "api_key_present": bool(API_KEY)
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


@app.route("/football-test")
def football_test():
    data, status_code = call_api_football("countries")

    if status_code != 200:
        return jsonify(data), status_code

    api_data = data["data"]
    results = api_data.get("results", 0)

    message = f"API-Football OK. Endpoint countries accessible. Résultats retournés: {results}"
    telegram_data, telegram_status = send_telegram_message(message)

    return jsonify({
        "status": "ok",
        "football_results": results,
        "telegram_status": telegram_data,
        "telegram_http_status": telegram_status,
        "api_sample": api_data.get("response", [])[:3]
    }), 200


@app.route("/fixtures-today")
def fixtures_today():
    today = datetime.utcnow().strftime("%Y-%m-%d")

    params = {
        "date": today
    }

    data, status_code = call_api_football("fixtures", params)

    if status_code != 200:
        return jsonify(data), status_code

    api_data = data["data"]
    fixtures = api_data.get("response", [])

    if not fixtures:
        message = "Aucun match trouvé aujourd'hui."
        send_telegram_message(message)
        return jsonify({
            "status": "ok",
            "message": message
        }), 200

    selected_matches = fixtures[:5]

    lines = []
    for match in selected_matches:
        home = match["teams"]["home"]["name"]
        away = match["teams"]["away"]["name"]
        league = match["league"]["name"]
        match_time = match["fixture"]["date"]

        lines.append(f"{home} vs {away} | {league} | {match_time}")

    message = "Matchs du jour :\n\n" + "\n".join(lines)
    telegram_data, telegram_status = send_telegram_message(message)

    return jsonify({
        "status": "ok",
        "matches_count": len(fixtures),
        "sample_sent": lines,
        "telegram_status": telegram_data,
        "telegram_http_status": telegram_status
    }), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
