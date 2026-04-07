import os
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
API_KEY = os.environ.get("API_KEY")

API_FOOTBALL_BASE_URL = "https://v3.football.api-sports.io"

EXCLUDED_KEYWORDS = [
    "youth", "u17", "u18", "u19", "u20", "u21", "u23",
    "women", "feminine", "female",
    "reserve", "reserves", "b team", "ii"
]

TARGET_LEAGUE_IDS = [
    39,   # Premier League (England)
    140,  # La Liga (Spain)
    78,   # Bundesliga (Germany)
    135,  # Serie A (Italy)
    61,   # Ligue 1 (France)
    40,   # Championship (England)
    71,   # Serie A (Brazil)
    128,  # Liga Profesional Argentina
    242,  # Liga Pro (Ecuador)
    265   # Primera División (Chile)
]


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


def is_priority_fixture(match):
    league_name = match.get("league", {}).get("name", "").lower()
    home_name = match.get("teams", {}).get("home", {}).get("name", "").lower()
    away_name = match.get("teams", {}).get("away", {}).get("name", "").lower()

    combined_text = f"{league_name} {home_name} {away_name}"

    for keyword in EXCLUDED_KEYWORDS:
        if keyword in combined_text:
            return False

    return True


def is_target_league_by_id(match):
    league_id = match.get("league", {}).get("id")
    return league_id in TARGET_LEAGUE_IDS


def format_match_time(iso_date: str):
    try:
        dt = datetime.fromisoformat(iso_date.replace("Z", "+00:00"))
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.strftime("%H:%M UTC")
    except Exception:
        return iso_date


def build_fixture_record(match):
    return {
        "fixture_id": match.get("fixture", {}).get("id"),
        "kickoff_utc": match.get("fixture", {}).get("date"),
        "status": match.get("fixture", {}).get("status", {}).get("short"),
        "league_id": match.get("league", {}).get("id"),
        "league_name": match.get("league", {}).get("name"),
        "country": match.get("league", {}).get("country"),
        "home": match.get("teams", {}).get("home", {}).get("name"),
        "away": match.get("teams", {}).get("away", {}).get("name")
    }


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
    params = {"date": today}

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
        country = match["league"]["country"]
        match_time = format_match_time(match["fixture"]["date"])

        lines.append(f"{match_time} | {home} vs {away} | {league} ({country})")

    message = "Matchs du jour :\n\n" + "\n".join(lines)
    telegram_data, telegram_status = send_telegram_message(message)

    return jsonify({
        "status": "ok",
        "matches_count": len(fixtures),
        "sample_sent": lines,
        "telegram_status": telegram_data,
        "telegram_http_status": telegram_status
    }), 200


@app.route("/fixtures-priority")
def fixtures_priority():
    today = datetime.utcnow().strftime("%Y-%m-%d")
    params = {"date": today}

    data, status_code = call_api_football("fixtures", params)

    if status_code != 200:
        return jsonify(data), status_code

    api_data = data["data"]
    fixtures = api_data.get("response", [])
    filtered_fixtures = [match for match in fixtures if is_priority_fixture(match)]

    if not filtered_fixtures:
        message = "Aucun match prioritaire trouvé aujourd'hui après filtrage."
        send_telegram_message(message)
        return jsonify({
            "status": "ok",
            "message": message,
            "raw_matches_count": len(fixtures),
            "filtered_matches_count": 0
        }), 200

    selected_matches = filtered_fixtures[:5]

    lines = []
    for match in selected_matches:
        home = match["teams"]["home"]["name"]
        away = match["teams"]["away"]["name"]
        league = match["league"]["name"]
        country = match["league"]["country"]
        match_time = format_match_time(match["fixture"]["date"])

        lines.append(f"{match_time} | {home} vs {away} | {league} ({country})")

    message = "Matchs prioritaires du jour :\n\n" + "\n".join(lines)
    telegram_data, telegram_status = send_telegram_message(message)

    return jsonify({
        "status": "ok",
        "raw_matches_count": len(fixtures),
        "filtered_matches_count": len(filtered_fixtures),
        "sample_sent": lines,
        "telegram_status": telegram_data,
        "telegram_http_status": telegram_status
    }), 200


@app.route("/fixtures-target-ids")
def fixtures_target_ids():
    today = datetime.utcnow().strftime("%Y-%m-%d")
    params = {"date": today}

    data, status_code = call_api_football("fixtures", params)

    if status_code != 200:
        return jsonify(data), status_code

    api_data = data["data"]
    fixtures = api_data.get("response", [])

    priority_fixtures = [match for match in fixtures if is_priority_fixture(match)]
    target_fixtures = [match for match in priority_fixtures if is_target_league_by_id(match)]

    if not target_fixtures:
        message = "Aucun match cible trouvé aujourd'hui avec les IDs sélectionnés."
        send_telegram_message(message)
        return jsonify({
            "status": "ok",
            "raw_matches_count": len(fixtures),
            "priority_matches_count": len(priority_fixtures),
            "target_matches_count": 0,
            "message": message
        }), 200

    selected_matches = target_fixtures[:8]

    lines = []
    for match in selected_matches:
        home = match["teams"]["home"]["name"]
        away = match["teams"]["away"]["name"]
        league = match["league"]["name"]
        country = match["league"]["country"]
        league_id = match["league"]["id"]
        match_time = format_match_time(match["fixture"]["date"])

        lines.append(
            f"{match_time} | {home} vs {away} | {league} ({country}) | league_id={league_id}"
        )

    message = "Matchs cibles du jour (IDs) :\n\n" + "\n".join(lines)
    telegram_data, telegram_status = send_telegram_message(message)

    return jsonify({
        "status": "ok",
        "raw_matches_count": len(fixtures),
        "priority_matches_count": len(priority_fixtures),
        "target_matches_count": len(target_fixtures),
        "sample_sent": lines,
        "telegram_status": telegram_data,
        "telegram_http_status": telegram_status
    }), 200


@app.route("/fixtures-ready")
def fixtures_ready():
    today = datetime.utcnow().strftime("%Y-%m-%d")
    params = {"date": today}

    data, status_code = call_api_football("fixtures", params)

    if status_code != 200:
        return jsonify(data), status_code

    api_data = data["data"]
    fixtures = api_data.get("response", [])

    priority_fixtures = [match for match in fixtures if is_priority_fixture(match)]
    target_fixtures = [match for match in priority_fixtures if is_target_league_by_id(match)]

    structured_fixtures = [build_fixture_record(match) for match in target_fixtures]

    if not structured_fixtures:
        message = "Aucun match prêt pour analyse aujourd'hui."
        send_telegram_message(message)
        return jsonify({
            "status": "ok",
            "raw_matches_count": len(fixtures),
            "priority_matches_count": len(priority_fixtures),
            "ready_matches_count": 0,
            "message": message
        }), 200

    selected = structured_fixtures[:8]

    lines = []
    for match in selected:
        kickoff = format_match_time(match["kickoff_utc"])
        lines.append(
            f"{kickoff} | {match['home']} vs {match['away']} | "
            f"{match['league_name']} ({match['country']}) | "
            f"fixture_id={match['fixture_id']}"
        )

    message = "Matchs prêts pour analyse :\n\n" + "\n".join(lines)
    telegram_data, telegram_status = send_telegram_message(message)

    return jsonify({
        "status": "ok",
        "raw_matches_count": len(fixtures),
        "priority_matches_count": len(priority_fixtures),
        "ready_matches_count": len(structured_fixtures),
        "sample_sent": lines,
        "ready_fixtures": selected,
        "telegram_status": telegram_data,
        "telegram_http_status": telegram_status
    }), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
