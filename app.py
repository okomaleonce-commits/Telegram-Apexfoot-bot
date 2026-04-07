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

TARGET_LEAGUE_IDS = [39, 140, 78, 135, 61, 40, 71, 128, 242, 265]


def send_telegram_message(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text}

    response = requests.post(url, json=payload)
    return response.json(), response.status_code


def call_api_football(endpoint: str, params=None):
    headers = {"x-apisports-key": API_KEY}
    url = f"{API_FOOTBALL_BASE_URL}/{endpoint}"

    response = requests.get(url, headers=headers, params=params or {})
    return response.json(), response.status_code


def is_priority_fixture(match):
    text = (
        match.get("league", {}).get("name", "").lower()
        + match.get("teams", {}).get("home", {}).get("name", "").lower()
        + match.get("teams", {}).get("away", {}).get("name", "").lower()
    )

    for keyword in EXCLUDED_KEYWORDS:
        if keyword in text:
            return False
    return True


def is_target_league_by_id(match):
    return match.get("league", {}).get("id") in TARGET_LEAGUE_IDS


def is_pre_match_fixture(match):
    return match.get("fixture", {}).get("status", {}).get("short") == "NS"


def format_match_time(iso_date: str):
    dt = datetime.fromisoformat(iso_date.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).strftime("%H:%M UTC")


def build_fixture_record(match):
    return {
        "fixture_id": match["fixture"]["id"],
        "kickoff_utc": match["fixture"]["date"],
        "status": match["fixture"]["status"]["short"],
        "league_id": match["league"]["id"],
        "league_name": match["league"]["name"],
        "country": match["league"]["country"],
        "home": match["teams"]["home"]["name"],
        "away": match["teams"]["away"]["name"],
    }


def build_fixture_detail(match):
    return {
        "fixture_id": match["fixture"]["id"],
        "date": match["fixture"]["date"],
        "status_long": match["fixture"]["status"]["long"],
        "status_short": match["fixture"]["status"]["short"],
        "league_name": match["league"]["name"],
        "country": match["league"]["country"],
        "round": match["league"]["round"],
        "season": match["league"]["season"],
        "home": match["teams"]["home"]["name"],
        "away": match["teams"]["away"]["name"],
        "venue_name": match["fixture"]["venue"]["name"],
        "venue_city": match["fixture"]["venue"]["city"],
    }


def build_fixture_teams_info(match):
    return {
        "fixture_id": match["fixture"]["id"],
        "league_id": match["league"]["id"],
        "league_name": match["league"]["name"],
        "country": match["league"]["country"],
        "season": match["league"]["season"],
        "home_team_id": match["teams"]["home"]["id"],
        "home_team_name": match["teams"]["home"]["name"],
        "away_team_id": match["teams"]["away"]["id"],
        "away_team_name": match["teams"]["away"]["name"],
    }


def get_fixture_by_id(fixture_id):
    data, _ = call_api_football("fixtures", {"id": fixture_id})
    return data["response"][0]


@app.route("/")
def home():
    return "Bot running"


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "bot_token": bool(BOT_TOKEN),
        "chat_id": bool(CHAT_ID),
        "api_key": bool(API_KEY),
    })


@app.route("/fixtures-prematch-ready")
def fixtures_prematch_ready():
    today = datetime.utcnow().strftime("%Y-%m-%d")
    data, _ = call_api_football("fixtures", {"date": today})

    fixtures = data["response"]

    prematch = [
        m for m in fixtures
        if is_priority_fixture(m)
        and is_target_league_by_id(m)
        and is_pre_match_fixture(m)
    ]

    structured = [build_fixture_record(m) for m in prematch]

    selected = structured[:5]

    lines = [
        f"{format_match_time(m['kickoff_utc'])} | {m['home']} vs {m['away']} | {m['league_name']} | id={m['fixture_id']}"
        for m in selected
    ]

    message = "PREMATCH:\n\n" + "\n".join(lines)
    send_telegram_message(message)

    return jsonify({
        "count": len(structured),
        "sample": selected
    })


@app.route("/fixture-detail")
def fixture_detail():
    fixture_id = request.args.get("fixture_id")

    match = get_fixture_by_id(fixture_id)
    detail = build_fixture_detail(match)

    message = (
        f"{detail['home']} vs {detail['away']}\n"
        f"{detail['league_name']} ({detail['country']})\n"
        f"{detail['status_long']}\n"
        f"{detail['venue_name']}"
    )

    send_telegram_message(message)

    return jsonify(detail)


@app.route("/fixture-teams")
def fixture_teams():
    fixture_id = request.args.get("fixture_id")

    match = get_fixture_by_id(fixture_id)
    teams = build_fixture_teams_info(match)

    message = (
        f"{teams['home_team_name']} (id={teams['home_team_id']})\n"
        f"vs\n"
        f"{teams['away_team_name']} (id={teams['away_team_id']})"
    )

    send_telegram_message(message)

    return jsonify(teams)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
