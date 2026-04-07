import os
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
API_KEY = os.environ.get("API_KEY")

API_FOOTBALL_BASE_URL = "https://v3.football.api-sports.io"

TARGET_LEAGUE_IDS = [39, 140, 78, 135, 61, 40, 71, 128, 242, 265]


def send_telegram_message(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    return requests.post(url, json={"chat_id": CHAT_ID, "text": text}).json(), 200


def call_api_football(endpoint: str, params=None):
    headers = {"x-apisports-key": API_KEY}
    url = f"{API_FOOTBALL_BASE_URL}/{endpoint}"
    response = requests.get(url, headers=headers, params=params or {})
    return response.json(), response.status_code


def format_match_time(iso_date):
    dt = datetime.fromisoformat(iso_date.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).strftime("%H:%M UTC")


def get_fixture_by_id(fixture_id):
    data, _ = call_api_football("fixtures", {"id": fixture_id})
    return data["response"][0]


def build_fixture_detail(match):
    return {
        "fixture_id": match["fixture"]["id"],
        "date": match["fixture"]["date"],
        "league_id": match["league"]["id"],
        "league_name": match["league"]["name"],
        "country": match["league"]["country"],
        "season": match["league"]["season"],
        "round": match["league"]["round"],
        "home": match["teams"]["home"]["name"],
        "away": match["teams"]["away"]["name"],
    }


def build_fixture_teams(match):
    return {
        "home_id": match["teams"]["home"]["id"],
        "home_name": match["teams"]["home"]["name"],
        "away_id": match["teams"]["away"]["id"],
        "away_name": match["teams"]["away"]["name"],
    }


def find_team(standings, team_id):
    for league in standings:
        for group in league["league"]["standings"]:
            for team in group:
                if team["team"]["id"] == team_id:
                    return team
    return None


@app.route("/fixture-context")
def fixture_context():
    fixture_id = request.args.get("fixture_id")

    match = get_fixture_by_id(fixture_id)

    detail = build_fixture_detail(match)
    teams = build_fixture_teams(match)

    standings_data, _ = call_api_football(
        "standings",
        {
            "league": detail["league_id"],
            "season": detail["season"]
        }
    )

    standings = standings_data["response"]

    home = find_team(standings, teams["home_id"])
    away = find_team(standings, teams["away_id"])

    context = {
        "fixture": detail,
        "teams": teams,
        "home_rank": home["rank"],
        "away_rank": away["rank"],
        "home_points": home["points"],
        "away_points": away["points"],
        "home_form": home["form"],
        "away_form": away["form"],
        "home_goals_for": home["all"]["goals"]["for"],
        "home_goals_against": home["all"]["goals"]["against"],
        "away_goals_for": away["all"]["goals"]["for"],
        "away_goals_against": away["all"]["goals"]["against"],
    }

    message = (
        f"{detail['home']} vs {detail['away']}\n\n"
        f"Classement: {home['rank']} vs {away['rank']}\n"
        f"Points: {home['points']} vs {away['points']}\n"
        f"Forme: {home['form']} vs {away['form']}\n"
        f"Buts: {home['all']['goals']['for']} vs {away['all']['goals']['for']}"
    )

    send_telegram_message(message)

    return jsonify(context)


@app.route("/")
def home():
    return "Bot running"


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
