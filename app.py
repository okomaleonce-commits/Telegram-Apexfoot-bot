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
    if not BOT_TOKEN:
        return {"status": "error", "message": "BOT_TOKEN is missing"}, 500
    if not CHAT_ID:
        return {"status": "error", "message": "CHAT_ID is missing"}, 500

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    try:
        response = requests.post(
            url,
            json={"chat_id": CHAT_ID, "text": text},
            timeout=15
        )
        data = response.json()
    except Exception as e:
        return {
            "status": "error",
            "message": "Telegram request failed",
            "details": str(e)
        }, 500

    if not response.ok or not data.get("ok"):
        return {
            "status": "error",
            "message": "Telegram API returned an error",
            "telegram_response": data
        }, 500

    return {"status": "ok", "telegram_response": data}, 200


def call_api_football(endpoint: str, params=None):
    if not API_KEY:
        return {"status": "error", "message": "API_KEY is missing"}, 500

    headers = {"x-apisports-key": API_KEY}
    url = f"{API_FOOTBALL_BASE_URL}/{endpoint}"

    try:
        response = requests.get(
            url,
            headers=headers,
            params=params or {},
            timeout=20
        )
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

    return {"status": "ok", "data": data}, 200


def is_priority_fixture(match):
    text = (
        match.get("league", {}).get("name", "").lower()
        + " "
        + match.get("teams", {}).get("home", {}).get("name", "").lower()
        + " "
        + match.get("teams", {}).get("away", {}).get("name", "").lower()
    )

    return not any(keyword in text for keyword in EXCLUDED_KEYWORDS)


def is_target_league_by_id(match):
    return match.get("league", {}).get("id") in TARGET_LEAGUE_IDS


def is_pre_match_fixture(match):
    return match.get("fixture", {}).get("status", {}).get("short") == "NS"


def format_match_time(iso_date: str):
    try:
        dt = datetime.fromisoformat(iso_date.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).strftime("%H:%M UTC")
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
        "away": match.get("teams", {}).get("away", {}).get("name"),
    }


def build_fixture_detail(match):
    fixture = match.get("fixture", {})
    league = match.get("league", {})
    teams = match.get("teams", {})
    goals = match.get("goals", {})
    score = match.get("score", {})

    return {
        "fixture_id": fixture.get("id"),
        "referee": fixture.get("referee"),
        "timezone": fixture.get("timezone"),
        "date": fixture.get("date"),
        "timestamp": fixture.get("timestamp"),
        "venue_name": fixture.get("venue", {}).get("name"),
        "venue_city": fixture.get("venue", {}).get("city"),
        "status_long": fixture.get("status", {}).get("long"),
        "status_short": fixture.get("status", {}).get("short"),
        "elapsed": fixture.get("status", {}).get("elapsed"),
        "league_id": league.get("id"),
        "league_name": league.get("name"),
        "country": league.get("country"),
        "season": league.get("season"),
        "round": league.get("round"),
        "home": teams.get("home", {}).get("name"),
        "away": teams.get("away", {}).get("name"),
        "home_winner": teams.get("home", {}).get("winner"),
        "away_winner": teams.get("away", {}).get("winner"),
        "goals_home": goals.get("home"),
        "goals_away": goals.get("away"),
        "halftime_home": score.get("halftime", {}).get("home"),
        "halftime_away": score.get("halftime", {}).get("away"),
        "fulltime_home": score.get("fulltime", {}).get("home"),
        "fulltime_away": score.get("fulltime", {}).get("away")
    }


def build_fixture_teams_info(match):
    league = match.get("league", {})
    teams = match.get("teams", {})

    return {
        "fixture_id": match.get("fixture", {}).get("id"),
        "league_id": league.get("id"),
        "league_name": league.get("name"),
        "country": league.get("country"),
        "season": league.get("season"),
        "home_team_id": teams.get("home", {}).get("id"),
        "home_team_name": teams.get("home", {}).get("name"),
        "away_team_id": teams.get("away", {}).get("id"),
        "away_team_name": teams.get("away", {}).get("name")
    }


def get_fixture_by_id(fixture_id):
    data, status_code = call_api_football("fixtures", {"id": fixture_id})

    if status_code != 200:
        return data, status_code

    response = data["data"].get("response", [])
    if not response:
        return {
            "status": "error",
            "message": f"No fixture found for fixture_id={fixture_id}"
        }, 404

    return {"status": "ok", "fixture": response[0]}, 200


def find_team_standing(standings_response, team_id):
    for league_block in standings_response:
        league = league_block.get("league", {})
        for standing_group in league.get("standings", []):
            for team_row in standing_group:
                if team_row.get("team", {}).get("id") == team_id:
                    return {
                        "rank": team_row.get("rank"),
                        "team_id": team_row.get("team", {}).get("id"),
                        "team_name": team_row.get("team", {}).get("name"),
                        "points": team_row.get("points"),
                        "goals_diff": team_row.get("goalsDiff"),
                        "group": team_row.get("group"),
                        "form": team_row.get("form"),
                        "status": team_row.get("status"),
                        "description": team_row.get("description"),
                        "played": team_row.get("all", {}).get("played"),
                        "win": team_row.get("all", {}).get("win"),
                        "draw": team_row.get("all", {}).get("draw"),
                        "lose": team_row.get("all", {}).get("lose"),
                        "goals_for": team_row.get("all", {}).get("goals", {}).get("for"),
                        "goals_against": team_row.get("all", {}).get("goals", {}).get("against")
                    }
    return None


def extract_odds_summary(odds_response):
    """
    Essaie d'extraire un résumé simple depuis la structure /odds.
    On cherche en priorité le marché 'Match Winner' du premier bookmaker disponible.
    Si introuvable, on retourne les 3 premiers marchés disponibles.
    """
    if not odds_response:
        return {
            "bookmaker_name": None,
            "match_winner": None,
            "markets_preview": []
        }

    first_fixture = odds_response[0]
    bookmakers = first_fixture.get("bookmakers", [])
    if not bookmakers:
        return {
            "bookmaker_name": None,
            "match_winner": None,
            "markets_preview": []
        }

    bookmaker = bookmakers[0]
    bookmaker_name = bookmaker.get("name")
    bets = bookmaker.get("bets", [])

    match_winner = None
    preview = []

    for bet in bets:
        bet_name = bet.get("name")
        values = bet.get("values", [])

        preview.append({
            "bet_name": bet_name,
            "values": values[:3]
        })

        if bet_name and bet_name.lower() in ["match winner", "winner", "1x2"]:
            match_winner = {
                "bet_name": bet_name,
                "values": values
            }

    return {
        "bookmaker_name": bookmaker_name,
        "match_winner": match_winner,
        "markets_preview": preview[:3]
    }


@app.route("/")
def home():
    return "Bot running"


@app.route("/ping")
def ping():
    return jsonify({"status": "ok", "message": "pong"})


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "bot_token_present": bool(BOT_TOKEN),
        "chat_id_present": bool(CHAT_ID),
        "api_key_present": bool(API_KEY),
    })


@app.route("/send-test")
def send_test():
    data, status = send_telegram_message("Test OK depuis Render vers Telegram.")
    return jsonify(data), status


@app.route("/send-custom")
def send_custom():
    text = request.args.get("text", "").strip()

    if not text:
        return jsonify({
            "status": "error",
            "message": "Missing 'text' query parameter"
        }), 400

    data, status = send_telegram_message(text)
    return jsonify(data), status


@app.route("/football-test")
def football_test():
    data, status_code = call_api_football("countries")

    if status_code != 200:
        return jsonify(data), status_code

    results = data["data"].get("results", 0)
    telegram_data, telegram_status = send_telegram_message(
        f"API-Football OK. Endpoint countries accessible. Résultats retournés: {results}"
    )

    return jsonify({
        "status": "ok",
        "football_results": results,
        "telegram_status": telegram_data,
        "telegram_http_status": telegram_status,
        "api_sample": data["data"].get("response", [])[:3]
    }), 200


@app.route("/fixtures-prematch-ready")
def fixtures_prematch_ready():
    today = datetime.utcnow().strftime("%Y-%m-%d")
    data, status_code = call_api_football("fixtures", {"date": today})

    if status_code != 200:
        return jsonify(data), status_code

    fixtures = data["data"].get("response", [])

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

    if lines:
        send_telegram_message("PREMATCH:\n\n" + "\n".join(lines))

    return jsonify({
        "status": "ok",
        "count": len(structured),
        "sample": selected
    }), 200


@app.route("/fixture-detail")
def fixture_detail():
    fixture_id = request.args.get("fixture_id", "").strip()

    if not fixture_id:
        return jsonify({"status": "error", "message": "Missing 'fixture_id' query parameter"}), 400

    if not fixture_id.isdigit():
        return jsonify({"status": "error", "message": "fixture_id must be numeric"}), 400

    data, status_code = get_fixture_by_id(fixture_id)
    if status_code != 200:
        return jsonify(data), status_code

    detail = build_fixture_detail(data["fixture"])
    message = (
        "Détail du match :\n\n"
        f"{format_match_time(detail['date'])} | {detail['home']} vs {detail['away']}\n"
        f"Ligue : {detail['league_name']} ({detail['country']})\n"
        f"Round : {detail['round']}\n"
        f"Statut : {detail['status_long']} ({detail['status_short']})\n"
        f"Stade : {detail['venue_name']} - {detail['venue_city']}\n"
        f"fixture_id={detail['fixture_id']}"
    )

    telegram_data, telegram_status = send_telegram_message(message)

    return jsonify({
        "status": "ok",
        "fixture_detail": detail,
        "telegram_status": telegram_data,
        "telegram_http_status": telegram_status
    }), 200


@app.route("/fixture-teams")
def fixture_teams():
    fixture_id = request.args.get("fixture_id", "").strip()

    if not fixture_id:
        return jsonify({"status": "error", "message": "Missing 'fixture_id' query parameter"}), 400

    if not fixture_id.isdigit():
        return jsonify({"status": "error", "message": "fixture_id must be numeric"}), 400

    data, status_code = get_fixture_by_id(fixture_id)
    if status_code != 200:
        return jsonify(data), status_code

    teams_info = build_fixture_teams_info(data["fixture"])

    message = (
        "Équipes du match :\n\n"
        f"{teams_info['home_team_name']} (team_id={teams_info['home_team_id']})\n"
        "vs\n"
        f"{teams_info['away_team_name']} (team_id={teams_info['away_team_id']})\n\n"
        f"Ligue : {teams_info['league_name']} ({teams_info['country']})\n"
        f"Saison : {teams_info['season']}\n"
        f"fixture_id={teams_info['fixture_id']}"
    )

    telegram_data, telegram_status = send_telegram_message(message)

    return jsonify({
        "status": "ok",
        "teams_info": teams_info,
        "telegram_status": telegram_data,
        "telegram_http_status": telegram_status
    }), 200


@app.route("/fixture-standings")
def fixture_standings():
    fixture_id = request.args.get("fixture_id", "").strip()

    if not fixture_id:
        return jsonify({"status": "error", "message": "Missing 'fixture_id' query parameter"}), 400

    if not fixture_id.isdigit():
        return jsonify({"status": "error", "message": "fixture_id must be numeric"}), 400

    fixture_data, fixture_status = get_fixture_by_id(fixture_id)
    if fixture_status != 200:
        return jsonify(fixture_data), fixture_status

    match = fixture_data["fixture"]
    teams_info = build_fixture_teams_info(match)

    standings_data, standings_status = call_api_football(
        "standings",
        {
            "league": teams_info["league_id"],
            "season": teams_info["season"]
        }
    )

    if standings_status != 200:
        return jsonify(standings_data), standings_status

    standings_response = standings_data["data"].get("response", [])

    home_standing = find_team_standing(standings_response, teams_info["home_team_id"])
    away_standing = find_team_standing(standings_response, teams_info["away_team_id"])

    message = (
        "Classement du match :\n\n"
        f"{teams_info['home_team_name']} : "
        f"#{home_standing['rank'] if home_standing else 'N/A'} | "
        f"{home_standing['points'] if home_standing else 'N/A'} pts | "
        f"Forme: {home_standing['form'] if home_standing else 'N/A'}\n"
        f"{teams_info['away_team_name']} : "
        f"#{away_standing['rank'] if away_standing else 'N/A'} | "
        f"{away_standing['points'] if away_standing else 'N/A'} pts | "
        f"Forme: {away_standing['form'] if away_standing else 'N/A'}\n\n"
        f"Ligue : {teams_info['league_name']} ({teams_info['country']})\n"
        f"Saison : {teams_info['season']}\n"
        f"fixture_id={teams_info['fixture_id']}"
    )

    telegram_data, telegram_status = send_telegram_message(message)

    return jsonify({
        "status": "ok",
        "teams_info": teams_info,
        "home_standing": home_standing,
        "away_standing": away_standing,
        "telegram_status": telegram_data,
        "telegram_http_status": telegram_status
    }), 200


@app.route("/fixture-context")
def fixture_context():
    fixture_id = request.args.get("fixture_id", "").strip()

    if not fixture_id:
        return jsonify({"status": "error", "message": "Missing 'fixture_id' query parameter"}), 400

    if not fixture_id.isdigit():
        return jsonify({"status": "error", "message": "fixture_id must be numeric"}), 400

    fixture_data, fixture_status = get_fixture_by_id(fixture_id)
    if fixture_status != 200:
        return jsonify(fixture_data), fixture_status

    match = fixture_data["fixture"]

    detail = build_fixture_detail(match)
    teams = build_fixture_teams_info(match)

    standings_data, standings_status = call_api_football(
        "standings",
        {
            "league": detail["league_id"],
            "season": detail["season"]
        }
    )

    if standings_status != 200:
        return jsonify(standings_data), standings_status

    standings = standings_data["data"].get("response", [])
    home = find_team_standing(standings, teams["home_team_id"])
    away = find_team_standing(standings, teams["away_team_id"])

    context = {
        "fixture": {
            "fixture_id": detail["fixture_id"],
            "date": detail["date"],
            "league_id": detail["league_id"],
            "league_name": detail["league_name"],
            "country": detail["country"],
            "round": detail["round"],
            "season": detail["season"],
            "home": detail["home"],
            "away": detail["away"]
        },
        "teams": {
            "home_id": teams["home_team_id"],
            "home_name": teams["home_team_name"],
            "away_id": teams["away_team_id"],
            "away_name": teams["away_team_name"]
        },
        "home_rank": home["rank"] if home else None,
        "away_rank": away["rank"] if away else None,
        "home_points": home["points"] if home else None,
        "away_points": away["points"] if away else None,
        "home_form": home["form"] if home else None,
        "away_form": away["form"] if away else None,
        "home_goals_for": home["goals_for"] if home else None,
        "home_goals_against": home["goals_against"] if home else None,
        "away_goals_for": away["goals_for"] if away else None,
        "away_goals_against": away["goals_against"] if away else None
    }

    message = (
        f"{detail['home']} vs {detail['away']}\n\n"
        f"Classement: {context['home_rank']} vs {context['away_rank']}\n"
        f"Points: {context['home_points']} vs {context['away_points']}\n"
        f"Forme: {context['home_form']} vs {context['away_form']}\n"
        f"Buts: {context['home_goals_for']} vs {context['away_goals_for']}"
    )

    telegram_data, telegram_status = send_telegram_message(message)

    return jsonify({
        "status": "ok",
        "context": context,
        "telegram_status": telegram_data,
        "telegram_http_status": telegram_status
    }), 200


@app.route("/fixture-odds")
def fixture_odds():
    fixture_id = request.args.get("fixture_id", "").strip()
    bookmaker_id = request.args.get("bookmaker", "").strip()
    bet_id = request.args.get("bet", "").strip()

    if not fixture_id:
        return jsonify({"status": "error", "message": "Missing 'fixture_id' query parameter"}), 400

    if not fixture_id.isdigit():
        return jsonify({"status": "error", "message": "fixture_id must be numeric"}), 400

    params = {"fixture": fixture_id}

    if bookmaker_id:
        if not bookmaker_id.isdigit():
            return jsonify({"status": "error", "message": "bookmaker must be numeric"}), 400
        params["bookmaker"] = bookmaker_id

    if bet_id:
        if not bet_id.isdigit():
            return jsonify({"status": "error", "message": "bet must be numeric"}), 400
        params["bet"] = bet_id

    odds_data, odds_status = call_api_football("odds", params)

    if odds_status != 200:
        return jsonify(odds_data), odds_status

    odds_response = odds_data["data"].get("response", [])

    if not odds_response:
        return jsonify({
            "status": "ok",
            "message": "No odds found for this fixture",
            "odds_response_count": 0
        }), 200

    summary = extract_odds_summary(odds_response)

    fixture_data, fixture_status = get_fixture_by_id(fixture_id)
    if fixture_status == 200:
        detail = build_fixture_detail(fixture_data["fixture"])
        match_label = f"{detail['home']} vs {detail['away']}"
    else:
        match_label = f"fixture_id={fixture_id}"

    if summary["match_winner"]:
        values = summary["match_winner"]["values"]
        values_text = " | ".join(
            f"{v.get('value')}: {v.get('odd')}" for v in values
        )
        message = (
            "Odds du match :\n\n"
            f"{match_label}\n"
            f"Bookmaker: {summary['bookmaker_name']}\n"
            f"{summary['match_winner']['bet_name']} -> {values_text}"
        )
    else:
        preview_lines = []
        for market in summary["markets_preview"]:
            vals = " | ".join(
                f"{v.get('value')}: {v.get('odd')}" for v in market.get("values", [])
            )
            preview_lines.append(f"{market.get('bet_name')} -> {vals}")

        message = (
            "Odds du match :\n\n"
            f"{match_label}\n"
            f"Bookmaker: {summary['bookmaker_name']}\n"
            + "\n".join(preview_lines)
        )

    telegram_data, telegram_status = send_telegram_message(message)

    return jsonify({
        "status": "ok",
        "fixture_id": int(fixture_id),
        "bookmaker_name": summary["bookmaker_name"],
        "match_winner": summary["match_winner"],
        "markets_preview": summary["markets_preview"],
        "raw_response_count": len(odds_response),
        "telegram_status": telegram_data,
        "telegram_http_status": telegram_status
    }), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
