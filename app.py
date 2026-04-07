import os
from datetime import datetime, timezone
import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
API_KEY = os.environ.get("API_KEY")

API_FOOTBALL_BASE_URL = "https://v3.football.api-sports.io"


# ================= TELEGRAM =================

def send_telegram_message(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    response = requests.post(url, json={"chat_id": CHAT_ID, "text": text})
    return response.json(), response.status_code


# ================= API =================

def call_api(endpoint, params=None):
    headers = {"x-apisports-key": API_KEY}
    url = f"{API_FOOTBALL_BASE_URL}/{endpoint}"
    response = requests.get(url, headers=headers, params=params)
    return response.json()


# ================= UTILS =================

def implied_prob(odd):
    return 1 / float(odd)


def normalize_probs(probs):
    total = sum(probs)
    return [p / total for p in probs]


def format_time(iso):
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).strftime("%H:%M UTC")


# ================= ROUTES =================

@app.route("/")
def home():
    return "OK"


@app.route("/fixture-value")
def fixture_value():
    fixture_id = request.args.get("fixture_id")

    # ========= 1. FIXTURE =========
    fixture = call_api("fixtures", {"id": fixture_id})["response"][0]

    home = fixture["teams"]["home"]["name"]
    away = fixture["teams"]["away"]["name"]
    league = fixture["league"]["name"]
    country = fixture["league"]["country"]
    season = fixture["league"]["season"]
    date = fixture["fixture"]["date"]

    league_id = fixture["league"]["id"]
    home_id = fixture["teams"]["home"]["id"]
    away_id = fixture["teams"]["away"]["id"]

    # ========= 2. STANDINGS =========
    standings = call_api("standings", {
        "league": league_id,
        "season": season
    })["response"]

    def find_team(team_id):
        for l in standings:
            for group in l["league"]["standings"]:
                for t in group:
                    if t["team"]["id"] == team_id:
                        return t
        return None

    home_s = find_team(home_id)
    away_s = find_team(away_id)

    # ========= 3. ODDS =========
    odds_data = call_api("odds", {"fixture": fixture_id})["response"]

    if not odds_data:
        return jsonify({"status": "no odds"}), 200

    bookmaker = odds_data[0]["bookmakers"][0]
    bets = bookmaker["bets"]

    match_winner = None
    for b in bets:
        if b["name"].lower() in ["match winner", "1x2"]:
            match_winner = b
            break

    if not match_winner:
        return jsonify({"status": "no 1x2 odds"}), 200

    odds = match_winner["values"]

    home_odd = float([o["odd"] for o in odds if o["value"] == "Home"][0])
    draw_odd = float([o["odd"] for o in odds if o["value"] == "Draw"][0])
    away_odd = float([o["odd"] for o in odds if o["value"] == "Away"][0])

    # ========= 4. PROBABILITÉS =========
    probs = normalize_probs([
        implied_prob(home_odd),
        implied_prob(draw_odd),
        implied_prob(away_odd)
    ])

    p_home, p_draw, p_away = probs

    # ========= 5. LECTURE CONTEXTE =========
    score_home = 0
    score_away = 0

    # Classement
    if home_s and away_s:
        if home_s["rank"] < away_s["rank"]:
            score_home += 1
        elif away_s["rank"] < home_s["rank"]:
            score_away += 1

    # Forme
    if home_s and away_s:
        if home_s["form"].count("W") > away_s["form"].count("W"):
            score_home += 1
        else:
            score_away += 1

    # Défense
    if home_s and away_s:
        if home_s["all"]["goals"]["against"] < away_s["all"]["goals"]["against"]:
            score_home += 1
        else:
            score_away += 1

    # ========= 6. DECISION =========

    decision = "NO VALUE"

    if score_home > score_away and p_home > p_away:
        decision = "LEAN HOME"
    elif score_away > score_home and p_away > p_home:
        decision = "LEAN AWAY"
    elif abs(p_home - p_away) < 0.02:
        decision = "COIN FLIP"

    # ========= 7. MESSAGE =========

    message = f"""
VALUE ANALYSIS

{home} vs {away}
{league} ({country})

Probabilités:
Home: {round(p_home*100,1)}%
Draw: {round(p_draw*100,1)}%
Away: {round(p_away*100,1)}%

Lecture:
Score interne: {score_home} - {score_away}

➡️ Decision: {decision}
"""

    send_telegram_message(message)

    return jsonify({
        "status": "ok",
        "decision": decision,
        "probabilities": {
            "home": p_home,
            "draw": p_draw,
            "away": p_away
        },
        "score_model": {
            "home": score_home,
            "away": score_away
        }
    }), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
