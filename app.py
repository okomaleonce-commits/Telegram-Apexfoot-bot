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
    "reserve", "reserves", "b team", "ii",
]

TARGET_LEAGUE_IDS = [
    # Europe - Top
    39,   # Premier League
    40,   # Championship
    41,   # League One
    140,  # La Liga
    141,  # La Liga 2
    78,   # Bundesliga
    79,   # 2. Bundesliga
    135,  # Serie A
    136,  # Serie B
    61,   # Ligue 1
    62,   # Ligue 2

    # Europe - compétitions UEFA
    2,    # Champions League
    3,    # Europa League
    848,  # Europa Conference League

    # Autres Europe
    94,   # Portugal Liga NOS
    95,   # Portugal LigaPro
    88,   # Netherlands Eredivisie
    89,   # Netherlands Eerste Divisie
    203,  # Turkey Süper Lig
    197,  # Greece Super League
    207,  # Switzerland Super League
    113,  # Denmark Superliga
    119,  # Sweden Allsvenskan
    103,  # Norway Eliteserien
    106,  # Poland Ekstraklasa
    179,  # Romania Liga I
    218,  # Hungary NB I
    235,  # Russia Premier League
    72,   # Scotland Premiership
    210,  # Croatia Prva HNL
    328,  # Ukraine Premier League
    244,  # Finland Veikkausliiga
    164,  # Iceland Úrvalsdeild

    # Amérique
    128,  # Argentina Primera División
    71,   # Brazil Serie A
    239,  # Colombia Primera A
    265,  # Chile Primera División
    262,  # Liga MX
    253,  # MLS

    # Afrique
    233,  # Egypt Premier League
    242,  # Morocco Botola
    343,  # Ivory Coast Ligue 1

    # Asie / Moyen-Orient
    292,  # Saudi Pro League
    307,  # UAE League (optionnel si utile)
    301,  # China Super League
    98,   # Japan J1 League
    292,  # Saudi League
    292,  # (doublon toléré mais inutile)
    307,  # Korea K League 1
    17,   # AFC Champions League

    # Océanie
    188,  # Australia A-League
]


@app.errorhandler(Exception)
def handle_exception(e):
    return jsonify({
        "status": "error",
        "message": "Unhandled server exception",
        "details": str(e)
    }), 500


# =========================
# TELEGRAM
# =========================
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


# =========================
# API FOOTBALL
# =========================
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


# =========================
# HELPERS
# =========================
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


def is_live_or_not_prematch(status_short):
    return status_short != "NS"


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
        "fulltime_away": score.get("fulltime", {}).get("away"),
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
        "away_team_name": teams.get("away", {}).get("name"),
    }


def build_fixture_detail_summary(match):
    detail = build_fixture_detail(match)
    return {
        "fixture_id": detail["fixture_id"],
        "home": detail["home"],
        "away": detail["away"],
        "league_id": detail["league_id"],
        "league_name": detail["league_name"],
        "country": detail["country"],
        "season": detail["season"],
        "round": detail["round"],
        "date": detail["date"],
        "status_short": detail["status_short"],
        "status_long": detail["status_long"],
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
                        "goals_against": team_row.get("all", {}).get("goals", {}).get("against"),
                    }
    return None


def extract_odds_summary(odds_response):
    if not odds_response:
        return {
            "bookmaker_name": None,
            "match_winner": None,
            "markets_preview": [],
        }

    first_fixture = odds_response[0]
    bookmakers = first_fixture.get("bookmakers", [])
    if not bookmakers:
        return {
            "bookmaker_name": None,
            "match_winner": None,
            "markets_preview": [],
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
            "values": values[:4],
        })

        if bet_name and bet_name.lower() in ["match winner", "winner", "1x2"]:
            match_winner = {
                "bet_name": bet_name,
                "values": values,
            }

    return {
        "bookmaker_name": bookmaker_name,
        "match_winner": match_winner,
        "markets_preview": preview[:8],
    }


def extract_match_winner_odds(match_winner_market):
    if not match_winner_market:
        return None

    result = {"Home": None, "Draw": None, "Away": None}

    for value in match_winner_market.get("values", []):
        label = value.get("value")
        odd = value.get("odd")
        if label in result:
            result[label] = odd

    return result


def find_market_values(markets_preview, market_names, accepted_labels=None):
    target_names = [m.lower() for m in market_names]

    for market in markets_preview:
        bet_name = (market.get("bet_name") or "").lower()
        if bet_name in target_names:
            values = market.get("values", [])
            extracted = {}
            for item in values:
                label = item.get("value")
                odd = item.get("odd")
                if accepted_labels is None or label in accepted_labels:
                    extracted[label] = odd
            return {
                "bet_name": market.get("bet_name"),
                "values": extracted,
            }
    return None


def implied_probability(decimal_odd):
    if decimal_odd is None:
        return None
    try:
        odd = float(decimal_odd)
        if odd <= 0:
            return None
        return 1 / odd
    except Exception:
        return None


def normalize_probabilities(prob_dict):
    valid_values = [v for v in prob_dict.values() if v is not None]
    total = sum(valid_values)

    if total <= 0:
        return {k: None for k in prob_dict}

    return {
        k: (v / total if v is not None else None)
        for k, v in prob_dict.items()
    }


def count_wins(form_string):
    if not form_string:
        return 0
    return form_string.count("W")


def build_value_decision(context, normalized_probs):
    home_score = 0
    away_score = 0

    if context["home_rank"] is not None and context["away_rank"] is not None:
        if context["home_rank"] < context["away_rank"]:
            home_score += 1
        elif context["away_rank"] < context["home_rank"]:
            away_score += 1

    if context["home_points"] is not None and context["away_points"] is not None:
        if context["home_points"] > context["away_points"]:
            home_score += 1
        elif context["away_points"] > context["home_points"]:
            away_score += 1

    home_wins = count_wins(context["home_form"])
    away_wins = count_wins(context["away_form"])
    if home_wins > away_wins:
        home_score += 1
    elif away_wins > home_wins:
        away_score += 1

    if context["home_goals_against"] is not None and context["away_goals_against"] is not None:
        if context["home_goals_against"] < context["away_goals_against"]:
            home_score += 1
        elif context["away_goals_against"] < context["home_goals_against"]:
            away_score += 1

    if context["home_goals_for"] is not None and context["away_goals_for"] is not None:
        if context["home_goals_for"] > context["away_goals_for"]:
            home_score += 1
        elif context["away_goals_for"] > context["home_goals_for"]:
            away_score += 1

    p_home = normalized_probs.get("Home")
    p_draw = normalized_probs.get("Draw")
    p_away = normalized_probs.get("Away")

    decision = "NO_BET"
    rationale = []

    if p_home is None or p_draw is None or p_away is None:
        rationale.append("Probabilités de marché incomplètes.")
        return {
            "decision": decision,
            "model_score_home": home_score,
            "model_score_away": away_score,
            "rationale": rationale,
        }

    market_gap = abs(p_home - p_away)

    if home_score >= away_score + 2 and p_home >= p_away + 0.03:
        decision = "LEAN_HOME"
        rationale.append("Le contexte favorise clairement l'équipe à domicile.")
        rationale.append("Le marché va aussi légèrement dans le même sens.")
    elif away_score >= home_score + 2 and p_away >= p_home + 0.03:
        decision = "LEAN_AWAY"
        rationale.append("Le contexte favorise clairement l'équipe à l'extérieur.")
        rationale.append("Le marché va aussi légèrement dans le même sens.")
    elif market_gap < 0.02 and abs(home_score - away_score) <= 1:
        decision = "LEAN_DRAW"
        rationale.append("Le marché voit un match quasi équilibré.")
        rationale.append("Le contexte ne crée pas d'écart structurel fort.")
    else:
        rationale.append("Le contexte et le marché ne créent pas d'écart suffisamment exploitable.")
        rationale.append("Absence de signal clair -> discipline NO_BET.")

    return {
        "decision": decision,
        "model_score_home": home_score,
        "model_score_away": away_score,
        "rationale": rationale,
    }


def build_goals_context(home_standing, away_standing):
    return {
        "home_goals_for": home_standing["goals_for"] if home_standing else None,
        "home_goals_against": home_standing["goals_against"] if home_standing else None,
        "away_goals_for": away_standing["goals_for"] if away_standing else None,
        "away_goals_against": away_standing["goals_against"] if away_standing else None,
        "home_form": home_standing["form"] if home_standing else None,
        "away_form": away_standing["form"] if away_standing else None,
        "home_played": home_standing["played"] if home_standing else None,
        "away_played": away_standing["played"] if away_standing else None,
    }


def avg_goals_for(goals_for, played):
    if goals_for is None or played in (None, 0):
        return None
    return goals_for / played


def avg_goals_against(goals_against, played):
    if goals_against is None or played in (None, 0):
        return None
    return goals_against / played


def build_goals_value_decision(goals_context, btts_market, ou25_market):
    rationale = []
    decision = "NO_BET"

    home_gf_avg = avg_goals_for(goals_context["home_goals_for"], goals_context["home_played"])
    away_gf_avg = avg_goals_for(goals_context["away_goals_for"], goals_context["away_played"])
    home_ga_avg = avg_goals_against(goals_context["home_goals_against"], goals_context["home_played"])
    away_ga_avg = avg_goals_against(goals_context["away_goals_against"], goals_context["away_played"])

    attack_signal = 0
    concede_signal = 0

    if home_gf_avg is not None and home_gf_avg >= 1.45:
        attack_signal += 1
    if away_gf_avg is not None and away_gf_avg >= 1.45:
        attack_signal += 1
    if home_ga_avg is not None and home_ga_avg >= 1.15:
        concede_signal += 1
    if away_ga_avg is not None and away_ga_avg >= 1.15:
        concede_signal += 1

    if btts_market:
        yes_odd = btts_market["values"].get("Yes")
        no_odd = btts_market["values"].get("No")

        if yes_odd and no_odd:
            if attack_signal >= 2 and concede_signal >= 1 and float(yes_odd) >= 1.60:
                decision = "LEAN_BTTS_YES"
                rationale.append("Les deux équipes affichent des signaux offensifs suffisants.")
                rationale.append("Les profils défensifs ne ferment pas naturellement le match.")
                return {
                    "decision": decision,
                    "rationale": rationale,
                    "home_gf_avg": home_gf_avg,
                    "away_gf_avg": away_gf_avg,
                    "home_ga_avg": home_ga_avg,
                    "away_ga_avg": away_ga_avg,
                }

            if attack_signal <= 1 and concede_signal <= 1 and float(no_odd) >= 1.60:
                decision = "LEAN_BTTS_NO"
                rationale.append("Le profil global ne pousse pas clairement vers deux équipes buteuses.")
                rationale.append("Signaux offensifs insuffisants pour un BTTS fort.")
                return {
                    "decision": decision,
                    "rationale": rationale,
                    "home_gf_avg": home_gf_avg,
                    "away_gf_avg": away_gf_avg,
                    "home_ga_avg": home_ga_avg,
                    "away_ga_avg": away_ga_avg,
                }

    if ou25_market:
        over_odd = ou25_market["values"].get("Over 2.5")
        under_odd = ou25_market["values"].get("Under 2.5")

        if over_odd and under_odd:
            if attack_signal >= 2 and concede_signal >= 2 and float(over_odd) >= 1.70:
                decision = "LEAN_OVER_2_5"
                rationale.append("Les moyennes offensives et défensives suggèrent un match ouvert.")
                rationale.append("Le profil global pousse vers 3 buts ou plus.")
                return {
                    "decision": decision,
                    "rationale": rationale,
                    "home_gf_avg": home_gf_avg,
                    "away_gf_avg": away_gf_avg,
                    "home_ga_avg": home_ga_avg,
                    "away_ga_avg": away_ga_avg,
                }

            if attack_signal <= 1 and concede_signal <= 1 and float(under_odd) >= 1.70:
                decision = "LEAN_UNDER_2_5"
                rationale.append("Le profil statistique ne soutient pas un match très ouvert.")
                rationale.append("Peu de signaux combinés pour dépasser 2.5 buts.")
                return {
                    "decision": decision,
                    "rationale": rationale,
                    "home_gf_avg": home_gf_avg,
                    "away_gf_avg": away_gf_avg,
                    "home_ga_avg": home_ga_avg,
                    "away_ga_avg": away_ga_avg,
                }

    rationale.append("Les marchés buts ne montrent pas d'avantage exploitable suffisant.")
    rationale.append("Discipline : NO_BET.")
    return {
        "decision": decision,
        "rationale": rationale,
        "home_gf_avg": home_gf_avg,
        "away_gf_avg": away_gf_avg,
        "home_ga_avg": home_ga_avg,
        "away_ga_avg": away_ga_avg,
    }


# =========================
# ROUTES
# =========================
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
        "sample": selected,
    }), 200


@app.route("/fixture-value")
def fixture_value():
    fixture_id = request.args.get("fixture_id", "").strip()

    if not fixture_id:
        return jsonify({"status": "error", "message": "Missing 'fixture_id' query parameter"}), 400
    if not fixture_id.isdigit():
        return jsonify({"status": "error", "message": "fixture_id must be numeric"}), 400

    fixture_data, fixture_status = get_fixture_by_id(fixture_id)
    if fixture_status != 200:
        return jsonify(fixture_data), fixture_status

    match = fixture_data["fixture"]
    detail = build_fixture_detail_summary(match)

    if is_live_or_not_prematch(detail["status_short"]):
        message = (
            "VALUE ANALYSIS BLOCKED\n\n"
            f"{detail['home']} vs {detail['away']}\n"
            f"Status: {detail['status_long']} ({detail['status_short']})\n"
            "Decision: NO_BET\n"
            "Reason: fixture is not pre-match anymore."
        )
        telegram_data, telegram_status = send_telegram_message(message)

        return jsonify({
            "status": "ok",
            "fixture": detail,
            "decision": "NO_BET",
            "message": "Fixture is not pre-match anymore",
            "telegram_status": telegram_data,
            "telegram_http_status": telegram_status,
        }), 200

    teams = build_fixture_teams_info(match)

    standings_data, standings_status = call_api_football(
        "standings",
        {"league": detail["league_id"], "season": detail["season"]}
    )
    if standings_status != 200:
        return jsonify(standings_data), standings_status

    standings_response = standings_data["data"].get("response", [])
    home_standing = find_team_standing(standings_response, teams["home_team_id"])
    away_standing = find_team_standing(standings_response, teams["away_team_id"])

    context = {
        "home_rank": home_standing["rank"] if home_standing else None,
        "away_rank": away_standing["rank"] if away_standing else None,
        "home_points": home_standing["points"] if home_standing else None,
        "away_points": away_standing["points"] if away_standing else None,
        "home_form": home_standing["form"] if home_standing else None,
        "away_form": away_standing["form"] if away_standing else None,
        "home_goals_for": home_standing["goals_for"] if home_standing else None,
        "home_goals_against": home_standing["goals_against"] if home_standing else None,
        "away_goals_for": away_standing["goals_for"] if away_standing else None,
        "away_goals_against": away_standing["goals_against"] if away_standing else None,
    }

    odds_data, odds_status = call_api_football("odds", {"fixture": fixture_id})
    if odds_status != 200:
        return jsonify(odds_data), odds_status

    odds_response = odds_data["data"].get("response", [])
    if not odds_response:
        return jsonify({
            "status": "ok",
            "fixture": detail,
            "decision": "NO_BET",
            "message": "No odds found for this fixture",
        }), 200

    odds_summary = extract_odds_summary(odds_response)
    match_winner_market = odds_summary.get("match_winner")

    if not match_winner_market:
        return jsonify({
            "status": "ok",
            "fixture": detail,
            "decision": "NO_BET",
            "message": "No 1X2 market found for this fixture",
        }), 200

    odds_1x2 = extract_match_winner_odds(match_winner_market)
    if not odds_1x2:
        return jsonify({
            "status": "ok",
            "fixture": detail,
            "decision": "NO_BET",
            "message": "Could not extract 1X2 odds",
        }), 200

    implied = {
        "Home": implied_probability(odds_1x2["Home"]),
        "Draw": implied_probability(odds_1x2["Draw"]),
        "Away": implied_probability(odds_1x2["Away"]),
    }
    normalized = normalize_probabilities(implied)
    decision_data = build_value_decision(context, normalized)

    message = (
        "VALUE ANALYSIS\n\n"
        f"{detail['home']} vs {detail['away']}\n"
        f"{detail['league_name']} ({detail['country']})\n"
        f"{format_match_time(detail['date'])}\n\n"
        f"Odds 1X2:\n"
        f"Home: {odds_1x2['Home']} | Draw: {odds_1x2['Draw']} | Away: {odds_1x2['Away']}\n\n"
        f"Probabilités normalisées:\n"
        f"Home: {round(normalized['Home'] * 100, 1) if normalized['Home'] is not None else 'N/A'}%\n"
        f"Draw: {round(normalized['Draw'] * 100, 1) if normalized['Draw'] is not None else 'N/A'}%\n"
        f"Away: {round(normalized['Away'] * 100, 1) if normalized['Away'] is not None else 'N/A'}%\n\n"
        f"Model score: {decision_data['model_score_home']} - {decision_data['model_score_away']}\n"
        f"Decision: {decision_data['decision']}\n"
        f"Rationale: {' | '.join(decision_data['rationale'])}"
    )

    telegram_data, telegram_status = send_telegram_message(message)

    return jsonify({
        "status": "ok",
        "fixture": detail,
        "context": context,
        "odds_1x2": odds_1x2,
        "implied_probabilities_raw": implied,
        "implied_probabilities_normalized": normalized,
        "decision": decision_data["decision"],
        "model_score_home": decision_data["model_score_home"],
        "model_score_away": decision_data["model_score_away"],
        "rationale": decision_data["rationale"],
        "telegram_status": telegram_data,
        "telegram_http_status": telegram_status,
    }), 200


@app.route("/fixture-goals-value")
def fixture_goals_value():
    fixture_id = request.args.get("fixture_id", "").strip()

    if not fixture_id:
        return jsonify({"status": "error", "message": "Missing 'fixture_id' query parameter"}), 400
    if not fixture_id.isdigit():
        return jsonify({"status": "error", "message": "fixture_id must be numeric"}), 400

    fixture_data, fixture_status = get_fixture_by_id(fixture_id)
    if fixture_status != 200:
        return jsonify(fixture_data), fixture_status

    match = fixture_data["fixture"]
    detail = build_fixture_detail_summary(match)

    if is_live_or_not_prematch(detail["status_short"]):
        message = (
            "GOALS VALUE ANALYSIS BLOCKED\n\n"
            f"{detail['home']} vs {detail['away']}\n"
            f"Status: {detail['status_long']} ({detail['status_short']})\n"
            "Decision: NO_BET\n"
            "Reason: fixture is not pre-match anymore."
        )
        telegram_data, telegram_status = send_telegram_message(message)

        return jsonify({
            "status": "ok",
            "fixture": detail,
            "decision": "NO_BET",
            "message": "Fixture is not pre-match anymore",
            "telegram_status": telegram_data,
            "telegram_http_status": telegram_status,
        }), 200

    teams = build_fixture_teams_info(match)

    standings_data, standings_status = call_api_football(
        "standings",
        {"league": detail["league_id"], "season": detail["season"]}
    )
    if standings_status != 200:
        return jsonify(standings_data), standings_status

    standings_response = standings_data["data"].get("response", [])
    home_standing = find_team_standing(standings_response, teams["home_team_id"])
    away_standing = find_team_standing(standings_response, teams["away_team_id"])

    goals_context = build_goals_context(home_standing, away_standing)

    odds_data, odds_status = call_api_football("odds", {"fixture": fixture_id})
    if odds_status != 200:
        return jsonify(odds_data), odds_status

    odds_response = odds_data["data"].get("response", [])
    if not odds_response:
        return jsonify({
            "status": "ok",
            "fixture": detail,
            "decision": "NO_BET",
            "message": "No odds found for this fixture",
        }), 200

    odds_summary = extract_odds_summary(odds_response)
    markets_preview = odds_summary.get("markets_preview", [])

    btts_market = find_market_values(
        markets_preview,
        ["Both Teams Score", "Both Teams To Score"],
        accepted_labels=["Yes", "No"],
    )

    ou25_market = find_market_values(
        markets_preview,
        ["Goals Over/Under", "Over/Under"],
        accepted_labels=["Over 2.5", "Under 2.5"],
    )

    decision_data = build_goals_value_decision(goals_context, btts_market, ou25_market)

    message = (
        "GOALS VALUE ANALYSIS\n\n"
        f"{detail['home']} vs {detail['away']}\n"
        f"{detail['league_name']} ({detail['country']})\n"
        f"{format_match_time(detail['date'])}\n\n"
        f"BTTS: {btts_market['values'] if btts_market else 'N/A'}\n"
        f"O/U 2.5: {ou25_market['values'] if ou25_market else 'N/A'}\n\n"
        f"GF avg: {round(decision_data['home_gf_avg'], 2) if decision_data['home_gf_avg'] is not None else 'N/A'} vs "
        f"{round(decision_data['away_gf_avg'], 2) if decision_data['away_gf_avg'] is not None else 'N/A'}\n"
        f"GA avg: {round(decision_data['home_ga_avg'], 2) if decision_data['home_ga_avg'] is not None else 'N/A'} vs "
        f"{round(decision_data['away_ga_avg'], 2) if decision_data['away_ga_avg'] is not None else 'N/A'}\n\n"
        f"Decision: {decision_data['decision']}\n"
        f"Rationale: {' | '.join(decision_data['rationale'])}"
    )

    telegram_data, telegram_status = send_telegram_message(message)

    return jsonify({
        "status": "ok",
        "fixture": detail,
        "goals_context": goals_context,
        "btts_market": btts_market,
        "over_under_2_5_market": ou25_market,
        "decision": decision_data["decision"],
        "rationale": decision_data["rationale"],
        "home_gf_avg": decision_data["home_gf_avg"],
        "away_gf_avg": decision_data["away_gf_avg"],
        "home_ga_avg": decision_data["home_ga_avg"],
        "away_ga_avg": decision_data["away_ga_avg"],
        "telegram_status": telegram_data,
        "telegram_http_status": telegram_status,
    }), 200


@app.route("/debug-fixture-value")
def debug_fixture_value():
    fixture_id = request.args.get("fixture_id", "").strip()

    if not fixture_id:
        return jsonify({"status": "error", "message": "Missing 'fixture_id' query parameter"}), 400
    if not fixture_id.isdigit():
        return jsonify({"status": "error", "message": "fixture_id must be numeric"}), 400

    debug = {"fixture_id": fixture_id}

    fixture_data, fixture_status = get_fixture_by_id(fixture_id)
    debug["fixture_status"] = fixture_status
    debug["fixture_data_keys"] = list(fixture_data.keys())

    if fixture_status != 200:
        return jsonify({"status": "ok", "debug": debug}), 200

    match = fixture_data["fixture"]
    detail = build_fixture_detail_summary(match)
    teams = build_fixture_teams_info(match)

    debug["detail"] = detail
    debug["teams"] = teams

    standings_data, standings_status = call_api_football(
        "standings",
        {"league": detail["league_id"], "season": detail["season"]}
    )
    debug["standings_status"] = standings_status

    if standings_status == 200:
        standings_response = standings_data["data"].get("response", [])
        debug["standings_response_count"] = len(standings_response)

        home_standing = find_team_standing(standings_response, teams["home_team_id"])
        away_standing = find_team_standing(standings_response, teams["away_team_id"])

        debug["home_standing_found"] = home_standing is not None
        debug["away_standing_found"] = away_standing is not None
    else:
        debug["standings_error"] = standings_data

    odds_data, odds_status = call_api_football("odds", {"fixture": fixture_id})
    debug["odds_status"] = odds_status

    if odds_status == 200:
        odds_response = odds_data["data"].get("response", [])
        debug["odds_response_count"] = len(odds_response)

        odds_summary = extract_odds_summary(odds_response)
        debug["bookmaker_name"] = odds_summary.get("bookmaker_name")
        debug["match_winner_found"] = odds_summary.get("match_winner") is not None
        debug["markets_preview"] = odds_summary.get("markets_preview", [])

        if odds_summary.get("match_winner"):
            debug["match_winner_market"] = odds_summary["match_winner"]
            debug["odds_1x2"] = extract_match_winner_odds(odds_summary["match_winner"])
    else:
        debug["odds_error"] = odds_data

    return jsonify({
        "status": "ok",
        "debug": debug,
    }), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
