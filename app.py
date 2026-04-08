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
    39,   # England - Premier League
    40,   # England - Championship
    41,   # England - League One
    140,  # Spain - La Liga
    78,   # Germany - Bundesliga
    79,   # Germany - 2. Bundesliga
    135,  # Italy - Serie A
    136,  # Italy - Serie B
    61,   # France - Ligue 1
    62,   # France - Ligue 2
    2,    # UEFA Champions League
    3,    # UEFA Europa League
    848,  # UEFA Europa Conference League
    94,   # Portugal - Primeira Liga
    95,   # Portugal - Liga Portugal 2
    88,   # Netherlands - Eredivisie
    89,   # Netherlands - Eerste Divisie
    203,  # Turkey - Süper Lig
    197,  # Greece - Super League 1
    207,  # Switzerland - Super League
    113,  # Denmark - Superliga
    119,  # Sweden - Allsvenskan
    103,  # Norway - Eliteserien
    106,  # Poland - Ekstraklasa
    179,  # Romania - Liga I
    218,  # Hungary - NB I
    235,  # Russia - Premier League
    72,   # Scotland - Premiership
    210,  # Croatia - HNL
    328,  # Ukraine - Premier League
    244,  # Finland - Veikkausliiga
    164,  # Iceland - Úrvalsdeild
    128,  # Argentina - Primera División
    71,   # Brazil - Serie A
    239,  # Colombia - Primera A
    265,  # Chile - Primera División
    262,  # Mexico - Liga MX
    253,  # USA - MLS
    233,  # Egypt - Premier League
    242,  # Morocco - Botola Pro
    343,  # Ivory Coast - Ligue 1
    307,  # Saudi Arabia - Pro League
    301,  # China - Super League
    98,   # Japan - J1 League
    292,  # South Korea - K League 1
    17,   # AFC Champions League
    188,  # Australia - A-League
]


@app.errorhandler(Exception)
def handle_exception(e):
    return jsonify({
        "status": "error",
        "message": "Unhandled server exception",
        "details": str(e),
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
            timeout=15,
        )
        data = response.json()
    except Exception as e:
        return {
            "status": "error",
            "message": "Telegram request failed",
            "details": str(e),
        }, 500

    if not response.ok or not data.get("ok"):
        return {
            "status": "error",
            "message": "Telegram API returned an error",
            "telegram_response": data,
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
            timeout=20,
        )
        data = response.json()
    except Exception as e:
        return {
            "status": "error",
            "message": "API-Football request failed",
            "details": str(e),
        }, 500

    if not response.ok:
        return {
            "status": "error",
            "message": "API-Football returned an HTTP error",
            "http_status": response.status_code,
            "api_response": data,
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

    return {
        "fixture_id": fixture.get("id"),
        "date": fixture.get("date"),
        "status_long": fixture.get("status", {}).get("long"),
        "status_short": fixture.get("status", {}).get("short"),
        "league_id": league.get("id"),
        "league_name": league.get("name"),
        "country": league.get("country"),
        "season": league.get("season"),
        "round": league.get("round"),
        "home": teams.get("home", {}).get("name"),
        "away": teams.get("away", {}).get("name"),
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


def get_fixture_by_id(fixture_id):
    data, status_code = call_api_football("fixtures", {"id": fixture_id})

    if status_code != 200:
        return data, status_code

    response = data["data"].get("response", [])
    if not response:
        return {
            "status": "error",
            "message": f"No fixture found for fixture_id={fixture_id}",
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
                        "form": team_row.get("form"),
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
            "values": values[:6],
        })

        if bet_name and bet_name.lower() in ["match winner", "winner", "1x2"]:
            match_winner = {
                "bet_name": bet_name,
                "values": values,
            }

    return {
        "bookmaker_name": bookmaker_name,
        "match_winner": match_winner,
        "markets_preview": preview[:10],
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
        return 1.0 / odd
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


def safe_div(a, b):
    if a is None or b in (None, 0):
        return None
    return a / b


def build_model_probabilities(context):
    home_score = 1.0
    draw_score = 0.8
    away_score = 1.0

    home_rank = context.get("home_rank")
    away_rank = context.get("away_rank")
    home_points = context.get("home_points")
    away_points = context.get("away_points")
    home_form = context.get("home_form") or ""
    away_form = context.get("away_form") or ""
    home_goals_for = context.get("home_goals_for")
    away_goals_for = context.get("away_goals_for")
    home_goals_against = context.get("home_goals_against")
    away_goals_against = context.get("away_goals_against")

    if home_rank is not None and away_rank is not None:
        rank_gap = away_rank - home_rank
        if rank_gap >= 4:
            home_score += 0.45
        elif rank_gap >= 2:
            home_score += 0.25
        elif rank_gap <= -4:
            away_score += 0.45
        elif rank_gap <= -2:
            away_score += 0.25
        else:
            draw_score += 0.20

    if home_points is not None and away_points is not None:
        point_gap = home_points - away_points
        if point_gap >= 8:
            home_score += 0.35
        elif point_gap >= 3:
            home_score += 0.20
        elif point_gap <= -8:
            away_score += 0.35
        elif point_gap <= -3:
            away_score += 0.20
        else:
            draw_score += 0.10

    home_wins = count_wins(home_form)
    away_wins = count_wins(away_form)
    if home_wins >= away_wins + 2:
        home_score += 0.30
    elif away_wins >= home_wins + 2:
        away_score += 0.30
    else:
        draw_score += 0.10

    if home_goals_for is not None and away_goals_for is not None:
        if home_goals_for >= away_goals_for + 8:
            home_score += 0.20
        elif away_goals_for >= home_goals_for + 8:
            away_score += 0.20

    if home_goals_against is not None and away_goals_against is not None:
        if away_goals_against >= home_goals_against + 8:
            home_score += 0.20
        elif home_goals_against >= away_goals_against + 8:
            away_score += 0.20

    raw = {"Home": home_score, "Draw": draw_score, "Away": away_score}
    total = sum(raw.values())

    return {
        "raw_scores": raw,
        "normalized_probabilities": {k: v / total for k, v in raw.items()},
    }


def compute_edges(model_probs, market_probs):
    result = {}
    for key in ["Home", "Draw", "Away"]:
        mp = model_probs.get(key)
        bp = market_probs.get(key)
        result[key] = None if mp is None or bp is None else mp - bp
    return result


def best_edge(edges):
    candidates = {k: v for k, v in edges.items() if v is not None}
    if not candidates:
        return None, None
    best_key = max(candidates, key=candidates.get)
    return best_key, candidates[best_key]


def build_value_decision_with_edge(detail, odds_1x2, model_probs, market_probs, edges):
    if is_live_or_not_prematch(detail.get("status_short")):
        return {
            "decision": "NO_BET",
            "rationale": ["Le match n'est plus en pré-match."],
            "best_edge_label": None,
            "best_edge_value": None,
        }

    try:
        away_odd = float(odds_1x2["Away"]) if odds_1x2.get("Away") else None
        home_odd = float(odds_1x2["Home"]) if odds_1x2.get("Home") else None
    except Exception:
        away_odd = None
        home_odd = None

    if away_odd is not None and away_odd < 1.60:
        return {
            "decision": "NO_BET",
            "rationale": ["Cote away trop basse (< 1.60) : favori déjà trop pricé."],
            "best_edge_label": None,
            "best_edge_value": None,
        }

    if home_odd is not None and home_odd < 1.60:
        return {
            "decision": "NO_BET",
            "rationale": ["Cote home trop basse (< 1.60) : favori déjà trop pricé."],
            "best_edge_label": None,
            "best_edge_value": None,
        }

    edge_label, edge_value = best_edge(edges)

    if edge_label is None or edge_value is None:
        return {
            "decision": "NO_BET",
            "rationale": ["Impossible de calculer un edge exploitable."],
            "best_edge_label": None,
            "best_edge_value": None,
        }

    if edge_value < 0.05:
        return {
            "decision": "NO_BET",
            "rationale": ["Best edge < 5% : pas d'avantage suffisant contre le marché."],
            "best_edge_label": edge_label,
            "best_edge_value": edge_value,
        }

    label_map = {
        "Home": "VALUE_HOME",
        "Draw": "VALUE_DRAW",
        "Away": "VALUE_AWAY",
    }

    return {
        "decision": label_map.get(edge_label, "NO_BET"),
        "rationale": [
            f"Edge positif sur {edge_label}.",
            f"Best edge = {round(edge_value * 100, 2)}%.",
        ],
        "best_edge_label": edge_label,
        "best_edge_value": edge_value,
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


def build_goals_value_decision(goals_context, btts_market, ou25_market):
    rationale = []
    decision = "NO_BET"

    home_gf_avg = safe_div(goals_context["home_goals_for"], goals_context["home_played"])
    away_gf_avg = safe_div(goals_context["away_goals_for"], goals_context["away_played"])
    home_ga_avg = safe_div(goals_context["home_goals_against"], goals_context["home_played"])
    away_ga_avg = safe_div(goals_context["away_goals_against"], goals_context["away_played"])

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
            yes_prob = implied_probability(yes_odd)
            no_prob = implied_probability(no_odd)

            if yes_prob is not None and no_prob is not None and abs(yes_prob - no_prob) < 0.05:
                rationale.append("BTTS trop équilibré par le marché : NO_BET.")
            else:
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
            over_prob = implied_probability(over_odd)
            under_prob = implied_probability(under_odd)

            if over_prob is not None and under_prob is not None and abs(over_prob - under_prob) < 0.05:
                rationale.append("O/U 2.5 trop équilibré par le marché : NO_BET.")
            else:
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

    market_implied_raw = {
        "Home": implied_probability(odds_1x2["Home"]),
        "Draw": implied_probability(odds_1x2["Draw"]),
        "Away": implied_probability(odds_1x2["Away"]),
    }
    market_implied_normalized = normalize_probabilities(market_implied_raw)

    model_data = build_model_probabilities(context)
    model_probs = model_data["normalized_probabilities"]
    edges = compute_edges(model_probs, market_implied_normalized)

    decision_data = build_value_decision_with_edge(
        detail=detail,
        odds_1x2=odds_1x2,
        model_probs=model_probs,
        market_probs=market_implied_normalized,
        edges=edges,
    )

    message = (
        "VALUE ANALYSIS\n\n"
        f"{detail['home']} vs {detail['away']}\n"
        f"{detail['league_name']} ({detail['country']})\n"
        f"{format_match_time(detail['date'])}\n\n"
        f"Odds 1X2:\n"
        f"Home: {odds_1x2['Home']} | Draw: {odds_1x2['Draw']} | Away: {odds_1x2['Away']}\n\n"
        f"Marché normalisé:\n"
        f"Home: {round(market_implied_normalized['Home'] * 100, 1) if market_implied_normalized['Home'] is not None else 'N/A'}%\n"
        f"Draw: {round(market_implied_normalized['Draw'] * 100, 1) if market_implied_normalized['Draw'] is not None else 'N/A'}%\n"
        f"Away: {round(market_implied_normalized['Away'] * 100, 1) if market_implied_normalized['Away'] is not None else 'N/A'}%\n\n"
        f"Modèle:\n"
        f"Home: {round(model_probs['Home'] * 100, 1)}%\n"
        f"Draw: {round(model_probs['Draw'] * 100, 1)}%\n"
        f"Away: {round(model_probs['Away'] * 100, 1)}%\n\n"
        f"Edges:\n"
        f"Home: {round(edges['Home'] * 100, 2) if edges['Home'] is not None else 'N/A'}%\n"
        f"Draw: {round(edges['Draw'] * 100, 2) if edges['Draw'] is not None else 'N/A'}%\n"
        f"Away: {round(edges['Away'] * 100, 2) if edges['Away'] is not None else 'N/A'}%\n\n"
        f"Decision: {decision_data['decision']}\n"
        f"Rationale: {' | '.join(decision_data['rationale'])}"
    )

    telegram_data, telegram_status = send_telegram_message(message)

    return jsonify({
        "status": "ok",
        "fixture": detail,
        "context": context,
        "odds_1x2": odds_1x2,
        "market_implied_raw": market_implied_raw,
        "market_implied_normalized": market_implied_normalized,
        "model_probabilities": model_probs,
        "model_raw_scores": model_data["raw_scores"],
        "edges": edges,
        "best_edge_label": decision_data["best_edge_label"],
        "best_edge_value": decision_data["best_edge_value"],
        "decision": decision_data["decision"],
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
