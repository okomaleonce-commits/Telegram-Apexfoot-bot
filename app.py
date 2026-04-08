import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
API_KEY = os.environ.get("API_KEY")

API_FOOTBALL_BASE_URL = "https://v3.football.api-sports.io"
REQUEST_TIMEOUT = 20

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

LEVELS_1X2 = {
    1: {"name": "WATCHLIST", "edge_min": 0.03},
    2: {"name": "VALUE", "edge_min": 0.05},
    3: {"name": "MAIN", "edge_min": 0.08},
}
LEVELS_GOALS = {
    1: {"name": "WATCHLIST", "confidence_min": 2},
    2: {"name": "VALUE", "confidence_min": 3},
    3: {"name": "MAIN", "confidence_min": 4},
}

MIN_ODD_HOME_AWAY = 1.60
MIN_ODD_DRAW = 2.80
MAX_ODD_MAIN_SIGNAL = 4.50
MAX_SCAN_RESULTS = 20


# =========================
# GENERIC
# =========================
def ok(payload: Dict[str, Any], status_code: int = 200):
    return jsonify(payload), status_code


def err(message: str, status_code: int = 400, **kwargs):
    payload = {"status": "error", "message": message}
    payload.update(kwargs)
    return jsonify(payload), status_code


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
def send_telegram_message(text: str) -> Tuple[Dict[str, Any], int]:
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
def call_api_football(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], int]:
    if not API_KEY:
        return {"status": "error", "message": "API_KEY is missing"}, 500

    headers = {"x-apisports-key": API_KEY}
    url = f"{API_FOOTBALL_BASE_URL}/{endpoint}"

    try:
        response = requests.get(
            url,
            headers=headers,
            params=params or {},
            timeout=REQUEST_TIMEOUT,
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
def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def utc_today_str() -> str:
    return now_utc().strftime("%Y-%m-%d")


def parse_iso_date(iso_date: Optional[str]) -> Optional[datetime]:
    if not iso_date:
        return None
    try:
        return datetime.fromisoformat(iso_date.replace("Z", "+00:00"))
    except Exception:
        return None


def format_match_time(iso_date: Optional[str]) -> Optional[str]:
    dt = parse_iso_date(iso_date)
    if not dt:
        return iso_date
    return dt.astimezone(timezone.utc).strftime("%H:%M UTC")


def is_priority_fixture(match: Dict[str, Any]) -> bool:
    text = (
        (match.get("league", {}).get("name") or "").lower()
        + " "
        + (match.get("teams", {}).get("home", {}).get("name") or "").lower()
        + " "
        + (match.get("teams", {}).get("away", {}).get("name") or "").lower()
    )
    return not any(keyword in text for keyword in EXCLUDED_KEYWORDS)


def is_target_league_by_id(match: Dict[str, Any]) -> bool:
    return match.get("league", {}).get("id") in TARGET_LEAGUE_IDS


def is_pre_match_fixture(match: Dict[str, Any]) -> bool:
    return match.get("fixture", {}).get("status", {}).get("short") == "NS"


def is_live_or_not_prematch(status_short: Optional[str]) -> bool:
    return status_short != "NS"


def build_fixture_detail(match: Dict[str, Any]) -> Dict[str, Any]:
    fixture = match.get("fixture", {})
    league = match.get("league", {})
    teams = match.get("teams", {})

    return {
        "fixture_id": fixture.get("id"),
        "date": fixture.get("date"),
        "kickoff_utc": fixture.get("date"),
        "status_long": fixture.get("status", {}).get("long"),
        "status_short": fixture.get("status", {}).get("short"),
        "league_id": league.get("id"),
        "league_name": league.get("name"),
        "country": league.get("country"),
        "season": league.get("season"),
        "round": league.get("round"),
        "home": teams.get("home", {}).get("name"),
        "away": teams.get("away", {}).get("name"),
        "home_team_id": teams.get("home", {}).get("id"),
        "away_team_id": teams.get("away", {}).get("id"),
    }


def get_fixture_by_id(fixture_id: str) -> Tuple[Dict[str, Any], int]:
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


def get_fixtures_by_date(date_str: Optional[str] = None) -> Tuple[Dict[str, Any], int]:
    date_str = date_str or utc_today_str()
    return call_api_football("fixtures", {"date": date_str})


def find_team_standing(standings_response: List[Dict[str, Any]], team_id: Optional[int]) -> Optional[Dict[str, Any]]:
    if team_id is None:
        return None

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


def count_wins(form_string: Optional[str]) -> int:
    if not form_string:
        return 0
    return form_string.count("W")


def safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b in (None, 0):
        return None
    return a / b


def implied_probability(decimal_odd: Optional[Any]) -> Optional[float]:
    if decimal_odd is None:
        return None
    try:
        odd = float(decimal_odd)
        if odd <= 0:
            return None
        return 1.0 / odd
    except Exception:
        return None


def normalize_probabilities(prob_dict: Dict[str, Optional[float]]) -> Dict[str, Optional[float]]:
    valid_values = [v for v in prob_dict.values() if v is not None]
    total = sum(valid_values)

    if total <= 0:
        return {k: None for k in prob_dict}

    return {
        k: (v / total if v is not None else None)
        for k, v in prob_dict.items()
    }


def compute_edges(model_probs: Dict[str, Optional[float]], market_probs: Dict[str, Optional[float]]) -> Dict[str, Optional[float]]:
    result = {}
    for key in ["Home", "Draw", "Away"]:
        mp = model_probs.get(key)
        bp = market_probs.get(key)
        result[key] = None if mp is None or bp is None else mp - bp
    return result


def build_model_probabilities(context: Dict[str, Any]) -> Dict[str, Any]:
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


def label_to_side(label: str, home_name: Optional[str], away_name: Optional[str]) -> Optional[str]:
    normalized = (label or "").strip().lower()
    home_name = (home_name or "").strip().lower()
    away_name = (away_name or "").strip().lower()

    if normalized in {"home", "1"}:
        return "Home"
    if normalized in {"draw", "x"}:
        return "Draw"
    if normalized in {"away", "2"}:
        return "Away"
    if home_name and normalized == home_name:
        return "Home"
    if away_name and normalized == away_name:
        return "Away"
    return None


def pick_best_1x2_market(odds_response: List[Dict[str, Any]], home_name: Optional[str], away_name: Optional[str]) -> Dict[str, Any]:
    market_names = {"match winner", "winner", "1x2"}
    for fixture_odds in odds_response:
        for bookmaker in fixture_odds.get("bookmakers", []):
            for bet in bookmaker.get("bets", []):
                bet_name = (bet.get("name") or "").strip().lower()
                if bet_name not in market_names:
                    continue

                extracted = {"Home": None, "Draw": None, "Away": None}
                for value in bet.get("values", []):
                    side = label_to_side(value.get("value"), home_name, away_name)
                    if side:
                        extracted[side] = value.get("odd")

                if all(extracted.values()):
                    return {
                        "bookmaker_name": bookmaker.get("name"),
                        "bet_name": bet.get("name"),
                        "odds_1x2": extracted,
                    }

    return {
        "bookmaker_name": None,
        "bet_name": None,
        "odds_1x2": None,
    }


def extract_markets_preview(odds_response: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    preview: List[Dict[str, Any]] = []
    for fixture_odds in odds_response:
        for bookmaker in fixture_odds.get("bookmakers", [])[:2]:
            for bet in bookmaker.get("bets", [])[:10]:
                preview.append({
                    "bookmaker_name": bookmaker.get("name"),
                    "bet_name": bet.get("name"),
                    "values": bet.get("values", [])[:6],
                })
        if preview:
            break
    return preview[:20]


def find_market_values_any_bookmaker(
    odds_response: List[Dict[str, Any]],
    market_names: List[str],
    accepted_labels: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    target_names = {m.lower() for m in market_names}

    for fixture_odds in odds_response:
        for bookmaker in fixture_odds.get("bookmakers", []):
            for bet in bookmaker.get("bets", []):
                bet_name = (bet.get("name") or "").lower()
                if bet_name not in target_names:
                    continue

                extracted = {}
                for item in bet.get("values", []):
                    label = item.get("value")
                    odd = item.get("odd")
                    if accepted_labels is None or label in accepted_labels:
                        extracted[label] = odd

                if extracted:
                    return {
                        "bookmaker_name": bookmaker.get("name"),
                        "bet_name": bet.get("name"),
                        "values": extracted,
                    }
    return None


def filtered_edges_by_issue(odds_1x2: Dict[str, Any], edges: Dict[str, Optional[float]]) -> Dict[str, Optional[float]]:
    filtered = dict(edges)

    try:
        home_odd = float(odds_1x2["Home"]) if odds_1x2.get("Home") else None
    except Exception:
        home_odd = None

    try:
        draw_odd = float(odds_1x2["Draw"]) if odds_1x2.get("Draw") else None
    except Exception:
        draw_odd = None

    try:
        away_odd = float(odds_1x2["Away"]) if odds_1x2.get("Away") else None
    except Exception:
        away_odd = None

    if home_odd is not None and home_odd < MIN_ODD_HOME_AWAY:
        filtered["Home"] = None

    if away_odd is not None and away_odd < MIN_ODD_HOME_AWAY:
        filtered["Away"] = None

    if draw_odd is not None and draw_odd < MIN_ODD_DRAW:
        filtered["Draw"] = None

    return filtered


def build_confluence_flags(context: Dict[str, Any], side: str) -> Dict[str, bool]:
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

    flags = {
        "rank_advantage": False,
        "points_advantage": False,
        "form_advantage": False,
        "goal_profile_advantage": False,
    }

    if side == "Home":
        if home_rank is not None and away_rank is not None and home_rank < away_rank:
            flags["rank_advantage"] = True
        if home_points is not None and away_points is not None and home_points > away_points:
            flags["points_advantage"] = True
        if count_wins(home_form) > count_wins(away_form):
            flags["form_advantage"] = True
        if (
            home_goals_for is not None and away_goals_for is not None
            and home_goals_against is not None and away_goals_against is not None
            and home_goals_for >= away_goals_for
            and home_goals_against <= away_goals_against
        ):
            flags["goal_profile_advantage"] = True

    elif side == "Away":
        if home_rank is not None and away_rank is not None and away_rank < home_rank:
            flags["rank_advantage"] = True
        if home_points is not None and away_points is not None and away_points > home_points:
            flags["points_advantage"] = True
        if count_wins(away_form) > count_wins(home_form):
            flags["form_advantage"] = True
        if (
            home_goals_for is not None and away_goals_for is not None
            and home_goals_against is not None and away_goals_against is not None
            and away_goals_for >= home_goals_for
            and away_goals_against <= home_goals_against
        ):
            flags["goal_profile_advantage"] = True

    else:
        if home_rank is not None and away_rank is not None and abs(home_rank - away_rank) <= 2:
            flags["rank_advantage"] = True
        if home_points is not None and away_points is not None and abs(home_points - away_points) <= 3:
            flags["points_advantage"] = True
        if abs(count_wins(home_form) - count_wins(away_form)) <= 1:
            flags["form_advantage"] = True
        if (
            home_goals_for is not None and away_goals_for is not None
            and home_goals_against is not None and away_goals_against is not None
            and abs(home_goals_for - away_goals_for) <= 5
            and abs(home_goals_against - away_goals_against) <= 5
        ):
            flags["goal_profile_advantage"] = True

    return flags


def build_value_decision_with_3_levels(
    detail: Dict[str, Any],
    odds_1x2: Dict[str, Any],
    model_probs: Dict[str, Optional[float]],
    market_probs: Dict[str, Optional[float]],
    edges: Dict[str, Optional[float]],
    context: Dict[str, Any],
) -> Dict[str, Any]:
    if is_live_or_not_prematch(detail.get("status_short")):
        return {
            "decision": "NO_BET",
            "side": None,
            "level": 0,
            "level_name": None,
            "rationale": ["Le match n'est plus en pré-match."],
            "best_edge_label": None,
            "best_edge_value": None,
            "allowed_edges": edges,
            "confluence_flags": {},
            "confluence_count": 0,
        }

    allowed_edges = filtered_edges_by_issue(odds_1x2, edges)
    candidates = {k: v for k, v in allowed_edges.items() if v is not None}

    if not candidates:
        return {
            "decision": "NO_BET",
            "side": None,
            "level": 0,
            "level_name": None,
            "rationale": ["Aucune issue autorisée après filtres de cotes minimales."],
            "best_edge_label": None,
            "best_edge_value": None,
            "allowed_edges": allowed_edges,
            "confluence_flags": {},
            "confluence_count": 0,
        }

    best_side = max(candidates, key=candidates.get)
    best_edge_value = candidates[best_side]
    confluence_flags = build_confluence_flags(context, best_side)
    confluence_count = sum(1 for value in confluence_flags.values() if value)

    level = 0
    if best_edge_value >= LEVELS_1X2[3]["edge_min"] and confluence_count >= 3:
        level = 3
    elif best_edge_value >= LEVELS_1X2[2]["edge_min"] and confluence_count >= 2:
        level = 2
    elif best_edge_value >= LEVELS_1X2[1]["edge_min"]:
        level = 1

    try:
        selected_odd = float(odds_1x2[best_side]) if odds_1x2.get(best_side) else None
    except Exception:
        selected_odd = None

    if level == 3 and selected_odd is not None and selected_odd > MAX_ODD_MAIN_SIGNAL:
        level = 2

    if level == 0:
        return {
            "decision": "NO_BET",
            "side": best_side,
            "level": 0,
            "level_name": None,
            "rationale": [
                "Edge réel mais insuffisant pour atteindre le niveau 1 utile.",
                f"Best edge = {round(best_edge_value * 100, 2)}%.",
            ],
            "best_edge_label": best_side,
            "best_edge_value": best_edge_value,
            "allowed_edges": allowed_edges,
            "confluence_flags": confluence_flags,
            "confluence_count": confluence_count,
        }

    label_map = {
        1: {
            "Home": "WATCH_HOME",
            "Draw": "WATCH_DRAW",
            "Away": "WATCH_AWAY",
        },
        2: {
            "Home": "VALUE_HOME",
            "Draw": "VALUE_DRAW",
            "Away": "VALUE_AWAY",
        },
        3: {
            "Home": "MAIN_HOME",
            "Draw": "MAIN_DRAW",
            "Away": "MAIN_AWAY",
        },
    }

    rationale = [
        f"Best allowed edge sur {best_side} = {round(best_edge_value * 100, 2)}%.",
        f"Confluence = {confluence_count}/4.",
    ]

    if level == 1:
        rationale.append("Signal exploitable mais encore léger: surveillance ou petite exposition.")
    elif level == 2:
        rationale.append("Value bet valide: edge + confluence suffisante.")
    elif level == 3:
        rationale.append("Signal principal: edge fort + confluence élevée.")

    return {
        "decision": label_map[level][best_side],
        "side": best_side,
        "level": level,
        "level_name": LEVELS_1X2[level]["name"],
        "rationale": rationale,
        "best_edge_label": best_side,
        "best_edge_value": best_edge_value,
        "allowed_edges": allowed_edges,
        "confluence_flags": confluence_flags,
        "confluence_count": confluence_count,
    }


def build_goals_context(home_standing: Optional[Dict[str, Any]], away_standing: Optional[Dict[str, Any]]) -> Dict[str, Any]:
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


def goals_level_from_confidence(confidence_count: int) -> int:
    if confidence_count >= LEVELS_GOALS[3]["confidence_min"]:
        return 3
    if confidence_count >= LEVELS_GOALS[2]["confidence_min"]:
        return 2
    if confidence_count >= LEVELS_GOALS[1]["confidence_min"]:
        return 1
    return 0


def build_goals_value_decision_3_levels(
    goals_context: Dict[str, Any],
    btts_market: Optional[Dict[str, Any]],
    ou25_market: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    rationale: List[str] = []
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

    best_market = None
    best_level = 0
    best_confidence = 0

    if btts_market:
        yes_odd = btts_market["values"].get("Yes")
        no_odd = btts_market["values"].get("No")

        if yes_odd and no_odd:
            confidence_yes = 0
            confidence_no = 0

            if attack_signal >= 2:
                confidence_yes += 2
            if concede_signal >= 1:
                confidence_yes += 1
            if home_gf_avg is not None and away_gf_avg is not None and home_gf_avg >= 1.20 and away_gf_avg >= 1.20:
                confidence_yes += 1

            if attack_signal <= 1:
                confidence_no += 2
            if concede_signal <= 1:
                confidence_no += 1
            if home_ga_avg is not None and away_ga_avg is not None and home_ga_avg < 1.10 and away_ga_avg < 1.10:
                confidence_no += 1

            level_yes = goals_level_from_confidence(confidence_yes) if float(yes_odd) >= 1.60 else 0
            level_no = goals_level_from_confidence(confidence_no) if float(no_odd) >= 1.60 else 0

            if level_yes > best_level or (level_yes == best_level and confidence_yes > best_confidence):
                best_level = level_yes
                best_confidence = confidence_yes
                best_market = ("BTTS_YES", yes_odd, confidence_yes)

            if level_no > best_level or (level_no == best_level and confidence_no > best_confidence):
                best_level = level_no
                best_confidence = confidence_no
                best_market = ("BTTS_NO", no_odd, confidence_no)

    if ou25_market:
        over_odd = ou25_market["values"].get("Over 2.5")
        under_odd = ou25_market["values"].get("Under 2.5")

        if over_odd and under_odd:
            confidence_over = 0
            confidence_under = 0

            if attack_signal >= 2:
                confidence_over += 2
            if concede_signal >= 2:
                confidence_over += 2
            if home_gf_avg is not None and away_gf_avg is not None and (home_gf_avg + away_gf_avg) >= 3.00:
                confidence_over += 1

            if attack_signal <= 1:
                confidence_under += 2
            if concede_signal <= 1:
                confidence_under += 2
            if home_gf_avg is not None and away_gf_avg is not None and (home_gf_avg + away_gf_avg) <= 2.20:
                confidence_under += 1

            if away_gf_avg is not None and away_gf_avg >= 1.80 and home_ga_avg is not None and home_ga_avg >= 1.30:
                confidence_under = max(confidence_under - 2, 0)

            level_over = goals_level_from_confidence(confidence_over) if float(over_odd) >= 1.70 else 0
            level_under = goals_level_from_confidence(confidence_under) if float(under_odd) >= 1.70 else 0

            if level_over > best_level or (level_over == best_level and confidence_over > best_confidence):
                best_level = level_over
                best_confidence = confidence_over
                best_market = ("OVER_2_5", over_odd, confidence_over)

            if level_under > best_level or (level_under == best_level and confidence_under > best_confidence):
                best_level = level_under
                best_confidence = confidence_under
                best_market = ("UNDER_2_5", under_odd, confidence_under)

    if not best_market or best_level == 0:
        rationale.append("Les marchés buts ne montrent pas un avantage net.")
        rationale.append("Discipline: NO_BET.")
        return {
            "decision": decision,
            "level": 0,
            "level_name": None,
            "market": None,
            "confidence_count": 0,
            "rationale": rationale,
            "home_gf_avg": home_gf_avg,
            "away_gf_avg": away_gf_avg,
            "home_ga_avg": home_ga_avg,
            "away_ga_avg": away_ga_avg,
        }

    market_name, selected_odd, confidence_count = best_market

    if market_name == "BTTS_YES":
        decision = ["", "WATCH_BTTS_YES", "VALUE_BTTS_YES", "MAIN_BTTS_YES"][best_level]
    elif market_name == "BTTS_NO":
        decision = ["", "WATCH_BTTS_NO", "VALUE_BTTS_NO", "MAIN_BTTS_NO"][best_level]
    elif market_name == "OVER_2_5":
        decision = ["", "WATCH_OVER_2_5", "VALUE_OVER_2_5", "MAIN_OVER_2_5"][best_level]
    else:
        decision = ["", "WATCH_UNDER_2_5", "VALUE_UNDER_2_5", "MAIN_UNDER_2_5"][best_level]

    rationale.append(f"Meilleur marché buts = {market_name} @ {selected_odd}.")
    rationale.append(f"Confiance = {confidence_count}.")
    if best_level == 1:
        rationale.append("Lecture exploitable mais encore prudente.")
    elif best_level == 2:
        rationale.append("Marché buts avec assez de confluence pour une vraie value.")
    else:
        rationale.append("Signal principal sur les buts: forte cohérence du profil statistique.")

    return {
        "decision": decision,
        "level": best_level,
        "level_name": LEVELS_GOALS[best_level]["name"],
        "market": market_name,
        "confidence_count": confidence_count,
        "rationale": rationale,
        "home_gf_avg": home_gf_avg,
        "away_gf_avg": away_gf_avg,
        "home_ga_avg": home_ga_avg,
        "away_ga_avg": away_ga_avg,
    }


def summarize_1x2_signal(detail: Dict[str, Any], decision_data: Dict[str, Any], odds_1x2: Dict[str, Any]) -> str:
    side = decision_data.get("side")
    return (
        "APEXFOOTBALL 1X2\n\n"
        f"{detail['home']} vs {detail['away']}\n"
        f"{detail['league_name']} ({detail['country']})\n"
        f"{format_match_time(detail['date'])}\n\n"
        f"Decision: {decision_data['decision']}\n"
        f"Level: {decision_data.get('level')} - {decision_data.get('level_name')}\n"
        f"Side: {side}\n"
        f"Odd: {odds_1x2.get(side) if side else 'N/A'}\n"
        f"Best edge: {round(decision_data['best_edge_value'] * 100, 2) if decision_data.get('best_edge_value') is not None else 'N/A'}%\n"
        f"Confluence: {decision_data.get('confluence_count', 0)}/4\n"
        f"Rationale: {' | '.join(decision_data.get('rationale', []))}"
    )


def summarize_goals_signal(detail: Dict[str, Any], decision_data: Dict[str, Any]) -> str:
    return (
        "APEXFOOTBALL GOALS\n\n"
        f"{detail['home']} vs {detail['away']}\n"
        f"{detail['league_name']} ({detail['country']})\n"
        f"{format_match_time(detail['date'])}\n\n"
        f"Decision: {decision_data['decision']}\n"
        f"Level: {decision_data.get('level')} - {decision_data.get('level_name')}\n"
        f"Market: {decision_data.get('market')}\n"
        f"Confidence: {decision_data.get('confidence_count', 0)}\n"
        f"Rationale: {' | '.join(decision_data.get('rationale', []))}"
    )


def analyse_fixture_value_core(fixture_id: str) -> Tuple[Dict[str, Any], int]:
    fixture_data, fixture_status = get_fixture_by_id(fixture_id)
    if fixture_status != 200:
        return fixture_data, fixture_status

    match = fixture_data["fixture"]
    detail = build_fixture_detail(match)

    if is_live_or_not_prematch(detail["status_short"]):
        return {
            "status": "ok",
            "fixture": detail,
            "decision": "NO_BET",
            "message": "Fixture is not pre-match anymore",
        }, 200

    standings_data, standings_status = call_api_football(
        "standings",
        {"league": detail["league_id"], "season": detail["season"]}
    )
    if standings_status != 200:
        return standings_data, standings_status

    standings_response = standings_data["data"].get("response", [])
    home_standing = find_team_standing(standings_response, detail["home_team_id"])
    away_standing = find_team_standing(standings_response, detail["away_team_id"])

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
        return odds_data, odds_status

    odds_response = odds_data["data"].get("response", [])
    if not odds_response:
        return {
            "status": "ok",
            "fixture": detail,
            "decision": "NO_BET",
            "message": "No odds found for this fixture",
        }, 200

    market_pick = pick_best_1x2_market(odds_response, detail["home"], detail["away"])
    odds_1x2 = market_pick["odds_1x2"]

    if not odds_1x2:
        return {
            "status": "ok",
            "fixture": detail,
            "decision": "NO_BET",
            "message": "No complete 1X2 market found for this fixture",
            "markets_preview": extract_markets_preview(odds_response),
        }, 200

    market_implied_raw = {
        "Home": implied_probability(odds_1x2["Home"]),
        "Draw": implied_probability(odds_1x2["Draw"]),
        "Away": implied_probability(odds_1x2["Away"]),
    }
    market_implied_normalized = normalize_probabilities(market_implied_raw)

    model_data = build_model_probabilities(context)
    model_probs = model_data["normalized_probabilities"]
    edges = compute_edges(model_probs, market_implied_normalized)

    decision_data = build_value_decision_with_3_levels(
        detail=detail,
        odds_1x2=odds_1x2,
        model_probs=model_probs,
        market_probs=market_implied_normalized,
        edges=edges,
        context=context,
    )

    return {
        "status": "ok",
        "fixture": detail,
        "context": context,
        "bookmaker_name": market_pick["bookmaker_name"],
        "market_name": market_pick["bet_name"],
        "odds_1x2": odds_1x2,
        "market_implied_raw": market_implied_raw,
        "market_implied_normalized": market_implied_normalized,
        "model_probabilities": model_probs,
        "model_raw_scores": model_data["raw_scores"],
        "edges": edges,
        "allowed_edges": decision_data["allowed_edges"],
        "best_edge_label": decision_data["best_edge_label"],
        "best_edge_value": decision_data["best_edge_value"],
        "side": decision_data["side"],
        "level": decision_data["level"],
        "level_name": decision_data["level_name"],
        "decision": decision_data["decision"],
        "rationale": decision_data["rationale"],
        "confluence_flags": decision_data["confluence_flags"],
        "confluence_count": decision_data["confluence_count"],
    }, 200


def analyse_fixture_goals_core(fixture_id: str) -> Tuple[Dict[str, Any], int]:
    fixture_data, fixture_status = get_fixture_by_id(fixture_id)
    if fixture_status != 200:
        return fixture_data, fixture_status

    match = fixture_data["fixture"]
    detail = build_fixture_detail(match)

    if is_live_or_not_prematch(detail["status_short"]):
        return {
            "status": "ok",
            "fixture": detail,
            "decision": "NO_BET",
            "message": "Fixture is not pre-match anymore",
        }, 200

    standings_data, standings_status = call_api_football(
        "standings",
        {"league": detail["league_id"], "season": detail["season"]}
    )
    if standings_status != 200:
        return standings_data, standings_status

    standings_response = standings_data["data"].get("response", [])
    home_standing = find_team_standing(standings_response, detail["home_team_id"])
    away_standing = find_team_standing(standings_response, detail["away_team_id"])
    goals_context = build_goals_context(home_standing, away_standing)

    odds_data, odds_status = call_api_football("odds", {"fixture": fixture_id})
    if odds_status != 200:
        return odds_data, odds_status

    odds_response = odds_data["data"].get("response", [])
    if not odds_response:
        return {
            "status": "ok",
            "fixture": detail,
            "decision": "NO_BET",
            "message": "No odds found for this fixture",
        }, 200

    btts_market = find_market_values_any_bookmaker(
        odds_response,
        ["Both Teams Score", "Both Teams To Score"],
        accepted_labels=["Yes", "No"],
    )

    ou25_market = find_market_values_any_bookmaker(
        odds_response,
        ["Goals Over/Under", "Over/Under"],
        accepted_labels=["Over 2.5", "Under 2.5"],
    )

    decision_data = build_goals_value_decision_3_levels(goals_context, btts_market, ou25_market)

    return {
        "status": "ok",
        "fixture": detail,
        "goals_context": goals_context,
        "btts_market": btts_market,
        "over_under_2_5_market": ou25_market,
        "level": decision_data["level"],
        "level_name": decision_data["level_name"],
        "market": decision_data["market"],
        "confidence_count": decision_data["confidence_count"],
        "decision": decision_data["decision"],
        "rationale": decision_data["rationale"],
        "home_gf_avg": decision_data["home_gf_avg"],
        "away_gf_avg": decision_data["away_gf_avg"],
        "home_ga_avg": decision_data["home_ga_avg"],
        "away_ga_avg": decision_data["away_ga_avg"],
    }, 200


# =========================
# ROUTES
# =========================
@app.route("/")
def home():
    return ok({
        "status": "ok",
        "service": "Telegram-Apexfoot-bot",
        "version": "core-v4-3-levels",
        "time_utc": now_utc().isoformat(),
        "routes": [
            "/ping",
            "/fixture-value?fixture_id=...",
            "/fixture-goals-value?fixture_id=...",
            "/debug-fixture-value?fixture_id=...",
            "/fixtures-today",
            "/scan-value",
            "/scan-goals",
        ],
    })


@app.route("/ping")
def ping():
    return ok({
        "status": "ok",
        "message": "pong",
        "time_utc": now_utc().isoformat(),
    })


@app.route("/fixtures-today")
def fixtures_today():
    date_str = request.args.get("date", "").strip() or utc_today_str()
    data, status_code = get_fixtures_by_date(date_str)
    if status_code != 200:
        return ok(data, status_code)

    fixtures = data["data"].get("response", [])
    selected = []

    for match in fixtures:
        if not is_target_league_by_id(match):
            continue
        if not is_priority_fixture(match):
            continue
        if not is_pre_match_fixture(match):
            continue

        detail = build_fixture_detail(match)
        selected.append({
            "fixture_id": detail["fixture_id"],
            "kickoff_utc": detail["kickoff_utc"],
            "league_id": detail["league_id"],
            "league_name": detail["league_name"],
            "country": detail["country"],
            "home": detail["home"],
            "away": detail["away"],
        })

    selected.sort(key=lambda x: x["kickoff_utc"] or "")

    return ok({
        "status": "ok",
        "date": date_str,
        "count": len(selected),
        "fixtures": selected,
    })


@app.route("/fixture-value")
def fixture_value():
    fixture_id = request.args.get("fixture_id", "").strip()
    send_to_telegram = request.args.get("send_telegram", "1").strip() == "1"

    if not fixture_id:
        return err("Missing 'fixture_id' query parameter", 400)
    if not fixture_id.isdigit():
        return err("fixture_id must be numeric", 400)

    payload, status_code = analyse_fixture_value_core(fixture_id)

    if status_code == 200 and payload.get("status") == "ok" and send_to_telegram:
        if payload.get("decision") != "NO_BET":
            message = summarize_1x2_signal(payload["fixture"], payload, payload["odds_1x2"])
            telegram_data, telegram_status = send_telegram_message(message)
            payload["telegram_status"] = telegram_data
            payload["telegram_http_status"] = telegram_status

    return ok(payload, status_code)


@app.route("/fixture-goals-value")
def fixture_goals_value():
    fixture_id = request.args.get("fixture_id", "").strip()
    send_to_telegram = request.args.get("send_telegram", "1").strip() == "1"

    if not fixture_id:
        return err("Missing 'fixture_id' query parameter", 400)
    if not fixture_id.isdigit():
        return err("fixture_id must be numeric", 400)

    payload, status_code = analyse_fixture_goals_core(fixture_id)

    if status_code == 200 and payload.get("status") == "ok" and send_to_telegram:
        if payload.get("decision") != "NO_BET":
            message = summarize_goals_signal(payload["fixture"], payload)
            telegram_data, telegram_status = send_telegram_message(message)
            payload["telegram_status"] = telegram_data
            payload["telegram_http_status"] = telegram_status

    return ok(payload, status_code)


@app.route("/scan-value")
def scan_value():
    date_str = request.args.get("date", "").strip() or utc_today_str()
    min_level_raw = request.args.get("min_level", "2").strip()
    send_to_telegram = request.args.get("send_telegram", "0").strip() == "1"

    if not min_level_raw.isdigit():
        return err("min_level must be numeric", 400)

    min_level = int(min_level_raw)
    if min_level not in {1, 2, 3}:
        return err("min_level must be 1, 2 or 3", 400)

    data, status_code = get_fixtures_by_date(date_str)
    if status_code != 200:
        return ok(data, status_code)

    fixtures = data["data"].get("response", [])
    selected = []
    telegram_results = []

    for match in fixtures:
        if not is_target_league_by_id(match):
            continue
        if not is_priority_fixture(match):
            continue
        if not is_pre_match_fixture(match):
            continue

        detail = build_fixture_detail(match)
        payload, analyse_status = analyse_fixture_value_core(str(detail["fixture_id"]))
        if analyse_status != 200 or payload.get("status") != "ok":
            continue
        if payload.get("level", 0) < min_level:
            continue

        selected.append({
            "fixture_id": payload["fixture"]["fixture_id"],
            "kickoff_utc": payload["fixture"]["kickoff_utc"],
            "league_name": payload["fixture"]["league_name"],
            "country": payload["fixture"]["country"],
            "home": payload["fixture"]["home"],
            "away": payload["fixture"]["away"],
            "decision": payload["decision"],
            "level": payload["level"],
            "level_name": payload["level_name"],
            "side": payload["side"],
            "odd": payload["odds_1x2"].get(payload["side"]) if payload.get("odds_1x2") and payload.get("side") else None,
            "best_edge_value": payload["best_edge_value"],
            "confluence_count": payload["confluence_count"],
        })

        if send_to_telegram:
            message = summarize_1x2_signal(payload["fixture"], payload, payload["odds_1x2"])
            telegram_data, telegram_status = send_telegram_message(message)
            telegram_results.append({
                "fixture_id": payload["fixture"]["fixture_id"],
                "telegram_http_status": telegram_status,
                "telegram_status": telegram_data,
            })

        if len(selected) >= MAX_SCAN_RESULTS:
            break

    selected.sort(key=lambda x: (-(x["level"]), -(x["best_edge_value"] or 0), x["kickoff_utc"] or ""))

    return ok({
        "status": "ok",
        "date": date_str,
        "min_level": min_level,
        "count": len(selected),
        "signals": selected,
        "telegram_results": telegram_results,
    })


@app.route("/scan-goals")
def scan_goals():
    date_str = request.args.get("date", "").strip() or utc_today_str()
    min_level_raw = request.args.get("min_level", "2").strip()
    send_to_telegram = request.args.get("send_telegram", "0").strip() == "1"

    if not min_level_raw.isdigit():
        return err("min_level must be numeric", 400)

    min_level = int(min_level_raw)
    if min_level not in {1, 2, 3}:
        return err("min_level must be 1, 2 or 3", 400)

    data, status_code = get_fixtures_by_date(date_str)
    if status_code != 200:
        return ok(data, status_code)

    fixtures = data["data"].get("response", [])
    selected = []
    telegram_results = []

    for match in fixtures:
        if not is_target_league_by_id(match):
            continue
        if not is_priority_fixture(match):
            continue
        if not is_pre_match_fixture(match):
            continue

        detail = build_fixture_detail(match)
        payload, analyse_status = analyse_fixture_goals_core(str(detail["fixture_id"]))
        if analyse_status != 200 or payload.get("status") != "ok":
            continue
        if payload.get("level", 0) < min_level:
            continue

        selected.append({
            "fixture_id": payload["fixture"]["fixture_id"],
            "kickoff_utc": payload["fixture"]["kickoff_utc"],
            "league_name": payload["fixture"]["league_name"],
            "country": payload["fixture"]["country"],
            "home": payload["fixture"]["home"],
            "away": payload["fixture"]["away"],
            "decision": payload["decision"],
            "level": payload["level"],
            "level_name": payload["level_name"],
            "market": payload["market"],
            "confidence_count": payload["confidence_count"],
        })

        if send_to_telegram:
            message = summarize_goals_signal(payload["fixture"], payload)
            telegram_data, telegram_status = send_telegram_message(message)
            telegram_results.append({
                "fixture_id": payload["fixture"]["fixture_id"],
                "telegram_http_status": telegram_status,
                "telegram_status": telegram_data,
            })

        if len(selected) >= MAX_SCAN_RESULTS:
            break

    selected.sort(key=lambda x: (-(x["level"]), -(x["confidence_count"] or 0), x["kickoff_utc"] or ""))

    return ok({
        "status": "ok",
        "date": date_str,
        "min_level": min_level,
        "count": len(selected),
        "signals": selected,
        "telegram_results": telegram_results,
    })


@app.route("/debug-fixture-value")
def debug_fixture_value():
    fixture_id = request.args.get("fixture_id", "").strip()

    if not fixture_id:
        return err("Missing 'fixture_id' query parameter", 400)
    if not fixture_id.isdigit():
        return err("fixture_id must be numeric", 400)

    debug: Dict[str, Any] = {"fixture_id": fixture_id}

    fixture_data, fixture_status = get_fixture_by_id(fixture_id)
    debug["fixture_status"] = fixture_status
    debug["fixture_data_keys"] = list(fixture_data.keys())

    if fixture_status != 200:
        return ok({"status": "ok", "debug": debug}, 200)

    match = fixture_data["fixture"]
    detail = build_fixture_detail(match)
    debug["detail"] = detail

    standings_data, standings_status = call_api_football(
        "standings",
        {"league": detail["league_id"], "season": detail["season"]}
    )
    debug["standings_status"] = standings_status

    if standings_status == 200:
        standings_response = standings_data["data"].get("response", [])
        debug["standings_response_count"] = len(standings_response)
        debug["home_standing_found"] = find_team_standing(standings_response, detail["home_team_id"]) is not None
        debug["away_standing_found"] = find_team_standing(standings_response, detail["away_team_id"]) is not None
    else:
        debug["standings_error"] = standings_data

    odds_data, odds_status = call_api_football("odds", {"fixture": fixture_id})
    debug["odds_status"] = odds_status

    if odds_status == 200:
        odds_response = odds_data["data"].get("response", [])
        debug["odds_response_count"] = len(odds_response)
        debug["markets_preview"] = extract_markets_preview(odds_response)
        market_pick = pick_best_1x2_market(odds_response, detail["home"], detail["away"])
        debug["bookmaker_name"] = market_pick["bookmaker_name"]
        debug["market_name"] = market_pick["bet_name"]
        debug["odds_1x2"] = market_pick["odds_1x2"]
    else:
        debug["odds_error"] = odds_data

    return ok({
        "status": "ok",
        "debug": debug,
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
