
import os
import re
import json
import time
import math
import logging
import sqlite3
import threading
import unicodedata
from contextlib import closing
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

import requests
import schedule
from flask import Flask, jsonify, request
from werkzeug.exceptions import HTTPException

app = Flask(__name__)

# Init SQLite DB at startup
# (called after all functions are defined — see bottom of CONFIG block)

# ============================================================
# CONFIG
# ============================================================
BUILD_ID = "apex-hybrid-footystats-2026-04-08-02"

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
API_KEY = os.environ.get("API_KEY")
FOOTYSTATS_KEY = os.environ.get("FOOTYSTATS_KEY")
ODDS_API_KEY = os.environ.get("ODDS_API_KEY")
ODDS_API_BOOKMAKERS = os.environ.get("ODDS_API_BOOKMAKERS", "bet365,unibet")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")

DB_PATH = os.environ.get("DB_PATH", "/tmp/apex_signals.db")
AUTO_RESOLVE_ENABLED = os.environ.get("AUTO_RESOLVE_ENABLED", "1") == "1"
DEFAULT_STAKE = float(os.environ.get("DEFAULT_STAKE", "1.0"))
RESOLVE_BATCH_LIMIT = int(os.environ.get("RESOLVE_BATCH_LIMIT", "200"))

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("apexbot")

API_FOOTBALL_BASE_URL = "https://v3.football.api-sports.io"
FOOTYSTATS_BASE_URL = "https://api.football-data-api.com"
ODDS_API_BASE_URL = "https://api.the-odds-api.com/v4"

REQUEST_TIMEOUT = 20
CACHE_TTL_SECONDS = 600

EXCLUDED_KEYWORDS = [
    "youth", "u17", "u18", "u19", "u20", "u21", "u23",
    "women", "feminine", "female",
    "reserve", "reserves", "b team", "ii",
]

TARGET_LEAGUE_IDS = [
    39, 40, 41, 140, 78, 79, 135, 136, 61, 62, 2, 3, 848, 94, 95, 88, 89,
    203, 197, 207, 113, 119, 103, 106, 179, 218, 235, 72, 210, 328, 244, 164,
    128, 71, 239, 265, 262, 253, 233, 242, 343, 307, 301, 98, 292, 17, 188,
]

ELITE_LEAGUE_IDS = {2, 3, 848, 17}

LEVELS_1X2 = {
    1: {"name": "WATCHLIST", "edge_min": 0.03},
    2: {"name": "VALUE", "edge_min": 0.05},
    3: {"name": "MAIN", "edge_min": 0.08},
}
LEVELS_GOALS = {
    1: {"name": "WATCHLIST", "confidence_min": 2},
    2: {"name": "VALUE", "confidence_min": 3},
    3: {"name": "MAIN", "confidence_min": 5},
}

MIN_ODD_HOME_AWAY = 1.60
MIN_ODD_DRAW = 2.80
MAX_ODD_MAIN_SIGNAL = 4.50
SCAN_START_HOUR = 7   # UTC == heure locale Abidjan (GMT+0)
SCAN_END_HOUR = 23
MIN_SIGNAL_LEVEL_AUTO = 2  # Niveau minimum pour envoi auto Telegram

MAX_SCAN_RESULTS = 20

# teams that often create fake glamour-away value when the model is too naive
GLAMOUR_NAMES = {
    "liverpool", "real madrid", "barcelona", "bayern munich", "psg",
    "paris saint germain", "manchester city", "manchester united",
    "arsenal", "chelsea", "juventus", "inter", "ac milan",
}

_MEMORY_CACHE: Dict[str, Dict[str, Any]] = {}


# ============================================================
# GENERIC
# ============================================================
def ok(payload: Dict[str, Any], status_code: int = 200):
    return jsonify(payload), status_code


def err(message: str, status_code: int = 400, **kwargs):
    payload = {"status": "error", "message": message}
    payload.update(kwargs)
    return jsonify(payload), status_code


@app.errorhandler(HTTPException)
def handle_http_exception(e):
    return jsonify({
        "status": "error",
        "message": e.name,
        "details": e.description,
        "build_id": BUILD_ID,
    }), e.code


@app.errorhandler(Exception)
def handle_exception(e):
    return jsonify({
        "status": "error",
        "message": "Unhandled server exception",
        "details": str(e),
        "build_id": BUILD_ID,
    }), 500


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def utc_today_str() -> str:
    return now_utc().strftime("%Y-%m-%d")


def parse_iso_date(iso_date: Optional[str]) -> Optional[datetime]:
    if not iso_date:
        return None
    try:
        return datetime.fromisoformat(str(iso_date).replace("Z", "+00:00"))
    except Exception:
        return None


def format_match_time(iso_date: Optional[str]) -> Optional[str]:
    dt = parse_iso_date(iso_date)
    if not dt:
        return iso_date
    return dt.astimezone(timezone.utc).strftime("%H:%M UTC")


def maybe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    # Sentinel values used by API-Football for "N/A"
    if f in (-1.0, -2.0) or str(value).strip() in ("", "-1", "-2"):
        return None
    return f


def maybe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if f in (-1.0, -2.0) or str(value).strip() in ("", "-1", "-2"):
        return None
    return int(f)


def safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b in (None, 0):
        return None
    return a / b


def count_wins(form_string: Optional[str]) -> int:
    if not form_string:
        return 0
    return str(form_string).count("W")


def implied_probability(decimal_odd: Optional[Any]) -> Optional[float]:
    odd = maybe_float(decimal_odd)
    if odd is None or odd <= 0:
        return None
    return 1.0 / odd


def normalize_probabilities(prob_dict: Dict[str, Optional[float]]) -> Dict[str, Optional[float]]:
    valid_values = [v for v in prob_dict.values() if v is not None]
    total = sum(valid_values)
    if total <= 0:
        return {k: None for k in prob_dict}
    return {k: (v / total if v is not None else None) for k, v in prob_dict.items()}


def weighted_blend_probabilities(
    left: Dict[str, Optional[float]],
    right: Dict[str, Optional[float]],
    left_weight: float,
    right_weight: float,
) -> Dict[str, Optional[float]]:
    raw: Dict[str, Optional[float]] = {}
    for key in ["Home", "Draw", "Away"]:
        lv = left.get(key)
        rv = right.get(key)
        if lv is None and rv is None:
            raw[key] = None
        elif lv is None:
            raw[key] = rv
        elif rv is None:
            raw[key] = lv
        else:
            raw[key] = lv * left_weight + rv * right_weight
    return normalize_probabilities(raw)


def compute_edges(model_probs: Dict[str, Optional[float]], market_probs: Dict[str, Optional[float]]) -> Dict[str, Optional[float]]:
    result = {}
    for key in ["Home", "Draw", "Away"]:
        mp = model_probs.get(key)
        bp = market_probs.get(key)
        result[key] = None if mp is None or bp is None else mp - bp
    return result


def cache_get(key: str, ttl_seconds: int = CACHE_TTL_SECONDS) -> Optional[Any]:
    item = _MEMORY_CACHE.get(key)
    if not item:
        return None
    if time.time() - item["ts"] > ttl_seconds:
        _MEMORY_CACHE.pop(key, None)
        return None
    return item["data"]


def cache_set(key: str, data: Any):
    _MEMORY_CACHE[key] = {"ts": time.time(), "data": data}


def normalize_name(name: Optional[str]) -> str:
    text = unicodedata.normalize("NFKD", str(name or "")).encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    tokens = [
        t for t in text.split()
        if t not in {
            "fc", "cf", "sc", "afc", "ac", "club", "deportivo",
            "the", "futbol", "football", "soccer",
        }
    ]
    return " ".join(tokens).strip()


def team_name_similarity(left: Optional[str], right: Optional[str]) -> float:
    a = normalize_name(left)
    b = normalize_name(right)
    if not a or not b:
        return 0.0

    ratio = SequenceMatcher(None, a, b).ratio()
    ta = set(a.split())
    tb = set(b.split())
    token_score = len(ta & tb) / max(len(ta | tb), 1)

    if a == b:
        return 1.0
    return max(ratio, token_score)


def kickoff_similarity(api_date: Optional[str], unix_ts: Optional[Any]) -> float:
    dt = parse_iso_date(api_date)
    ts = maybe_float(unix_ts)
    if not dt or ts is None:
        return 0.0
    delta_minutes = abs(dt.timestamp() - ts) / 60.0
    if delta_minutes <= 5:
        return 1.0
    if delta_minutes <= 20:
        return 0.9
    if delta_minutes <= 60:
        return 0.7
    if delta_minutes <= 180:
        return 0.45
    return 0.0


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


# ============================================================
# TELEGRAM
# ============================================================
def send_telegram_message(text: str) -> Tuple[Dict[str, Any], int]:
    config = {
        "bot_token_present": bool(BOT_TOKEN),
        "chat_id_present": bool(CHAT_ID),
    }

    if not BOT_TOKEN:
        return {"status": "error", "message": "BOT_TOKEN is missing", "config": config}, 500
    if not CHAT_ID:
        return {"status": "error", "message": "CHAT_ID is missing", "config": config}, 500

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
            "config": config,
        }, 500

    if not response.ok or not data.get("ok"):
        return {
            "status": "error",
            "message": "Telegram API returned an error",
            "telegram_response": data,
            "config": config,
        }, 500

    return {"status": "ok", "telegram_response": data, "config": config}, 200


# ============================================================
# API-FOOTBALL
# ============================================================
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


def get_fixture_by_id(fixture_id: str) -> Tuple[Dict[str, Any], int]:
    data, status_code = call_api_football("fixtures", {"id": fixture_id})
    if status_code != 200:
        return data, status_code

    response = data["data"].get("response", [])
    if not response:
        return {"status": "error", "message": f"No fixture found for fixture_id={fixture_id}"}, 404

    return {"status": "ok", "fixture": response[0]}, 200


def get_fixtures_by_date(date_str: Optional[str] = None) -> Tuple[Dict[str, Any], int]:
    date_str = date_str or utc_today_str()
    return call_api_football("fixtures", {"date": date_str})


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


def get_api_context(detail: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
    standings_data, standings_status = call_api_football(
        "standings",
        {"league": detail["league_id"], "season": detail["season"]},
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
        "home_played": home_standing["played"] if home_standing else None,
        "away_played": away_standing["played"] if away_standing else None,
    }
    return {"status": "ok", "context": context}, 200


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
                if all(v is not None for v in extracted.values()):
                    return {
                        "bookmaker_name": bookmaker.get("name"),
                        "bet_name": bet.get("name"),
                        "odds_1x2": extracted,
                    }
    return {"bookmaker_name": None, "bet_name": None, "odds_1x2": None}


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


# ============================================================
# FOOTYSTATS
# ============================================================
def call_footystats(endpoint: str, params: Optional[Dict[str, Any]] = None, cache_key: Optional[str] = None) -> Tuple[Dict[str, Any], int]:
    if not FOOTYSTATS_KEY:
        return {"status": "error", "message": "FOOTYSTATS_KEY is missing"}, 500

    if cache_key:
        cached = cache_get(cache_key)
        if cached is not None:
            return {"status": "ok", "data": cached, "cached": True}, 200

    query = dict(params or {})
    query["key"] = FOOTYSTATS_KEY
    url = f"{FOOTYSTATS_BASE_URL}/{endpoint}"

    try:
        response = requests.get(url, params=query, timeout=REQUEST_TIMEOUT)
        data = response.json()
    except Exception as e:
        return {
            "status": "error",
            "message": "FootyStats request failed",
            "details": str(e),
        }, 500

    if not response.ok:
        return {
            "status": "error",
            "message": "FootyStats returned an HTTP error",
            "http_status": response.status_code,
            "api_response": data,
        }, 500

    if cache_key:
        cache_set(cache_key, data)

    return {"status": "ok", "data": data, "cached": False}, 200


def footystats_data_as_list(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = payload.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        if isinstance(data.get("matches"), list):
            return data["matches"]
        if isinstance(data.get("data"), list):
            return data["data"]
        return [data]
    return []


def get_footystats_matches_by_date(date_str: str) -> Tuple[Dict[str, Any], int]:
    cache_key = f"footystats:todays_matches:{date_str}"
    return call_footystats(
        "todays-matches",
        {"date": date_str, "timezone": "Etc/UTC"},
        cache_key=cache_key,
    )


def get_footystats_match_details(match_id: int) -> Tuple[Dict[str, Any], int]:
    cache_key = f"footystats:match:{match_id}"
    return call_footystats("match", {"match_id": match_id}, cache_key=cache_key)


def map_api_fixture_to_footystats(detail: Dict[str, Any], footy_matches: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    best: Optional[Dict[str, Any]] = None
    best_score = 0.0

    for item in footy_matches:
        home_score = team_name_similarity(detail["home"], item.get("home_name"))
        away_score = team_name_similarity(detail["away"], item.get("away_name"))
        time_score = kickoff_similarity(detail["date"], item.get("date_unix"))

        # hard rejection on obviously wrong team names
        if home_score < 0.60 or away_score < 0.60:
            continue

        score = home_score * 0.42 + away_score * 0.42 + time_score * 0.16
        if score > best_score:
            best = item
            best_score = score

    if best is None or best_score < 0.72:
        return None

    matched = dict(best)
    matched["_mapping_score"] = round(best_score, 4)
    return matched


def get_footystats_for_fixture(detail: Dict[str, Any], preloaded_matches: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    date_str = parse_iso_date(detail["date"]).strftime("%Y-%m-%d") if parse_iso_date(detail["date"]) else utc_today_str()

    matches_payload = None
    if preloaded_matches is None:
        matches_payload, matches_status = get_footystats_matches_by_date(date_str)
        if matches_status != 200:
            return {
                "enabled": bool(FOOTYSTATS_KEY),
                "match_found": False,
                "error": matches_payload,
            }
        preloaded_matches = footystats_data_as_list(matches_payload["data"])

    mapped = map_api_fixture_to_footystats(detail, preloaded_matches or [])
    if not mapped:
        return {
            "enabled": bool(FOOTYSTATS_KEY),
            "match_found": False,
            "date": date_str,
            "candidate_count": len(preloaded_matches or []),
        }

    detail_payload, detail_status = get_footystats_match_details(maybe_int(mapped.get("id")) or 0)
    if detail_status != 200:
        return {
            "enabled": bool(FOOTYSTATS_KEY),
            "match_found": True,
            "match_id": mapped.get("id"),
            "mapping_score": mapped.get("_mapping_score"),
            "summary": mapped,
            "error": detail_payload,
        }

    detail_data = detail_payload["data"].get("data") if isinstance(detail_payload["data"], dict) else detail_payload["data"]
    if isinstance(detail_data, list) and detail_data:
        detail_data = detail_data[0]

    return {
        "enabled": bool(FOOTYSTATS_KEY),
        "match_found": True,
        "match_id": mapped.get("id"),
        "mapping_score": mapped.get("_mapping_score"),
        "summary": mapped,
        "match": detail_data if isinstance(detail_data, dict) else mapped,
    }


def build_footystats_features(footy_payload: Dict[str, Any]) -> Dict[str, Any]:
    match = footy_payload.get("match") if footy_payload.get("match_found") else None
    if not isinstance(match, dict):
        return {
            "enabled": bool(FOOTYSTATS_KEY),
            "match_found": False,
        }

    features = {
        "match_id": maybe_int(match.get("id")),
        "mapping_score": footy_payload.get("mapping_score"),
        "home_name": match.get("home_name"),
        "away_name": match.get("away_name"),
        "home_ppg": maybe_float(match.get("home_ppg")),
        "away_ppg": maybe_float(match.get("away_ppg")),
        "pre_match_home_ppg": maybe_float(match.get("pre_match_home_ppg")),
        "pre_match_away_ppg": maybe_float(match.get("pre_match_away_ppg")),
        "team_a_xg_prematch": maybe_float(match.get("team_a_xg_prematch")),
        "team_b_xg_prematch": maybe_float(match.get("team_b_xg_prematch")),
        "total_xg_prematch": maybe_float(match.get("total_xg_prematch")),
        "btts_potential": maybe_float(match.get("btts_potential")),
        "o25_potential": maybe_float(match.get("o25_potential")),
        "u25_potential": maybe_float(match.get("u25_potential")),
        "avg_potential": maybe_float(match.get("avg_potential")),
        "home_adv_ppg": maybe_float(match.get("pre_match_home_ppg")),
        "away_adv_ppg": maybe_float(match.get("pre_match_away_ppg")),
        "odds_ft_1": maybe_float(match.get("odds_ft_1")),
        "odds_ft_x": maybe_float(match.get("odds_ft_x")),
        "odds_ft_2": maybe_float(match.get("odds_ft_2")),
        "odds_btts_yes": maybe_float(match.get("odds_btts_yes")),
        "odds_btts_no": maybe_float(match.get("odds_btts_no")),
        "odds_ft_over25": maybe_float(match.get("odds_ft_over25")),
        "odds_ft_under25": maybe_float(match.get("odds_ft_under25")),
        "no_home_away": maybe_int(match.get("no_home_away")),
        "competition_id": maybe_int(match.get("competition_id")),
    }
    features["match_found"] = True
    features["enabled"] = True
    return features


def build_fs_odds_1x2(fs: Dict[str, Any]) -> Optional[Dict[str, float]]:
    if not fs.get("match_found"):
        return None
    odds = {
        "Home": fs.get("odds_ft_1"),
        "Draw": fs.get("odds_ft_x"),
        "Away": fs.get("odds_ft_2"),
    }
    return odds if all(v is not None for v in odds.values()) else None


# ============================================================
# ODDS-API (The Odds API v4) — secondary odds source
# ============================================================

# Maps API-Football league_id → The Odds API sport key
LEAGUE_TO_ODDS_API_SPORT: Dict[int, str] = {
    39: "soccer_epl",
    40: "soccer_england_league1",
    41: "soccer_england_league2",
    140: "soccer_spain_la_liga",
    141: "soccer_spain_segunda_division",
    78: "soccer_germany_bundesliga",
    79: "soccer_germany_bundesliga2",
    135: "soccer_italy_serie_a",
    136: "soccer_italy_serie_b",
    61: "soccer_france_ligue_one",
    62: "soccer_france_ligue_two",
    2: "soccer_uefa_champs_league",
    3: "soccer_uefa_europa_league",
    848: "soccer_uefa_europa_conference_league",
    94: "soccer_portugal_primeira_liga",
    88: "soccer_netherlands_eredivisie",
    71: "soccer_brazil_campeonato",
    128: "soccer_argentina_primera_division",
}


def call_odds_api(sport_key: str, cache_key: Optional[str] = None) -> Tuple[Dict[str, Any], int]:
    """Fetch live/upcoming odds from The Odds API v4 for a given sport key."""
    if not ODDS_API_KEY:
        return {"status": "error", "message": "ODDS_API_KEY is missing"}, 500

    if cache_key:
        cached = cache_get(cache_key)
        if cached is not None:
            return {"status": "ok", "data": cached, "cached": True}, 200

    params = {
        "apiKey": ODDS_API_KEY,
        "regions": "eu",
        "markets": "h2h",
        "bookmakers": ODDS_API_BOOKMAKERS,
        "oddsFormat": "decimal",
    }
    url = f"{ODDS_API_BASE_URL}/sports/{sport_key}/odds/"

    try:
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        data = response.json()
    except Exception as e:
        return {
            "status": "error",
            "message": "OddsAPI request failed",
            "details": str(e),
        }, 500

    if not response.ok:
        return {
            "status": "error",
            "message": "OddsAPI returned an HTTP error",
            "http_status": response.status_code,
            "api_response": data,
        }, 500

    if cache_key:
        cache_set(cache_key, data)

    return {"status": "ok", "data": data, "cached": False}, 200


def _extract_odds_api_h2h(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract 1X2 odds from a single Odds API event object."""
    bookmakers_raw = event.get("bookmakers") or []
    for bm in bookmakers_raw:
        for market in (bm.get("markets") or []):
            if market.get("key") != "h2h":
                continue
            outcomes = market.get("outcomes") or []
            home_team = event.get("home_team", "")
            away_team = event.get("away_team", "")
            h = d = a = None
            for o in outcomes:
                name = o.get("name", "")
                price = maybe_float(o.get("price"))
                if name == home_team:
                    h = price
                elif name == away_team:
                    a = price
                elif name.lower() in {"draw", "x"}:
                    d = price
            if h is not None and d is not None and a is not None:
                return {
                    "bookmaker_name": bm.get("title"),
                    "odds_1x2": {"Home": h, "Draw": d, "Away": a},
                    "source": "odds_api",
                }
    return None


def get_odds_api_1x2(detail: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Try to find 1X2 odds for a fixture via The Odds API, matched by team name + time."""
    if not ODDS_API_KEY:
        return None

    league_id = detail.get("league_id")
    sport_key = LEAGUE_TO_ODDS_API_SPORT.get(league_id)  # type: ignore[arg-type]
    if not sport_key:
        return None  # league not mapped — skip silently

    cache_key = f"oddsapi:{sport_key}"
    payload, status = call_odds_api(sport_key, cache_key=cache_key)
    if status != 200 or not isinstance(payload.get("data"), list):
        return None

    events: List[Dict[str, Any]] = payload["data"]
    best_event: Optional[Dict[str, Any]] = None
    best_score = 0.0

    for ev in events:
        h_sim = team_name_similarity(detail.get("home"), ev.get("home_team"))
        a_sim = team_name_similarity(detail.get("away"), ev.get("away_team"))
        if h_sim < 0.60 or a_sim < 0.60:
            continue
        t_sim = kickoff_similarity(detail.get("date"), ev.get("commence_time"))
        score = h_sim * 0.42 + a_sim * 0.42 + t_sim * 0.16
        if score > best_score:
            best_score = score
            best_event = ev

    if best_event is None or best_score < 0.72:
        return None

    return _extract_odds_api_h2h(best_event)


# ============================================================
# ============================================================
def build_api_football_model(context: Dict[str, Any]) -> Dict[str, Any]:
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
    probs = {k: v / total for k, v in raw.items()}
    return {"raw_scores": raw, "probabilities": probs}


def build_footystats_model(fs: Dict[str, Any]) -> Dict[str, Any]:
    if not fs.get("match_found"):
        return {"raw_scores": {}, "probabilities": {"Home": None, "Draw": None, "Away": None}}

    home_ppg = fs.get("pre_match_home_ppg") or fs.get("home_ppg")
    away_ppg = fs.get("pre_match_away_ppg") or fs.get("away_ppg")
    home_xg = fs.get("team_a_xg_prematch")
    away_xg = fs.get("team_b_xg_prematch")
    no_home_away = fs.get("no_home_away") == 1

    home_score = 1.0
    draw_score = 0.82
    away_score = 1.0

    ppg_gap = (home_ppg or 0.0) - (away_ppg or 0.0)
    xg_gap = (home_xg or 0.0) - (away_xg or 0.0)

    if not no_home_away:
        home_score += 0.18

    if ppg_gap >= 0.8:
        home_score += 0.45
    elif ppg_gap >= 0.35:
        home_score += 0.22
    elif ppg_gap <= -0.8:
        away_score += 0.45
    elif ppg_gap <= -0.35:
        away_score += 0.22
    else:
        draw_score += 0.12

    if xg_gap >= 0.55:
        home_score += 0.30
    elif xg_gap >= 0.20:
        home_score += 0.15
    elif xg_gap <= -0.55:
        away_score += 0.30
    elif xg_gap <= -0.20:
        away_score += 0.15
    else:
        draw_score += 0.08

    if abs(ppg_gap) <= 0.15:
        draw_score += 0.10
    if abs(xg_gap) <= 0.10:
        draw_score += 0.08

    raw = {"Home": home_score, "Draw": draw_score, "Away": away_score}
    total = sum(raw.values())
    probs = {k: v / total for k, v in raw.items()}
    return {"raw_scores": raw, "probabilities": probs}


def build_confluence_flags(api_context: Dict[str, Any], fs: Dict[str, Any], side: str) -> Dict[str, bool]:
    flags = {
        "api_rank_advantage": False,
        "api_points_advantage": False,
        "api_form_advantage": False,
        "fs_ppg_advantage": False,
        "fs_xg_advantage": False,
        "fs_mapping_quality": bool(fs.get("mapping_score", 0) >= 0.80),
    }

    home_rank = api_context.get("home_rank")
    away_rank = api_context.get("away_rank")
    home_points = api_context.get("home_points")
    away_points = api_context.get("away_points")
    home_form = api_context.get("home_form") or ""
    away_form = api_context.get("away_form") or ""

    pre_home_ppg = fs.get("pre_match_home_ppg") or fs.get("home_ppg")
    pre_away_ppg = fs.get("pre_match_away_ppg") or fs.get("away_ppg")
    home_xg = fs.get("team_a_xg_prematch")
    away_xg = fs.get("team_b_xg_prematch")

    if side == "Home":
        if home_rank is not None and away_rank is not None and home_rank < away_rank:
            flags["api_rank_advantage"] = True
        if home_points is not None and away_points is not None and home_points > away_points:
            flags["api_points_advantage"] = True
        if count_wins(home_form) > count_wins(away_form):
            flags["api_form_advantage"] = True
        if pre_home_ppg is not None and pre_away_ppg is not None and pre_home_ppg > pre_away_ppg:
            flags["fs_ppg_advantage"] = True
        if home_xg is not None and away_xg is not None and home_xg > away_xg:
            flags["fs_xg_advantage"] = True

    elif side == "Away":
        if home_rank is not None and away_rank is not None and away_rank < home_rank:
            flags["api_rank_advantage"] = True
        if home_points is not None and away_points is not None and away_points > home_points:
            flags["api_points_advantage"] = True
        if count_wins(away_form) > count_wins(home_form):
            flags["api_form_advantage"] = True
        if pre_home_ppg is not None and pre_away_ppg is not None and pre_away_ppg > pre_home_ppg:
            flags["fs_ppg_advantage"] = True
        if home_xg is not None and away_xg is not None and away_xg > home_xg:
            flags["fs_xg_advantage"] = True

    else:
        if home_rank is not None and away_rank is not None and abs(home_rank - away_rank) <= 2:
            flags["api_rank_advantage"] = True
        if home_points is not None and away_points is not None and abs(home_points - away_points) <= 3:
            flags["api_points_advantage"] = True
        if abs(count_wins(home_form) - count_wins(away_form)) <= 1:
            flags["api_form_advantage"] = True
        if pre_home_ppg is not None and pre_away_ppg is not None and abs(pre_home_ppg - pre_away_ppg) <= 0.18:
            flags["fs_ppg_advantage"] = True
        if home_xg is not None and away_xg is not None and abs(home_xg - away_xg) <= 0.15:
            flags["fs_xg_advantage"] = True

    return flags


def filtered_edges_by_issue(odds_1x2: Dict[str, Any], edges: Dict[str, Optional[float]]) -> Dict[str, Optional[float]]:
    filtered = dict(edges)

    home_odd = maybe_float(odds_1x2.get("Home"))
    draw_odd = maybe_float(odds_1x2.get("Draw"))
    away_odd = maybe_float(odds_1x2.get("Away"))

    if home_odd is not None and home_odd < MIN_ODD_HOME_AWAY:
        filtered["Home"] = None
    if away_odd is not None and away_odd < MIN_ODD_HOME_AWAY:
        filtered["Away"] = None
    if draw_odd is not None and draw_odd < MIN_ODD_DRAW:
        filtered["Draw"] = None

    return filtered


def is_glamour_team(name: Optional[str]) -> bool:
    normalized = normalize_name(name)
    return normalized in {normalize_name(x) for x in GLAMOUR_NAMES}


def apply_contextual_penalties(
    detail: Dict[str, Any],
    fs: Dict[str, Any],
    side: str,
    raw_edge: float,
    selected_odd: Optional[float],
    confluence_count: int,
) -> Dict[str, Any]:
    penalties: List[Dict[str, Any]] = []
    flags: Dict[str, bool] = {}
    adjusted_edge = raw_edge
    level_cap = 3

    elite_match = detail["league_id"] in ELITE_LEAGUE_IDS
    flags["elite_match"] = elite_match
    if elite_match:
        adjusted_edge -= 0.02
        penalties.append({"reason": "elite_competition", "edge_penalty": 0.02})

    glamour_outsider = (
        elite_match
        and side in {"Away", "Home"}
        and selected_odd is not None
        and selected_odd >= 3.40
        and is_glamour_team(detail["away"] if side == "Away" else detail["home"])
    )
    flags["glamour_outsider"] = glamour_outsider
    if glamour_outsider:
        adjusted_edge -= 0.03
        penalties.append({"reason": "glamour_outsider", "edge_penalty": 0.03})
        level_cap = min(level_cap, 2)

    weak_mapping = fs.get("match_found") and (fs.get("mapping_score") or 0) < 0.78
    flags["weak_footystats_mapping"] = bool(weak_mapping)
    if weak_mapping:
        adjusted_edge -= 0.01
        penalties.append({"reason": "weak_footystats_mapping", "edge_penalty": 0.01})

    if elite_match and confluence_count < 4:
        level_cap = min(level_cap, 2)
        penalties.append({"reason": "elite_requires_4_confluence", "level_cap": 2})

    if selected_odd is not None and selected_odd > MAX_ODD_MAIN_SIGNAL:
        level_cap = min(level_cap, 2)
        penalties.append({"reason": "odd_too_high_for_main", "level_cap": 2})

    return {
        "adjusted_edge": adjusted_edge,
        "flags": flags,
        "penalties": penalties,
        "level_cap": level_cap,
    }


def decide_value_signal(
    detail: Dict[str, Any],
    odds_1x2: Dict[str, Any],
    api_context: Dict[str, Any],
    api_probs: Dict[str, Optional[float]],
    fs_probs: Dict[str, Optional[float]],
    hybrid_probs: Dict[str, Optional[float]],
    fs: Dict[str, Any],
) -> Dict[str, Any]:
    market_raw = {
        "Home": implied_probability(odds_1x2["Home"]),
        "Draw": implied_probability(odds_1x2["Draw"]),
        "Away": implied_probability(odds_1x2["Away"]),
    }
    market_probs = normalize_probabilities(market_raw)
    raw_edges = compute_edges(hybrid_probs, market_probs)
    allowed_edges = filtered_edges_by_issue(odds_1x2, raw_edges)
    candidates = {k: v for k, v in allowed_edges.items() if v is not None}

    if not candidates:
        return {
            "decision": "NO_BET",
            "side": None,
            "level": 0,
            "level_name": None,
            "rationale": ["Aucune issue autorisée après filtres de cotes minimales."],
            "market_implied_raw": market_raw,
            "market_implied_normalized": market_probs,
            "edges": raw_edges,
            "allowed_edges": allowed_edges,
            "best_edge_label": None,
            "raw_best_edge_value": None,
            "best_edge_value": None,
            "confluence_flags": {},
            "confluence_count": 0,
            "contextual_flags": {},
            "contextual_penalties": [],
        }

    best_side = max(candidates, key=candidates.get)
    raw_best_edge_value = candidates[best_side]
    confluence_flags = build_confluence_flags(api_context, fs, best_side)
    confluence_count = sum(1 for v in confluence_flags.values() if v)

    selected_odd = maybe_float(odds_1x2.get(best_side))
    penalty_payload = apply_contextual_penalties(
        detail=detail,
        fs=fs,
        side=best_side,
        raw_edge=raw_best_edge_value,
        selected_odd=selected_odd,
        confluence_count=confluence_count,
    )
    adjusted_edge = penalty_payload["adjusted_edge"]

    level = 0
    if adjusted_edge >= LEVELS_1X2[3]["edge_min"] and confluence_count >= 4:
        level = 3
    elif adjusted_edge >= LEVELS_1X2[2]["edge_min"] and confluence_count >= 3:
        level = 2
    elif adjusted_edge >= LEVELS_1X2[1]["edge_min"] and confluence_count >= 2:
        level = 1

    level = min(level, penalty_payload["level_cap"])

    if level == 0:
        return {
            "decision": "NO_BET",
            "side": best_side,
            "level": 0,
            "level_name": None,
            "rationale": [
                f"Edge brut = {round(raw_best_edge_value * 100, 2)}%.",
                f"Edge ajusté = {round(adjusted_edge * 100, 2)}%.",
                "Le signal ne survit pas aux garde-fous contextuels.",
            ],
            "market_implied_raw": market_raw,
            "market_implied_normalized": market_probs,
            "edges": raw_edges,
            "allowed_edges": allowed_edges,
            "best_edge_label": best_side,
            "raw_best_edge_value": raw_best_edge_value,
            "best_edge_value": adjusted_edge,
            "confluence_flags": confluence_flags,
            "confluence_count": confluence_count,
            "contextual_flags": penalty_payload["flags"],
            "contextual_penalties": penalty_payload["penalties"],
        }

    label_map = {
        1: {"Home": "WATCH_HOME", "Draw": "WATCH_DRAW", "Away": "WATCH_AWAY"},
        2: {"Home": "VALUE_HOME", "Draw": "VALUE_DRAW", "Away": "VALUE_AWAY"},
        3: {"Home": "MAIN_HOME", "Draw": "MAIN_DRAW", "Away": "MAIN_AWAY"},
    }

    rationale = [
        f"Best allowed raw edge sur {best_side} = {round(raw_best_edge_value * 100, 2)}%.",
        f"Edge ajusté = {round(adjusted_edge * 100, 2)}%.",
        f"Confluence = {confluence_count}/6.",
    ]
    if fs.get("match_found"):
        rationale.append("FootyStats intégré au moteur (PPG + xG prématch + potentials).")
    if penalty_payload["penalties"]:
        reasons = ", ".join(p["reason"] for p in penalty_payload["penalties"])
        rationale.append(f"Pénalités contextuelles appliquées: {reasons}.")

    return {
        "decision": label_map[level][best_side],
        "side": best_side,
        "level": level,
        "level_name": LEVELS_1X2[level]["name"],
        "rationale": rationale,
        "market_implied_raw": market_raw,
        "market_implied_normalized": market_probs,
        "edges": raw_edges,
        "allowed_edges": allowed_edges,
        "best_edge_label": best_side,
        "raw_best_edge_value": raw_best_edge_value,
        "best_edge_value": adjusted_edge,
        "confluence_flags": confluence_flags,
        "confluence_count": confluence_count,
        "contextual_flags": penalty_payload["flags"],
        "contextual_penalties": penalty_payload["penalties"],
    }


def decide_goals_signal(detail: Dict[str, Any], api_context: Dict[str, Any], fs: Dict[str, Any]) -> Dict[str, Any]:
    home_gf_avg = safe_div(api_context.get("home_goals_for"), api_context.get("home_played"))
    away_gf_avg = safe_div(api_context.get("away_goals_for"), api_context.get("away_played"))
    home_ga_avg = safe_div(api_context.get("home_goals_against"), api_context.get("home_played"))
    away_ga_avg = safe_div(api_context.get("away_goals_against"), api_context.get("away_played"))

    options: List[Dict[str, Any]] = []

    btts_potential = fs.get("btts_potential")
    o25_potential = fs.get("o25_potential")
    u25_potential = fs.get("u25_potential")
    avg_potential = fs.get("avg_potential")
    total_xg = fs.get("total_xg_prematch")
    home_xg = fs.get("team_a_xg_prematch")
    away_xg = fs.get("team_b_xg_prematch")
    home_ppg = fs.get("pre_match_home_ppg") or fs.get("home_ppg")
    away_ppg = fs.get("pre_match_away_ppg") or fs.get("away_ppg")

    # BTTS YES
    conf_yes = 0
    yes_reasons = []
    if btts_potential is not None and btts_potential >= 62:
        conf_yes += 2
        yes_reasons.append(f"BTTS potential {btts_potential}")
    if total_xg is not None and total_xg >= 2.70:
        conf_yes += 1
        yes_reasons.append(f"Total xG prematch {total_xg}")
    if home_xg is not None and away_xg is not None and home_xg >= 1.05 and away_xg >= 1.00:
        conf_yes += 1
        yes_reasons.append("Les deux équipes dépassent ~1.0 xG prématch")
    if avg_potential is not None and avg_potential >= 2.7:
        conf_yes += 1
        yes_reasons.append(f"Avg potential {avg_potential}")
    if home_ga_avg is not None and away_ga_avg is not None and home_ga_avg >= 1.0 and away_ga_avg >= 1.0:
        conf_yes += 1
        yes_reasons.append("Les deux profils encaissent assez")

    # BTTS NO
    conf_no = 0
    no_reasons = []
    if btts_potential is not None and btts_potential <= 48:
        conf_no += 2
        no_reasons.append(f"BTTS potential bas {btts_potential}")
    if home_xg is not None and away_xg is not None and (home_xg <= 0.85 or away_xg <= 0.85):
        conf_no += 1
        no_reasons.append("Une équipe sous 0.85 xG prématch")
    if avg_potential is not None and avg_potential <= 2.35:
        conf_no += 1
        no_reasons.append(f"Avg potential bas {avg_potential}")
    if u25_potential is not None and u25_potential >= 58:
        conf_no += 1
        no_reasons.append(f"Under 2.5 potential {u25_potential}")
    if home_ppg is not None and away_ppg is not None and abs((home_ppg or 0) - (away_ppg or 0)) >= 0.70:
        conf_no += 1
        no_reasons.append("Mismatch PPG: un camp peut gagner sans encaisser")

    # OVER 2.5
    conf_over = 0
    over_reasons = []
    if o25_potential is not None and o25_potential >= 60:
        conf_over += 2
        over_reasons.append(f"Over 2.5 potential {o25_potential}")
    if total_xg is not None and total_xg >= 2.8:
        conf_over += 1
        over_reasons.append(f"Total xG prematch {total_xg}")
    if home_xg is not None and away_xg is not None and home_xg >= 1.15 and away_xg >= 0.95:
        conf_over += 1
        over_reasons.append("xG prématch combiné cohérent avec over")
    if avg_potential is not None and avg_potential >= 2.85:
        conf_over += 1
        over_reasons.append(f"Avg potential {avg_potential}")
    if home_gf_avg is not None and away_gf_avg is not None and home_gf_avg + away_gf_avg >= 2.7:
        conf_over += 1
        over_reasons.append("GF moyens combinés élevés")

    # UNDER 2.5
    conf_under = 0
    under_reasons = []
    if u25_potential is not None and u25_potential >= 60:
        conf_under += 2
        under_reasons.append(f"Under 2.5 potential {u25_potential}")
    if total_xg is not None and total_xg <= 2.35:
        conf_under += 1
        under_reasons.append(f"Total xG prematch {total_xg}")
    if avg_potential is not None and avg_potential <= 2.4:
        conf_under += 1
        under_reasons.append(f"Avg potential bas {avg_potential}")
    if home_gf_avg is not None and away_gf_avg is not None and home_gf_avg + away_gf_avg <= 2.3:
        conf_under += 1
        under_reasons.append("GF moyens combinés modestes")

    big_favorite_home = (
        home_ppg is not None and away_ppg is not None and (home_ppg - away_ppg) >= 1.0
        and home_xg is not None and home_xg >= 1.8
    )
    if big_favorite_home:
        conf_under -= 2
        under_reasons.append("Pénalité anti faux under: gros favori offensif")

    options.append({"market": "BTTS_YES", "confidence": conf_yes, "reasons": yes_reasons})
    options.append({"market": "BTTS_NO", "confidence": conf_no, "reasons": no_reasons})
    options.append({"market": "OVER_2_5", "confidence": conf_over, "reasons": over_reasons})
    options.append({"market": "UNDER_2_5", "confidence": conf_under, "reasons": under_reasons})

    best = max(options, key=lambda x: x["confidence"])
    confidence_count = max(best["confidence"], 0)

    level = 0
    if confidence_count >= LEVELS_GOALS[3]["confidence_min"]:
        level = 3
    elif confidence_count >= LEVELS_GOALS[2]["confidence_min"]:
        level = 2
    elif confidence_count >= LEVELS_GOALS[1]["confidence_min"]:
        level = 1

    if level == 0:
        return {
            "decision": "NO_BET",
            "level": 0,
            "level_name": None,
            "market": None,
            "confidence_count": confidence_count,
            "rationale": ["Aucun marché buts ne présente assez de confluence."],
            "home_gf_avg": home_gf_avg,
            "away_gf_avg": away_gf_avg,
            "home_ga_avg": home_ga_avg,
            "away_ga_avg": away_ga_avg,
            "footystats_features_used": {
                "btts_potential": btts_potential,
                "o25_potential": o25_potential,
                "u25_potential": u25_potential,
                "avg_potential": avg_potential,
                "total_xg_prematch": total_xg,
            },
        }

    decision_prefix = {1: "WATCH", 2: "VALUE", 3: "MAIN"}[level]
    decision = f"{decision_prefix}_{best['market']}"
    rationale = [
        f"Marché buts retenu = {best['market']}.",
        f"Confiance = {confidence_count}.",
        "FootyStats intégré au moteur (potentials + xG prématch + PPG).",
    ]
    if best["reasons"]:
        rationale.append("Signaux: " + " | ".join(best["reasons"]))

    return {
        "decision": decision,
        "level": level,
        "level_name": LEVELS_GOALS[level]["name"],
        "market": best["market"],
        "confidence_count": confidence_count,
        "rationale": rationale,
        "home_gf_avg": home_gf_avg,
        "away_gf_avg": away_gf_avg,
        "home_ga_avg": home_ga_avg,
        "away_ga_avg": away_ga_avg,
        "footystats_features_used": {
            "btts_potential": btts_potential,
            "o25_potential": o25_potential,
            "u25_potential": u25_potential,
            "avg_potential": avg_potential,
            "total_xg_prematch": total_xg,
            "team_a_xg_prematch": home_xg,
            "team_b_xg_prematch": away_xg,
            "pre_match_home_ppg": home_ppg,
            "pre_match_away_ppg": away_ppg,
        },
    }


# ============================================================
# CORE ANALYSIS
# ============================================================
def analyse_fixture_value_core(fixture_id: str, preloaded_footy_matches: Optional[List[Dict[str, Any]]] = None) -> Tuple[Dict[str, Any], int]:
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

    api_context_payload, api_context_status = get_api_context(detail)
    if api_context_status != 200:
        return api_context_payload, api_context_status

    api_context = api_context_payload["context"]
    api_model = build_api_football_model(api_context)

    odds_data, odds_status = call_api_football("odds", {"fixture": fixture_id})
    if odds_status != 200:
        return odds_data, odds_status

    odds_response = odds_data["data"].get("response", [])
    market_pick = pick_best_1x2_market(odds_response, detail["home"], detail["away"])
    odds_1x2 = market_pick["odds_1x2"]

    footy_payload = get_footystats_for_fixture(detail, preloaded_footy_matches)
    fs = build_footystats_features(footy_payload)
    fs_model = build_footystats_model(fs)

    fs_odds_1x2 = build_fs_odds_1x2(fs)
    if not odds_1x2 and fs_odds_1x2:
        odds_1x2 = fs_odds_1x2
        market_pick = {
            "bookmaker_name": "FootyStats",
            "bet_name": "1x2",
            "odds_1x2": fs_odds_1x2,
        }

    # Fallback 2 — The Odds API (Bet365 / Unibet)
    if not odds_1x2:
        odds_api_result = get_odds_api_1x2(detail)
        if odds_api_result:
            odds_1x2 = odds_api_result["odds_1x2"]
            market_pick = {
                "bookmaker_name": odds_api_result.get("bookmaker_name", "OddsAPI"),
                "bet_name": "h2h",
                "odds_1x2": odds_1x2,
            }

    if not odds_1x2:
        return {
            "status": "ok",
            "fixture": detail,
            "decision": "NO_BET",
            "message": "No complete 1X2 market found for this fixture",
            "footystats": footy_payload,
        }, 200

    hybrid_probs = weighted_blend_probabilities(
        api_model["probabilities"],
        fs_model["probabilities"],
        left_weight=0.55,
        right_weight=0.45,
    )

    decision_data = decide_value_signal(
        detail=detail,
        odds_1x2=odds_1x2,
        api_context=api_context,
        api_probs=api_model["probabilities"],
        fs_probs=fs_model["probabilities"],
        hybrid_probs=hybrid_probs,
        fs=fs,
    )

    return {
        "status": "ok",
        "build_id": BUILD_ID,
        "fixture": detail,
        "context": api_context,
        "bookmaker_name": market_pick["bookmaker_name"],
        "market_name": market_pick["bet_name"],
        "odds_1x2": odds_1x2,
        "api_model_probabilities": api_model["probabilities"],
        "api_model_raw_scores": api_model["raw_scores"],
        "footystats_model_probabilities": fs_model["probabilities"],
        "footystats_model_raw_scores": fs_model["raw_scores"],
        "hybrid_model_probabilities": hybrid_probs,
        "market_implied_raw": decision_data["market_implied_raw"],
        "market_implied_normalized": decision_data["market_implied_normalized"],
        "edges": decision_data["edges"],
        "allowed_edges": decision_data["allowed_edges"],
        "best_edge_label": decision_data["best_edge_label"],
        "raw_best_edge_value": decision_data["raw_best_edge_value"],
        "best_edge_value": decision_data["best_edge_value"],
        "side": decision_data["side"],
        "level": decision_data["level"],
        "level_name": decision_data["level_name"],
        "decision": decision_data["decision"],
        "rationale": decision_data["rationale"],
        "confluence_flags": decision_data["confluence_flags"],
        "confluence_count": decision_data["confluence_count"],
        "contextual_flags": decision_data["contextual_flags"],
        "contextual_penalties": decision_data["contextual_penalties"],
        "footystats": {
            "enabled": bool(FOOTYSTATS_KEY),
            "match_found": fs.get("match_found", False),
            "match_id": fs.get("match_id"),
            "mapping_score": fs.get("mapping_score"),
            "features": fs,
        },
    }, 200


def analyse_fixture_goals_core(fixture_id: str, preloaded_footy_matches: Optional[List[Dict[str, Any]]] = None) -> Tuple[Dict[str, Any], int]:
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

    api_context_payload, api_context_status = get_api_context(detail)
    if api_context_status != 200:
        return api_context_payload, api_context_status

    api_context = api_context_payload["context"]
    footy_payload = get_footystats_for_fixture(detail, preloaded_footy_matches)
    fs = build_footystats_features(footy_payload)

    decision_data = decide_goals_signal(detail, api_context, fs)

    return {
        "status": "ok",
        "build_id": BUILD_ID,
        "fixture": detail,
        "goals_context": api_context,
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
        "footystats": {
            "enabled": bool(FOOTYSTATS_KEY),
            "match_found": fs.get("match_found", False),
            "match_id": fs.get("match_id"),
            "mapping_score": fs.get("mapping_score"),
            "features": decision_data["footystats_features_used"],
        },
    }, 200


# ============================================================
# FORMATTERS
# ============================================================
def summarize_1x2_signal(detail: Dict[str, Any], analysis: Dict[str, Any], odds_1x2: Dict[str, Any]) -> str:
    side = analysis.get("side")
    return (
        "APEXFOOTBALL 1X2 HYBRID\n\n"
        f"{detail['home']} vs {detail['away']}\n"
        f"{detail['league_name']} ({detail['country']})\n"
        f"{format_match_time(detail['date'])}\n\n"
        f"Decision: {analysis['decision']}\n"
        f"Level: {analysis.get('level')} - {analysis.get('level_name')}\n"
        f"Side: {side}\n"
        f"Odd: {odds_1x2.get(side) if side else 'N/A'}\n"
        f"Raw edge: {round(analysis['raw_best_edge_value'] * 100, 2) if analysis.get('raw_best_edge_value') is not None else 'N/A'}%\n"
        f"Adjusted edge: {round(analysis['best_edge_value'] * 100, 2) if analysis.get('best_edge_value') is not None else 'N/A'}%\n"
        f"Confluence: {analysis.get('confluence_count', 0)}/6\n"
        f"FootyStats match: {'YES' if analysis.get('footystats', {}).get('match_found') else 'NO'}\n"
        f"Rationale: {' | '.join(analysis.get('rationale', []))}"
    )


def summarize_goals_signal(detail: Dict[str, Any], analysis: Dict[str, Any]) -> str:
    return (
        "APEXFOOTBALL GOALS HYBRID\n\n"
        f"{detail['home']} vs {detail['away']}\n"
        f"{detail['league_name']} ({detail['country']})\n"
        f"{format_match_time(detail['date'])}\n\n"
        f"Decision: {analysis['decision']}\n"
        f"Level: {analysis.get('level')} - {analysis.get('level_name')}\n"
        f"Market: {analysis.get('market')}\n"
        f"Confidence: {analysis.get('confidence_count', 0)}\n"
        f"FootyStats match: {'YES' if analysis.get('footystats', {}).get('match_found') else 'NO'}\n"
        f"Rationale: {' | '.join(analysis.get('rationale', []))}"
    )


# ============================================================
# SQLITE / JOURNALISATION / BACKTEST
# ============================================================
def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with closing(db_connect()) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_uid TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            build_id TEXT NOT NULL,
            fixture_id INTEGER NOT NULL,
            match_date TEXT,
            kickoff_utc TEXT,
            league_id INTEGER,
            league_name TEXT,
            country TEXT,
            home_team TEXT,
            away_team TEXT,
            market TEXT,
            side TEXT,
            decision TEXT NOT NULL,
            level INTEGER NOT NULL,
            level_name TEXT,
            odd REAL,
            raw_edge REAL,
            adjusted_edge REAL,
            confluence_count INTEGER,
            confidence_count INTEGER,
            rationale TEXT,
            contextual_flags TEXT,
            contextual_penalties TEXT,
            telegram_sent INTEGER DEFAULT 0,
            telegram_http_status INTEGER,
            telegram_message_id TEXT,
            result_status TEXT DEFAULT 'pending',
            match_status TEXT,
            home_goals INTEGER,
            away_goals INTEGER,
            bet_outcome TEXT,
            stake REAL DEFAULT 1.0,
            profit REAL DEFAULT 0.0,
            resolved_at TEXT
        )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_fixture_id ON signals(fixture_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_created_at ON signals(created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_result_status ON signals(result_status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_level ON signals(level)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_market ON signals(market)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_build_id ON signals(build_id)")
        conn.commit()


def json_dumps_safe(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return json.dumps(str(value), ensure_ascii=False)


def extract_telegram_message_id(telegram_data: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(telegram_data, dict):
        return None
    try:
        return str(
            telegram_data.get("telegram_response", {})
            .get("result", {})
            .get("message_id")
        )
    except Exception:
        return None


def build_signal_uid(
    fixture_id: Any,
    market: Optional[str],
    side: Optional[str],
    decision: Optional[str],
    level: Optional[int],
    kickoff_utc: Optional[str],
) -> str:
    match_day = str(kickoff_utc or "")[:10]
    return "|".join([
        str(BUILD_ID),
        str(fixture_id),
        str(match_day),
        str(market or ""),
        str(side or ""),
        str(decision or ""),
        str(level or 0),
    ])


def infer_market_from_decision(decision: Optional[str]) -> Optional[str]:
    if not decision:
        return None
    d = str(decision).upper()
    if d.endswith("_HOME") or d.endswith("_DRAW") or d.endswith("_AWAY"):
        return "1X2"
    for market in ["BTTS_YES", "BTTS_NO", "OVER_2_5", "UNDER_2_5"]:
        if market in d:
            return market
    return None


def pick_goals_logged_odd(analysis: Dict[str, Any], footystats: Optional[Dict[str, Any]] = None) -> Optional[float]:
    market = analysis.get("market")
    if not market:
        return None
    fs = footystats or analysis.get("footystats_features_used") or {}
    if not isinstance(fs, dict):
        return None
    if market == "BTTS_YES":
        return maybe_float(fs.get("odds_btts_yes"))
    if market == "BTTS_NO":
        return maybe_float(fs.get("odds_btts_no"))
    if market == "OVER_2_5":
        return maybe_float(fs.get("odds_ft_over25"))
    if market == "UNDER_2_5":
        return maybe_float(fs.get("odds_ft_under25"))
    return None


def save_signal_record(record: Dict[str, Any]) -> Dict[str, Any]:
    required = ["signal_uid", "created_at", "build_id", "fixture_id", "decision", "level"]
    for key in required:
        if record.get(key) is None:
            raise ValueError(f"Missing required record field: {key}")
    with closing(db_connect()) as conn:
        conn.execute("""
        INSERT OR IGNORE INTO signals (
            signal_uid, created_at, build_id, fixture_id, match_date, kickoff_utc,
            league_id, league_name, country, home_team, away_team,
            market, side, decision, level, level_name,
            odd, raw_edge, adjusted_edge, confluence_count, confidence_count,
            rationale, contextual_flags, contextual_penalties,
            telegram_sent, telegram_http_status, telegram_message_id, stake
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            record.get("signal_uid"), record.get("created_at"), record.get("build_id"),
            record.get("fixture_id"), record.get("match_date"), record.get("kickoff_utc"),
            record.get("league_id"), record.get("league_name"), record.get("country"),
            record.get("home_team"), record.get("away_team"),
            record.get("market"), record.get("side"), record.get("decision"),
            record.get("level"), record.get("level_name"),
            record.get("odd"), record.get("raw_edge"), record.get("adjusted_edge"),
            record.get("confluence_count"), record.get("confidence_count"),
            record.get("rationale"), record.get("contextual_flags"), record.get("contextual_penalties"),
            1 if record.get("telegram_sent") else 0,
            record.get("telegram_http_status"), record.get("telegram_message_id"),
            record.get("stake", DEFAULT_STAKE),
        ))
        conn.commit()
        row = conn.execute(
            "SELECT id, signal_uid FROM signals WHERE signal_uid = ?",
            (record["signal_uid"],)
        ).fetchone()
    return {
        "status": "ok",
        "signal_uid": record["signal_uid"],
        "row_id": int(row["id"]) if row else None,
    }


def log_1x2_signal(
    detail: Dict[str, Any],
    analysis: Dict[str, Any],
    odds_1x2: Dict[str, Any],
    telegram_data: Optional[Dict[str, Any]] = None,
    telegram_http_status: Optional[int] = None,
) -> Dict[str, Any]:
    market = "1X2"
    side = analysis.get("side")
    odd = maybe_float(odds_1x2.get(side)) if side else None
    record = {
        "signal_uid": build_signal_uid(
            detail.get("fixture_id"), market, side,
            analysis.get("decision"), analysis.get("level"),
            detail.get("kickoff_utc") or detail.get("date"),
        ),
        "created_at": now_utc().isoformat(),
        "build_id": BUILD_ID,
        "fixture_id": maybe_int(detail.get("fixture_id")),
        "match_date": str(detail.get("date") or "")[:10],
        "kickoff_utc": detail.get("kickoff_utc") or detail.get("date"),
        "league_id": maybe_int(detail.get("league_id")),
        "league_name": detail.get("league_name"),
        "country": detail.get("country"),
        "home_team": detail.get("home"),
        "away_team": detail.get("away"),
        "market": market,
        "side": side,
        "decision": analysis.get("decision"),
        "level": maybe_int(analysis.get("level")) or 0,
        "level_name": analysis.get("level_name"),
        "odd": odd,
        "raw_edge": maybe_float(analysis.get("raw_best_edge_value")),
        "adjusted_edge": maybe_float(analysis.get("best_edge_value")),
        "confluence_count": maybe_int(analysis.get("confluence_count")),
        "confidence_count": None,
        "rationale": json_dumps_safe(analysis.get("rationale", [])),
        "contextual_flags": json_dumps_safe(analysis.get("contextual_flags", {})),
        "contextual_penalties": json_dumps_safe(analysis.get("contextual_penalties", [])),
        "telegram_sent": telegram_http_status == 200,
        "telegram_http_status": telegram_http_status,
        "telegram_message_id": extract_telegram_message_id(telegram_data),
        "stake": DEFAULT_STAKE,
    }
    return save_signal_record(record)


def log_goals_signal(
    detail: Dict[str, Any],
    analysis: Dict[str, Any],
    telegram_data: Optional[Dict[str, Any]] = None,
    telegram_http_status: Optional[int] = None,
) -> Dict[str, Any]:
    market = analysis.get("market") or infer_market_from_decision(analysis.get("decision"))
    odd = pick_goals_logged_odd(analysis)
    record = {
        "signal_uid": build_signal_uid(
            detail.get("fixture_id"), market, None,
            analysis.get("decision"), analysis.get("level"),
            detail.get("kickoff_utc") or detail.get("date"),
        ),
        "created_at": now_utc().isoformat(),
        "build_id": BUILD_ID,
        "fixture_id": maybe_int(detail.get("fixture_id")),
        "match_date": str(detail.get("date") or "")[:10],
        "kickoff_utc": detail.get("kickoff_utc") or detail.get("date"),
        "league_id": maybe_int(detail.get("league_id")),
        "league_name": detail.get("league_name"),
        "country": detail.get("country"),
        "home_team": detail.get("home"),
        "away_team": detail.get("away"),
        "market": market,
        "side": None,
        "decision": analysis.get("decision"),
        "level": maybe_int(analysis.get("level")) or 0,
        "level_name": analysis.get("level_name"),
        "odd": odd,
        "raw_edge": None,
        "adjusted_edge": None,
        "confluence_count": None,
        "confidence_count": maybe_int(analysis.get("confidence_count")),
        "rationale": json_dumps_safe(analysis.get("rationale", [])),
        "contextual_flags": json_dumps_safe({}),
        "contextual_penalties": json_dumps_safe([]),
        "telegram_sent": telegram_http_status == 200,
        "telegram_http_status": telegram_http_status,
        "telegram_message_id": extract_telegram_message_id(telegram_data),
        "stake": DEFAULT_STAKE,
    }
    return save_signal_record(record)


# ============================================================
# RESOLUTION / RESULTATS / PROFIT
# ============================================================
FINAL_STATUSES = {"FT", "AET", "PEN"}
VOID_STATUSES = {"CANC", "PST", "ABD", "AWD", "WO"}


def compute_bet_outcome(
    market: Optional[str],
    side: Optional[str],
    home_goals: Optional[int],
    away_goals: Optional[int],
    match_status: Optional[str],
) -> str:
    if match_status in VOID_STATUSES:
        return "void"
    if home_goals is None or away_goals is None:
        return "pending"
    total_goals = home_goals + away_goals
    if market == "1X2":
        if side == "Home":
            return "win" if home_goals > away_goals else "loss"
        if side == "Draw":
            return "win" if home_goals == away_goals else "loss"
        if side == "Away":
            return "win" if away_goals > home_goals else "loss"
        return "loss"
    if market == "BTTS_YES":
        return "win" if home_goals > 0 and away_goals > 0 else "loss"
    if market == "BTTS_NO":
        return "win" if home_goals == 0 or away_goals == 0 else "loss"
    if market == "OVER_2_5":
        return "win" if total_goals >= 3 else "loss"
    if market == "UNDER_2_5":
        return "win" if total_goals <= 2 else "loss"
    return "loss"


def compute_profit(outcome: str, odd: Optional[float], stake: float) -> float:
    if outcome == "win":
        if odd is None or odd <= 1:
            return 0.0
        return round((odd - 1.0) * stake, 4)
    if outcome == "loss":
        return round(-stake, 4)
    return 0.0


def resolve_fixture_signals(fixture_id: int) -> Dict[str, Any]:
    fixture_data, fixture_status = get_fixture_by_id(str(fixture_id))
    if fixture_status != 200:
        return {"status": "error", "fixture_id": fixture_id, "details": fixture_data}

    match = fixture_data["fixture"]
    status_short = match.get("fixture", {}).get("status", {}).get("short")
    home_goals = maybe_int(match.get("goals", {}).get("home"))
    away_goals = maybe_int(match.get("goals", {}).get("away"))

    if status_short not in FINAL_STATUSES and status_short not in VOID_STATUSES:
        return {"status": "pending", "fixture_id": fixture_id, "match_status": status_short}

    with closing(db_connect()) as conn:
        rows = conn.execute("""
            SELECT * FROM signals
            WHERE fixture_id = ? AND result_status = 'pending'
        """, (fixture_id,)).fetchall()

        resolved_count = 0
        for row in rows:
            outcome = compute_bet_outcome(
                market=row["market"], side=row["side"],
                home_goals=home_goals, away_goals=away_goals,
                match_status=status_short,
            )
            profit = compute_profit(
                outcome=outcome,
                odd=maybe_float(row["odd"]),
                stake=maybe_float(row["stake"]) or DEFAULT_STAKE,
            )
            conn.execute("""
                UPDATE signals
                SET result_status = 'resolved', match_status = ?,
                    home_goals = ?, away_goals = ?,
                    bet_outcome = ?, profit = ?, resolved_at = ?
                WHERE id = ?
            """, (status_short, home_goals, away_goals, outcome, profit, now_utc().isoformat(), row["id"]))
            resolved_count += 1
        conn.commit()

    return {
        "status": "ok",
        "fixture_id": fixture_id,
        "match_status": status_short,
        "home_goals": home_goals,
        "away_goals": away_goals,
        "resolved_count": resolved_count,
    }


def resolve_pending_signals(limit: int = RESOLVE_BATCH_LIMIT) -> Dict[str, Any]:
    with closing(db_connect()) as conn:
        rows = conn.execute("""
            SELECT DISTINCT fixture_id FROM signals
            WHERE result_status = 'pending'
            ORDER BY created_at ASC LIMIT ?
        """, (limit,)).fetchall()

    fixture_ids = [int(r["fixture_id"]) for r in rows]
    results = []
    total_resolved = 0

    for fixture_id in fixture_ids:
        try:
            result = resolve_fixture_signals(fixture_id)
            results.append(result)
            if result.get("status") == "ok":
                total_resolved += int(result.get("resolved_count", 0))
        except Exception as exc:
            logger.exception("resolve_fixture_signals failed for fixture_id=%s", fixture_id)
            results.append({"status": "error", "fixture_id": fixture_id, "details": str(exc)})

    return {
        "status": "ok",
        "checked_fixtures": len(fixture_ids),
        "resolved_signals": total_resolved,
        "results": results,
    }


# ============================================================
# SCHEDULER — scan toutes les heures de 07h00 à 23h00 UTC
# ============================================================
def run_full_scan_job():
    """Lance automatiquement le scan 1X2 + Buts/BTTS si dans la plage horaire."""
    hour = now_utc().hour
    if not (SCAN_START_HOUR <= hour <= SCAN_END_HOUR):
        return  # Hors plage, on dort

    date_str = utc_today_str()
    timestamp = now_utc().strftime("%H:%M UTC")

    fixtures_data, fixtures_status = get_fixtures_by_date(date_str)
    if fixtures_status != 200:
        send_telegram_message(f"⚠️ Scan {timestamp} — Impossible de récupérer les matchs.")
        return

    fixtures = fixtures_data["data"].get("response", [])
    footy_matches = None
    if FOOTYSTATS_KEY:
        footy_payload, footy_status = get_footystats_matches_by_date(date_str)
        if footy_status == 200:
            footy_matches = footystats_data_as_list(footy_payload["data"])

    signals_sent = 0

    for match in fixtures:
        if not is_target_league_by_id(match):
            continue
        if not is_priority_fixture(match):
            continue
        if not is_pre_match_fixture(match):
            continue

        detail = build_fixture_detail(match)
        fixture_id = str(detail["fixture_id"])

        # --- 1X2 ---
        try:
            a1x2, s1x2 = analyse_fixture_value_core(fixture_id, preloaded_footy_matches=footy_matches)
            if s1x2 == 200 and a1x2.get("decision") != "NO_BET" and (a1x2.get("level") or 0) >= MIN_SIGNAL_LEVEL_AUTO:
                tg_data, tg_status = send_telegram_message(
                    summarize_1x2_signal(a1x2["fixture"], a1x2, a1x2["odds_1x2"])
                )
                log_1x2_signal(
                    detail=a1x2["fixture"],
                    analysis=a1x2,
                    odds_1x2=a1x2["odds_1x2"],
                    telegram_data=tg_data,
                    telegram_http_status=tg_status,
                )
                signals_sent += 1
        except Exception:
            logger.exception("run_full_scan_job 1X2 failed for fixture_id=%s", fixture_id)

        # --- GOALS / BTTS ---
        try:
            agoals, sgoals = analyse_fixture_goals_core(fixture_id, preloaded_footy_matches=footy_matches)
            if sgoals == 200 and agoals.get("decision") != "NO_BET" and (agoals.get("level") or 0) >= MIN_SIGNAL_LEVEL_AUTO:
                tg_data, tg_status = send_telegram_message(
                    summarize_goals_signal(agoals["fixture"], agoals)
                )
                log_goals_signal(
                    detail=agoals["fixture"],
                    analysis=agoals,
                    telegram_data=tg_data,
                    telegram_http_status=tg_status,
                )
                signals_sent += 1
        except Exception:
            logger.exception("run_full_scan_job GOALS failed for fixture_id=%s", fixture_id)

        if signals_sent >= MAX_SCAN_RESULTS:
            break

    if signals_sent == 0:
        send_telegram_message(
            f"🔍 Scan automatique {date_str} {timestamp}\n"
            f"Aucun signal VALUE/MAIN détecté sur les ligues surveillées."
        )


def resolve_pending_signals_job():
    if not AUTO_RESOLVE_ENABLED:
        return
    try:
        result = resolve_pending_signals(limit=RESOLVE_BATCH_LIMIT)
        logger.info(
            "resolve_pending_signals_job done | checked=%s | resolved=%s",
            result.get("checked_fixtures"),
            result.get("resolved_signals"),
        )
    except Exception:
        logger.exception("resolve_pending_signals_job failed")


def _scheduler_loop():
    schedule.every().hour.at(":00").do(run_full_scan_job)
    schedule.every(30).minutes.do(resolve_pending_signals_job)
    while True:
        schedule.run_pending()
        time.sleep(30)


# Lancement du scheduler en thread daemon
threading.Thread(target=_scheduler_loop, daemon=True, name="ApexScheduler").start()

# Init DB au démarrage (gunicorn + dev)
init_db()
logger.info("DB initialized at %s", DB_PATH)


# ============================================================
# TELEGRAM WEBHOOK
# ============================================================
def set_telegram_webhook(url: str) -> Tuple[Dict[str, Any], int]:
    if not BOT_TOKEN:
        return {"status": "error", "message": "BOT_TOKEN is missing"}, 500
    params: Dict[str, Any] = {"url": url}
    if WEBHOOK_SECRET:
        params["secret_token"] = WEBHOOK_SECRET
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook",
            json=params,
            timeout=15,
        )
        return {"status": "ok", "telegram_response": r.json()}, 200
    except Exception as exc:
        return {"status": "error", "details": str(exc)}, 500


def _handle_telegram_command(cmd: str) -> None:
    """Traite une commande Telegram reçue via webhook."""
    if cmd == "/scan":
        send_telegram_message(
            "🔍 Scan lancé manuellement...\n"
            "Analyse 1X2 + Buts + BTTS en cours. Résultats dans quelques instants."
        )
        threading.Thread(target=run_full_scan_job, daemon=True).start()

    elif cmd == "/status":
        send_telegram_message(
            f"✅ APEX-SIRIUS BOT ACTIF\n\n"
            f"🔧 Build: {BUILD_ID}\n"
            f"🕐 Heure UTC: {now_utc().strftime('%Y-%m-%d %H:%M')}\n\n"
            f"API-Football : {'✅' if API_KEY else '❌ MANQUANT'}\n"
            f"FootyStats   : {'✅' if FOOTYSTATS_KEY else '❌ MANQUANT'}\n"
            f"Odds API     : {'✅' if ODDS_API_KEY else '❌ MANQUANT'}\n"
            f"Telegram     : ✅\n\n"
            f"⏰ Scheduler : toutes les heures, {SCAN_START_HOUR}h00–{SCAN_END_HOUR}h00 UTC\n"
            f"📚 Bookmakers : {ODDS_API_BOOKMAKERS}"
        )

    elif cmd == "/ping":
        send_telegram_message(f"🏓 Pong! Bot actif — {now_utc().strftime('%H:%M')} UTC")

    elif cmd == "/help":
        send_telegram_message(
            "📋 Commandes disponibles :\n\n"
            "/scan — Lance un scan complet (1X2 + Buts + BTTS)\n"
            "/status — État du bot et des APIs\n"
            "/ping — Test de connexion\n"
            "/help — Cette aide\n\n"
            f"⏰ Scan automatique toutes les heures de {SCAN_START_HOUR}h00 à {SCAN_END_HOUR}h00 UTC"
        )


@app.route("/webhook", methods=["POST"])
def telegram_webhook():
    # Vérification du secret header si configuré
    if WEBHOOK_SECRET:
        incoming = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if incoming != WEBHOOK_SECRET:
            return jsonify({"status": "forbidden"}), 403

    update = request.get_json(silent=True) or {}
    message = update.get("message") or update.get("channel_post") or {}
    text = (message.get("text") or "").strip()
    chat_id = str(message.get("chat", {}).get("id") or "")

    if not text or not chat_id:
        return ok({"status": "ok", "ignored": True})

    # Supprime la mention @bot si présente
    cmd = text.split()[0].lower().split("@")[0]
    _handle_telegram_command(cmd)

    return ok({"status": "ok"})


@app.route("/set-webhook")
def set_webhook_route():
    """Active le webhook Telegram — appelle cette route une fois après déploiement."""
    webhook_url = request.args.get("url", "").strip()
    if not webhook_url:
        # Auto-détection de l'URL publique
        webhook_url = request.url_root.rstrip("/") + "/webhook"
    payload, status = set_telegram_webhook(webhook_url)
    return ok({
        "status": "ok" if status == 200 else "error",
        "webhook_url": webhook_url,
        "result": payload,
    }, status)


# ============================================================
# ROUTES
# ============================================================
@app.route("/")
def home():
    return ok({
        "status": "ok",
        "build_id": BUILD_ID,
        "routes": [
            "/", "/version", "/ping", "/telegram-test",
            "/footystats-test", "/odds-api-test?sport=soccer_epl",
            "/set-webhook", "/webhook (POST)",
            "/fixtures-today", "/fixtures-prematch-ready",
            "/fixture-value?fixture_id=...",
            "/fixture-goals-value?fixture_id=...",
            "/scan-value?date=YYYY-MM-DD&min_level=2&send_telegram=1",
            "/scan-goals?date=YYYY-MM-DD&min_level=2&send_telegram=1",
            "/debug-fixture-value?fixture_id=...",
        ],
        "config": {
            "bot_token_present": bool(BOT_TOKEN),
            "chat_id_present": bool(CHAT_ID),
            "api_key_present": bool(API_KEY),
            "footystats_key_present": bool(FOOTYSTATS_KEY),
            "odds_api_key_present": bool(ODDS_API_KEY),
            "odds_api_bookmakers": ODDS_API_BOOKMAKERS,
        },
    })


@app.route("/version")
def version():
    return ok({
        "status": "ok",
        "build_id": BUILD_ID,
        "config": {
            "api_key_present": bool(API_KEY),
            "footystats_key_present": bool(FOOTYSTATS_KEY),
            "odds_api_key_present": bool(ODDS_API_KEY),
            "odds_api_bookmakers": ODDS_API_BOOKMAKERS,
            "bot_token_present": bool(BOT_TOKEN),
            "chat_id_present": bool(CHAT_ID),
        },
    })


@app.route("/ping")
def ping():
    return ok({
        "status": "ok",
        "message": "pong",
        "utc_now": now_utc().isoformat(),
        "build_id": BUILD_ID,
    })


@app.route("/telegram-test")
def telegram_test():
    payload, status_code = send_telegram_message(f"Test Telegram Apexfoot OK - {now_utc().isoformat()} | build={BUILD_ID}")
    return ok({
        "status": "ok" if status_code == 200 else "error",
        "message_sent": status_code == 200,
        "telegram_http_status": status_code,
        "telegram_status": payload,
        "build_id": BUILD_ID,
    }, 200 if status_code == 200 else 500)


@app.route("/footystats-test")
def footystats_test():
    date_str = request.args.get("date", utc_today_str()).strip()
    payload, status = get_footystats_matches_by_date(date_str)
    if status != 200:
        return ok({
            "status": "error",
            "build_id": BUILD_ID,
            "footystats_key_present": bool(FOOTYSTATS_KEY),
            "footystats_status": payload,
        }, 500)

    matches = footystats_data_as_list(payload["data"])
    sample = matches[0] if matches else None
    return ok({
        "status": "ok",
        "build_id": BUILD_ID,
        "footystats_key_present": bool(FOOTYSTATS_KEY),
        "date": date_str,
        "count": len(matches),
        "sample": {
            "id": sample.get("id") if isinstance(sample, dict) else None,
            "home_name": sample.get("home_name") if isinstance(sample, dict) else None,
            "away_name": sample.get("away_name") if isinstance(sample, dict) else None,
            "date_unix": sample.get("date_unix") if isinstance(sample, dict) else None,
            "competition_id": sample.get("competition_id") if isinstance(sample, dict) else None,
        } if sample else None,
    })


@app.route("/odds-api-test")
def odds_api_test():
    if not ODDS_API_KEY:
        return ok({
            "status": "error",
            "message": "ODDS_API_KEY is missing",
            "build_id": BUILD_ID,
        }, 500)
    sport_key = request.args.get("sport", "soccer_epl").strip()
    payload, status = call_odds_api(sport_key)
    if status != 200:
        return ok({
            "status": "error",
            "build_id": BUILD_ID,
            "odds_api_key_present": bool(ODDS_API_KEY),
            "odds_api_status": payload,
        }, 500)
    events = payload.get("data") or []
    sample = events[0] if events else None
    return ok({
        "status": "ok",
        "build_id": BUILD_ID,
        "odds_api_key_present": bool(ODDS_API_KEY),
        "odds_api_bookmakers": ODDS_API_BOOKMAKERS,
        "sport_key": sport_key,
        "count": len(events),
        "sample": {
            "id": sample.get("id") if isinstance(sample, dict) else None,
            "home_team": sample.get("home_team") if isinstance(sample, dict) else None,
            "away_team": sample.get("away_team") if isinstance(sample, dict) else None,
            "commence_time": sample.get("commence_time") if isinstance(sample, dict) else None,
        } if sample else None,
    })


@app.route("/fixtures-today")
def fixtures_today():
    date_str = request.args.get("date", utc_today_str()).strip()
    data, status_code = get_fixtures_by_date(date_str)
    if status_code != 200:
        return ok(data, status_code)

    fixtures = data["data"].get("response", [])
    filtered = []
    for match in fixtures:
        if not is_target_league_by_id(match):
            continue
        if not is_priority_fixture(match):
            continue
        if not is_pre_match_fixture(match):
            continue
        detail = build_fixture_detail(match)
        filtered.append({
            "fixture_id": detail["fixture_id"],
            "kickoff_utc": detail["kickoff_utc"],
            "league_id": detail["league_id"],
            "league_name": detail["league_name"],
            "country": detail["country"],
            "home": detail["home"],
            "away": detail["away"],
        })

    filtered.sort(key=lambda x: x["kickoff_utc"] or "")
    return ok({
        "status": "ok",
        "build_id": BUILD_ID,
        "date": date_str,
        "count": len(filtered),
        "fixtures": filtered,
    })


@app.route("/fixtures-prematch-ready")
def fixtures_prematch_ready():
    return fixtures_today()


@app.route("/fixture-value")
def fixture_value():
    fixture_id = request.args.get("fixture_id", "").strip()
    if not fixture_id:
        return err("Missing 'fixture_id' query parameter", 400)
    if not fixture_id.isdigit():
        return err("fixture_id must be numeric", 400)

    analysis, status = analyse_fixture_value_core(fixture_id)
    if status != 200:
        return ok(analysis, status)

    send_telegram = request.args.get("send_telegram", "0").strip() == "1"
    telegram_status = None
    telegram_http_status = None

    if send_telegram and analysis.get("decision") != "NO_BET":
        telegram_status, telegram_http_status = send_telegram_message(
            summarize_1x2_signal(analysis["fixture"], analysis, analysis["odds_1x2"])
        )

    analysis["telegram_status"] = telegram_status
    analysis["telegram_http_status"] = telegram_http_status
    return ok(analysis, 200)


@app.route("/fixture-goals-value")
def fixture_goals_value():
    fixture_id = request.args.get("fixture_id", "").strip()
    if not fixture_id:
        return err("Missing 'fixture_id' query parameter", 400)
    if not fixture_id.isdigit():
        return err("fixture_id must be numeric", 400)

    analysis, status = analyse_fixture_goals_core(fixture_id)
    if status != 200:
        return ok(analysis, status)

    send_telegram = request.args.get("send_telegram", "0").strip() == "1"
    telegram_status = None
    telegram_http_status = None

    if send_telegram and analysis.get("decision") != "NO_BET":
        telegram_status, telegram_http_status = send_telegram_message(
            summarize_goals_signal(analysis["fixture"], analysis)
        )

    analysis["telegram_status"] = telegram_status
    analysis["telegram_http_status"] = telegram_http_status
    return ok(analysis, 200)


@app.route("/scan-value")
def scan_value():
    date_str = request.args.get("date", utc_today_str()).strip()
    min_level = maybe_int(request.args.get("min_level", 2)) or 2
    send_telegram = request.args.get("send_telegram", "1").strip() != "0"

    fixtures_data, fixtures_status = get_fixtures_by_date(date_str)
    if fixtures_status != 200:
        return ok(fixtures_data, fixtures_status)

    fixtures = fixtures_data["data"].get("response", [])
    footy_matches = None
    if FOOTYSTATS_KEY:
        footy_payload, footy_status = get_footystats_matches_by_date(date_str)
        if footy_status == 200:
            footy_matches = footystats_data_as_list(footy_payload["data"])

    signals = []
    telegram_results = []

    for match in fixtures:
        if not is_target_league_by_id(match):
            continue
        if not is_priority_fixture(match):
            continue
        if not is_pre_match_fixture(match):
            continue

        detail = build_fixture_detail(match)
        analysis, status = analyse_fixture_value_core(str(detail["fixture_id"]), preloaded_footy_matches=footy_matches)
        if status != 200 or analysis.get("decision") == "NO_BET":
            continue
        if (analysis.get("level") or 0) < min_level:
            continue

        signal = {
            "fixture_id": detail["fixture_id"],
            "kickoff_utc": detail["kickoff_utc"],
            "league_name": detail["league_name"],
            "country": detail["country"],
            "home": detail["home"],
            "away": detail["away"],
            "decision": analysis["decision"],
            "side": analysis["side"],
            "odd": analysis["odds_1x2"].get(analysis["side"]) if analysis.get("side") else None,
            "level": analysis["level"],
            "level_name": analysis["level_name"],
            "best_edge_value": analysis["best_edge_value"],
            "raw_best_edge_value": analysis["raw_best_edge_value"],
            "confluence_count": analysis["confluence_count"],
            "contextual_flags": analysis["contextual_flags"],
            "contextual_penalties": analysis["contextual_penalties"],
            "footystats_match_found": analysis["footystats"]["match_found"],
        }
        signals.append(signal)

        if send_telegram:
            tg_payload, tg_status = send_telegram_message(
                summarize_1x2_signal(analysis["fixture"], analysis, analysis["odds_1x2"])
            )
            telegram_results.append({
                "fixture_id": detail["fixture_id"],
                "telegram_http_status": tg_status,
                "telegram_status": tg_payload,
            })

        if len(signals) >= MAX_SCAN_RESULTS:
            break

    signals.sort(key=lambda x: (x["level"], x["best_edge_value"] or -999), reverse=True)

    if not signals and send_telegram:
        telegram_results.append({
            "status": "info",
            "message": "Aucun signal à envoyer pour ce scan.",
            "config": {
                "bot_token_present": bool(BOT_TOKEN),
                "chat_id_present": bool(CHAT_ID),
            },
        })

    return ok({
        "status": "ok",
        "build_id": BUILD_ID,
        "date": date_str,
        "min_level": min_level,
        "count": len(signals),
        "signals": signals,
        "telegram_enabled": send_telegram,
        "telegram_config": {
            "bot_token_present": bool(BOT_TOKEN),
            "chat_id_present": bool(CHAT_ID),
        },
        "telegram_results": telegram_results,
        "footystats_key_present": bool(FOOTYSTATS_KEY),
    })


@app.route("/scan-goals")
def scan_goals():
    date_str = request.args.get("date", utc_today_str()).strip()
    min_level = maybe_int(request.args.get("min_level", 2)) or 2
    send_telegram = request.args.get("send_telegram", "1").strip() != "0"

    fixtures_data, fixtures_status = get_fixtures_by_date(date_str)
    if fixtures_status != 200:
        return ok(fixtures_data, fixtures_status)

    fixtures = fixtures_data["data"].get("response", [])
    footy_matches = None
    if FOOTYSTATS_KEY:
        footy_payload, footy_status = get_footystats_matches_by_date(date_str)
        if footy_status == 200:
            footy_matches = footystats_data_as_list(footy_payload["data"])

    signals = []
    telegram_results = []

    for match in fixtures:
        if not is_target_league_by_id(match):
            continue
        if not is_priority_fixture(match):
            continue
        if not is_pre_match_fixture(match):
            continue

        detail = build_fixture_detail(match)
        analysis, status = analyse_fixture_goals_core(str(detail["fixture_id"]), preloaded_footy_matches=footy_matches)
        if status != 200 or analysis.get("decision") == "NO_BET":
            continue
        if (analysis.get("level") or 0) < min_level:
            continue

        signal = {
            "fixture_id": detail["fixture_id"],
            "kickoff_utc": detail["kickoff_utc"],
            "league_name": detail["league_name"],
            "country": detail["country"],
            "home": detail["home"],
            "away": detail["away"],
            "decision": analysis["decision"],
            "market": analysis["market"],
            "level": analysis["level"],
            "level_name": analysis["level_name"],
            "confidence_count": analysis["confidence_count"],
            "footystats_match_found": analysis["footystats"]["match_found"],
        }
        signals.append(signal)

        if send_telegram:
            tg_payload, tg_status = send_telegram_message(summarize_goals_signal(analysis["fixture"], analysis))
            telegram_results.append({
                "fixture_id": detail["fixture_id"],
                "telegram_http_status": tg_status,
                "telegram_status": tg_payload,
            })

        if len(signals) >= MAX_SCAN_RESULTS:
            break

    signals.sort(key=lambda x: (x["level"], x["confidence_count"]), reverse=True)

    if not signals and send_telegram:
        telegram_results.append({
            "status": "info",
            "message": "Aucun signal à envoyer pour ce scan.",
            "config": {
                "bot_token_present": bool(BOT_TOKEN),
                "chat_id_present": bool(CHAT_ID),
            },
        })

    return ok({
        "status": "ok",
        "build_id": BUILD_ID,
        "date": date_str,
        "min_level": min_level,
        "count": len(signals),
        "signals": signals,
        "telegram_enabled": send_telegram,
        "telegram_config": {
            "bot_token_present": bool(BOT_TOKEN),
            "chat_id_present": bool(CHAT_ID),
        },
        "telegram_results": telegram_results,
        "footystats_key_present": bool(FOOTYSTATS_KEY),
    })


@app.route("/debug-fixture-value")
def debug_fixture_value():
    fixture_id = request.args.get("fixture_id", "").strip()
    if not fixture_id:
        return err("Missing 'fixture_id' query parameter", 400)
    if not fixture_id.isdigit():
        return err("fixture_id must be numeric", 400)

    fixture_payload, fixture_status = get_fixture_by_id(fixture_id)
    if fixture_status != 200:
        return ok({
            "status": "error",
            "build_id": BUILD_ID,
            "fixture_lookup": fixture_payload,
        }, fixture_status)

    detail = build_fixture_detail(fixture_payload["fixture"])
    footy_payload = get_footystats_for_fixture(detail)
    footy_features = build_footystats_features(footy_payload)

    return ok({
        "status": "ok",
        "build_id": BUILD_ID,
        "fixture": detail,
        "footystats": footy_payload,
        "footystats_features": footy_features,
    })


# ============================================================
# ROUTES SQLITE / BACKTEST
# ============================================================
@app.route("/resolve-pending-signals")
def resolve_pending_signals_route():
    limit = maybe_int(request.args.get("limit")) or RESOLVE_BATCH_LIMIT
    result = resolve_pending_signals(limit=limit)
    return ok(result, 200)


@app.route("/signals-summary")
def signals_summary():
    with closing(db_connect()) as conn:
        overall = conn.execute("""
            SELECT
                COUNT(*) AS total_bets,
                SUM(CASE WHEN bet_outcome = 'win' THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN bet_outcome = 'loss' THEN 1 ELSE 0 END) AS losses,
                SUM(CASE WHEN bet_outcome = 'void' THEN 1 ELSE 0 END) AS voids,
                ROUND(COALESCE(SUM(profit), 0), 4) AS profit_total,
                ROUND(COALESCE(SUM(stake), 0), 4) AS stake_total,
                ROUND(
                    CASE WHEN COALESCE(SUM(stake), 0) > 0
                    THEN (SUM(profit) / SUM(stake)) * 100
                    ELSE 0 END, 2
                ) AS roi_percent
            FROM signals WHERE result_status = 'resolved'
        """).fetchone()

        by_market = conn.execute("""
            SELECT market,
                COUNT(*) AS total_bets,
                SUM(CASE WHEN bet_outcome = 'win' THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN bet_outcome = 'loss' THEN 1 ELSE 0 END) AS losses,
                ROUND(COALESCE(SUM(profit), 0), 4) AS profit_total,
                ROUND(
                    CASE WHEN COALESCE(SUM(stake), 0) > 0
                    THEN (SUM(profit) / SUM(stake)) * 100
                    ELSE 0 END, 2
                ) AS roi_percent
            FROM signals WHERE result_status = 'resolved'
            GROUP BY market ORDER BY profit_total DESC
        """).fetchall()

        by_level = conn.execute("""
            SELECT level, level_name,
                COUNT(*) AS total_bets,
                SUM(CASE WHEN bet_outcome = 'win' THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN bet_outcome = 'loss' THEN 1 ELSE 0 END) AS losses,
                ROUND(COALESCE(SUM(profit), 0), 4) AS profit_total,
                ROUND(
                    CASE WHEN COALESCE(SUM(stake), 0) > 0
                    THEN (SUM(profit) / SUM(stake)) * 100
                    ELSE 0 END, 2
                ) AS roi_percent
            FROM signals WHERE result_status = 'resolved'
            GROUP BY level, level_name ORDER BY level DESC
        """).fetchall()

    return ok({
        "status": "ok",
        "build_id": BUILD_ID,
        "overall": dict(overall) if overall else {},
        "by_market": [dict(r) for r in by_market],
        "by_level": [dict(r) for r in by_level],
    }, 200)


@app.route("/signals-recent")
def signals_recent():
    limit = maybe_int(request.args.get("limit")) or 20
    with closing(db_connect()) as conn:
        rows = conn.execute("""
            SELECT * FROM signals ORDER BY created_at DESC LIMIT ?
        """, (limit,)).fetchall()
    return ok({
        "status": "ok",
        "count": len(rows),
        "signals": [dict(r) for r in rows],
    }, 200)


if __name__ == "__main__":
    init_db()
    print(f"🚀 BUILD_ID={BUILD_ID}")
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
