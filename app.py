"""
APEX-HYBRID-ULTIMATE v2.0
Fusion de :
  - app.py existant (prod-safe: HTTP retry, SCAN_LOCK, webhook, journalisation, résolution)
  - Nouveau moteur (Dixon-Coles, LEAGUE_CONFIG, Kelly, tiers, smart stats, dual mode BET/SIGNAL)
"""

import os
import re
import json
import time
import math
import logging
import sqlite3
import hashlib
import threading
import unicodedata
from contextlib import closing
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

import requests
import anthropic
import schedule
from flask import Flask, jsonify, request
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from werkzeug.exceptions import HTTPException

app = Flask(__name__)

# ============================================================
# CONFIG — Environnement
# ============================================================
BUILD_ID = "apex-hybrid-ultimate-v2.0"

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID   = os.environ.get("CHAT_ID")
API_KEY   = os.environ.get("API_KEY")
FOOTYSTATS_KEY      = os.environ.get("FOOTYSTATS_KEY")
ODDS_API_KEY        = os.environ.get("ODDS_API_KEY")
ODDS_API_BOOKMAKERS = os.environ.get("ODDS_API_BOOKMAKERS", "bet365,unibet")
WEBHOOK_SECRET      = os.environ.get("WEBHOOK_SECRET", "")
ANTHROPIC_API_KEY   = os.environ.get("ANTHROPIC_API_KEY", "")

DB_PATH              = os.environ.get("DB_PATH", "/tmp/apex_signals.db")
DEFAULT_BANKROLL     = float(os.environ.get("DEFAULT_BANKROLL", "100.0"))
AUTO_RESOLVE_ENABLED = os.environ.get("AUTO_RESOLVE_ENABLED", "1") == "1"
RESOLVE_BATCH_LIMIT  = int(os.environ.get("RESOLVE_BATCH_LIMIT", "200"))

ENABLE_SCHEDULER     = os.environ.get("ENABLE_SCHEDULER", "1") == "1"
SCAN_COOLDOWN_SECONDS= int(os.environ.get("SCAN_COOLDOWN_SECONDS", "300"))
SCAN_START_HOUR      = int(os.environ.get("SCAN_START_HOUR", "7"))
SCAN_END_HOUR        = int(os.environ.get("SCAN_END_HOUR", "23"))

HTTP_RETRY_TOTAL    = int(os.environ.get("HTTP_RETRY_TOTAL", "3"))
HTTP_BACKOFF_FACTOR = float(os.environ.get("HTTP_BACKOFF_FACTOR", "0.7"))

# ============================================================
# CONFIG — Ligues (tiers, home_adv, rho Dixon-Coles)
# ============================================================
LEAGUE_CONFIG: Dict[int, Dict[str, Any]] = {
    # P0 — UEFA
    2:   {"tier": "P0", "name": "UEFA Champions League",          "home_adv": 1.25, "rho": -0.13},
    3:   {"tier": "P0", "name": "UEFA Europa League",             "home_adv": 1.25, "rho": -0.13},
    848: {"tier": "P0", "name": "UEFA Europa Conference League",  "home_adv": 1.25, "rho": -0.13},
    17:  {"tier": "P0", "name": "AFC Champions League",           "home_adv": 1.20, "rho": -0.13},
    # N1 — Top 5 + grands championnats
    39:  {"tier": "N1", "name": "Premier League",          "home_adv": 1.08, "rho": -0.13},
    140: {"tier": "N1", "name": "La Liga",                 "home_adv": 1.12, "rho": -0.13},
    78:  {"tier": "N1", "name": "Bundesliga",              "home_adv": 1.10, "rho": -0.13},
    135: {"tier": "N1", "name": "Serie A",                 "home_adv": 1.13, "rho": -0.13},
    61:  {"tier": "N1", "name": "Ligue 1",                 "home_adv": 1.11, "rho": -0.13},
    # N2 — Ligues secondaires
    40:  {"tier": "N2", "name": "Championship",            "home_adv": 1.14, "rho": -0.13},
    62:  {"tier": "N2", "name": "Ligue 2",                 "home_adv": 1.12, "rho": -0.13},
    79:  {"tier": "N2", "name": "2. Bundesliga",           "home_adv": 1.10, "rho": -0.13},
    136: {"tier": "N2", "name": "Serie B",                 "home_adv": 1.12, "rho": -0.13},
    88:  {"tier": "N2", "name": "Eredivisie",              "home_adv": 1.09, "rho": -0.13},
    94:  {"tier": "N2", "name": "Primeira Liga",           "home_adv": 1.11, "rho": -0.13},
    203: {"tier": "N2", "name": "Süper Lig",               "home_adv": 1.14, "rho": -0.13},
    71:  {"tier": "N2", "name": "Serie A Brazil",          "home_adv": 1.18, "rho": -0.16},
    128: {"tier": "N2", "name": "Primera Division Arg.",   "home_adv": 1.20, "rho": -0.18},
    # N3 — Autres
    41:  {"tier": "N3", "name": "League One",              "home_adv": 1.12, "rho": -0.13},
    95:  {"tier": "N3", "name": "Liga Portugal B",         "home_adv": 1.10, "rho": -0.13},
    89:  {"tier": "N3", "name": "Eredivisie B",            "home_adv": 1.10, "rho": -0.13},
    113: {"tier": "N3", "name": "Allsvenskan",             "home_adv": 1.10, "rho": -0.13},
    119: {"tier": "N3", "name": "Superliga Denmark",       "home_adv": 1.10, "rho": -0.13},
    103: {"tier": "N3", "name": "Eliteserien Norway",      "home_adv": 1.10, "rho": -0.13},
    106: {"tier": "N3", "name": "Veikkausliiga Finland",   "home_adv": 1.10, "rho": -0.13},
    179: {"tier": "N3", "name": "Scottish Premiership",   "home_adv": 1.12, "rho": -0.13},
    197: {"tier": "N3", "name": "Super League Greece",     "home_adv": 1.13, "rho": -0.13},
    207: {"tier": "N3", "name": "Jupiler Pro League",      "home_adv": 1.11, "rho": -0.13},
    218: {"tier": "N3", "name": "Fortuna Liga",            "home_adv": 1.10, "rho": -0.13},
    235: {"tier": "N3", "name": "Ukrainian Premier League","home_adv": 1.10, "rho": -0.13},
    72:  {"tier": "N3", "name": "Brasileirão Serie B",     "home_adv": 1.16, "rho": -0.16},
    233: {"tier": "N3", "name": "Egyptian Premier League", "home_adv": 1.15, "rho": -0.14},
    307: {"tier": "N3", "name": "Saudi Pro League",        "home_adv": 1.17, "rho": -0.14},
    301: {"tier": "N3", "name": "AFC Cup",                 "home_adv": 1.18, "rho": -0.13},
    98:  {"tier": "N3", "name": "J-League",                "home_adv": 1.10, "rho": -0.13},
    292: {"tier": "N3", "name": "K-League",                "home_adv": 1.10, "rho": -0.13},
    210: {"tier": "N3", "name": "Super Lig (2nd)",         "home_adv": 1.13, "rho": -0.13},
    188: {"tier": "N3", "name": "Bundesliga Austria",      "home_adv": 1.11, "rho": -0.13},
    239: {"tier": "N3", "name": "Premier League Russia",   "home_adv": 1.12, "rho": -0.13},
    265: {"tier": "N3", "name": "Liga MX",                 "home_adv": 1.16, "rho": -0.15},
    262: {"tier": "N3", "name": "MLS",                     "home_adv": 1.10, "rho": -0.13},
    253: {"tier": "N3", "name": "USL Championship",        "home_adv": 1.10, "rho": -0.13},
    242: {"tier": "N3", "name": "Ligue Professionnelle 1", "home_adv": 1.14, "rho": -0.14},
    343: {"tier": "N3", "name": "Süper Lig (Cyprus)",      "home_adv": 1.14, "rho": -0.13},
    164: {"tier": "N3", "name": "Ekstraklasa",             "home_adv": 1.12, "rho": -0.13},
    244: {"tier": "N3", "name": "NB I Hungary",            "home_adv": 1.12, "rho": -0.13},
    328: {"tier": "N3", "name": "Botola Pro",              "home_adv": 1.15, "rho": -0.14},
}

TARGET_LEAGUE_IDS = list(LEAGUE_CONFIG.keys())

# Seuils d'edge minimum par tier
MIN_EDGE: Dict[str, float] = {"P0": 0.03, "N1": 0.03, "N2": 0.02, "N3": 0.015}

# Seuils de confiance
MIN_CONFIDENCE_BET    = 10  # Mode BET (cotes disponibles)
MIN_CONFIDENCE_SIGNAL = 15  # Mode SIGNAL (sans cotes)
MIN_PROB_SIGNAL       = 0.55

# Kelly
KELLY_FRACTION = 0.25
MAX_STAKE_PCT  = 0.05

# Oddss
MIN_ODD_ANY   = 1.40   # Minimum absolu toutes issues
MIN_ODD_DRAW  = 2.00   # Minimum cote nul
MAX_ODD_BET   = 3.80   # Maximum cote marché principal (favoris + légèrs outsiders)
MAX_ODD_SIGNAL = 5.50  # Garde pour SIGNAL pur (sans cote)

# Scan
MAX_SCAN_RESULTS     = 20
MIN_SIGNAL_LEVEL_AUTO = 2

# Glamour teams (biais outsider)
GLAMOUR_NAMES = {
    "liverpool", "real madrid", "barcelona", "bayern munich", "psg",
    "paris saint germain", "manchester city", "manchester united",
    "arsenal", "chelsea", "juventus", "inter", "ac milan",
}

EXCLUDED_KEYWORDS = [
    "youth", "u17", "u18", "u19", "u20", "u21", "u23",
    "women", "feminine", "female", "reserve", "reserves", "b team", "ii",
]

# Mapping pays → ligue domestique (pour fallback UEFA)
COUNTRY_TO_LEAGUE: Dict[str, int] = {
    "England": 39, "Spain": 140, "Germany": 78, "Italy": 135,
    "France": 61, "Portugal": 94, "Netherlands": 88, "Turkey": 203,
}

# ============================================================
# LOGGING & ÉTAT
# ============================================================
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("apexbot")

SCAN_LOCK: threading.Lock = threading.Lock()
SCAN_STATE: Dict[str, Any] = {
    "running": False,
    "started_at": None,
    "last_success": None,
    "last_error": None,
    "last_duration_seconds": None,
    "last_signals_sent": 0,
    "last_manual_trigger_at": None,
}

# ============================================================
# HTTP SESSION PARTAGÉE (retry + backoff)
# ============================================================
def build_http_session() -> requests.Session:
    retry = Retry(
        total=HTTP_RETRY_TOTAL,
        connect=HTTP_RETRY_TOTAL,
        read=HTTP_RETRY_TOTAL,
        backoff_factor=HTTP_BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

HTTP = build_http_session()

# ============================================================
# URLS API
# ============================================================
API_FOOTBALL_BASE_URL = "https://v3.football.api-sports.io"
FOOTYSTATS_BASE_URL   = "https://api.football-data-api.com"
ODDS_API_BASE_URL     = "https://api.the-odds-api.com/v4"

REQUEST_TIMEOUT    = 20
CACHE_TTL_SECONDS  = 600
_MEMORY_CACHE: Dict[str, Dict[str, Any]] = {}

# Cache FootyStats intra-session
_FS_MATCHES_CACHE: List[Dict[str, Any]] = []
_FS_CACHE_TS: float = 0.0
_FS_TTL: float = 25 * 60.0

# ============================================================
# FLASK HELPERS
# ============================================================
def ok(payload: Dict[str, Any], status_code: int = 200):
    return jsonify(payload), status_code

def err(message: str, status_code: int = 400, **kwargs):
    payload = {"status": "error", "message": message}
    payload.update(kwargs)
    return jsonify(payload), status_code

@app.errorhandler(HTTPException)
def handle_http_exception(e):
    return jsonify({"status": "error", "message": e.name,
                    "details": e.description, "build_id": BUILD_ID}), e.code

@app.errorhandler(Exception)
def handle_exception(e):
    return jsonify({"status": "error", "message": "Unhandled server exception",
                    "details": str(e), "build_id": BUILD_ID}), 500

# ============================================================
# UTILS TEMPORELS
# ============================================================
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

def format_match_time(iso_date: Optional[str]) -> str:
    dt = parse_iso_date(iso_date)
    if not dt:
        return iso_date or ""
    return dt.astimezone(timezone.utc).strftime("%H:%M UTC")

# ============================================================
# UTILS NUMÉRIQUES
# ============================================================
def maybe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
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

# ============================================================
# UTILS TEXTE / NOMS
# ============================================================
def normalize_name(name: Optional[str]) -> str:
    """Normalisation avancée: suppression accents + stopwords + suffixes."""
    text = unicodedata.normalize("NFKD", str(name or "")).encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    stop = {
        "fc", "cf", "sc", "afc", "ac", "club", "deportivo", "futbol",
        "football", "soccer", "united", "city", "town", "athletic",
        "sporting", "the", "de", "du", "el", "al",
    }
    tokens = [t for t in text.split() if t not in stop]
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
    if delta_minutes <= 5:   return 1.0
    if delta_minutes <= 20:  return 0.9
    if delta_minutes <= 60:  return 0.7
    if delta_minutes <= 180: return 0.45
    return 0.0

def is_glamour_team(name: Optional[str]) -> bool:
    return normalize_name(name) in {normalize_name(x) for x in GLAMOUR_NAMES}

def json_dumps_safe(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return json.dumps(str(value), ensure_ascii=False)

def count_wins(form_string: Optional[str]) -> int:
    if not form_string:
        return 0
    return str(form_string).count("W")

# ============================================================
# MATHS — DIXON-COLES + KELLY
# ============================================================
def poisson_pmf(lmb: float, k: int) -> float:
    if lmb <= 0:
        return 1.0 if k == 0 else 0.0
    return math.exp(-lmb) * (lmb ** k) / math.factorial(k)

def calculate_probs_dc(hxg: float, axg: float, league_id: int) -> Dict[str, float]:
    """Modèle Dixon-Coles: probabilités Home/Draw/Away avec correction rho."""
    cfg = LEAGUE_CONFIG.get(league_id, {"rho": -0.13})
    rho = cfg["rho"]

    hp = [poisson_pmf(hxg, i) for i in range(7)]
    ap = [poisson_pmf(axg, i) for i in range(7)]

    probs: Dict[str, float] = {"H": 0.0, "D": 0.0, "A": 0.0}

    for h in range(7):
        for a in range(7):
            # Correction Dixon-Coles scores bas
            if   h == 0 and a == 0: tau = 1.0 - (hxg * axg * rho)
            elif h == 1 and a == 0: tau = 1.0 + (axg * rho)
            elif h == 0 and a == 1: tau = 1.0 + (hxg * rho)
            elif h == 1 and a == 1: tau = 1.0 - rho
            else:                   tau = 1.0

            p = max(hp[h] * ap[a] * tau, 0.0)
            if   h > a: probs["H"] += p
            elif h == a: probs["D"] += p
            else:        probs["A"] += p

    total = sum(probs.values())
    if total > 0:
        probs = {k: v / total for k, v in probs.items()}
    return probs

def kelly_stake(prob: float, odd: float, bankroll: float) -> float:
    """Critère de Kelly fractionné."""
    if not odd or odd <= 1.0:
        return 0.0
    b = odd - 1.0
    q = 1.0 - prob
    f = (b * prob - q) / b
    if f <= 0:
        return 0.0
    raw = bankroll * KELLY_FRACTION * f
    return round(min(raw, bankroll * MAX_STAKE_PCT), 2)

# ============================================================
# CACHE MÉMOIRE
# ============================================================
def cache_get(key: str, ttl_seconds: int = CACHE_TTL_SECONDS) -> Optional[Any]:
    item = _MEMORY_CACHE.get(key)
    if not item:
        return None
    if time.time() - item["ts"] > ttl_seconds:
        _MEMORY_CACHE.pop(key, None)
        return None
    return item["data"]

def cache_set(key: str, data: Any) -> None:
    _MEMORY_CACHE[key] = {"ts": time.time(), "data": data}

# ============================================================
# SQLITE — DB
# ============================================================
def db_connect() -> sqlite3.Connection:
    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        try:
            os.makedirs(db_dir, exist_ok=True)
        except OSError as exc:
            logger.warning("Cannot create DB dir %s: %s — falling back to /tmp", db_dir, exc)
            fallback = os.path.join("/tmp", os.path.basename(DB_PATH))
            conn = sqlite3.connect(fallback, timeout=30, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            return conn
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db() -> None:
    try:
        with closing(db_connect()) as conn:
            # Table signaux
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
                tier TEXT,
                country TEXT,
                home_team TEXT,
                away_team TEXT,
                market TEXT,
                side TEXT,
                mode TEXT DEFAULT 'BET',
                decision TEXT NOT NULL,
                level INTEGER NOT NULL,
                level_name TEXT,
                odd REAL,
                raw_edge REAL,
                adjusted_edge REAL,
                prob REAL,
                hxg REAL,
                axg REAL,
                xg_source TEXT,
                dcs REAL,
                confidence INTEGER,
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
                stake REAL DEFAULT 0.0,
                profit REAL DEFAULT 0.0,
                resolved_at TEXT
            )
            """)
            # Table bankroll
            conn.execute("""
            CREATE TABLE IF NOT EXISTS bankroll (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                amount REAL NOT NULL
            )
            """)
            conn.execute("INSERT OR IGNORE INTO bankroll (id, amount) VALUES (1, ?)",
                         (DEFAULT_BANKROLL,))
            # Index
            for idx_sql in [
                "CREATE INDEX IF NOT EXISTS idx_signals_fixture_id ON signals(fixture_id)",
                "CREATE INDEX IF NOT EXISTS idx_signals_created_at ON signals(created_at)",
                "CREATE INDEX IF NOT EXISTS idx_signals_result_status ON signals(result_status)",
                "CREATE INDEX IF NOT EXISTS idx_signals_level ON signals(level)",
                "CREATE INDEX IF NOT EXISTS idx_signals_market ON signals(market)",
                "CREATE INDEX IF NOT EXISTS idx_signals_tier ON signals(tier)",
                "CREATE INDEX IF NOT EXISTS idx_signals_mode ON signals(mode)",
            ]:
                conn.execute(idx_sql)
            conn.commit()
            logger.info("DB ready at %s", DB_PATH)
    except Exception as exc:
        logger.error("init_db failed: %s — bot will start but DB may be unavailable", exc)

def get_bankroll() -> float:
    try:
        with closing(db_connect()) as conn:
            row = conn.execute("SELECT amount FROM bankroll WHERE id=1").fetchone()
            return float(row["amount"]) if row else DEFAULT_BANKROLL
    except Exception as e:
        logger.error("get_bankroll error: %s", e)
        return DEFAULT_BANKROLL

def set_bankroll(amount: float) -> None:
    try:
        with closing(db_connect()) as conn:
            conn.execute("UPDATE bankroll SET amount=? WHERE id=1", (round(amount, 2),))
            conn.commit()
    except Exception as e:
        logger.error("set_bankroll error: %s", e)

# ============================================================
# JOURNALISATION SIGNAUX
# ============================================================
def build_signal_uid(fixture_id, market, side, decision, level, kickoff_utc) -> str:
    match_day = str(kickoff_utc or "")[:10]
    return "|".join([str(BUILD_ID), str(fixture_id), str(match_day),
                     str(market or ""), str(side or ""), str(decision or ""), str(level or 0)])

def extract_telegram_message_id(tg_data: Optional[Dict]) -> Optional[str]:
    if not isinstance(tg_data, dict):
        return None
    try:
        return str(tg_data.get("telegram_response", {}).get("result", {}).get("message_id"))
    except Exception:
        return None

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

def save_signal_record(record: Dict[str, Any]) -> Dict[str, Any]:
    required = ["signal_uid", "created_at", "build_id", "fixture_id", "decision", "level"]
    for key in required:
        if record.get(key) is None:
            raise ValueError(f"Missing required record field: {key}")
    try:
        with closing(db_connect()) as conn:
            conn.execute("""
            INSERT OR IGNORE INTO signals (
                signal_uid, created_at, build_id, fixture_id, match_date, kickoff_utc,
                league_id, league_name, tier, country, home_team, away_team,
                market, side, mode, decision, level, level_name,
                odd, raw_edge, adjusted_edge, prob, hxg, axg, xg_source, dcs, confidence,
                confluence_count, confidence_count,
                rationale, contextual_flags, contextual_penalties,
                telegram_sent, telegram_http_status, telegram_message_id, stake
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """, (
                record.get("signal_uid"), record.get("created_at"), record.get("build_id"),
                record.get("fixture_id"), record.get("match_date"), record.get("kickoff_utc"),
                record.get("league_id"), record.get("league_name"), record.get("tier"),
                record.get("country"), record.get("home_team"), record.get("away_team"),
                record.get("market"), record.get("side"), record.get("mode", "BET"),
                record.get("decision"), record.get("level"), record.get("level_name"),
                record.get("odd"), record.get("raw_edge"), record.get("adjusted_edge"),
                record.get("prob"), record.get("hxg"), record.get("axg"),
                record.get("xg_source"), record.get("dcs"), record.get("confidence"),
                record.get("confluence_count"), record.get("confidence_count"),
                record.get("rationale"), record.get("contextual_flags"),
                record.get("contextual_penalties"),
                1 if record.get("telegram_sent") else 0,
                record.get("telegram_http_status"), record.get("telegram_message_id"),
                record.get("stake", 0.0),
            ))
            conn.commit()
            row = conn.execute("SELECT id FROM signals WHERE signal_uid=?",
                               (record["signal_uid"],)).fetchone()
        return {"status": "ok", "signal_uid": record["signal_uid"],
                "row_id": int(row["id"]) if row else None}
    except Exception as exc:
        logger.error("save_signal_record failed: %s", exc)
        return {"status": "error", "details": str(exc)}

# ============================================================
# RÉSOLUTION AUTOMATIQUE
# ============================================================
FINAL_STATUSES = {"FT", "AET", "PEN"}
VOID_STATUSES  = {"CANC", "PST", "ABD", "AWD", "WO"}

def compute_bet_outcome(market, side, home_goals, away_goals, match_status) -> str:
    if match_status in VOID_STATUSES:
        return "void"
    if home_goals is None or away_goals is None:
        return "pending"
    total = home_goals + away_goals
    if market == "1X2":
        if side == "Home":  return "win" if home_goals > away_goals else "loss"
        if side == "Draw":  return "win" if home_goals == away_goals else "loss"
        if side == "Away":  return "win" if away_goals > home_goals else "loss"
        return "loss"
    if market == "BTTS_YES":  return "win" if home_goals > 0 and away_goals > 0 else "loss"
    if market == "BTTS_NO":   return "win" if home_goals == 0 or away_goals == 0 else "loss"
    if market == "OVER_2_5":  return "win" if total >= 3 else "loss"
    if market == "UNDER_2_5": return "win" if total <= 2 else "loss"
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
        rows = conn.execute("SELECT * FROM signals WHERE fixture_id=? AND result_status='pending'",
                            (fixture_id,)).fetchall()
        resolved_count = 0
        for row in rows:
            outcome = compute_bet_outcome(row["market"], row["side"],
                                          home_goals, away_goals, status_short)
            profit = compute_profit(outcome, maybe_float(row["odd"]),
                                    maybe_float(row["stake"]) or 0.0)
            conn.execute("""
                UPDATE signals SET result_status='resolved', match_status=?,
                    home_goals=?, away_goals=?, bet_outcome=?, profit=?, resolved_at=?
                WHERE id=?
            """, (status_short, home_goals, away_goals, outcome, profit,
                  now_utc().isoformat(), row["id"]))
            # Mettre à jour la bankroll si mode BET
            if row["mode"] == "BET" and outcome in ("win", "loss"):
                current = get_bankroll()
                set_bankroll(current + profit)
            resolved_count += 1
        conn.commit()
    return {"status": "ok", "fixture_id": fixture_id, "match_status": status_short,
            "home_goals": home_goals, "away_goals": away_goals, "resolved_count": resolved_count}

def resolve_pending_signals(limit: int = RESOLVE_BATCH_LIMIT) -> Dict[str, Any]:
    with closing(db_connect()) as conn:
        rows = conn.execute("""
            SELECT DISTINCT fixture_id FROM signals WHERE result_status='pending'
            ORDER BY created_at ASC LIMIT ?
        """, (limit,)).fetchall()
    fixture_ids = [int(r["fixture_id"]) for r in rows]
    results = []
    total_resolved = 0
    for fid in fixture_ids:
        try:
            result = resolve_fixture_signals(fid)
            results.append(result)
            if result.get("status") == "ok":
                total_resolved += int(result.get("resolved_count", 0))
        except Exception as exc:
            logger.exception("resolve_fixture_signals failed for fixture_id=%s", fid)
            results.append({"status": "error", "fixture_id": fid, "details": str(exc)})
    return {"status": "ok", "checked_fixtures": len(fixture_ids),
            "resolved_signals": total_resolved, "results": results}

# ============================================================
# TELEGRAM
# ============================================================
def send_telegram_message(text: str, parse_mode: str = "HTML") -> Tuple[Dict[str, Any], int]:
    config = {"bot_token_present": bool(BOT_TOKEN), "chat_id_present": bool(CHAT_ID)}
    if not BOT_TOKEN:
        return {"status": "error", "message": "BOT_TOKEN is missing", "config": config}, 500
    if not CHAT_ID:
        return {"status": "error", "message": "CHAT_ID is missing", "config": config}, 500
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        response = HTTP.post(url, json={"chat_id": CHAT_ID, "text": text,
                                        "parse_mode": parse_mode}, timeout=15)
        data = response.json()
    except Exception as e:
        logger.exception("Telegram request failed")
        return {"status": "error", "message": "Telegram request failed",
                "details": str(e), "config": config}, 500
    if not response.ok or not data.get("ok"):
        return {"status": "error", "message": "Telegram API returned an error",
                "telegram_response": data, "config": config}, 500
    return {"status": "ok", "telegram_response": data, "config": config}, 200

def set_telegram_webhook(url: str) -> Tuple[Dict[str, Any], int]:
    if not BOT_TOKEN:
        return {"status": "error", "message": "BOT_TOKEN is missing"}, 500
    params: Dict[str, Any] = {"url": url}
    if WEBHOOK_SECRET:
        params["secret_token"] = WEBHOOK_SECRET
    try:
        r = HTTP.post(f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook",
                      json=params, timeout=15)
        return {"status": "ok", "telegram_response": r.json()}, 200
    except Exception as exc:
        return {"status": "error", "details": str(exc)}, 500

def format_signal_message(signal: Dict[str, Any]) -> str:
    """Message Telegram HTML — mode BET ou SIGNAL avec marchés secondaires."""
    mode = signal.get("mode", "BET")
    icon = "🚀" if mode == "BET" else "📡"
    tier = signal.get("tier", "")
    tier_line = f"[{tier}] " if tier else ""

    odds_line = ""
    if signal.get("odd"):
        edge_pct = f" ({signal['edge']*100:.1f}% edge)" if signal.get("edge") else ""
        odds_line = f"@ {signal['odd']:.2f}{edge_pct}"
    else:
        odds_line = "(Signal modèle — sans cote)"

    stake_line = ""
    if mode == "BET" and signal.get("stake") and signal["stake"] > 0:
        stake_line = f"\n💰 Mise Kelly: <b>{signal['stake']:.2f}u</b>"

    xg_src = signal.get("xg_source", "proxy")
    src_icon = "📊" if xg_src == "footystats" else "🔢"

    # DC probs
    dc = signal.get("dc_probs", {})
    dc_line = ""
    if dc:
        dc_line = (f"\n📐 H:{dc.get('H',0)*100:.0f}% "
                   f"D:{dc.get('D',0)*100:.0f}% "
                   f"A:{dc.get('A',0)*100:.0f}%")

    # H2H
    h2h = signal.get("h2h", {})
    h2h_line = ""
    if h2h.get("available"):
        h2h_line = (f"\n🔄 H2H({h2h['matches']}): "
                    f"H{int(h2h['home_win_pct']*100)}% "
                    f"N{int(h2h['draw_pct']*100)}% "
                    f"A{int(h2h['away_win_pct']*100)}% "
                    f"| Moy:{h2h['avg_goals']}b "
                    f"BTTS:{int(h2h['btts_rate']*100)}%")

    # Marchés secondaires
    secondary = signal.get("secondary_markets", [])
    sec_lines = ""
    market_icons = {
        "BTTS_YES": "🟢 GG",
        "BTTS_NO":  "🔴 PAS GG",
        "OVER_2_5": "⬆️ O2.5",
        "UNDER_2_5":"⬇️ U2.5",
        "OVER_3_5": "⬆️ O3.5",
        "OVER_4_5": "⬆️ O4.5",
        "DC_1X":    "🔵 DC 1X",
        "DC_X2":    "🔵 DC X2",
        "DC_12":    "🔵 DC 12",
        "CORNERS_OVER_9_5":  "📐 +9.5 Coins",
        "CORNERS_UNDER_9_5": "📐 -9.5 Coins",
        "CARDS_OVER_3_5":    "🟨 +3.5 CJ",
    }
    if secondary:
        sec_lines = "\n━━━━━━━━━━━━━━━━━━━\n📋 <b>Marchés secondaires:</b>"
        for s in secondary[:4]:
            mkt = s.get("market", "")
            icon_m = market_icons.get(mkt, mkt)
            prob_m = s.get("prob", 0)
            conf_m = s.get("confidence", 0)
            sec_lines += f"\n   {icon_m} | Prob: {prob_m*100:.0f}% | Conf: {conf_m}/50"
            if s.get("xg_total"):
                sec_lines += f" | xGtot: {s['xg_total']}"

    return (
        f"{icon} <b>APEX-ULTIMATE — {mode}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"🏆 {tier_line}{signal.get('league_name', '')}\n"
        f"⚽ <b>{signal.get('home', '')} vs {signal.get('away', '')}</b>\n"
        f"⏱ {format_match_time(signal.get('kickoff_utc'))}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 <b>{signal.get('side', '')} — {signal.get('decision', '')}</b>\n"
        f"   {odds_line}\n"
        f"📈 Prob: {signal.get('prob', 0)*100:.1f}% | Conf: {signal.get('confidence', 0)}/50\n"
        f"{src_icon} xG: {signal.get('hxg', 0):.2f}⚡{signal.get('axg', 0):.2f} ({xg_src})\n"
        f"🔎 DCS: {signal.get('dcs', 0):.2f}"
        f"{dc_line}"
        f"{h2h_line}"
        f"{stake_line}"
        f"{sec_lines}"
    )

# ============================================================
# API-FOOTBALL — CALLS
# ============================================================
def call_api_football(endpoint: str, params: Optional[Dict] = None) -> Tuple[Dict, int]:
    if not API_KEY:
        return {"status": "error", "message": "API_KEY is missing"}, 500
    headers = {"x-apisports-key": API_KEY}
    url = f"{API_FOOTBALL_BASE_URL}/{endpoint}"
    try:
        response = HTTP.get(url, headers=headers, params=params or {}, timeout=REQUEST_TIMEOUT)
        data = response.json()
    except Exception as e:
        return {"status": "error", "message": "API-Football request failed", "details": str(e)}, 500
    if not response.ok:
        return {"status": "error", "message": "API-Football HTTP error",
                "http_status": response.status_code, "api_response": data}, 500
    return {"status": "ok", "data": data}, 200

def get_fixture_by_id(fixture_id: str) -> Tuple[Dict, int]:
    data, status = call_api_football("fixtures", {"id": fixture_id})
    if status != 200:
        return data, status
    response = data["data"].get("response", [])
    if not response:
        return {"status": "error", "message": f"No fixture for fixture_id={fixture_id}"}, 404
    return {"status": "ok", "fixture": response[0]}, 200

def get_fixtures_by_date(date_str: Optional[str] = None) -> Tuple[Dict, int]:
    return call_api_football("fixtures", {"date": date_str or utc_today_str()})

def get_predictions_api(fixture_id: str) -> Optional[Dict]:
    data, status = call_api_football("predictions", {"fixture": fixture_id})
    if status != 200 or not data["data"].get("response"):
        return None
    return data["data"]["response"][0]

def get_stats_smart(team_id: int, league_id: int, season: int) -> Optional[Dict]:
    """Récupère stats équipe. Fallback ligue domestique si UEFA."""
    data, status = call_api_football("teams/statistics",
                                     {"team": team_id, "league": league_id, "season": season})
    if status == 200 and data["data"].get("response"):
        return data["data"]["response"]

    # Fallback pour UEFA: chercher ligue domestique
    if league_id in {2, 3, 848, 17}:
        team_data, t_status = call_api_football("teams", {"id": team_id})
        if t_status == 200:
            country = (team_data["data"].get("response") or [{}])[0].get("team", {}).get("country", "")
            domestic = COUNTRY_TO_LEAGUE.get(country)
            if domestic:
                data2, s2 = call_api_football("teams/statistics",
                                              {"team": team_id, "league": domestic, "season": season})
                if s2 == 200 and data2["data"].get("response"):
                    logger.debug("Stats fallback %s → league %s (country %s)", team_id, domestic, country)
                    return data2["data"]["response"]
    return None

def get_standings_cached(league_id: int, season: int,
                          cache: Optional[Dict[Tuple, Any]] = None) -> List[Dict]:
    key = (league_id, season)
    if cache is not None and key in cache:
        return cache[key]
    data, status = call_api_football("standings", {"league": league_id, "season": season})
    if status != 200:
        return []
    result = data["data"].get("response", [])
    if cache is not None:
        cache[key] = result
    return result

# ============================================================
# API-FOOTBALL — ODDS
# ============================================================
def label_to_side(label: str, home_name: str, away_name: str) -> Optional[str]:
    normalized = (label or "").strip().lower()
    home_name = (home_name or "").strip().lower()
    away_name = (away_name or "").strip().lower()
    if normalized in {"home", "1"}: return "Home"
    if normalized in {"draw", "x"}: return "Draw"
    if normalized in {"away", "2"}: return "Away"
    if home_name and normalized == home_name: return "Home"
    if away_name and normalized == away_name: return "Away"
    return None

def pick_best_1x2_odds(fixture_id: str, home_name: str,
                        away_name: str) -> Optional[Dict[str, float]]:
    """Récupère les meilleures cotes 1X2 depuis API-Football."""
    data, status = call_api_football("odds", {"fixture": fixture_id})
    if status != 200:
        return None
    market_names = {"match winner", "winner", "1x2"}
    for fixture_odds in data["data"].get("response", []):
        for bookmaker in fixture_odds.get("bookmakers", []):
            for bet in bookmaker.get("bets", []):
                if (bet.get("name") or "").strip().lower() not in market_names:
                    continue
                extracted: Dict[str, Optional[float]] = {"Home": None, "Draw": None, "Away": None}
                for value in bet.get("values", []):
                    side = label_to_side(value.get("value"), home_name, away_name)
                    if side:
                        extracted[side] = maybe_float(value.get("odd"))
                if all(v is not None for v in extracted.values()):
                    return extracted  # type: ignore
    return None

# ============================================================
# THE ODDS API — FALLBACK COTES
# ============================================================
LEAGUE_TO_ODDS_API_SPORT: Dict[int, str] = {
    39: "soccer_epl", 140: "soccer_spain_la_liga", 78: "soccer_germany_bundesliga",
    135: "soccer_italy_serie_a", 61: "soccer_france_ligue_one",
    2: "soccer_uefa_champs_league", 3: "soccer_uefa_europa_league",
    848: "soccer_uefa_europa_conference_league", 94: "soccer_portugal_primeira_liga",
    88: "soccer_netherlands_eredivisie", 71: "soccer_brazil_campeonato",
    128: "soccer_argentina_primera_division",
}

def call_odds_api(sport_key: str, cache_key: Optional[str] = None) -> Tuple[Dict, int]:
    if not ODDS_API_KEY:
        return {"status": "error", "message": "ODDS_API_KEY is missing"}, 500
    if cache_key:
        cached = cache_get(cache_key)
        if cached is not None:
            return {"status": "ok", "data": cached, "cached": True}, 200
    params = {"apiKey": ODDS_API_KEY, "regions": "eu", "markets": "h2h",
              "bookmakers": ODDS_API_BOOKMAKERS, "oddsFormat": "decimal"}
    url = f"{ODDS_API_BASE_URL}/sports/{sport_key}/odds/"
    try:
        response = HTTP.get(url, params=params, timeout=REQUEST_TIMEOUT)
        data = response.json()
    except Exception as e:
        return {"status": "error", "message": "OddsAPI request failed", "details": str(e)}, 500
    if not response.ok:
        return {"status": "error", "message": "OddsAPI HTTP error",
                "http_status": response.status_code}, 500
    if cache_key:
        cache_set(cache_key, data)
    return {"status": "ok", "data": data, "cached": False}, 200

def get_odds_api_1x2(detail: Dict) -> Optional[Dict[str, float]]:
    league_id = detail.get("league_id")
    sport_key = LEAGUE_TO_ODDS_API_SPORT.get(league_id)
    if not sport_key:
        return None
    payload, status = call_odds_api(sport_key, cache_key=f"oddsapi:{sport_key}")
    if status != 200 or not isinstance(payload.get("data"), list):
        return None
    best_event = None
    best_score = 0.0
    for ev in payload["data"]:
        h_sim = team_name_similarity(detail.get("home"), ev.get("home_team"))
        a_sim = team_name_similarity(detail.get("away"), ev.get("away_team"))
        if h_sim < 0.60 or a_sim < 0.60:
            continue
        t_sim = kickoff_similarity(detail.get("date"), ev.get("commence_time"))
        score = h_sim * 0.42 + a_sim * 0.42 + t_sim * 0.16
        if score > best_score:
            best_score = score
            best_event = ev
    if not best_event or best_score < 0.72:
        return None
    for bm in (best_event.get("bookmakers") or []):
        for market in (bm.get("markets") or []):
            if market.get("key") != "h2h":
                continue
            home_team = best_event.get("home_team", "")
            away_team = best_event.get("away_team", "")
            h = d = a = None
            for o in (market.get("outcomes") or []):
                name = o.get("name", "")
                price = maybe_float(o.get("price"))
                if name == home_team:         h = price
                elif name == away_team:       a = price
                elif name.lower() in {"draw", "x"}: d = price
            if h is not None and d is not None and a is not None:
                return {"Home": h, "Draw": d, "Away": a}
    return None

# ============================================================
# FOOTYSTATS
# ============================================================
def call_footystats(endpoint: str, params: Optional[Dict] = None,
                    cache_key: Optional[str] = None) -> Tuple[Dict, int]:
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
        response = HTTP.get(url, params=query, timeout=REQUEST_TIMEOUT)
        data = response.json()
    except Exception as e:
        return {"status": "error", "message": "FootyStats request failed", "details": str(e)}, 500
    if not response.ok:
        return {"status": "error", "message": "FootyStats HTTP error",
                "http_status": response.status_code}, 500
    if cache_key:
        cache_set(cache_key, data)
    return {"status": "ok", "data": data, "cached": False}, 200

def get_footystats_matches_cached() -> List[Dict]:
    """Cache FootyStats todays-matches sur 25 min."""
    global _FS_MATCHES_CACHE, _FS_CACHE_TS
    now = time.time()
    if now - _FS_CACHE_TS < _FS_TTL and _FS_MATCHES_CACHE:
        return _FS_MATCHES_CACHE
    if not FOOTYSTATS_KEY:
        return []
    date_str = utc_today_str()
    payload, status = call_footystats("todays-matches",
                                       {"date": date_str, "timezone": "Etc/UTC"},
                                       cache_key=f"fs:todays:{date_str}")
    if status == 200:
        raw = payload["data"]
        data = raw.get("data") if isinstance(raw, dict) else raw
        if isinstance(data, list):
            _FS_MATCHES_CACHE = data
            _FS_CACHE_TS = now
            logger.info("FootyStats updated: %d matches", len(data))
            return data
    return []

def find_fs_match(home: str, away: str,
                   fs_matches: List[Dict]) -> Optional[Dict]:
    best_score = 0.0
    best = None
    for m in fs_matches:
        score = (team_name_similarity(home, m.get("home_name", "")) +
                 team_name_similarity(away, m.get("away_name", ""))) / 2
        if score > best_score and score > 0.75:
            best_score = score
            best = m
    return best

def get_fs_match_details(match_id: int) -> Optional[Dict]:
    payload, status = call_footystats("match", {"match_id": match_id},
                                       cache_key=f"fs:match:{match_id}")
    if status != 200:
        return None
    raw = payload["data"]
    detail_data = raw.get("data") if isinstance(raw, dict) else raw
    if isinstance(detail_data, list) and detail_data:
        detail_data = detail_data[0]
    return detail_data if isinstance(detail_data, dict) else None

# ============================================================
# CALCUL DCS + CONFIANCE
# ============================================================
def calculate_dcs(stats_h: Optional[Dict], stats_a: Optional[Dict],
                  xg_source: str) -> float:
    """Data Confidence Score: 0.0 → 1.0"""
    score = 0.0
    gp_h = 0
    gp_a = 0
    if stats_h:
        gp_h = (stats_h.get("fixtures", {}).get("played", {}).get("total", 0) or 0)
    if stats_a:
        gp_a = (stats_a.get("fixtures", {}).get("played", {}).get("total", 0) or 0)
    min_gp = min(gp_h, gp_a)
    if min_gp >= 10:   score += 0.4
    elif min_gp >= 5:  score += 0.2
    if xg_source == "footystats": score += 0.6
    elif xg_source == "proxy":    score += 0.2
    return min(score, 1.0)

def calculate_confidence(prob: float, edge: float, dcs: float, tier: str) -> int:
    """Score de confiance /50."""
    score = 0
    if edge >= 0.10:   score += 15
    elif edge >= 0.07: score += 10
    elif edge >= 0.04: score += 5
    tier_bonus = {"P0": 15, "N1": 10, "N2": 5, "N3": 2}.get(tier, 0)
    score += tier_bonus
    if dcs >= 0.80:    score += 15
    elif dcs >= 0.60:  score += 10
    elif dcs >= 0.40:  score += 5
    if prob >= 0.65:   score += 5
    elif prob >= 0.55: score += 2
    return min(score, 50)

# ============================================================
# H2H — HEAD TO HEAD
# ============================================================
def get_h2h(home_id: int, away_id: int, last: int = 5) -> List[Dict]:
    """Récupère les 5 derniers H2H entre deux équipes."""
    cache_key = f"h2h:{home_id}:{away_id}"
    cached = cache_get(cache_key, ttl_seconds=3600)
    if cached is not None:
        return cached
    data, status = call_api_football("fixtures/headtohead",
                                     {"h2h": f"{home_id}-{away_id}", "last": last})
    if status != 200:
        return []
    result = data["data"].get("response", [])
    cache_set(cache_key, result)
    return result

def analyse_h2h(h2h_fixtures: List[Dict], home_id: int) -> Dict[str, Any]:
    """Extrait les stats H2H: % victoire home, buts moyens, BTTS rate."""
    if not h2h_fixtures:
        return {"available": False}
    home_wins = draws = away_wins = 0
    total_goals = 0
    btts_count = 0
    for f in h2h_fixtures:
        hg = maybe_int(f.get("goals", {}).get("home")) or 0
        ag = maybe_int(f.get("goals", {}).get("away")) or 0
        fh_id = f.get("teams", {}).get("home", {}).get("id")
        total_goals += hg + ag
        if hg > 0 and ag > 0:
            btts_count += 1
        if hg > ag:
            if fh_id == home_id: home_wins += 1
            else: away_wins += 1
        elif hg == ag:
            draws += 1
        else:
            if fh_id == home_id: away_wins += 1
            else: home_wins += 1
    n = len(h2h_fixtures)
    return {
        "available": True,
        "matches": n,
        "home_win_pct": round(home_wins / n, 2),
        "draw_pct": round(draws / n, 2),
        "away_win_pct": round(away_wins / n, 2),
        "avg_goals": round(total_goals / n, 2),
        "btts_rate": round(btts_count / n, 2),
    }

# ============================================================
# NOUVEAUX MARCHÉS — BTTS / DC / OVER-UNDER / CORNERS / CARDS
# ============================================================
def analyse_btts_market(fs_match: Optional[Dict],
                         h2h: Dict, hxg: float, axg: float) -> Optional[Dict]:
    """
    BTTS (Both Teams To Score / GG).
    Utilise FootyStats btts_potential + xG + H2H btts_rate.
    """
    signals = []
    btts_pot = maybe_float((fs_match or {}).get("btts_potential"))
    total_xg = hxg + axg

    # xG individuels
    if hxg >= 0.90 and axg >= 0.90:
        signals.append(("xG_both_above_0.9", True))
    if btts_pot is not None:
        if btts_pot >= 60:
            signals.append(("btts_potential_high", True))
        elif btts_pot <= 40:
            signals.append(("btts_potential_low", False))

    h2h_btts = h2h.get("btts_rate", 0.5)
    if h2h.get("available") and h2h_btts >= 0.60:
        signals.append(("h2h_btts_high", True))
    elif h2h.get("available") and h2h_btts <= 0.30:
        signals.append(("h2h_btts_low", False))

    yes_count = sum(1 for _, v in signals if v)
    no_count  = sum(1 for _, v in signals if not v)

    if yes_count >= 2 and no_count == 0:
        prob_est = min(0.45 + yes_count * 0.08, 0.75)
        return {"market": "BTTS_YES", "prob": prob_est,
                "confidence": yes_count * 12, "signals": [s[0] for s in signals if s[1]]}
    if no_count >= 2 and yes_count == 0:
        prob_est = min(0.45 + no_count * 0.08, 0.70)
        return {"market": "BTTS_NO", "prob": prob_est,
                "confidence": no_count * 10, "signals": [s[0] for s, v in zip(signals, [v for _, v in signals]) if not v]}
    return None

def analyse_double_chance(dc_probs: Dict, odds_1x2: Optional[Dict],
                           tier: str) -> Optional[Dict]:
    """
    Double Chance (1X / X2 / 12).
    Pertinent uniquement si la cote DC est dans 1.10–1.80 (valeur sur favori étendu).
    """
    if not odds_1x2:
        return None
    ph = dc_probs.get("H", 0.0)
    pd = dc_probs.get("D", 0.0)
    pa = dc_probs.get("A", 0.0)

    combos = {
        "1X": {"prob": ph + pd, "sides": ["Home", "Draw"]},
        "X2": {"prob": pd + pa, "sides": ["Draw", "Away"]},
        "12": {"prob": ph + pa, "sides": ["Home", "Away"]},
    }
    # Cherche le combo avec prob > 0.70 ET edge positif vs cote DC implicite
    best: Optional[Dict] = None
    for name, info in combos.items():
        prob = info["prob"]
        if prob < 0.68:
            continue
        # Estime cote DC = 1 / prob (marché équilibré)
        implied_odd = round(1 / prob, 2)
        if 1.10 <= implied_odd <= 1.80:
            if best is None or prob > best["prob"]:
                best = {"market": f"DC_{name}", "dc_combo": name,
                        "prob": round(prob, 3), "implied_odd": implied_odd,
                        "confidence": int(prob * 40)}
    return best

def analyse_over_under(fs_match: Optional[Dict], h2h: Dict,
                        hxg: float, axg: float) -> Optional[Dict]:
    """
    Over/Under 2.5 / 3.5 / 4.5.
    Priorité: FootyStats potentials + xG total + H2H avg_goals.
    """
    total_xg = hxg + axg
    o25_pot = maybe_float((fs_match or {}).get("o25_potential"))
    u25_pot = maybe_float((fs_match or {}).get("u25_potential"))
    avg_pot = maybe_float((fs_match or {}).get("avg_potential"))
    h2h_avg = h2h.get("avg_goals", 2.5) if h2h.get("available") else None

    results = []

    # ---- OVER 2.5 ---- (bloqué si xG total < 2.3)
    if total_xg >= 2.30:
        o25_signals = 0
        if total_xg >= 2.60: o25_signals += 1
        if o25_pot is not None and o25_pot >= 58: o25_signals += 1
        if h2h_avg is not None and h2h_avg >= 2.8: o25_signals += 1
        if avg_pot is not None and avg_pot >= 2.70: o25_signals += 1
        if o25_signals >= 2:
            prob = min(0.45 + o25_signals * 0.07, 0.78)
            results.append({"market": "OVER_2_5", "prob": prob,
                             "confidence": o25_signals * 12, "xg_total": round(total_xg, 2)})

    # ---- UNDER 2.5 ---- (bloqué si xG total > 2.8)
    if total_xg <= 2.80:
        u25_signals = 0
        if total_xg <= 2.20: u25_signals += 1
        if u25_pot is not None and u25_pot >= 58: u25_signals += 1
        if h2h_avg is not None and h2h_avg <= 2.2: u25_signals += 1
        if avg_pot is not None and avg_pot <= 2.30: u25_signals += 1
        if u25_signals >= 2:
            prob = min(0.45 + u25_signals * 0.07, 0.75)
            results.append({"market": "UNDER_2_5", "prob": prob,
                             "confidence": u25_signals * 11, "xg_total": round(total_xg, 2)})

    # ---- OVER 3.5 ---- (uniquement si xG total >= 3.0)
    if total_xg >= 3.00:
        o35_signals = 0
        if total_xg >= 3.40: o35_signals += 1
        if o25_pot is not None and o25_pot >= 70: o35_signals += 1
        if h2h_avg is not None and h2h_avg >= 3.5: o35_signals += 1
        if o35_signals >= 2:
            prob = min(0.35 + o35_signals * 0.08, 0.65)
            results.append({"market": "OVER_3_5", "prob": prob,
                             "confidence": o35_signals * 10, "xg_total": round(total_xg, 2)})

    # ---- OVER 4.5 ---- (uniquement si xG total >= 4.0 ET H2H confirme)
    if total_xg >= 4.00 and h2h_avg is not None and h2h_avg >= 4.0:
        results.append({"market": "OVER_4_5", "prob": 0.38,
                         "confidence": 15, "xg_total": round(total_xg, 2)})

    if not results:
        return None
    return max(results, key=lambda x: x["confidence"])

def analyse_corners_market(stats_h: Optional[Dict],
                            stats_a: Optional[Dict],
                            fs_match: Optional[Dict]) -> Optional[Dict]:
    """
    Total Corners. Utilise FootyStats si disponible.
    Retourne None si pas de données suffisantes (évite les valeurs statiques inutiles).
    """
    if not fs_match:
        return None  # Pas de données FootyStats → pas de signal corners

    # FootyStats fournit parfois avg_corners_home / avg_corners_away
    h_corners = maybe_float(fs_match.get("home_avg_corners") or
                             fs_match.get("team_a_corners_for_avg"))
    a_corners  = maybe_float(fs_match.get("away_avg_corners") or
                              fs_match.get("team_b_corners_for_avg"))

    if not h_corners or not a_corners:
        return None  # Pas assez de données

    total_est = h_corners + a_corners
    if total_est >= 11.0:
        return {"market": "CORNERS_OVER_10_5", "prob": 0.58,
                "confidence": 14, "estimated_total": round(total_est, 1)}
    if total_est >= 10.0:
        return {"market": "CORNERS_OVER_9_5", "prob": 0.54,
                "confidence": 12, "estimated_total": round(total_est, 1)}
    if total_est <= 8.0:
        return {"market": "CORNERS_UNDER_9_5", "prob": 0.54,
                "confidence": 12, "estimated_total": round(total_est, 1)}
    return None


def analyse_cards_market(stats_h: Optional[Dict],
                          stats_a: Optional[Dict],
                          fs_match: Optional[Dict]) -> Optional[Dict]:
    """
    Total Cartons Jaunes. Utilise FootyStats si disponible.
    Retourne None si pas de données (évite les valeurs statiques).
    """
    if not fs_match:
        return None

    h_cards = maybe_float(fs_match.get("home_avg_cards") or
                           fs_match.get("team_a_cards_avg"))
    a_cards  = maybe_float(fs_match.get("away_avg_cards") or
                            fs_match.get("team_b_cards_avg"))

    if not h_cards or not a_cards:
        return None  # Pas de données suffisantes

    total_est = h_cards + a_cards
    if total_est >= 4.5:
        return {"market": "CARDS_OVER_3_5", "prob": 0.58,
                "confidence": 12, "estimated_total": round(total_est, 1)}
    if total_est <= 2.5:
        return {"market": "CARDS_UNDER_3_5", "prob": 0.55,
                "confidence": 10, "estimated_total": round(total_est, 1)}
    return None

# ============================================================
# CORE ANALYSE — FIXTURE
def build_fixture_detail(match: Dict) -> Dict:
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

def is_pre_match_fixture(match: Dict) -> bool:
    return match.get("fixture", {}).get("status", {}).get("short") == "NS"

def is_priority_fixture(match: Dict) -> bool:
    text = (
        (match.get("league", {}).get("name") or "").lower() + " " +
        (match.get("teams", {}).get("home", {}).get("name") or "").lower() + " " +
        (match.get("teams", {}).get("away", {}).get("name") or "").lower()
    )
    return not any(kw in text for kw in EXCLUDED_KEYWORDS)

def analyse_fixture_core(fixture_id: str,
                          preloaded_fs_matches: Optional[List[Dict]] = None,
                          standings_cache: Optional[Dict[Tuple, Any]] = None,
                          ) -> Tuple[Optional[Dict], int]:
    """
    Analyse complète d'un fixture.
    Retourne (signal_dict, http_status) ou (None, 200) si NO_BET.
    """
    fixture_data, fixture_status = get_fixture_by_id(fixture_id)
    if fixture_status != 200:
        return fixture_data, fixture_status

    match = fixture_data["fixture"]
    detail = build_fixture_detail(match)

    # Gate 0 — match pas encore commencé
    if detail["status_short"] != "NS":
        return None, 200

    league_id = detail["league_id"]
    cfg = LEAGUE_CONFIG.get(league_id)
    if not cfg:
        return None, 200  # Ligue non configurée

    tier = cfg["tier"]
    season = detail["season"]
    home_id = detail["home_team_id"]
    away_id = detail["away_team_id"]

    # ---- 1. Statistiques équipes ----
    stats_h = get_stats_smart(home_id, league_id, season)
    stats_a = get_stats_smart(away_id, league_id, season)

    # ---- 2. xG source ----
    hxg = axg = 1.0
    xg_source = "proxy"
    fs_match_data = None

    fs_matches = preloaded_fs_matches or get_footystats_matches_cached()
    fs_raw = find_fs_match(detail["home"], detail["away"], fs_matches)

    if fs_raw:
        match_id = maybe_int(fs_raw.get("id"))
        if match_id:
            fs_match_data = get_fs_match_details(match_id) or fs_raw
        else:
            fs_match_data = fs_raw
        _hxg = maybe_float(fs_match_data.get("team_a_xg_prematch") or
                            fs_match_data.get("team_a_xg_avg"))
        _axg = maybe_float(fs_match_data.get("team_b_xg_prematch") or
                            fs_match_data.get("team_b_xg_avg"))
        if _hxg and _axg:
            hxg = _hxg
            axg = _axg
            xg_source = "footystats"

    # Fallback proxy via goals/jeux si FootyStats indispo
    if xg_source == "proxy" and stats_h and stats_a:
        h_for    = (stats_h.get("goals", {}).get("for", {}).get("total", {}).get("total") or 0)
        h_played = (stats_h.get("fixtures", {}).get("played", {}).get("total") or 1)
        a_for    = (stats_a.get("goals", {}).get("for", {}).get("total", {}).get("total") or 0)
        a_played = (stats_a.get("fixtures", {}).get("played", {}).get("total") or 1)
        hxg = max((h_for / h_played), 0.5) if h_played > 0 else 1.0
        axg = max((a_for / a_played), 0.5) if a_played > 0 else 1.0
        # Applique l'avantage domicile
        hxg *= cfg.get("home_adv", 1.10)

    # ---- 3. Probabilités Dixon-Coles ----
    dc_probs = calculate_probs_dc(hxg, axg, league_id)

    # ---- 4. DCS ----
    dcs = calculate_dcs(stats_h, stats_a, xg_source)

    # ---- 4b. H2H ----
    h2h_fixtures = get_h2h(home_id, away_id, last=5)
    h2h_data = analyse_h2h(h2h_fixtures, home_id)

    # ---- 5. Récupération cotes (pipeline 3 niveaux) ----
    odds_1x2: Optional[Dict[str, float]] = None
    odds_source = None

    # Niveau 1 — API-Football
    odds_1x2 = pick_best_1x2_odds(fixture_id, detail["home"], detail["away"])
    if odds_1x2:
        odds_source = "api_football"

    # Niveau 2 — FootyStats odds
    if not odds_1x2 and fs_match_data:
        h_odd = maybe_float(fs_match_data.get("odds_ft_1"))
        d_odd = maybe_float(fs_match_data.get("odds_ft_x"))
        a_odd = maybe_float(fs_match_data.get("odds_ft_2"))
        if h_odd and d_odd and a_odd:
            odds_1x2 = {"Home": h_odd, "Draw": d_odd, "Away": a_odd}
            odds_source = "footystats"

    # Niveau 3 — The Odds API
    if not odds_1x2:
        odds_1x2 = get_odds_api_1x2(detail)
        if odds_1x2:
            odds_source = "odds_api"

    # ---- 6. MODE BET (cotes disponibles) ----
    best_signal: Optional[Dict] = None

    if odds_1x2:
        side_map = {"Home": "H", "Draw": "D", "Away": "A"}
        min_edge_tier = MIN_EDGE.get(tier, 0.03)

        # Construction de tous les candidats valides
        candidates = []
        for side in ["Home", "Draw", "Away"]:
            odd = odds_1x2.get(side)
            if not odd:
                continue
            # Filtre plage de cotes
            if odd < MIN_ODD_ANY:
                continue
            if side == "Draw" and odd < MIN_ODD_DRAW:
                continue
            if odd > MAX_ODD_BET:
                continue

            prob = dc_probs.get(side_map[side], 0.0)
            edge = prob - (1.0 / odd)

            if edge <= min_edge_tier:
                continue

            # Filtre glamour outsider sur P0
            if tier == "P0" and side == "Away" and prob < 0.35:
                continue

            conf = calculate_confidence(prob, edge, dcs, tier)
            if conf < MIN_CONFIDENCE_BET:
                continue

            candidates.append({
                "side": side,
                "odd": odd,
                "prob": prob,
                "edge": edge,
                "conf": conf,
            })

        # Sélection: trier par probabilité décroissante (favoris en premier)
        # puis par edge si égalité — évite le biais outsider
        candidates.sort(key=lambda x: (x["prob"], x["edge"]), reverse=True)

        if candidates:
            best = candidates[0]
            side = best["side"]
            odd = best["odd"]
            prob = best["prob"]
            edge = best["edge"]
            conf = best["conf"]

            prefix = {"P0": "ELITE", "N1": "MAIN", "N2": "VALUE", "N3": "WATCH"}.get(tier, "WATCH")
            decision = f"{prefix}_{side.upper()}"
            level = {"P0": 3, "N1": 3, "N2": 2, "N3": 1}.get(tier, 1)
            bankroll = get_bankroll()
            stake = kelly_stake(prob, odd, bankroll)

            best_signal = {
                "mode": "BET",
                "market": "1X2",
                "side": side,
                "decision": decision,
                "level": level,
                "level_name": prefix,
                "odd": odd,
                "raw_edge": edge,
                "adjusted_edge": edge,
                "edge": edge,
                "prob": prob,
                "confidence": conf,
                "stake": stake,
                "odds_source": odds_source,
            }

    # ---- 7. MODE SIGNAL (pas de cotes ou aucun BET trouvé) ----
    if not best_signal:
        for dc_key, side in [("H", "Home"), ("A", "Away")]:
            prob = dc_probs.get(dc_key, 0.0)
            if prob < MIN_PROB_SIGNAL:
                continue

            # Vérification cohérence via predictions API
            pred = get_predictions_api(fixture_id)
            if pred:
                pct = pred.get("predictions", {}).get("percent", {})
                winner_key = max({"home": pct.get("home", "0%"), "draw": pct.get("draw", "0%"),
                                  "away": pct.get("away", "0%")}.items(),
                                 key=lambda x: float(str(x[1]).replace("%", "")) if x[1] else 0)
                api_side = winner_key[0].capitalize()
                if api_side and api_side != side:
                    continue  # Incohérence modèle / API

            conf = calculate_confidence(prob, 0.05, dcs, tier)
            if conf < MIN_CONFIDENCE_SIGNAL:
                continue

            prefix = {"P0": "ELITE", "N1": "MAIN", "N2": "VALUE", "N3": "WATCH"}.get(tier, "WATCH")
            decision = f"{prefix}_{side.upper()}_SIGNAL"
            level = {"P0": 3, "N1": 3, "N2": 2, "N3": 1}.get(tier, 1)

            best_signal = {
                "mode": "SIGNAL",
                "side": side,
                "decision": decision,
                "level": level,
                "level_name": prefix,
                "odd": None,
                "raw_edge": None,
                "adjusted_edge": None,
                "edge": None,
                "prob": prob,
                "confidence": conf,
                "stake": 0.0,
                "odds_source": None,
            }
            break

    if not best_signal:
        return None, 200

    # ---- 8. Marchés secondaires (BTTS, DC, O/U, Corners, Cards) ----
    btts_signal    = analyse_btts_market(fs_match_data, h2h_data, hxg, axg)
    dc_signal      = analyse_double_chance(dc_probs, odds_1x2, tier)
    ou_signal      = analyse_over_under(fs_match_data, h2h_data, hxg, axg)
    corners_signal = analyse_corners_market(stats_h, stats_a, fs_match_data)
    cards_signal   = analyse_cards_market(stats_h, stats_a, fs_match_data)

    secondary_markets = []
    for s in [btts_signal, dc_signal, ou_signal, corners_signal, cards_signal]:
        if s and s.get("confidence", 0) >= 10:
            secondary_markets.append(s)

    # ---- 9. Construction du signal complet ----
    fs_match_found = fs_raw is not None
    return {
        "status": "ok",
        "build_id": BUILD_ID,
        "fixture": detail,
        "fixture_id": detail["fixture_id"],
        "kickoff_utc": detail["kickoff_utc"],
        "league_id": league_id,
        "league_name": detail["league_name"],
        "tier": tier,
        "country": detail["country"],
        "home": detail["home"],
        "away": detail["away"],
        "hxg": round(hxg, 3),
        "axg": round(axg, 3),
        "xg_source": xg_source,
        "dcs": round(dcs, 3),
        "dc_probs": {k: round(v, 4) for k, v in dc_probs.items()},
        "h2h": h2h_data,
        "footystats_match_found": fs_match_found,
        "odds_1x2": odds_1x2,
        "secondary_markets": secondary_markets,
        **best_signal,
    }, 200

# ============================================================
# FORMATTERS TELEGRAM
# ============================================================
def log_signal(signal: Dict, tg_data: Optional[Dict] = None,
               tg_status: Optional[int] = None) -> Dict:
    """Enregistre un signal 1X2 en DB."""
    odd = signal.get("odd")
    # Pour mode SIGNAL sans cote → on enregistre quand même (sans odd)
    record = {
        "signal_uid": build_signal_uid(
            signal.get("fixture_id"), "1X2", signal.get("side"),
            signal.get("decision"), signal.get("level"),
            signal.get("kickoff_utc")
        ),
        "created_at": now_utc().isoformat(),
        "build_id": BUILD_ID,
        "fixture_id": maybe_int(signal.get("fixture_id")),
        "match_date": str(signal.get("kickoff_utc") or "")[:10],
        "kickoff_utc": signal.get("kickoff_utc"),
        "league_id": maybe_int(signal.get("league_id")),
        "league_name": signal.get("league_name"),
        "tier": signal.get("tier"),
        "country": signal.get("country"),
        "home_team": signal.get("home"),
        "away_team": signal.get("away"),
        "market": "1X2",
        "side": signal.get("side"),
        "mode": signal.get("mode", "BET"),
        "decision": signal.get("decision"),
        "level": maybe_int(signal.get("level")) or 0,
        "level_name": signal.get("level_name"),
        "odd": odd,
        "raw_edge": maybe_float(signal.get("raw_edge")),
        "adjusted_edge": maybe_float(signal.get("adjusted_edge")),
        "prob": maybe_float(signal.get("prob")),
        "hxg": maybe_float(signal.get("hxg")),
        "axg": maybe_float(signal.get("axg")),
        "xg_source": signal.get("xg_source"),
        "dcs": maybe_float(signal.get("dcs")),
        "confidence": maybe_int(signal.get("confidence")),
        "confluence_count": None,
        "confidence_count": None,
        "rationale": json_dumps_safe(signal.get("dc_probs", {})),
        "contextual_flags": json_dumps_safe({"tier": signal.get("tier"),
                                              "footystats": signal.get("footystats_match_found")}),
        "contextual_penalties": json_dumps_safe([]),
        "telegram_sent": tg_status == 200,
        "telegram_http_status": tg_status,
        "telegram_message_id": extract_telegram_message_id(tg_data),
        "stake": maybe_float(signal.get("stake")) or 0.0,
    }
    # Mode SIGNAL sans cote: enregistre quand même pour tracking de précision
    return save_signal_record(record)

# ============================================================
# SCHEDULER — SCAN PRINCIPAL
# ============================================================
def _run_full_scan_job_core(trigger: str = "scheduler") -> int:
    hour = now_utc().hour
    if trigger == "scheduler" and not (SCAN_START_HOUR <= hour <= SCAN_END_HOUR):
        logger.info("Scan skipped: outside scan window | hour=%s", hour)
        return 0

    date_str = utc_today_str()
    timestamp = now_utc().strftime("%H:%M UTC")

    fixtures_data, fixtures_status = get_fixtures_by_date(date_str)
    if fixtures_status != 200:
        send_telegram_message(f"⚠️ Scan {timestamp} — Impossible de récupérer les matchs.")
        raise RuntimeError(f"get_fixtures_by_date failed: status={fixtures_status}")

    fixtures = fixtures_data["data"].get("response", [])
    if not fixtures:
        logger.warning("Scan %s | get_fixtures_by_date returned empty response — possible API quota/rate-limit", trigger)
        send_telegram_message(f"⚠️ Scan {trigger} {timestamp}\nAPI-Football: réponse vide (quota ou rate-limit). Réessaie dans quelques minutes.")
        return 0
    fs_matches = get_footystats_matches_cached()
    standings_cache: Dict[Tuple, Any] = {}

    signals_sent = 0

    for match in fixtures:
        league_id = match.get("league", {}).get("id")
        if league_id not in LEAGUE_CONFIG:
            continue
        if not is_priority_fixture(match):
            continue
        if not is_pre_match_fixture(match):
            continue

        fixture_id = str(match.get("fixture", {}).get("id", ""))
        if not fixture_id:
            continue

        try:
            signal, status = analyse_fixture_core(
                fixture_id,
                preloaded_fs_matches=fs_matches,
                standings_cache=standings_cache,
            )
            if status == 200 and signal and signal.get("level", 0) >= MIN_SIGNAL_LEVEL_AUTO:
                msg = format_signal_message(signal)
                tg_data, tg_status = send_telegram_message(msg)
                log_signal(signal, tg_data=tg_data, tg_status=tg_status)
                signals_sent += 1
                logger.info("Signal | %s | %s %s | mode=%s | conf=%s",
                            signal.get("league_name"), signal.get("home"),
                            signal.get("away"), signal.get("mode"), signal.get("confidence"))
        except Exception:
            logger.exception("Scan failed | fixture_id=%s", fixture_id)

        if signals_sent >= MAX_SCAN_RESULTS:
            break

    if signals_sent == 0:
        send_telegram_message(
            f"🔍 Scan {trigger} {date_str} {timestamp}\n"
            f"Aucun signal détecté sur les ligues surveillées."
        )
    return signals_sent

def run_full_scan_job(trigger: str = "scheduler") -> Dict[str, Any]:
    if not SCAN_LOCK.acquire(blocking=False):
        logger.warning("Scan refused: already running | trigger=%s", trigger)
        return {"status": "busy", "message": "scan already running"}
    started = time.time()
    SCAN_STATE["running"] = True
    SCAN_STATE["started_at"] = now_utc().isoformat()
    SCAN_STATE["last_error"] = None
    try:
        signals_sent = _run_full_scan_job_core(trigger=trigger)
        SCAN_STATE["last_success"] = now_utc().isoformat()
        SCAN_STATE["last_signals_sent"] = signals_sent
        SCAN_STATE["last_duration_seconds"] = round(time.time() - started, 2)
        logger.info("Scan completed | trigger=%s | signals=%s | duration=%ss",
                    trigger, signals_sent, SCAN_STATE["last_duration_seconds"])
        return {"status": "ok", "signals_sent": signals_sent,
                "duration_seconds": SCAN_STATE["last_duration_seconds"]}
    except Exception as exc:
        SCAN_STATE["last_error"] = str(exc)
        SCAN_STATE["last_duration_seconds"] = round(time.time() - started, 2)
        logger.exception("Scan crashed | trigger=%s", trigger)
        return {"status": "error", "message": str(exc),
                "duration_seconds": SCAN_STATE["last_duration_seconds"]}
    finally:
        SCAN_STATE["running"] = False
        SCAN_LOCK.release()

def resolve_pending_signals_job() -> None:
    if not AUTO_RESOLVE_ENABLED:
        return
    try:
        result = resolve_pending_signals(limit=RESOLVE_BATCH_LIMIT)
        logger.info("resolve_job done | checked=%s | resolved=%s",
                    result.get("checked_fixtures"), result.get("resolved_signals"))
    except Exception:
        logger.exception("resolve_pending_signals_job failed")

def _scheduler_loop() -> None:
    schedule.every().hour.at(":00").do(lambda: run_full_scan_job(trigger="scheduler"))
    schedule.every(30).minutes.do(resolve_pending_signals_job)
    while True:
        try:
            schedule.run_pending()
        except Exception:
            logger.exception("Scheduler loop failed")
        time.sleep(30)

def start_scheduler_if_enabled() -> None:
    if not ENABLE_SCHEDULER:
        logger.info("Scheduler disabled (ENABLE_SCHEDULER=0)")
        return
    logger.info("Scheduler enabled — scan every hour %sh00–%sh00 UTC",
                SCAN_START_HOUR, SCAN_END_HOUR)
    threading.Thread(target=_scheduler_loop, daemon=True, name="ApexScheduler").start()

# ============================================================
# TELEGRAM WEBHOOK
# ============================================================
def _handle_telegram_command(cmd: str) -> None:
    if cmd == "/scan":
        now_ts = time.time()
        last_manual = SCAN_STATE.get("last_manual_trigger_at")
        if SCAN_STATE.get("running"):
            send_telegram_message("⏳ Un scan est déjà en cours. Attends sa fin.")
            return
        if last_manual and (now_ts - last_manual) < SCAN_COOLDOWN_SECONDS:
            wait_left = int(SCAN_COOLDOWN_SECONDS - (now_ts - last_manual))
            send_telegram_message(f"⏳ Cooldown actif. Réessaie dans {wait_left}s.")
            return
        SCAN_STATE["last_manual_trigger_at"] = now_ts
        send_telegram_message("🔍 Scan lancé manuellement...\nAnalyse Dixon-Coles en cours.")
        threading.Thread(target=lambda: run_full_scan_job(trigger="manual"),
                         daemon=True, name="ManualScan").start()

    elif cmd == "/status":
        bankroll = get_bankroll()
        send_telegram_message(
            f"✅ <b>APEX-HYBRID-ULTIMATE v2.0</b>\n\n"
            f"🔧 Build: <code>{BUILD_ID}</code>\n"
            f"🕐 UTC: {now_utc().strftime('%Y-%m-%d %H:%M')}\n\n"
            f"API-Football : {'✅' if API_KEY else '❌'}\n"
            f"FootyStats   : {'✅' if FOOTYSTATS_KEY else '❌'}\n"
            f"Odds API     : {'✅' if ODDS_API_KEY else '❌'}\n"
            f"Telegram     : ✅\n"
            f"Agent Claude : {'✅' if ANTHROPIC_API_KEY else '❌ ANTHROPIC_API_KEY manquante'}\n\n"
            f"💰 Bankroll: <b>{bankroll:.2f}</b>\n"
            f"⏰ Scheduler: {SCAN_START_HOUR}h–{SCAN_END_HOUR}h UTC\n"
            f"🎯 Moteur: Dixon-Coles + Kelly\n"
            f"🧠 Agent: Claude Haiku (texte libre)"
        )

    elif cmd == "/bankroll":
        bankroll = get_bankroll()
        send_telegram_message(f"💰 Bankroll actuelle: <b>{bankroll:.2f}</b>")

    elif cmd == "/ping":
        send_telegram_message(f"🏓 Pong! Bot actif — {now_utc().strftime('%H:%M')} UTC")

    elif cmd == "/help":
        send_telegram_message(
            "📋 <b>Commandes APEX-ULTIMATE</b>\n\n"
            "/scan — Lance un scan complet\n"
            "/status — État bot + bankroll\n"
            "/bankroll — Bankroll actuelle\n"
            "/ping — Test connexion\n"
            "/help — Cette aide\n\n"
            "🧠 <b>Mode Agent (texte libre)</b>\n"
            "Tu peux aussi écrire directement :\n"
            "<i>\"Arsenal vs Chelsea demain\"</i>\n"
            "<i>\"Analyse Real Madrid Barca\"</i>\n"
            "<i>\"C'est quoi le Kelly ?\"</i>\n\n"
            f"⏰ Scan auto: toutes les heures {SCAN_START_HOUR}h–{SCAN_END_HOUR}h UTC\n"
            "🎲 Modes: BET (cotes + Kelly) | SIGNAL (modèle seul)"
        )

# ============================================================
# AGENT CLAUDE — CERVEAU CONVERSATIONNEL
# ============================================================
AGENT_SYSTEM_PROMPT = """Tu es APEX-AGENT, un assistant expert en analyse de paris sportifs.
Tu travailles avec le moteur APEX-HYBRID-ULTIMATE basé sur Dixon-Coles et Kelly.

Quand l'utilisateur te parle d'un match, tu dois extraire :
- Les noms des équipes (home, away)
- La date si mentionnée (optionnel)
- La ligue si mentionnée (optionnel)

Si l'utilisateur demande une analyse de match, réponds UNIQUEMENT avec du JSON valide :
{"action": "analyse", "home": "NomEquipe1", "away": "NomEquipe2", "date": "YYYY-MM-DD ou null", "league": "nom ou null"}

Si l'utilisateur pose une question générale sur les paris, les probabilités, la stratégie Kelly, ou ton fonctionnement, réponds en français de façon concise et experte.

Si le message n'a rien à voir avec le football ou les paris, réponds :
{"action": "hors_sujet"}

Ne produis JAMAIS de JSON sauf dans les deux cas ci-dessus."""

def analyze_user_intent_claude(user_text: str) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Utilise Claude pour comprendre l'intention de l'utilisateur.
    Retourne (data_dict, None) si analyse de match détectée,
    ou (None, texte_réponse) pour une conversation générale.
    """
    if not ANTHROPIC_API_KEY:
        return None, "⚠️ ANTHROPIC_API_KEY manquante. Configure-la sur Render."

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",  # rapide + économique
            max_tokens=512,
            system=AGENT_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_text}]
        )
        content = message.content[0].text.strip()

        # Tente de parser en JSON
        try:
            data = json.loads(content)
            if data.get("action") == "analyse" and data.get("home") and data.get("away"):
                return data, None
            elif data.get("action") == "hors_sujet":
                return None, "⚽ Je suis spécialisé dans l'analyse de matchs de football. Dis-moi un match à analyser !"
            else:
                return None, content  # Réponse conversationnelle directe
        except json.JSONDecodeError:
            # Claude a répondu en texte libre (réponse générale)
            return None, content

    except anthropic.APIError as e:
        logger.error("Claude API error: %s", e)
        return None, f"⚠️ Erreur API Claude: {str(e)[:100]}"
    except Exception as e:
        logger.exception("analyze_user_intent_claude failed")
        return None, "⚠️ Je suis indisponible momentanément. Réessaie."


def handle_agent_message(user_text: str) -> None:
    """
    Traite un message libre (non-commande) envoyé au bot Telegram.
    Orchestration : Claude → moteur APEX → Telegram.
    """
    # 1. Claude analyse l'intention
    send_telegram_message("🧠 Analyse en cours...")

    data, response_text = analyze_user_intent_claude(user_text)

    # 2. Réponse conversationnelle pure
    if response_text is not None:
        send_telegram_message(response_text)
        return

    # 3. Demande d'analyse de match détectée
    home = data.get("home", "")
    away = data.get("away", "")
    league_hint = data.get("league") or ""
    date_hint = data.get("date") or utc_today_str()

    send_telegram_message(
        f"🔍 Match détecté : <b>{home} vs {away}</b>\n"
        f"📅 Date: {date_hint}\n"
        f"Récupération des données...",
    )

    # 4. Cherche le fixture_id via API-Football
    params: Dict[str, Any] = {"date": date_hint}
    fixtures_data, status = get_fixtures_by_date(date_hint)
    if status != 200:
        send_telegram_message(f"❌ Impossible de récupérer les fixtures pour {date_hint}.")
        return

    fixtures = fixtures_data["data"].get("response", [])
    best_fixture = None
    best_score = 0.0

    for f in fixtures:
        fh = f.get("teams", {}).get("home", {}).get("name", "")
        fa = f.get("teams", {}).get("away", {}).get("name", "")
        score = (team_name_similarity(home, fh) + team_name_similarity(away, fa)) / 2
        if score > best_score:
            best_score = score
            best_fixture = f

    if not best_fixture or best_score < 0.55:
        # Réessaie sans filtre date (matchs des 3 prochains jours)
        send_telegram_message(
            f"⚠️ Match introuvable le {date_hint}.\n"
            f"Essaie avec la date exacte : ex. *\"Arsenal Bournemouth 2026-04-15\"*"
        )
        return

    fixture_id = str(best_fixture.get("fixture", {}).get("id", ""))
    detail = build_fixture_detail(best_fixture)
    league_name = detail.get("league_name", "")
    kickoff = detail.get("kickoff_utc", "")

    send_telegram_message(
        f"✅ Fixture trouvé : <b>{detail['home']} vs {detail['away']}</b>\n"
        f"🏆 {league_name} — {format_match_time(kickoff)}\n"
        f"⚙️ Lancement du moteur Dixon-Coles..."
    )

    # 5. Analyse complète via le moteur
    try:
        signal, sig_status = analyse_fixture_core(fixture_id)
    except Exception as exc:
        logger.exception("analyse_fixture_core failed in agent | fixture_id=%s", fixture_id)
        send_telegram_message(f"❌ Erreur moteur sur fixture {fixture_id}: {str(exc)[:150]}")
        return

    if sig_status != 200:
        send_telegram_message(f"❌ Erreur API sur fixture {fixture_id}.")
        return

    if signal is None:
        # Pas de signal BET/SIGNAL — on donne quand même les probs
        send_telegram_message(
            f"📊 <b>APEX-AGENT — {detail['home']} vs {detail['away']}</b>\n"
            f"🏆 {league_name} | {format_match_time(kickoff)}\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"🧮 Résultat: <b>NO BET</b>\n"
            f"Aucun edge détecté dans les paramètres actuels.\n\n"
            f"Consulte /fixture-analyse?fixture_id={fixture_id} pour le détail complet."
        )
        return

    # 6. Signal trouvé — formater et envoyer
    msg = format_signal_message(signal)
    tg_data, tg_status = send_telegram_message(msg)
    log_signal(signal, tg_data=tg_data, tg_status=tg_status)


@app.route("/webhook", methods=["POST"])
def telegram_webhook():
    if WEBHOOK_SECRET:
        incoming = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if incoming != WEBHOOK_SECRET:
            return jsonify({"status": "forbidden"}), 403
    update = request.get_json(silent=True) or {}
    message = update.get("message") or update.get("channel_post") or {}
    text = (message.get("text") or "").strip()
    if not text:
        return ok({"status": "ok", "ignored": True})

    # Commande explicite (débute par /)
    if text.startswith("/"):
        cmd = text.split()[0].lower().split("@")[0]
        _handle_telegram_command(cmd)
    else:
        # Message libre → Agent Claude
        threading.Thread(
            target=handle_agent_message,
            args=(text,),
            daemon=True,
            name="AgentThread"
        ).start()

    return ok({"status": "ok"})

@app.route("/set-webhook")
def set_webhook_route():
    webhook_url = request.args.get("url", "").strip()
    if not webhook_url:
        webhook_url = request.url_root.rstrip("/") + "/webhook"
    payload, status = set_telegram_webhook(webhook_url)
    return ok({"status": "ok" if status == 200 else "error",
               "webhook_url": webhook_url, "result": payload}, status)

# ============================================================
# FLASK ROUTES
# ============================================================
@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "build_id": BUILD_ID,
        "scheduler_enabled": ENABLE_SCHEDULER,
        "scan_running": SCAN_STATE.get("running"),
        "scan_started_at": SCAN_STATE.get("started_at"),
        "last_success": SCAN_STATE.get("last_success"),
        "last_error": SCAN_STATE.get("last_error"),
        "last_duration_seconds": SCAN_STATE.get("last_duration_seconds"),
        "last_signals_sent": SCAN_STATE.get("last_signals_sent"),
        "bankroll": get_bankroll(),
        "config": {
            "api_football": bool(API_KEY),
            "footystats": bool(FOOTYSTATS_KEY),
            "odds_api": bool(ODDS_API_KEY),
            "telegram_bot": bool(BOT_TOKEN),
            "telegram_chat": bool(CHAT_ID),
            "webhook_secret": bool(WEBHOOK_SECRET),
            "engine": "Dixon-Coles + Kelly",
            "leagues_configured": len(LEAGUE_CONFIG),
        },
    }), 200

@app.route("/")
def home():
    return ok({
        "status": "ok",
        "build_id": BUILD_ID,
        "engine": "Dixon-Coles + Kelly",
        "routes": [
            "/", "/health", "/version", "/ping",
            "/telegram-test", "/set-webhook", "/webhook (POST)",
            "/footystats-test", "/odds-api-test?sport=soccer_epl",
            "/fixtures-today",
            "/fixture-analyse?fixture_id=...",
            "/scan-trigger",
            "/bankroll", "/update-bankroll?amount=...",
            "/signals-recent?limit=20",
            "/signals-summary",
            "/resolve-pending-signals",
            "/admin/clean-null-odds",
        ],
        "config": {
            "bot_token_present": bool(BOT_TOKEN),
            "chat_id_present": bool(CHAT_ID),
            "api_key_present": bool(API_KEY),
            "footystats_key_present": bool(FOOTYSTATS_KEY),
            "odds_api_key_present": bool(ODDS_API_KEY),
            "leagues_configured": len(LEAGUE_CONFIG),
        },
    })

@app.route("/version")
def version():
    return ok({
        "status": "ok",
        "build_id": BUILD_ID,
        "engine": "Dixon-Coles + Kelly",
        "tiers": {"P0": "UEFA", "N1": "Top5", "N2": "Secondary", "N3": "Others"},
        "config": {
            "api_football": bool(API_KEY),
            "footystats": bool(FOOTYSTATS_KEY),
            "odds_api": bool(ODDS_API_KEY),
            "telegram_bot": bool(BOT_TOKEN),
        },
    })

@app.route("/ping")
def ping():
    return ok({"status": "ok", "message": "pong",
                "utc_now": now_utc().isoformat(), "build_id": BUILD_ID})

@app.route("/telegram-test")
def telegram_test():
    payload, status_code = send_telegram_message(
        f"✅ Test APEX-ULTIMATE OK\n{now_utc().isoformat()}\nbuild={BUILD_ID}"
    )
    return ok({"status": "ok" if status_code == 200 else "error",
               "message_sent": status_code == 200,
               "telegram_http_status": status_code,
               "telegram_status": payload, "build_id": BUILD_ID},
              200 if status_code == 200 else 500)

@app.route("/footystats-test")
def footystats_test():
    matches = get_footystats_matches_cached()
    sample = matches[0] if matches else None
    return ok({
        "status": "ok",
        "build_id": BUILD_ID,
        "footystats_key_present": bool(FOOTYSTATS_KEY),
        "count": len(matches),
        "sample": {
            "id": sample.get("id") if isinstance(sample, dict) else None,
            "home_name": sample.get("home_name") if isinstance(sample, dict) else None,
            "away_name": sample.get("away_name") if isinstance(sample, dict) else None,
        } if sample else None,
    })

@app.route("/odds-api-test")
def odds_api_test():
    if not ODDS_API_KEY:
        return ok({"status": "error", "message": "ODDS_API_KEY is missing"}, 500)
    sport_key = request.args.get("sport", "soccer_epl").strip()
    payload, status = call_odds_api(sport_key)
    if status != 200:
        return ok({"status": "error", "odds_api_status": payload}, 500)
    events = payload.get("data") or []
    sample = events[0] if events else None
    return ok({"status": "ok", "sport_key": sport_key, "count": len(events),
               "sample": {k: sample.get(k) for k in ["id", "home_team", "away_team", "commence_time"]}
               if isinstance(sample, dict) else None})

@app.route("/fixtures-today")
def fixtures_today():
    date_str = request.args.get("date", utc_today_str()).strip()
    data, status_code = get_fixtures_by_date(date_str)
    if status_code != 200:
        return ok(data, status_code)
    fixtures = data["data"].get("response", [])
    filtered = []
    for match in fixtures:
        lid = match.get("league", {}).get("id")
        if lid not in LEAGUE_CONFIG:
            continue
        if not is_priority_fixture(match):
            continue
        if not is_pre_match_fixture(match):
            continue
        d = build_fixture_detail(match)
        cfg = LEAGUE_CONFIG.get(lid, {})
        filtered.append({
            "fixture_id": d["fixture_id"],
            "kickoff_utc": d["kickoff_utc"],
            "league_id": d["league_id"],
            "league_name": d["league_name"],
            "tier": cfg.get("tier"),
            "country": d["country"],
            "home": d["home"],
            "away": d["away"],
        })
    filtered.sort(key=lambda x: x["kickoff_utc"] or "")
    return ok({"status": "ok", "build_id": BUILD_ID, "date": date_str,
               "count": len(filtered), "fixtures": filtered})

@app.route("/fixture-analyse")
def fixture_analyse():
    fixture_id = request.args.get("fixture_id", "").strip()
    if not fixture_id or not fixture_id.isdigit():
        return err("fixture_id numeric requis", 400)
    signal, status = analyse_fixture_core(fixture_id)
    if status != 200:
        return ok(signal, status)
    if signal is None:
        return ok({"status": "ok", "decision": "NO_BET", "fixture_id": fixture_id})
    # Envoi Telegram optionnel
    if request.args.get("send_telegram") == "1" and signal:
        tg_data, tg_status = send_telegram_message(format_signal_message(signal))
        log_signal(signal, tg_data=tg_data, tg_status=tg_status)
        signal["telegram_sent"] = tg_status == 200
    return ok(signal)

@app.route("/scan-trigger")
def scan_trigger():
    """Déclenche un scan manuel via HTTP."""
    if SCAN_STATE.get("running"):
        return ok({"status": "busy", "message": "scan already running"}, 202)
    threading.Thread(target=lambda: run_full_scan_job(trigger="http"),
                     daemon=True, name="HttpScan").start()
    return ok({"status": "ok", "message": "scan triggered"})

@app.route("/bankroll")
def bankroll_route():
    amount = get_bankroll()
    return ok({"status": "ok", "bankroll": amount, "currency": "units"})

@app.route("/update-bankroll")
def update_bankroll():
    amount_str = request.args.get("amount", "").strip()
    amount = maybe_float(amount_str)
    if amount is None or amount <= 0:
        return err("amount doit être un nombre positif", 400)
    set_bankroll(amount)
    return ok({"status": "ok", "bankroll": amount})

@app.route("/resolve-pending-signals")
def resolve_pending_signals_route():
    limit = maybe_int(request.args.get("limit")) or RESOLVE_BATCH_LIMIT
    result = resolve_pending_signals(limit=limit)
    return ok(result)

@app.route("/signals-summary")
def signals_summary():
    with closing(db_connect()) as conn:
        overall = conn.execute("""
            SELECT
                COUNT(*) AS total_bets,
                SUM(CASE WHEN bet_outcome='win' THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN bet_outcome='loss' THEN 1 ELSE 0 END) AS losses,
                SUM(CASE WHEN bet_outcome='void' THEN 1 ELSE 0 END) AS voids,
                ROUND(COALESCE(SUM(profit),0),4) AS profit_total,
                ROUND(COALESCE(SUM(stake),0),4) AS stake_total,
                ROUND(CASE WHEN COALESCE(SUM(stake),0)>0
                    THEN (SUM(profit)/SUM(stake))*100 ELSE 0 END,2) AS roi_percent
            FROM signals WHERE result_status='resolved'
        """).fetchone()
        by_tier = conn.execute("""
            SELECT tier,
                COUNT(*) AS total_bets,
                SUM(CASE WHEN bet_outcome='win' THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN bet_outcome='loss' THEN 1 ELSE 0 END) AS losses,
                ROUND(COALESCE(SUM(profit),0),4) AS profit_total,
                ROUND(CASE WHEN COALESCE(SUM(stake),0)>0
                    THEN (SUM(profit)/SUM(stake))*100 ELSE 0 END,2) AS roi_percent
            FROM signals WHERE result_status='resolved'
            GROUP BY tier ORDER BY profit_total DESC
        """).fetchall()
        by_mode = conn.execute("""
            SELECT mode,
                COUNT(*) AS total_bets,
                SUM(CASE WHEN bet_outcome='win' THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN bet_outcome='loss' THEN 1 ELSE 0 END) AS losses,
                ROUND(COALESCE(SUM(profit),0),4) AS profit_total
            FROM signals WHERE result_status='resolved'
            GROUP BY mode
        """).fetchall()
        by_side = conn.execute("""
            SELECT side,
                COUNT(*) AS total_bets,
                SUM(CASE WHEN bet_outcome='win' THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN bet_outcome='loss' THEN 1 ELSE 0 END) AS losses,
                ROUND(COALESCE(SUM(profit),0),4) AS profit_total
            FROM signals WHERE result_status='resolved'
            GROUP BY side ORDER BY profit_total DESC
        """).fetchall()
    return ok({
        "status": "ok",
        "build_id": BUILD_ID,
        "bankroll": get_bankroll(),
        "overall": dict(overall) if overall else {},
        "by_tier": [dict(r) for r in by_tier],
        "by_mode": [dict(r) for r in by_mode],
        "by_side": [dict(r) for r in by_side],
    })

@app.route("/signals-recent")
def signals_recent():
    limit = maybe_int(request.args.get("limit")) or 20
    tier = request.args.get("tier")
    mode = request.args.get("mode")
    with closing(db_connect()) as conn:
        query = "SELECT * FROM signals"
        conditions = []
        params = []
        if tier:
            conditions.append("tier=?")
            params.append(tier)
        if mode:
            conditions.append("mode=?")
            params.append(mode.upper())
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(query, params).fetchall()
    return ok({"status": "ok", "count": len(rows),
               "signals": [dict(r) for r in rows]})

@app.route("/admin/clean-null-odds")
def clean_null_odds():
    with closing(db_connect()) as conn:
        cur = conn.execute("""
            DELETE FROM signals
            WHERE market='1X2' AND mode='BET' AND odd IS NULL AND result_status='pending'
        """)
        deleted = cur.rowcount
        conn.commit()
        remaining = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    logger.info("clean-null-odds: deleted=%s remaining=%s", deleted, remaining)
    return ok({"status": "ok", "deleted": deleted, "remaining": remaining})

# ============================================================
# DÉMARRAGE
# ============================================================
start_scheduler_if_enabled()
init_db()
logger.info("DB initialized at %s | BUILD=%s", DB_PATH, BUILD_ID)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    logger.info("🚀 BUILD_ID=%s | port=%s", BUILD_ID, port)
    app.run(host="0.0.0.0", port=port)
