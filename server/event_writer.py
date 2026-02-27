import json
import os
import sys
import sqlite3
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

# Allow importing from scripts/ (sibling directory)
_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_SCRIPTS_DIR = os.path.join(_ROOT_DIR, "scripts")
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

try:
    from migrate_db import migrate_connection
except ImportError:
    migrate_connection = None  # type: ignore

DB_PATH = os.getenv("PIVOT_DB", "data/pivot_events.sqlite")
HOST = os.getenv("EVENT_WRITER_BIND", "127.0.0.1")
PORT = int(os.getenv("EVENT_WRITER_PORT", "5002"))
_SCHEMA_READY = False
_DEFAULT_CORS_ORIGINS = "http://127.0.0.1:3000,http://localhost:3000"


def _parse_allowed_origins() -> list[str]:
    origins = [
        origin.strip()
        for origin in os.getenv("ML_CORS_ORIGINS", _DEFAULT_CORS_ORIGINS).split(",")
        if origin.strip()
    ]
    return origins or ["http://127.0.0.1:3000"]


ALLOWED_ORIGINS = _parse_allowed_origins()


def _cors_origin(request_origin: str | None) -> str:
    if request_origin and request_origin in ALLOWED_ORIGINS:
        return request_origin
    return ALLOWED_ORIGINS[0]


def connect():
    global _SCHEMA_READY
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-200000;")
    if not _SCHEMA_READY:
        _ensure_schema(conn)
        _SCHEMA_READY = True
    return conn


def _ensure_schema(conn):
    """Run schema migrations via migrate_db if available, else minimal ensure."""
    if migrate_connection is not None:
        migrate_connection(conn, verbose=False)
        return
    # Fallback: ensure tables exist with minimal DDL (standalone mode)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS bar_data (
            symbol TEXT NOT NULL, ts INTEGER NOT NULL,
            open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL,
            close REAL NOT NULL, volume REAL, bar_interval_sec INTEGER,
            PRIMARY KEY (symbol, ts, bar_interval_sec));"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS touch_events (
            event_id TEXT PRIMARY KEY, symbol TEXT NOT NULL,
            ts_event INTEGER NOT NULL, session TEXT,
            level_type TEXT NOT NULL, level_price REAL NOT NULL,
            touch_price REAL NOT NULL, touch_side INTEGER,
            distance_bps REAL NOT NULL, is_first_touch_today INTEGER DEFAULT 0,
            touch_count_today INTEGER DEFAULT 1, confluence_count INTEGER DEFAULT 0,
            confluence_types TEXT, ema9 REAL, ema21 REAL, ema_state INTEGER,
            vwap REAL, vwap_dist_bps REAL, atr REAL, rv_30 REAL, rv_regime INTEGER,
            iv_rv_state INTEGER, gamma_mode INTEGER, gamma_flip REAL,
            gamma_flip_dist_bps REAL, gamma_confidence INTEGER,
            oi_concentration_top5 REAL, zero_dte_share REAL, data_quality REAL,
            bar_interval_sec INTEGER, source TEXT, created_at INTEGER NOT NULL,
            vpoc REAL, vpoc_dist_bps REAL, volume_at_level REAL,
            mtf_confluence INTEGER DEFAULT 0, mtf_confluence_types TEXT,
            weekly_pivot REAL, monthly_pivot REAL, level_age_days INTEGER DEFAULT 0,
            hist_reject_rate REAL, hist_break_rate REAL, hist_sample_size INTEGER DEFAULT 0,
            regime_type INTEGER, overnight_gap_atr REAL, or_high REAL, or_low REAL,
            or_size_atr REAL, or_breakout INTEGER, or_high_dist_bps REAL,
            or_low_dist_bps REAL, session_std REAL, sigma_band_position REAL,
            distance_to_upper_sigma_bps REAL, distance_to_lower_sigma_bps REAL);"""
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_touch_natural_key "
        "ON touch_events(symbol, ts_event, level_type, level_price, bar_interval_sec);"
    )
    conn.commit()


def read_schema_version(conn):
    try:
        row = conn.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone()
    except sqlite3.Error:
        return None
    if not row:
        return None
    try:
        return int(row[0])
    except (TypeError, ValueError):
        return None


def insert_events(conn, events):
    if not events:
        return 0
    columns = [
        "event_id",
        "symbol",
        "ts_event",
        "session",
        "level_type",
        "level_price",
        "touch_price",
        "touch_side",
        "distance_bps",
        "is_first_touch_today",
        "touch_count_today",
        "confluence_count",
        "confluence_types",
        "ema9",
        "ema21",
        "ema_state",
        "vwap",
        "vwap_dist_bps",
        "atr",
        "rv_30",
        "rv_regime",
        "iv_rv_state",
        "gamma_mode",
        "gamma_flip",
        "gamma_flip_dist_bps",
        "gamma_confidence",
        "oi_concentration_top5",
        "zero_dte_share",
        "data_quality",
        "bar_interval_sec",
        "source",
        "created_at",
        "vpoc",
        "vpoc_dist_bps",
        "volume_at_level",
        "mtf_confluence",
        "mtf_confluence_types",
        "weekly_pivot",
        "monthly_pivot",
        "level_age_days",
        "hist_reject_rate",
        "hist_break_rate",
        "hist_sample_size",
        # v3 features
        "regime_type",
        "overnight_gap_atr",
        "or_high",
        "or_low",
        "or_size_atr",
        "or_breakout",
        "or_high_dist_bps",
        "or_low_dist_bps",
        "session_std",
        "sigma_band_position",
        "distance_to_upper_sigma_bps",
        "distance_to_lower_sigma_bps",
    ]
    placeholders = ", ".join(["?"] * len(columns))
    sql = f"INSERT OR IGNORE INTO touch_events ({', '.join(columns)}) VALUES ({placeholders})"

    values = []
    for ev in events:
        row = [ev.get(col) for col in columns]
        values.append(row)

    before = conn.total_changes
    conn.executemany(sql, values)
    return conn.total_changes - before


def insert_bars(conn, bars, max_interval_sec=3600):
    if not bars:
        return 0
    sql = """
        INSERT OR REPLACE INTO bar_data (symbol, ts, open, high, low, close, volume, bar_interval_sec)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    values = []
    for bar in bars:
        interval = bar.get("bar_interval_sec")
        if interval is None:
            interval = bar.get("interval_sec")
        if interval is None:
            continue
        try:
            interval = int(interval)
        except (TypeError, ValueError):
            continue
        if interval > max_interval_sec:
            continue

        values.append(
            (
                bar["symbol"],
                bar["ts"],
                bar["open"],
                bar["high"],
                bar["low"],
                bar["close"],
                bar.get("volume", 0),
                interval,
            )
        )
    if not values:
        return 0
    before = conn.total_changes
    conn.executemany(sql, values)
    return conn.total_changes - before


def aggregate_daily_candles(conn, symbol, limit=200):
    """Aggregate 1-minute (or 5-minute) bars into daily OHLCV candles.

    Only uses RTH bars: 14:30-21:00 UTC (9:30 AM - 4:00 PM ET).
    Returns list of dicts with keys: time (epoch seconds), open, high, low,
    close, volume — matching the dashboard candle format.
    """
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT
                date(ts / 1000, 'unixepoch') AS trade_date,
                ts, open, high, low, close, volume,
                ROW_NUMBER() OVER (
                    PARTITION BY date(ts / 1000, 'unixepoch') ORDER BY ts ASC
                ) AS rn_first,
                ROW_NUMBER() OVER (
                    PARTITION BY date(ts / 1000, 'unixepoch') ORDER BY ts DESC
                ) AS rn_last
            FROM bar_data
            WHERE symbol = ?
              AND bar_interval_sec <= 300
              AND time(ts / 1000, 'unixepoch') >= '14:30:00'
              AND time(ts / 1000, 'unixepoch') < '21:00:00'
        )
        SELECT
            trade_date,
            MIN(CASE WHEN rn_first = 1 THEN ts END) / 1000 AS time_sec,
            MIN(CASE WHEN rn_first = 1 THEN open END)       AS day_open,
            MAX(high)                                        AS day_high,
            MIN(low)                                         AS day_low,
            MAX(CASE WHEN rn_last = 1 THEN close END)       AS day_close,
            CAST(SUM(volume) AS INTEGER)                     AS day_volume
        FROM ranked
        GROUP BY trade_date
        HAVING day_open IS NOT NULL AND day_close IS NOT NULL
        ORDER BY trade_date ASC
        LIMIT ?
        """,
        (symbol.upper(), limit),
    ).fetchall()

    candles = []
    for row in rows:
        candles.append(
            {
                "time": row[1],
                "open": round(row[2], 2),
                "high": round(row[3], 2),
                "low": round(row[4], 2),
                "close": round(row[5], 2),
                "volume": row[6] or 0,
            }
        )
    return {
        "symbol": symbol.upper(),
        "candles": candles,
        "source": "persisted_bars",
        "days": len(candles),
    }


class WriterHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code, payload):
        response = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", _cors_origin(self.headers.get("Origin")))
        self.send_header("Vary", "Origin")
        self.end_headers()
        self.wfile.write(response)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", _cors_origin(self.headers.get("Origin")))
        self.send_header("Vary", "Origin")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.send_header("Access-Control-Max-Age", "600")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path in ("/", "/health"):
            conn = connect()
            try:
                schema_version = read_schema_version(conn)
            finally:
                conn.close()
            self._send_json(
                200,
                {
                    "status": "ok",
                    "service": "event_writer",
                    "db": DB_PATH,
                    "schema_version": schema_version,
                },
            )
            return

        if parsed.path == "/daily-candles":
            params = parse_qs(parsed.query)
            symbol = params.get("symbol", ["SPY"])[0]
            limit = min(int(params.get("limit", ["200"])[0]), 500)
            conn = connect()
            try:
                result = aggregate_daily_candles(conn, symbol, limit)
                self._send_json(200, result)
            except Exception as exc:
                self._send_json(500, {"error": str(exc)})
            finally:
                conn.close()
            return

        self.send_response(404)
        self.end_headers()
        self.wfile.write(b"Not found")

    def do_POST(self):
        parsed = urlparse(self.path)
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8") if length else ""
        try:
            payload = json.loads(body) if body else {}
        except json.JSONDecodeError:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Invalid JSON")
            return

        conn = connect()
        try:
            if parsed.path == "/events":
                events = payload.get("events", [])
                inserted = insert_events(conn, events)
                conn.commit()
                self._send_json(200, {"inserted": inserted})
                return

            if parsed.path == "/bars":
                bars = payload.get("bars", [])
                inserted = insert_bars(conn, bars)
                conn.commit()
                self._send_json(200, {"inserted": inserted})
                return
        finally:
            conn.close()

        self.send_response(404)
        self.end_headers()
        self.wfile.write(b"Not found")


def run_server():
    server = HTTPServer((HOST, PORT), WriterHandler)
    print(f"Event writer running at http://{HOST}:{PORT}")
    server.serve_forever()


if __name__ == "__main__":
    run_server()
