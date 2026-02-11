import json
import os
import sqlite3
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

DB_PATH = os.getenv("PIVOT_DB", "data/pivot_events.sqlite")
HOST = os.getenv("EVENT_WRITER_BIND", "127.0.0.1")
PORT = int(os.getenv("EVENT_WRITER_PORT", "5002"))


def connect():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-200000;")
    ensure_bar_schema(conn)
    return conn


def ensure_bar_schema(conn):
    cur = conn.execute("PRAGMA table_info(bar_data)")
    cols = {row[1] for row in cur.fetchall()}
    if "bar_interval_sec" not in cols:
        conn.execute("ALTER TABLE bar_data ADD COLUMN bar_interval_sec INTEGER")
        conn.commit()


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
    ]
    placeholders = ", ".join(["?"] * len(columns))
    sql = f"INSERT OR IGNORE INTO touch_events ({', '.join(columns)}) VALUES ({placeholders})"

    values = []
    for ev in events:
        row = [ev.get(col) for col in columns]
        values.append(row)

    conn.executemany(sql, values)
    return conn.total_changes


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
    conn.executemany(sql, values)
    return conn.total_changes


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
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(response)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path in ("/", "/health"):
            self._send_json(200, {"status": "ok", "service": "event_writer", "db": DB_PATH})
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
