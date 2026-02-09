import json
import os
import sqlite3
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse

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


class WriterHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path in ("/", "/health"):
            payload = {"status": "ok", "service": "event_writer", "db": DB_PATH}
            response = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(response)
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
                response = json.dumps({"inserted": inserted}).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(response)
                return

            if parsed.path == "/bars":
                bars = payload.get("bars", [])
                inserted = insert_bars(conn, bars)
                conn.commit()
                response = json.dumps({"inserted": inserted}).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(response)
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
