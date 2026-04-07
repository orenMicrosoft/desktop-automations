"""
Dashboard server for Teams Summary automation.
Serves the dashboard UI and provides an API to scan the local Teams cache.
"""
import http.server
import json
import os
import sys
import threading
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

PORT = 8095
BASE_DIR = Path(__file__).parent
CACHE_FILE = BASE_DIR / "last-scan.json"

sys.path.insert(0, str(BASE_DIR))
import teams_scanner

_scan_lock = threading.Lock()
_BENIGN = (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, OSError)


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _load_cached():
    """Load the last scan result from disk (if any)."""
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return None


def _save_cached(data: dict):
    CACHE_FILE.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(BASE_DIR), **kwargs)

    def handle(self):
        try:
            super().handle()
        except _BENIGN:
            pass

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        super().end_headers()

    def log_message(self, *args):
        pass  # quiet

    # ── GET routes ────────────────────────────────────────────────────────
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == "/api/scan-data":
            self._serve_json(_load_cached() or {"error": "No scan yet. Click Refresh."})
        elif parsed.path == "/api/status":
            self._serve_json({"status": "running", "port": PORT})
        else:
            super().do_GET()

    # ── POST routes ───────────────────────────────────────────────────────
    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if parsed.path == "/api/scan":
            days = int(params.get("days", ["3"])[0])
            self._trigger_scan(days)
        else:
            self.send_error(404)

    # ── helpers ───────────────────────────────────────────────────────────
    def _serve_json(self, obj):
        body = json.dumps(obj, default=str).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def _trigger_scan(self, days: int):
        def run():
            with _scan_lock:
                data = teams_scanner.scan(days_back=days)
                _save_cached(data)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()

        body = json.dumps({"status": "scanning", "days": days}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)


def main():
    no_browser = "--no-browser" in sys.argv

    # Run an initial scan on startup
    print("Running initial Teams cache scan...")
    try:
        data = teams_scanner.scan(days_back=3)
        _save_cached(data)
        msg_count = len(data.get("messages", []))
        people_count = len(data.get("people", []))
        print(f"  Found {msg_count} messages, {people_count} people")
        if data.get("error"):
            print(f"  Warning: {data['error']}")
    except Exception as exc:
        print(f"  Initial scan failed: {exc}")

    server = http.server.ThreadingHTTPServer(("127.0.0.1", PORT), DashboardHandler)
    url = f"http://127.0.0.1:{PORT}/dashboard.html"
    print(f"Dashboard: {url}")
    print("Press Ctrl+C to stop.")

    if not no_browser:
        import webbrowser
        webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
