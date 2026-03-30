"""Dashboard server for CoreIdentity Auto-Extend automation."""
import http.server
import json
import os
import subprocess
import sys
import threading
import urllib.parse
from pathlib import Path

PORT = 8090
BASE_DIR = Path(__file__).parent
SCRIPT = BASE_DIR / "renew_entitlements.py"


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(BASE_DIR), **kwargs)

    def handle(self):
        try:
            super().handle()
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            pass

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        super().end_headers()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if parsed.path == "/api/run":
            dry_run = params.get("dry", ["false"])[0] == "true"
            self._trigger_run(dry_run)
        else:
            self.send_error(404)

    def _trigger_run(self, dry_run: bool):
        cmd = [sys.executable, str(SCRIPT)]
        if dry_run:
            cmd.append("--dry-run")

        def run_in_background():
            try:
                env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=300,
                    cwd=str(BASE_DIR), env=env
                )
                log_file = BASE_DIR / "last-manual-run.log"
                log_file.write_text(
                    f"exit_code: {result.returncode}\n\n--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}",
                    encoding="utf-8",
                )
            except Exception as e:
                log_file = BASE_DIR / "last-manual-run.log"
                log_file.write_text(f"Error: {e}", encoding="utf-8")

        thread = threading.Thread(target=run_in_background, daemon=True)
        thread.start()

        mode = "dry-run" if dry_run else "live"
        body = json.dumps({"status": "started", "mode": mode}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    server = http.server.ThreadingHTTPServer(("127.0.0.1", PORT), DashboardHandler)
    print(f"Dashboard: http://127.0.0.1:{PORT}/dashboard.html")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
