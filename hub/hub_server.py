"""
Automation Hub Server
Single entry point that manages all automation dashboard servers.
Starts child servers on demand and serves the hub UI.
"""
import subprocess
import sys
import os
import json
import socket
import time
import webbrowser
import http.server
import http.client
import threading
import signal

DIR = os.path.dirname(os.path.abspath(__file__))
HUB_PORT = 8091
AUTOMATIONS_FILE = os.path.join(DIR, "automations.json")

# Suppress noisy ConnectionResetError from browsers closing connections early
_BENIGN_ERRORS = (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, OSError)

class QuietThreadingServer(http.server.ThreadingHTTPServer):
    def handle_error(self, request, client_address):
        import sys
        exc_type = sys.exc_info()[0]
        if exc_type and issubclass(exc_type, _BENIGN_ERRORS):
            return
        super().handle_error(request, client_address)

# Map automation IDs to their server start commands
SERVER_COMMANDS = {
    "coreidentity-autoextend": {
        "port": 8090,
        "cwd": r"C:\Users\orenhorowitz\Code\CoreIdentityRenewal",
        "cmd": [sys.executable, "dashboard_server.py"],
        "health_path": "/dashboard.html",
    },
    "copilot-dashboard": {
        "port": 8787,
        "cwd": r"C:\Users\orenhorowitz\Tools\CopilotDashboard",
        "cmd": [sys.executable, "launch.py", "--no-browser"],
        "health_path": "/dashboard-data.json",
    },
    "course-workflow": {
        "port": 8092,
        "cwd": r"C:\Users\orenhorowitz\Code\CourseWorkflow",
        "cmd": [sys.executable, "dashboard_server.py", "--no-browser"],
        "health_path": "/dashboard.html",
    },
    "pipeline-dashboard": {
        "port": 8093,
        "cwd": r"C:\Users\orenhorowitz\Code\PipelineDashboard",
        "cmd": [sys.executable, "pipeline_dashboard.py", "--no-browser"],
        "health_path": "/dashboard.html",
    },
    "teams-summary": {
        "port": 8095,
        "cwd": r"C:\Users\orenhorowitz\desktop-automations\teams-summary",
        "cmd": [sys.executable, "dashboard_server.py", "--no-browser"],
        "health_path": "/dashboard.html",
    },
    "email-digest": {
        "port": 8094,
        "cwd": r"C:\Users\orenhorowitz\Code\email-digest",
        "cmd": [sys.executable, "email_digest.py", "--no-browser"],
        "health_path": "/api/health",
    },
    "openclaw-meni": {
        "port": 8096,
        "cwd": r"C:\Users\orenhorowitz\My Automations\OpenClaw Meni",
        "cmd": [sys.executable, "dashboard_server.py", "--no-browser"],
        "health_path": "/api/status",
    },
    "pr-reviewer": {
        "port": 8097,
        "cwd": r"C:\Users\orenhorowitz\desktop-automations\pr-reviewer",
        "cmd": [sys.executable, "pr_reviewer_server.py", "--no-browser"],
        "health_path": "/api/health",
    },
}

child_processes = {}


def is_port_open(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(2)
        return s.connect_ex(("127.0.0.1", port)) == 0


def is_http_ready(port, path="/", timeout=3):
    """Check if the server is actually responding to HTTP requests."""
    try:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
        conn.request("HEAD", path)
        resp = conn.getresponse()
        resp.read()  # consume any body to fully release the connection
        conn.close()
        return resp.status < 500
    except Exception:
        return False


def start_server(auto_id):
    """Start a child server if it's not already running."""
    cfg = SERVER_COMMANDS.get(auto_id)
    if not cfg:
        return False, "No server config"

    health_path = cfg.get("health_path", "/")
    if is_http_ready(cfg["port"], health_path):
        return True, "Already running"

    try:
        proc = subprocess.Popen(
            cfg["cmd"],
            cwd=cfg["cwd"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        child_processes[auto_id] = proc

        # Wait for server to actually respond to HTTP (not just port open)
        for _ in range(30):
            time.sleep(1)
            if proc.poll() is not None:
                return False, f"Process exited with code {proc.returncode}"
            if is_http_ready(cfg["port"], health_path, timeout=2):
                return True, "Started"
        return False, "Timeout waiting for server"
    except Exception as e:
        return False, str(e)


def get_all_status():
    """Return status for all registered servers."""
    statuses = {}
    for auto_id, cfg in SERVER_COMMANDS.items():
        health_path = cfg.get("health_path", "/")
        statuses[auto_id] = {
            "port": cfg["port"],
            "running": is_http_ready(cfg["port"], health_path, timeout=2),
        }
    return statuses


def start_all_servers():
    """Start all registered servers."""
    results = {}
    for auto_id in SERVER_COMMANDS:
        ok, msg = start_server(auto_id)
        results[auto_id] = {"ok": ok, "message": msg}
        print(f"  {auto_id}: {msg}")
    return results


class HubHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def handle(self):
        try:
            super().handle()
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            pass  # browser closed connection early — harmless

    def do_POST(self):
        if self.path == "/api/start-all":
            results = start_all_servers()
            self._json_response(results)
        elif self.path.startswith("/api/start/"):
            auto_id = self.path.split("/api/start/")[1]
            ok, msg = start_server(auto_id)
            self._json_response({"ok": ok, "message": msg})
        elif self.path == "/api/status":
            self._json_response(get_all_status())
        elif self.path == "/api/e2e-tests":
            import e2e_tests
            import importlib
            importlib.reload(e2e_tests)
            report = e2e_tests.run_all_tests()
            self._json_response(report)
        else:
            self.send_error(404)

    def do_GET(self):
        if self.path == "/api/status":
            self._json_response(get_all_status())
        else:
            super().do_GET()

    def _json_response(self, data):
        body = json.dumps(data).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)


def cleanup():
    print("\nStopping child servers...")
    for auto_id, proc in child_processes.items():
        try:
            proc.terminate()
            proc.wait(timeout=5)
            print(f"  Stopped {auto_id}")
        except Exception:
            proc.kill()


def main():
    if is_port_open(HUB_PORT):
        print(f"Hub already running at http://localhost:{HUB_PORT}")
        webbrowser.open(f"http://localhost:{HUB_PORT}")
        return

    print("=" * 44)
    print("  Starting Automation Hub")
    print("=" * 44)
    print()

    # Start all child servers
    print("Starting automation servers...")
    start_all_servers()
    print()

    # Start hub server
    os.chdir(DIR)
    server = QuietThreadingServer(("127.0.0.1", HUB_PORT), HubHandler)

    url = f"http://localhost:{HUB_PORT}"
    print(f"Hub running at: {url}")
    print("Press Ctrl+C to stop.\n")

    webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        cleanup()
        server.shutdown()


if __name__ == "__main__":
    main()
