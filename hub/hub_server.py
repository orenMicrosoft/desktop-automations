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

DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(DIR)
HUB_PORT = int(os.environ.get("HUB_PORT", "8091"))
AUTOMATIONS_FILE = os.path.join(DIR, "automations.json")

_BENIGN_ERRORS = (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, OSError)


class QuietThreadingServer(http.server.ThreadingHTTPServer):
    def handle_error(self, request, client_address):
        exc_type = sys.exc_info()[0]
        if exc_type and issubclass(exc_type, _BENIGN_ERRORS):
            return
        super().handle_error(request, client_address)


def _load_automations():
    """Load automation configs from automations.json."""
    if not os.path.exists(AUTOMATIONS_FILE):
        return []
    with open(AUTOMATIONS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def _build_server_commands():
    """Build server commands dynamically from automations.json."""
    commands = {}
    for auto in _load_automations():
        auto_id = auto["id"]
        folder = auto.get("folder", "")
        if not os.path.isabs(folder):
            folder = os.path.join(REPO_ROOT, folder)
        if not os.path.isdir(folder):
            continue

        commands[auto_id] = {
            "port": auto.get("port", 8080),
            "cwd": folder,
            "cmd": [sys.executable] + auto.get("cmd", ["dashboard_server.py"]),
            "health_path": auto.get("health_path", "/dashboard.html"),
        }
    return commands


child_processes = {}


def is_port_open(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(2)
        return s.connect_ex(("127.0.0.1", port)) == 0


def is_http_ready(port, path="/", timeout=3):
    try:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
        conn.request("HEAD", path)
        resp = conn.getresponse()
        resp.read()
        conn.close()
        return resp.status < 500
    except Exception:
        return False


def start_server(auto_id):
    """Start a child server if it's not already running."""
    commands = _build_server_commands()
    cfg = commands.get(auto_id)
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
    commands = _build_server_commands()
    statuses = {}
    for auto_id, cfg in commands.items():
        health_path = cfg.get("health_path", "/")
        statuses[auto_id] = {
            "port": cfg["port"],
            "running": is_http_ready(cfg["port"], health_path, timeout=2),
        }
    return statuses


def start_all_servers():
    commands = _build_server_commands()
    results = {}
    for auto_id in commands:
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
        except _BENIGN_ERRORS:
            pass

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
        else:
            self.send_error(404)

    def do_GET(self):
        if self.path == "/api/status":
            self._json_response(get_all_status())
        elif self.path == "/api/automations":
            self._json_response(_load_automations())
        else:
            super().do_GET()

    def _json_response(self, data):
        body = json.dumps(data).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
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

    print("Starting automation servers...")
    start_all_servers()
    print()

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
