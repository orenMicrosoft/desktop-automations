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
SKILLS_FILE = os.path.join(DIR, "skills.json")
# Copilot CLI skills are auto-loaded by every session from here. The hub reads
# the same folder so the dashboard and your sessions stay in sync (single source of truth).
COPILOT_SKILLS_DIR = os.path.join(os.path.expanduser("~"), ".copilot", "skills")

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
    "pr-reviewer": {
        "port": 8097,
        "cwd": r"C:\Users\orenhorowitz\desktop-automations\pr-reviewer",
        "cmd": [sys.executable, "pr_reviewer_server.py", "--no-browser"],
        "health_path": "/api/health",
    },
    "clawpilot-pr-reviews": {
        "port": 8765,
        "cwd": r"C:\Users\orenhorowitz\.copilot\pr-reviews",
        "cmd": [sys.executable, "-m", "http.server", "8765", "--bind", "127.0.0.1"],
        "health_path": "/index.html",
    },
    "realestate-finder": {
        "port": 8098,
        "cwd": r"C:\Users\orenhorowitz\desktop-automations\realestate-finder",
        "cmd": [sys.executable, "dashboard_server.py", "--no-browser"],
        "health_path": "/api/health",
    },
    "status-dashboard": {
        "port": 8099,
        "cwd": r"C:\Users\orenhorowitz\desktop-automations\status-dashboard",
        "cmd": [sys.executable, "status_dashboard.py", "--no-browser"],
        "health_path": "/api/health",
    },
    "cef-screen": {
        "port": 8100,
        "cwd": r"C:\Users\orenhorowitz\Code\CefScreen",
        "cmd": [sys.executable, "-m", "cef_screener.web", "--no-browser"],
        "health_path": "/api/health",
    },
    "daily-planner": {
        "port": 8101,
        "cwd": r"C:\Users\orenhorowitz\desktop-automations\daily-planner",
        "cmd": [sys.executable, "server.py", "8101"],
        "health_path": "/todo-data.js",
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
    """Return status for all registered servers.

    Checks run concurrently with a short timeout so that an unreachable or
    slow automation can never stall the whole /api/status response (which the
    hub UI awaits before opening any dashboard).
    """
    from concurrent.futures import ThreadPoolExecutor

    def _check(item):
        auto_id, cfg = item
        return auto_id, {
            "port": cfg["port"],
            "running": is_http_ready(cfg["port"], cfg.get("health_path", "/"), timeout=1),
        }

    statuses = {}
    items = list(SERVER_COMMANDS.items())
    with ThreadPoolExecutor(max_workers=max(1, len(items))) as ex:
        for auto_id, st in ex.map(_check, items):
            statuses[auto_id] = st
    return statuses


def start_all_servers():
    """Start all registered servers."""
    results = {}
    for auto_id in SERVER_COMMANDS:
        ok, msg = start_server(auto_id)
        results[auto_id] = {"ok": ok, "message": msg}
        print(f"  {auto_id}: {msg}")
    return results


def _parse_frontmatter(text):
    """Parse a SKILL.md: returns (meta_dict, body_str).

    Frontmatter is the block between the first two '---' lines. Each metadata
    line is 'key: value' where value may be wrapped in single/double quotes and
    is expected to be on a single physical line (true for our SKILL.md files).
    """
    meta, body = {}, text
    if text.startswith("---"):
        end = text.find("\n---", 3)
        if end != -1:
            fm = text[3:end]
            body = text[end + 4:].lstrip("\n")
            for line in fm.splitlines():
                line = line.rstrip()
                if not line or line.lstrip().startswith("#"):
                    continue
                if ":" not in line:
                    continue
                key, val = line.split(":", 1)
                key = key.strip()
                if not key.replace("_", "").isalnum():
                    continue
                val = val.strip()
                if len(val) >= 2 and val[0] in "\"'" and val[-1] == val[0]:
                    val = val[1:-1]
                meta[key] = val
    return meta, body


def get_all_skills():
    """Catalog every Copilot CLI skill in ~/.copilot/skills plus any legacy
    prompt skills in skills.json. Single source of truth = the skills folder."""
    # Fallback icons for bundled/vendor skills that don't declare an `icon:`
    # in their frontmatter (so we don't have to edit vendor-managed files).
    default_icons = {
        "docx": "📄", "pptx": "📊", "xlsx": "📈", "loop": "🔁",
        "excalidraw": "✏️", "expense-report": "🧾", "web-artifacts-builder": "🌐",
    }
    skills = []
    seen = set()
    if os.path.isdir(COPILOT_SKILLS_DIR):
        for name in sorted(os.listdir(COPILOT_SKILLS_DIR)):
            sp = os.path.join(COPILOT_SKILLS_DIR, name, "SKILL.md")
            if not os.path.isfile(sp):
                continue
            try:
                with open(sp, "r", encoding="utf-8") as f:
                    text = f.read()
            except Exception:
                continue
            meta, body = _parse_frontmatter(text)
            sid = meta.get("name", name)
            seen.add(sid)
            icon = meta.get("icon") or default_icons.get(name) or default_icons.get(sid) or "🧠"
            skills.append({
                "id": sid,
                "name": meta.get("name", name),
                "icon": icon,
                "description": meta.get("description", ""),
                "prompt": meta.get("prompt", ""),
                "has_prompt": bool(meta.get("prompt")),
                "body": body,
                "source": "copilot-cli",
                "path": sp,
            })
    # Legacy prompt skills (not yet migrated to a SKILL.md folder)
    try:
        with open(SKILLS_FILE, "r", encoding="utf-8") as f:
            for s in json.load(f):
                if s.get("id") in seen or s.get("name") in seen:
                    continue
                s = dict(s)
                s.setdefault("source", "prompt")
                s["has_prompt"] = bool(s.get("prompt"))
                s.setdefault("body", s.get("prompt", ""))
                skills.append(s)
    except Exception:
        pass
    return skills


class HubHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def end_headers(self):
        # Never let the browser cache the hub UI or API responses, so skill
        # changes show up on a normal reload (no hard-refresh needed).
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        super().end_headers()

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
        elif self.path == "/api/skills":
            self._json_response(get_all_skills())
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

    # Note: child servers are NOT started here — each one starts on demand
    # when you click its card in the hub (POST /api/start/<id>).

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
