"""
Copilot CLI Dashboard Launcher
Starts a local HTTP server with APIs for data refresh, session resume,
managed session control with live SSE streaming, and desktop app management.
"""
import subprocess
import sys
import os
import json
import time
import webbrowser
import http.server
import http.client
import threading
import socket
import socketserver


class QuietThreadingServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    """ThreadingHTTPServer that suppresses benign connection reset errors."""
    daemon_threads = True

    def handle_error(self, request, client_address):
        import traceback
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
            return
        traceback.print_exc()

DIR = os.path.dirname(os.path.abspath(__file__))
PORT = 8787
COLLECTOR = os.path.join(DIR, "collect_data.py")
SESSION_STORE = os.environ.get("COPILOT_SESSION_DB", os.path.join(os.path.expanduser("~"), ".copilot", "session-store.db"))
AUTOMATIONS_FILE = os.environ.get("AUTOMATIONS_FILE", os.path.join(os.path.dirname(DIR), "hub", "automations.json"))
HUB_PORT = 8091

# Import session manager
sys.path.insert(0, DIR)
import session_manager

refresh_lock = threading.Lock()


def search_sessions(query, limit=20):
    """Search session-store.db using FTS5 + metadata matching."""
    import sqlite3
    if not os.path.exists(SESSION_STORE):
        return []

    conn = sqlite3.connect(SESSION_STORE)
    conn.row_factory = sqlite3.Row
    results = []
    seen_ids = set()

    try:
        # FTS5 search on search_index
        fts_query = " OR ".join(query.split())
        cur = conn.execute("""
            SELECT si.session_id, si.source_type, si.content,
                   s.summary, s.cwd, s.branch, s.repository, s.created_at, s.updated_at,
                   rank
            FROM search_index si
            JOIN sessions s ON s.id = si.session_id
            WHERE search_index MATCH ?
            ORDER BY rank
            LIMIT ?
        """, (fts_query, limit * 3))

        for row in cur.fetchall():
            sid = row["session_id"]
            if sid in seen_ids:
                continue
            seen_ids.add(sid)
            # Extract a snippet from matching content
            content = row["content"] or ""
            snippet = _extract_snippet(content, query, 150)
            results.append({
                "id": sid,
                "summary": row["summary"],
                "cwd": row["cwd"],
                "branch": row["branch"],
                "repository": row["repository"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "match_source": row["source_type"],
                "snippet": snippet,
            })

        # Also search sessions.summary directly with LIKE for broader matching
        for word in query.split():
            like = f"%{word}%"
            cur2 = conn.execute("""
                SELECT id, summary, cwd, branch, repository, created_at, updated_at
                FROM sessions
                WHERE (summary LIKE ? OR cwd LIKE ? OR branch LIKE ? OR repository LIKE ?)
                AND id NOT IN ({})
                ORDER BY updated_at DESC
                LIMIT ?
            """.format(",".join("?" * len(seen_ids)) if seen_ids else "'__none__'"),
                [like, like, like, like] + list(seen_ids) + [limit])
            for row in cur2.fetchall():
                if row["id"] in seen_ids:
                    continue
                seen_ids.add(row["id"])
                results.append({
                    "id": row["id"],
                    "summary": row["summary"],
                    "cwd": row["cwd"],
                    "branch": row["branch"],
                    "repository": row["repository"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                    "match_source": "metadata",
                    "snippet": row["summary"] or "",
                })
    except Exception:
        pass
    finally:
        conn.close()

    return results[:limit]


def _extract_snippet(content, query, max_len=150):
    """Extract a relevant snippet around the first match."""
    lower = content.lower()
    words = query.lower().split()
    best_pos = -1
    for w in words:
        pos = lower.find(w)
        if pos >= 0:
            best_pos = pos
            break
    if best_pos < 0:
        return content[:max_len] + ("..." if len(content) > max_len else "")
    start = max(0, best_pos - 40)
    end = min(len(content), start + max_len)
    snippet = content[start:end]
    if start > 0:
        snippet = "..." + snippet
    if end < len(content):
        snippet += "..."
    return snippet


def launch_copilot_resume(session_id, cwd, prompt=None, autopilot=True,
                         allow_all=True, same_window=True):
    """Open a new terminal tab with copilot, optionally resuming a session."""
    if not os.path.isdir(cwd):
        cwd = os.path.expanduser("~")
    copilot_args = ["copilot"]
    if session_id:
        copilot_args.append(f"--resume={session_id}")
    if autopilot:
        copilot_args.append("--autopilot")
    if allow_all:
        copilot_args.append("--allow-all")
    if prompt:
        copilot_args.extend(["--message", prompt])
    title = f"Copilot {'Resume' if session_id else 'Agentic'}"
    try:
        wt_cmd = ["wt"]
        if same_window:
            wt_cmd += ["-w", "0"]
        wt_cmd += ["new-tab", "--title", title,
                    "-d", cwd, "--"] + copilot_args
        subprocess.Popen(wt_cmd, cwd=cwd)
        msg = f"Resumed session {session_id[:8]}…" if session_id else f"Opened agentic session in {os.path.basename(cwd)}"
        return True, msg
    except FileNotFoundError:
        cmd_str = f'cd /d {cwd} && ' + ' '.join(copilot_args)
        subprocess.Popen(
            ["cmd", "/c", "start", "cmd", "/k", cmd_str],
        )
        msg = f"Resumed session {session_id[:8]}…" if session_id else f"Opened agentic session in {os.path.basename(cwd)}"
        return True, msg


def run_collector():
    """Run the data collector script, returns True on success."""
    with refresh_lock:
        result = subprocess.run(
            [sys.executable, COLLECTOR],
            cwd=DIR, capture_output=True, text=True, timeout=60
        )
        return result.returncode == 0, result.stdout + result.stderr


def _is_http_ready(port, path="/", timeout=2):
    """Check if a server is responding to HTTP requests."""
    try:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
        conn.request("HEAD", path)
        resp = conn.getresponse()
        resp.read()
        conn.close()
        return resp.status < 500
    except Exception:
        return False


def get_desktop_apps():
    """Load automations.json and check running status of each app."""
    if not os.path.exists(AUTOMATIONS_FILE):
        return []

    try:
        with open(AUTOMATIONS_FILE, "r", encoding="utf-8") as f:
            apps = json.load(f)
    except (json.JSONDecodeError, OSError):
        return []

    # Extract port from dashboard URL and check status
    for app in apps:
        url = app.get("dashboard", "")
        port = None
        if "localhost:" in url:
            try:
                port = int(url.split("localhost:")[1].split("/")[0])
            except (ValueError, IndexError):
                pass
        app["port"] = port
        app["running"] = _is_http_ready(port) if port else False

    return apps


def launch_desktop_app(app_id):
    """Launch a desktop app by proxying to the Automation Hub, or starting directly."""
    # Try the hub first
    if _is_http_ready(HUB_PORT, "/api/status"):
        try:
            conn = http.client.HTTPConnection("127.0.0.1", HUB_PORT, timeout=30)
            conn.request("POST", f"/api/start/{app_id}")
            resp = conn.getresponse()
            data = json.loads(resp.read())
            conn.close()
            return data.get("ok", False), data.get("message", "Unknown")
        except Exception as e:
            return False, f"Hub proxy failed: {e}"

    return False, "Automation Hub not running. Start it first."


def get_quick_launch_dirs():
    """Get common working directories from recent sessions for quick launch."""
    import sqlite3
    if not os.path.exists(SESSION_STORE):
        return []
    try:
        conn = sqlite3.connect(SESSION_STORE, timeout=3)
        cur = conn.execute("""
            SELECT cwd, repository, COUNT(*) as cnt,
                   MAX(created_at) as last_used
            FROM sessions
            WHERE cwd IS NOT NULL AND cwd != ''
            GROUP BY cwd
            ORDER BY cnt DESC
            LIMIT 10
        """)
        dirs = []
        for row in cur.fetchall():
            cwd, repo, cnt, last_used = row
            if os.path.isdir(cwd):
                name = repo.split("/")[-1] if repo and "/" in repo else os.path.basename(cwd)
                dirs.append({
                    "cwd": cwd,
                    "name": name,
                    "session_count": cnt,
                    "last_used": last_used,
                    "repository": repo,
                })
        conn.close()
        return dirs
    except Exception:
        return []


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def handle(self):
        try:
            super().handle()
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            pass

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def _json_response(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self):
        content_len = int(self.headers.get("Content-Length", 0))
        if content_len:
            return json.loads(self.rfile.read(content_len))
        return {}

    def do_GET(self):
        # --- Session Manager APIs ---
        if self.path == "/api/managed-sessions":
            self._json_response({"sessions": session_manager.list_sessions()})

        elif self.path.startswith("/api/managed-session/") and self.path.endswith("/events"):
            sid = self.path.split("/")[3]
            session = session_manager.get_session(sid)
            if not session:
                self._json_response({"error": "Session not found"}, 404)
                return
            self._json_response({
                "info": session.to_dict(),
                "events": session.events,
            })

        elif self.path.startswith("/api/managed-session/") and self.path.endswith("/stream"):
            # SSE endpoint for live streaming
            sid = self.path.split("/")[3]
            session = session_manager.get_session(sid)
            if not session:
                self._json_response({"error": "Session not found"}, 404)
                return
            self._handle_sse(session)

        elif self.path.startswith("/api/managed-session/") and self.path.endswith("/status"):
            sid = self.path.split("/")[3]
            session = session_manager.get_session(sid)
            if not session:
                self._json_response({"ok": False, "error": "Session not found"}, 404)
                return
            d = session.to_dict()
            d["ok"] = True
            self._json_response(d)

        # --- Existing APIs ---
        elif self.path == "/api/status":
            self._json_response({"ok": True, "sessions": session_manager.list_sessions()})

        elif self.path.startswith("/api/search-sessions"):
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            q = qs.get("q", [""])[0]
            limit = int(qs.get("limit", ["20"])[0])
            results = search_sessions(q, limit) if q else []
            self._json_response({"results": results, "query": q})

        # --- Desktop Apps APIs ---
        elif self.path == "/api/desktop-apps":
            apps = get_desktop_apps()
            self._json_response({"apps": apps})

        elif self.path == "/api/quick-launch-dirs":
            dirs = get_quick_launch_dirs()
            self._json_response({"dirs": dirs})

        else:
            super().do_GET()

    def do_POST(self):
        if self.path == "/api/refresh":
            ok, output = run_collector()
            self._json_response({"ok": ok, "output": output})

        elif self.path == "/api/resume-session":
            body = self._read_body()
            session_id = body.get("session_id", "")
            cwd = body.get("cwd", os.path.expanduser("~"))
            prompt = body.get("prompt")
            autopilot = body.get("autopilot", True)
            allow_all = body.get("allow_all", True)
            same_window = body.get("same_window", True)
            ok, msg = launch_copilot_resume(session_id, cwd, prompt, autopilot,
                                            allow_all, same_window)
            self._json_response({"ok": ok, "message": msg})

        elif self.path == "/api/managed-session/start":
            body = self._read_body()
            session = session_manager.start_session(
                resume_id=body.get("resume_id"),
                cwd=body.get("cwd", os.path.expanduser("~")),
                initial_prompt=body.get("prompt"),
                autopilot=body.get("autopilot", True),
                allow_all=body.get("allow_all", True),
            )
            self._json_response({"ok": True, "managed_id": session.id, "session": session.to_dict()})

        elif self.path.startswith("/api/managed-session/") and self.path.endswith("/input"):
            sid = self.path.split("/")[3]
            session = session_manager.get_session(sid)
            if not session:
                self._json_response({"error": "Session not found"}, 404)
                return
            body = self._read_body()
            text = body.get("message") or body.get("text", "")
            ok = session.send_input(text)
            self._json_response({"ok": ok, "message": "Input sent" if ok else "Session not accepting input"})

        elif self.path.startswith("/api/managed-session/") and self.path.endswith("/stop"):
            sid = self.path.split("/")[3]
            ok = session_manager.stop_session(sid)
            self._json_response({"ok": ok, "message": "Session stopped" if ok else "Session not found"})

        elif self.path == "/api/e2e-tests":
            import e2e_tests
            # Reload to pick up changes
            import importlib
            importlib.reload(e2e_tests)
            report = e2e_tests.run_tests()
            self._json_response(report)

        elif self.path.startswith("/api/desktop-apps/launch/"):
            app_id = self.path.split("/api/desktop-apps/launch/")[1]
            ok, msg = launch_desktop_app(app_id)
            self._json_response({"ok": ok, "message": msg})

        elif self.path == "/api/quick-launch":
            body = self._read_body()
            cwd = body.get("cwd", os.path.expanduser("~"))
            prompt = body.get("prompt", "")
            mode = body.get("mode", "managed")  # "managed" or "terminal"
            if mode == "terminal":
                ok, msg = launch_copilot_resume(
                    None, cwd, prompt, autopilot=True, allow_all=True, same_window=True
                )
                self._json_response({"ok": ok, "message": msg})
            else:
                session = session_manager.start_session(
                    cwd=cwd, initial_prompt=prompt,
                    autopilot=True, allow_all=True,
                )
                self._json_response({
                    "ok": True, "managed_id": session.id,
                    "session": session.to_dict()
                })

        else:
            self.send_error(404)

    def _handle_sse(self, session):
        """Server-Sent Events stream for live session output."""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        q, unsubscribe = session.subscribe_sse()

        # First, send all existing events as a catchup burst
        for evt in list(session.events):
            line = f"data: {json.dumps(evt, default=str)}\n\n"
            try:
                self.wfile.write(line.encode())
                self.wfile.flush()
            except Exception:
                unsubscribe()
                return

        # Then stream new events as they arrive via Queue
        import queue as q_mod
        try:
            while session.status == "running":
                try:
                    evt = q.get(timeout=0.5)
                    line = f"data: {json.dumps(evt, default=str)}\n\n"
                    self.wfile.write(line.encode())
                    self.wfile.flush()
                except q_mod.Empty:
                    # Send heartbeat to detect disconnected clients
                    self.wfile.write(b": heartbeat\n\n")
                    self.wfile.flush()

            # Drain remaining events
            while not q.empty():
                evt = q.get_nowait()
                line = f"data: {json.dumps(evt, default=str)}\n\n"
                self.wfile.write(line.encode())
            self.wfile.flush()

            # Send final status
            final = f"data: {json.dumps({'type': 'session.ended', 'data': session.to_dict()}, default=str)}\n\n"
            self.wfile.write(final.encode())
            self.wfile.flush()
        except Exception:
            pass
        finally:
            unsubscribe()


def is_port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def main():
    url = f"http://localhost:{PORT}/dashboard.html"

    if is_port_in_use(PORT):
        print(f"Server already running at {url}")
        webbrowser.open(url)
        return

    print("Refreshing dashboard data...")
    run_collector()

    os.chdir(DIR)
    server = QuietThreadingServer(("127.0.0.1", PORT), DashboardHandler)

    print(f"\nDashboard running at: {url}")
    print("Press Ctrl+C to stop.\n")

    if "--no-browser" not in sys.argv:
        webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down managed sessions...")
        for s in session_manager.list_sessions():
            session_manager.stop_session(s["id"])
        server.shutdown()


if __name__ == "__main__":
    main()
