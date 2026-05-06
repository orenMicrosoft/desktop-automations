"""
Status Dashboard Server
Tracks work streams, tasks, and progress with an interactive UI.
"""
import http.server
import json
import os
import sys
import socket
import webbrowser
import time
from datetime import datetime, timezone

DIR = os.path.dirname(os.path.abspath(__file__))
PORT = 8099
DATA_FILE = os.path.join(DIR, "status_data.json")

_BENIGN = (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, OSError)


def _load_data():
    if not os.path.exists(DATA_FILE):
        return {"lastUpdated": "", "streams": [], "people": [], "goals": []}
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_data(data):
    data["lastUpdated"] = datetime.now(timezone.utc).isoformat()
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


class QuietServer(http.server.ThreadingHTTPServer):
    def handle_error(self, request, client_address):
        exc_type = sys.exc_info()[0]
        if exc_type and issubclass(exc_type, _BENIGN):
            return
        super().handle_error(request, client_address)


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def handle(self):
        try:
            super().handle()
        except _BENIGN:
            pass

    def do_GET(self):
        if self.path == "/api/data":
            self._json_response(_load_data())
        elif self.path == "/api/health":
            self._json_response({"status": "ok"})
        elif self.path == "/api/export-markdown":
            md = self._export_markdown()
            body = md.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/markdown; charset=utf-8")
            self.send_header("Content-Length", len(body))
            self.end_headers()
            self.wfile.write(body)
        else:
            super().do_GET()

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b"{}"
        body = json.loads(raw) if raw else {}

        if self.path == "/api/toggle-task":
            self._toggle_task(body)
        elif self.path == "/api/update-stream":
            self._update_stream(body)
        elif self.path == "/api/add-stream":
            self._add_stream(body)
        elif self.path == "/api/delete-stream":
            self._delete_stream(body)
        elif self.path == "/api/add-task":
            self._add_task(body)
        elif self.path == "/api/delete-task":
            self._delete_task(body)
        elif self.path == "/api/update-goals":
            self._update_goals(body)
        elif self.path == "/api/update-people":
            self._update_people(body)
        elif self.path == "/api/reorder-streams":
            self._reorder_streams(body)
        else:
            self.send_error(404)

    def _toggle_task(self, body):
        data = _load_data()
        stream_id = body.get("streamId")
        task_idx = body.get("taskIndex")
        for s in data["streams"]:
            if s["id"] == stream_id and 0 <= task_idx < len(s["tasks"]):
                s["tasks"][task_idx]["done"] = not s["tasks"][task_idx]["done"]
                break
        _save_data(data)
        self._json_response({"ok": True})

    def _update_stream(self, body):
        data = _load_data()
        stream_id = body.get("id")
        for s in data["streams"]:
            if s["id"] == stream_id:
                for key in ("title", "repo", "status", "eta", "owner", "reviewer", "notes"):
                    if key in body:
                        s[key] = body[key]
                break
        _save_data(data)
        self._json_response({"ok": True})

    def _add_stream(self, body):
        data = _load_data()
        new_id = body.get("title", "new").lower().replace(" ", "-").replace("/", "-")[:30]
        new_id += f"-{int(time.time()) % 10000}"
        stream = {
            "id": new_id,
            "title": body.get("title", "New Work Stream"),
            "repo": body.get("repo", ""),
            "status": body.get("status", "active"),
            "eta": body.get("eta", ""),
            "owner": body.get("owner", "Oren"),
            "reviewer": body.get("reviewer", ""),
            "tasks": [],
            "blockers": [],
            "notes": body.get("notes", "")
        }
        data["streams"].insert(0, stream)
        _save_data(data)
        self._json_response({"ok": True, "id": new_id})

    def _delete_stream(self, body):
        data = _load_data()
        stream_id = body.get("id")
        data["streams"] = [s for s in data["streams"] if s["id"] != stream_id]
        _save_data(data)
        self._json_response({"ok": True})

    def _add_task(self, body):
        data = _load_data()
        stream_id = body.get("streamId")
        text = body.get("text", "New task")
        for s in data["streams"]:
            if s["id"] == stream_id:
                s["tasks"].append({"text": text, "done": False})
                break
        _save_data(data)
        self._json_response({"ok": True})

    def _delete_task(self, body):
        data = _load_data()
        stream_id = body.get("streamId")
        task_idx = body.get("taskIndex")
        for s in data["streams"]:
            if s["id"] == stream_id and 0 <= task_idx < len(s["tasks"]):
                s["tasks"].pop(task_idx)
                break
        _save_data(data)
        self._json_response({"ok": True})

    def _update_goals(self, body):
        data = _load_data()
        data["goals"] = body.get("goals", [])
        _save_data(data)
        self._json_response({"ok": True})

    def _update_people(self, body):
        data = _load_data()
        data["people"] = body.get("people", [])
        _save_data(data)
        self._json_response({"ok": True})

    def _reorder_streams(self, body):
        data = _load_data()
        order = body.get("order", [])
        if order:
            by_id = {s["id"]: s for s in data["streams"]}
            reordered = [by_id[sid] for sid in order if sid in by_id]
            remaining = [s for s in data["streams"] if s["id"] not in order]
            data["streams"] = reordered + remaining
        _save_data(data)
        self._json_response({"ok": True})

    def _export_markdown(self):
        data = _load_data()
        lines = [f"# Status — Oren Horowitz", f"> Last updated: {data.get('lastUpdated', 'N/A')}", ""]

        for section, label in [("active", "🔴 ACTIVE"), ("paused", "🟡 PAUSED"), ("done", "✅ DONE")]:
            streams = [s for s in data["streams"] if s["status"] == section]
            if streams:
                lines.append(f"## {label}")
                lines.append("")
                for s in streams:
                    lines.append(f"### {s['title']}")
                    if s.get("repo"):
                        lines.append(f"- **Repo:** {s['repo']}")
                    if s.get("eta"):
                        lines.append(f"- **ETA:** {s['eta']}")
                    if s.get("owner"):
                        lines.append(f"- **Owner:** {s['owner']}")
                    for t in s.get("tasks", []):
                        check = "x" if t["done"] else " "
                        lines.append(f"- [{check}] {t['text']}")
                    if s.get("notes"):
                        lines.append(f"- *{s['notes']}*")
                    lines.append("")

        if data.get("goals"):
            lines.append("## 🎯 Goals")
            for i, g in enumerate(data["goals"], 1):
                lines.append(f"{i}. {g}")
            lines.append("")

        return "\n".join(lines)

    def _json_response(self, data):
        body = json.dumps(data).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)


def main():
    port = PORT
    no_browser = "--no-browser" in sys.argv

    if socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect_ex(("127.0.0.1", port)) == 0:
        print(f"Status Dashboard already running at http://localhost:{port}")
        if not no_browser:
            webbrowser.open(f"http://localhost:{port}/dashboard.html")
        return

    os.chdir(DIR)
    server = QuietServer(("127.0.0.1", port), DashboardHandler)

    url = f"http://localhost:{port}/dashboard.html"
    print(f"Status Dashboard running at: {url}")
    if not no_browser:
        webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
