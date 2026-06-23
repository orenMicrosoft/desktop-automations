#!/usr/bin/env python3
"""Tiny local server for Oren's Daily Planner.
- Serves the app + data from this folder.
- POST /save  -> writes todo-data.js (auto-save) + a timestamped backup in history/.
Run: python server.py   (default port 8787)
"""
import http.server, socketserver, json, os, datetime, sys

PORT = 8101
ROOT = os.path.dirname(os.path.abspath(__file__))
HIST = os.path.join(ROOT, "history")
os.makedirs(HIST, exist_ok=True)

class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *a, **k):
        super().__init__(*a, directory=ROOT, **k)

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def do_GET(self):
        if self.path in ("/", ""):
            self.path = "/app.html"
        return super().do_GET()

    def do_POST(self):
        if self.path != "/save":
            self.send_error(404); return
        try:
            n = int(self.headers.get("Content-Length", 0))
            data = json.loads(self.rfile.read(n).decode("utf-8"))
            data["updated"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            content = "window.TODO_DATA = " + json.dumps(data, ensure_ascii=False, indent=2) + ";\n"
            with open(os.path.join(ROOT, "todo-data.js"), "w", encoding="utf-8") as f:
                f.write(content)
            # daily backup snapshot (overwrites within the same day, keeps one per day)
            stamp = datetime.datetime.now().strftime("%Y-%m-%d")
            with open(os.path.join(HIST, f"todo-data-{stamp}.js"), "w", encoding="utf-8") as f:
                f.write(content)
            self.send_response(200); self.send_header("Content-Type","application/json"); self.end_headers()
            self.wfile.write(b'{"ok":true}')
        except Exception as e:
            self.send_response(500); self.end_headers()
            self.wfile.write(json.dumps({"ok":False,"error":str(e)}).encode())

    def log_message(self, *a):
        pass  # quiet

if __name__ == "__main__":
    for a in sys.argv[1:]:
        if a.isdigit():
            PORT = int(a)
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("127.0.0.1", PORT), Handler) as httpd:
        print(f"Daily Planner running at http://localhost:{PORT}/")
        httpd.serve_forever()
