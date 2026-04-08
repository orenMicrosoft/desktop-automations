"""
Meni (OpenClaw) Dashboard Server
Shows Meni's status, connects WhatsApp if needed (QR in browser),
and verifies the אני ומני group allowlist.
"""
import subprocess
import sys
import os
import json
import time
import socket
import http.server
import http.client
import threading
import webbrowser

PORT = int(os.environ.get("MENI_PORT", "8096"))
DIR = os.path.dirname(os.path.abspath(__file__))
OPENCLAW_DIR = os.path.join(os.path.expanduser("~"), ".openclaw")
OPENCLAW_JSON = os.path.join(OPENCLAW_DIR, "openclaw.json")
GATEWAY_PORT = 18789
GROUP_ID = "120363407470486178@g.us"
GROUP_NAME = "אני ומני"

# Find node + openclaw paths
NODE_EXE = "node"
OPENCLAW_INDEX = None
for candidate in [
    os.path.join(os.path.expanduser("~"), "AppData", "Roaming", "nvm", "v22.16.0",
                 "node_modules", "openclaw", "dist", "index.js"),
    os.path.join(os.environ.get("APPDATA", ""), "nvm", "v22.16.0",
                 "node_modules", "openclaw", "dist", "index.js"),
]:
    if os.path.isfile(candidate):
        OPENCLAW_INDEX = candidate
        break


def gateway_healthy():
    """Check if the OpenClaw gateway is responding."""
    try:
        conn = http.client.HTTPConnection("127.0.0.1", GATEWAY_PORT, timeout=3)
        conn.request("GET", "/health")
        resp = conn.getresponse()
        data = json.loads(resp.read())
        conn.close()
        return data.get("ok", False)
    except Exception:
        return False


def get_whatsapp_status():
    """Run openclaw channels status and parse output."""
    if not OPENCLAW_INDEX:
        return {"error": "openclaw not found"}
    try:
        result = subprocess.run(
            [NODE_EXE, OPENCLAW_INDEX, "channels", "status"],
            capture_output=True, text=True, timeout=30,
            env={**os.environ, "NO_COLOR": "1"}
        )
        output = result.stdout + result.stderr
        status = {
            "raw": output,
            "linked": "linked" in output,
            "connected": "connected" in output,
            "running": "running" in output,
            "dm_disabled": "dm:disabled" in output,
        }
        return status
    except Exception as e:
        return {"error": str(e)}


def get_config_summary():
    """Read openclaw.json and return relevant settings."""
    try:
        with open(OPENCLAW_JSON, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        wa = cfg.get("channels", {}).get("whatsapp", {})
        return {
            "enabled": wa.get("enabled", False),
            "dmPolicy": wa.get("dmPolicy", "unknown"),
            "groupPolicy": wa.get("groupPolicy", "unknown"),
            "groupAllowFrom": wa.get("groupAllowFrom", []),
            "debounceMs": wa.get("debounceMs", 0),
            "model": cfg.get("agents", {}).get("defaults", {}).get("model", {}).get("primary", "unknown"),
        }
    except Exception as e:
        return {"error": str(e)}


def start_gateway():
    """Start the OpenClaw gateway if not running."""
    gateway_cmd = os.path.join(OPENCLAW_DIR, "gateway.cmd")
    if os.path.isfile(gateway_cmd):
        subprocess.Popen(
            ["cmd.exe", "/c", gateway_cmd],
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        return True
    elif OPENCLAW_INDEX:
        subprocess.Popen(
            [NODE_EXE, OPENCLAW_INDEX, "gateway", "--port", str(GATEWAY_PORT)],
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        return True
    return False


_qr_process = None
_qr_lines = []
_qr_lock = threading.Lock()
_qr_connected = False


def start_qr_capture():
    """Start WhatsApp login and capture QR codes."""
    global _qr_process, _qr_lines, _qr_connected
    if not OPENCLAW_INDEX:
        return False

    with _qr_lock:
        if _qr_process and _qr_process.poll() is None:
            _qr_process.kill()
        _qr_lines = []
        _qr_connected = False

    def _reader():
        global _qr_process, _qr_lines, _qr_connected
        proc = subprocess.Popen(
            [NODE_EXE, OPENCLAW_INDEX, "channels", "login", "--channel", "whatsapp"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            env={**os.environ, "NO_COLOR": "1"},
        )
        _qr_process = proc
        buf = ""
        for raw in iter(proc.stdout.readline, b""):
            line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            buf += line + "\n"
            # Detect QR block characters
            if any(c in line for c in "▄█▀"):
                with _qr_lock:
                    _qr_lines.append(line)
            # Detect successful connection
            if "ready" in line.lower() or "linked" in line.lower() or "authenticated" in line.lower():
                with _qr_lock:
                    _qr_connected = True

    t = threading.Thread(target=_reader, daemon=True)
    t.start()
    return True


def get_latest_qr():
    """Return the most recent QR block (last ~29 lines of block chars)."""
    with _qr_lock:
        if _qr_connected:
            return None  # connected, no QR needed
        qr = [l for l in _qr_lines if any(c in l for c in "▄█▀")]
        if len(qr) >= 20:
            return "\n".join(qr[-29:])
    return ""


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        if self.path == "/api/status":
            gw = gateway_healthy()
            cfg = get_config_summary()
            wa = get_whatsapp_status() if gw else {"error": "gateway down"}
            self._json({
                "gateway": gw,
                "whatsapp": wa,
                "config": cfg,
                "group_id": GROUP_ID,
                "group_name": GROUP_NAME,
            })
        elif self.path == "/api/qr":
            qr = get_latest_qr()
            if qr is None:
                self._json({"connected": True, "qr": ""})
            else:
                self._json({"connected": False, "qr": qr})
        elif self.path == "/dashboard-data.json":
            # For hub health check compatibility
            gw = gateway_healthy()
            self._json({"ok": gw, "gateway": gw})
        else:
            super().do_GET()

    def do_POST(self):
        if self.path == "/api/start-gateway":
            ok = start_gateway()
            # Wait for gateway
            for _ in range(20):
                time.sleep(1)
                if gateway_healthy():
                    self._json({"ok": True, "message": "Gateway started"})
                    return
            self._json({"ok": ok, "message": "Started but not yet healthy"})
        elif self.path == "/api/connect-whatsapp":
            ok = start_qr_capture()
            self._json({"ok": ok, "message": "QR capture started" if ok else "Failed"})
        else:
            self.send_error(404)

    def _json(self, data):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)


def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex(("127.0.0.1", PORT)) == 0:
            print(f"Already running at http://localhost:{PORT}")
            if "--no-browser" not in sys.argv:
                webbrowser.open(f"http://localhost:{PORT}/dashboard.html")
            return

    os.chdir(DIR)
    server = http.server.ThreadingHTTPServer(("127.0.0.1", PORT), DashboardHandler)
    url = f"http://localhost:{PORT}/dashboard.html"
    print(f"Meni dashboard at: {url}")
    if "--no-browser" not in sys.argv:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
