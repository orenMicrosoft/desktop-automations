"""
Meni (OpenClaw) Dashboard Server
Full management dashboard: status, sessions, QR relink, gateway control.
"""
import subprocess
import sys
import os
import json
import time
import glob as globmod
import socket
import http.server
import http.client
import threading
import webbrowser
import re

PORT = int(os.environ.get("MENI_PORT", "8096"))
DIR = os.path.dirname(os.path.abspath(__file__))
OPENCLAW_DIR = os.path.join(os.path.expanduser("~"), ".openclaw")
OPENCLAW_JSON = os.path.join(OPENCLAW_DIR, "openclaw.json")
SESSIONS_DIR = os.path.join(OPENCLAW_DIR, "agents", "main", "sessions")
GATEWAY_PORT = 18789
BROWSER_PORT = 18791
GROUP_ID = "120363407470486178@g.us"
GROUP_NAME = "אני ומני"
GW_TOKEN = "442e153deef8bc2719ed21a9a373285a3eabd05512d44767"

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


# ---------------------------------------------------------------------------
#  Gateway & WhatsApp helpers
# ---------------------------------------------------------------------------

def gateway_healthy():
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
    if not OPENCLAW_INDEX:
        return {"error": "openclaw not found"}
    try:
        result = subprocess.run(
            [NODE_EXE, OPENCLAW_INDEX, "channels", "status"],
            capture_output=True, text=True, timeout=30,
            env={**os.environ, "NO_COLOR": "1"}
        )
        output = result.stdout + result.stderr
        return {
            "raw": output.strip(),
            "linked": "linked" in output,
            "connected": "connected" in output,
            "running": "running" in output,
            "dm_disabled": "dm:disabled" in output,
        }
    except Exception as e:
        return {"error": str(e)}


def get_config_summary():
    try:
        with open(OPENCLAW_JSON, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        wa = cfg.get("channels", {}).get("whatsapp", {})
        tts = cfg.get("messages", {}).get("tts", {})
        audio = cfg.get("tools", {}).get("media", {}).get("audio", {})
        return {
            "enabled": wa.get("enabled", False),
            "dmPolicy": wa.get("dmPolicy", "unknown"),
            "groupPolicy": wa.get("groupPolicy", "unknown"),
            "groupAllowFrom": wa.get("groupAllowFrom", []),
            "debounceMs": wa.get("debounceMs", 0),
            "model": cfg.get("agents", {}).get("defaults", {}).get("model", {}).get("primary", "unknown"),
            "ttsAuto": tts.get("auto", "off"),
            "ttsProvider": tts.get("provider", "none"),
            "audioEnabled": audio.get("enabled", False),
            "audioLanguage": audio.get("language", "?"),
        }
    except Exception as e:
        return {"error": str(e)}


def start_gateway():
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


def stop_gateway():
    """Stop the gateway process by finding its PID."""
    try:
        import psutil
        for proc in psutil.process_iter(["pid", "cmdline"]):
            cmd = " ".join(proc.info.get("cmdline") or [])
            if "openclaw" in cmd and "gateway" in cmd:
                proc.kill()
                return True
    except ImportError:
        pass
    # Fallback: find node processes with openclaw gateway
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "Get-CimInstance Win32_Process -Filter \"Name='node.exe'\" | "
             "Where-Object { $_.CommandLine -like '*openclaw*' -and $_.CommandLine -like '*gateway*' } | "
             "Select-Object -ExpandProperty ProcessId"],
            capture_output=True, text=True, timeout=10,
        )
        for line in result.stdout.strip().split("\n"):
            pid = line.strip()
            if pid.isdigit():
                subprocess.run(["powershell", "-NoProfile", "-Command",
                                f"Stop-Process -Id {pid} -Force"], timeout=5)
                return True
    except Exception:
        pass
    return False


def restart_gateway():
    """Stop then start the gateway."""
    stop_gateway()
    time.sleep(3)
    return start_gateway()


# ---------------------------------------------------------------------------
#  Sessions
# ---------------------------------------------------------------------------

def get_sessions():
    """Parse session JSONL files and return summaries."""
    sessions = []
    if not os.path.isdir(SESSIONS_DIR):
        return sessions
    files = sorted(globmod.glob(os.path.join(SESSIONS_DIR, "*.jsonl")),
                   key=os.path.getmtime, reverse=True)
    for fpath in files[:20]:
        try:
            sid = os.path.basename(fpath).replace(".jsonl", "")
            user_msgs = 0
            assistant_msgs = 0
            first_ts = None
            last_ts = None
            preview = ""
            with open(fpath, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    ts = entry.get("timestamp")
                    if entry.get("type") == "session":
                        first_ts = ts
                    if entry.get("type") == "message":
                        role = entry.get("message", {}).get("role")
                        if role == "user":
                            user_msgs += 1
                            if user_msgs <= 2 and not preview:
                                contents = entry.get("message", {}).get("content", [])
                                for c in contents:
                                    if isinstance(c, dict) and c.get("type") == "text":
                                        text = c["text"]
                                        # Extract actual user text after metadata
                                        m = re.search(r'"""(.+?)"""', text, re.DOTALL)
                                        if m:
                                            preview = m.group(1).strip()[:120]
                                        elif "[media attached" in text:
                                            preview = "🎵 Voice message"
                                        break
                        elif role == "assistant":
                            assistant_msgs += 1
                        last_ts = ts
            sessions.append({
                "id": sid,
                "firstTs": first_ts,
                "lastTs": last_ts,
                "userMsgs": user_msgs,
                "assistantMsgs": assistant_msgs,
                "preview": preview or "(session start)",
                "sizeKb": round(os.path.getsize(fpath) / 1024),
            })
        except Exception:
            continue
    return sessions


# ---------------------------------------------------------------------------
#  QR Code capture
# ---------------------------------------------------------------------------

_qr_process = None
_qr_blocks = []
_qr_current = []
_qr_lock = threading.Lock()
_qr_connected = False
_qr_last_time = 0
QR_BLOCK_SIZE = 29


def start_qr_capture():
    global _qr_process, _qr_blocks, _qr_current, _qr_connected, _qr_last_time
    if not OPENCLAW_INDEX:
        return False

    with _qr_lock:
        if _qr_process and _qr_process.poll() is None:
            _qr_process.kill()
            _qr_process.wait()
        _qr_blocks = []
        _qr_current = []
        _qr_connected = False
        _qr_last_time = 0

    def _reader():
        global _qr_process, _qr_blocks, _qr_current, _qr_connected, _qr_last_time
        proc = subprocess.Popen(
            [NODE_EXE, OPENCLAW_INDEX, "channels", "login", "--channel", "whatsapp"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            env={**os.environ, "NO_COLOR": "1"},
        )
        _qr_process = proc
        for raw in iter(proc.stdout.readline, b""):
            line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            is_qr_line = any(c in line for c in "▄█▀")
            if "ready" in line.lower() or "linked" in line.lower() or "authenticated" in line.lower():
                with _qr_lock:
                    _qr_connected = True
                continue
            if is_qr_line:
                with _qr_lock:
                    if len(_qr_current) >= QR_BLOCK_SIZE:
                        _qr_current = []
                    _qr_current.append(line)
                    if len(_qr_current) >= QR_BLOCK_SIZE:
                        _qr_blocks.append("\n".join(_qr_current))
                        _qr_last_time = time.time()

    t = threading.Thread(target=_reader, daemon=True)
    t.start()
    return True


def get_latest_qr():
    with _qr_lock:
        if _qr_connected:
            return None, 0
        if _qr_blocks:
            age = time.time() - _qr_last_time
            return _qr_blocks[-1], age
    return "", 0


# ---------------------------------------------------------------------------
#  Logs
# ---------------------------------------------------------------------------

def get_recent_logs(lines=50):
    """Get the last N lines from today's gateway log."""
    log_dir = os.path.join(os.environ.get("LOCALAPPDATA", ""), "Temp", "openclaw")
    today = time.strftime("%Y-%m-%d")
    log_file = os.path.join(log_dir, f"openclaw-{today}.log")
    if not os.path.isfile(log_file):
        return []
    try:
        with open(log_file, "r", encoding="utf-8", errors="replace") as f:
            all_lines = f.readlines()
        return [l.rstrip() for l in all_lines[-lines:]]
    except Exception:
        return []


# ---------------------------------------------------------------------------
#  HTTP Handler
# ---------------------------------------------------------------------------

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
                "openclawUrl": f"http://127.0.0.1:{BROWSER_PORT}/",
                "gatewayUrl": f"http://127.0.0.1:{GATEWAY_PORT}/",
            })
        elif self.path == "/api/qr":
            qr, age = get_latest_qr()
            if qr is None:
                self._json({"connected": True, "qr": "", "age": 0})
            else:
                self._json({"connected": False, "qr": qr, "age": round(age, 1)})
        elif self.path == "/api/sessions":
            self._json({"sessions": get_sessions()})
        elif self.path.startswith("/api/logs"):
            self._json({"lines": get_recent_logs(80)})
        elif self.path == "/dashboard-data.json":
            gw = gateway_healthy()
            self._json({"ok": gw, "gateway": gw})
        else:
            super().do_GET()

    def do_POST(self):
        if self.path == "/api/start-gateway":
            ok = start_gateway()
            for _ in range(25):
                time.sleep(1)
                if gateway_healthy():
                    self._json({"ok": True, "message": "Gateway started"})
                    return
            self._json({"ok": ok, "message": "Started but not yet healthy"})
        elif self.path == "/api/restart-gateway":
            ok = restart_gateway()
            for _ in range(25):
                time.sleep(1)
                if gateway_healthy():
                    self._json({"ok": True, "message": "Gateway restarted"})
                    return
            self._json({"ok": ok, "message": "Restart initiated"})
        elif self.path == "/api/stop-gateway":
            ok = stop_gateway()
            self._json({"ok": ok, "message": "Gateway stopped" if ok else "Could not stop"})
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
