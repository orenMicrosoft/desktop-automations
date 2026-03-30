"""
Managed Copilot CLI Sessions
Launches copilot as subprocess with JSON output, streams events via SSE,
accepts input via POST.
"""
import subprocess
import threading
import json
import time
import os
import sys
import queue as queue_mod
from collections import defaultdict

# Active managed sessions: id -> ManagedSession
_sessions = {}
_lock = threading.Lock()


class ManagedSession:
    def __init__(self, session_id, cwd, resume_id=None, initial_prompt=None,
                 autopilot=True, allow_all=True):
        self.id = session_id
        self.cwd = cwd
        self.resume_id = resume_id
        self.events = []
        self.status = "starting"  # starting, running, completed, error
        self.proc = None
        self.started_at = time.time()
        self.ended_at = None
        self.usage = {}
        self.total_output_tokens = 0
        self.total_input_tokens = 0
        self.turn_count = 0
        self._sse_clients = []  # list of queues for SSE subscribers
        self._sse_lock = threading.Lock()

        cmd = ["copilot", "--output-format", "json"]
        if allow_all:
            cmd.append("--allow-all")
        if autopilot:
            cmd.append("--autopilot")
        if resume_id:
            cmd.append(f"--resume={resume_id}")
        if initial_prompt:
            cmd.extend(["-i", initial_prompt])

        try:
            self.proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=cwd if os.path.isdir(cwd) else os.path.expanduser("~"),
                text=True,
                bufsize=1,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            )
            self.status = "running"
            # Start reader threads
            self._stdout_thread = threading.Thread(
                target=self._read_stdout, daemon=True)
            self._stdout_thread.start()
            self._stderr_thread = threading.Thread(
                target=self._read_stderr, daemon=True)
            self._stderr_thread.start()
        except Exception as e:
            self.status = "error"
            self._add_event({"type": "error", "data": {"content": str(e)}})

    def _add_event(self, event):
        self.events.append(event)
        # Update stats from events
        evt_type = event.get("type", "")
        data = event.get("data", {})

        if evt_type == "assistant.message":
            self.total_output_tokens += data.get("outputTokens", 0)
        elif evt_type == "assistant.turn_start":
            self.turn_count += 1
        elif evt_type == "result":
            self.usage = data.get("usage", {})
            self.status = "completed"
            self.ended_at = time.time()

        # Notify SSE clients
        with self._sse_lock:
            for q in self._sse_clients:
                q.put(event)

    def _read_stdout(self):
        try:
            for line in self.proc.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                    self._add_event(event)
                except json.JSONDecodeError:
                    self._add_event({
                        "type": "raw_output",
                        "data": {"content": line},
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")
                    })
        except Exception:
            pass
        finally:
            if self.status == "running":
                self.status = "completed"
                self.ended_at = time.time()

    def _read_stderr(self):
        try:
            for line in self.proc.stderr:
                line = line.strip()
                if line:
                    self._add_event({
                        "type": "stderr",
                        "data": {"content": line},
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")
                    })
        except Exception:
            pass

    def send_input(self, text):
        """Send input to the running copilot process."""
        if self.proc and self.proc.poll() is None and self.proc.stdin:
            try:
                self.proc.stdin.write(text + "\n")
                self.proc.stdin.flush()
                self._add_event({
                    "type": "user.input_sent",
                    "data": {"content": text},
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
                })
                return True
            except Exception:
                return False
        return False

    def subscribe_sse(self):
        """Subscribe to SSE events. Returns a Queue and unsubscribe fn."""
        q = queue_mod.Queue()
        with self._sse_lock:
            self._sse_clients.append(q)

        def unsubscribe():
            with self._sse_lock:
                if q in self._sse_clients:
                    self._sse_clients.remove(q)
        return q, unsubscribe

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
            self.status = "stopped"
            self.ended_at = time.time()

    def to_dict(self):
        elapsed = (self.ended_at or time.time()) - self.started_at
        return {
            "id": self.id,
            "resume_id": self.resume_id,
            "cwd": self.cwd,
            "status": self.status,
            "started_at": self.started_at,
            "elapsed_seconds": round(elapsed),
            "turn_count": self.turn_count,
            "total_output_tokens": self.total_output_tokens,
            "usage": self.usage,
            "event_count": len(self.events),
        }


def start_session(resume_id=None, cwd=None, initial_prompt=None,
                  autopilot=True, allow_all=True):
    """Start a new managed copilot session."""
    import uuid
    sid = f"managed-{uuid.uuid4().hex[:8]}"
    if not cwd:
        cwd = os.path.expanduser("~")
    session = ManagedSession(
        session_id=sid, cwd=cwd, resume_id=resume_id,
        initial_prompt=initial_prompt, autopilot=autopilot, allow_all=allow_all
    )
    with _lock:
        _sessions[sid] = session
    return session


def get_session(sid):
    with _lock:
        return _sessions.get(sid)


def list_sessions():
    with _lock:
        return [s.to_dict() for s in _sessions.values()]


def stop_session(sid):
    s = get_session(sid)
    if s:
        s.stop()
        return True
    return False
