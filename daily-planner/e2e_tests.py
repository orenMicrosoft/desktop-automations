#!/usr/bin/env python3
"""E2E tests for the Daily Planner automation.
Covers: server start, endpoints, save persistence (refresh survives clicks),
daily history snapshot, data-schema integrity, and front-end JS execution.
Non-destructive: restores the original todo-data.js afterwards.
Run:  python e2e_tests.py
"""
import json, os, sys, time, socket, subprocess, urllib.request, urllib.error, copy

DIR = os.path.dirname(os.path.abspath(__file__))
PORT = 8101
BASE = f"http://127.0.0.1:{PORT}"
DATA = os.path.join(DIR, "todo-data.js")
HIST = os.path.join(DIR, "history")

results = []
def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))

def port_open(p):
    with socket.socket() as s:
        s.settimeout(1); return s.connect_ex(("127.0.0.1", p)) == 0

def http(method, path, body=None):
    url = BASE + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
        headers={"Content-Type": "application/json"} if data else {})
    with urllib.request.urlopen(req, timeout=8) as r:
        return r.status, r.read().decode("utf-8")

def parse_data_js(text):
    body = text.strip()
    body = body[body.index("=") + 1:].strip()
    if body.endswith(";"): body = body[:-1]
    return json.loads(body)

def ensure_server():
    if port_open(PORT):
        return None
    proc = subprocess.Popen([sys.executable, "server.py", str(PORT)], cwd=DIR,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    for _ in range(20):
        time.sleep(0.5)
        if port_open(PORT): break
    return proc

def run_all_tests():
    print("=== Daily Planner E2E ===")
    started = ensure_server()
    original = open(DATA, encoding="utf-8").read()
    try:
        # 1. app served
        try:
            st, html = http("GET", "/")
            check("GET / serves app", st == 200 and "Daily Planner" in html, f"status {st}")
        except Exception as e:
            check("GET / serves app", False, str(e))

        # 2. data endpoint
        try:
            st, txt = http("GET", "/todo-data.js")
            check("GET /todo-data.js", st == 200 and "window.TODO_DATA" in txt, f"status {st}")
            obj = parse_data_js(txt)
        except Exception as e:
            check("GET /todo-data.js", False, str(e)); obj = None

        # 3. schema integrity
        if obj:
            ok = isinstance(obj.get("days"), dict) and "backlog" in obj
            day_ok = all("tasks" in d and isinstance(d["tasks"], list) for d in obj["days"].values())
            task_ok = all(all(k in t for k in ("id", "t", "st")) for d in obj["days"].values() for t in d["tasks"])
            check("data schema (days/backlog/tasks)", ok and day_ok and task_ok)

        # 4. save round-trip (non-destructive) + history snapshot
        if obj:
            try:
                st, _ = http("POST", "/save", obj)
                today = time.strftime("%Y-%m-%d")
                hist = os.path.join(HIST, f"todo-data-{today}.js")
                check("POST /save persists", st == 200 and os.path.exists(hist), f"status {st}, history {os.path.exists(hist)}")
            except Exception as e:
                check("POST /save persists", False, str(e))

        # 5. persistence: flip a status, save, re-read, assert it stuck (== survives refresh)
        if obj:
            try:
                clone = copy.deepcopy(obj)
                fk = next(iter(clone["days"]))
                t0 = clone["days"][fk]["tasks"][0]
                orig_st = t0["st"]
                t0["st"] = "done" if orig_st != "done" else "pending"
                http("POST", "/save", clone)
                _, txt2 = http("GET", "/todo-data.js")
                obj2 = parse_data_js(txt2)
                got = obj2["days"][fk]["tasks"][0]["st"]
                check("click persists across refresh", got == t0["st"], f"saved={t0['st']} read={got}")
            except Exception as e:
                check("click persists across refresh", False, str(e))

        # 6. front-end JS executes across all views (headless harness)
        try:
            r = subprocess.run(["node", "_jscheck.js"], cwd=DIR, capture_output=True, text=True, timeout=30)
            out = (r.stdout + r.stderr).strip()
            check("front-end JS runs (all tabs)", r.returncode == 0 and "JS_OK" in out, out.splitlines()[-1] if out else "no output")
        except Exception as e:
            check("front-end JS runs (all tabs)", False, str(e))

    finally:
        # restore original data so tests never alter the real schedule
        with open(DATA, "w", encoding="utf-8") as f:
            f.write(original)
        # also restore the http-served copy by saving the original object back
        try:
            http("POST", "/save", parse_data_js(original))
        except Exception:
            pass

    passed = sum(1 for _, ok, _ in results if ok)
    total = len(results)
    print(f"\n=== {passed}/{total} passed ===")
    return {"passed": passed, "total": total,
            "tests": [{"name": n, "ok": ok, "detail": d} for n, ok, d in results]}

if __name__ == "__main__":
    rep = run_all_tests()
    sys.exit(0 if rep["passed"] == rep["total"] else 1)
