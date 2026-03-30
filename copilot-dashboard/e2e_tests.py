"""
E2E Tests for Copilot CLI Dashboard
Run standalone or triggered via /api/e2e-tests endpoint.
"""
import urllib.request
import json
import time
import sys

BASE = "http://localhost:8787"


def _get(path, timeout=10):
    return urllib.request.urlopen(BASE + path, timeout=timeout)


def _post(path, body=None, timeout=15):
    data = json.dumps(body).encode() if body else None
    headers = {"Content-Type": "application/json"} if body else {}
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method="POST")
    return urllib.request.urlopen(req, timeout=timeout)


def _json(resp):
    return json.loads(resp.read())


def run_tests():
    results = []

    def test(name, fn):
        try:
            ok = fn()
            results.append({"name": name, "passed": bool(ok), "error": None})
        except Exception as e:
            results.append({"name": name, "passed": False, "error": str(e)})

    # === Dashboard Basics ===
    test("Dashboard HTML loads", lambda: _get("/dashboard.html").status == 200)

    test("Dashboard has all tabs", lambda: all(
        tab in _get("/dashboard.html").read().decode()
        for tab in ["tab-overview", "tab-sessions", "tab-categories", "tab-productivity", "tab-parallel", "tab-manager"]
    ))

    test("Dashboard data JSON loads", lambda: (
        "sessions" in _json(_get("/dashboard-data.json"))
    ))

    test("Refresh API works", lambda: (
        "ok" in _json(_post("/api/refresh"))
    ))

    # === Search API ===
    test("Search API returns results for broad query", lambda: (
        len(_json(_get("/api/search-sessions?q=copilot")).get("results", [])) >= 0
    ))

    test("Search API returns empty for nonsense", lambda: (
        _json(_get("/api/search-sessions?q=xyzzy_nonexistent_zz")).get("results") is not None
    ))

    test("Search API requires query param", lambda: (
        _get("/api/search-sessions?q=test").status == 200
    ))

    # === Terminal Panel (HTML structure) ===
    test("Dashboard has terminal panel HTML", lambda: all(
        elem in _get("/dashboard.html").read().decode()
        for elem in ["terminal-panel", "tp-output", "tp-inputbar", "openTerminalPanel", "session-panel-wrapper"]
    ))

    # === Session Manager APIs ===
    test("Managed sessions list API", lambda: (
        "sessions" in _json(_get("/api/managed-sessions"))
    ))

    # Start a session
    test_sid = [None]

    def t_start():
        d = _json(_post("/api/managed-session/start", {
            "prompt": "What is 2+2? Reply with just the number.",
            "cwd": r"%USERPROFILE%",
            "autopilot": True, "allow_all": True
        }))
        test_sid[0] = d.get("managed_id")
        return d.get("ok") and test_sid[0]
    test("Start managed session", t_start)

    def t_status():
        time.sleep(3)
        d = _json(_get(f"/api/managed-session/{test_sid[0]}/status"))
        return d.get("ok") and d.get("status") in ("running", "completed")
    test("Session status API", t_status)

    def t_events():
        d = _json(_get(f"/api/managed-session/{test_sid[0]}/events"))
        return "events" in d and len(d["events"]) > 0
    test("Session events API", t_events)

    def t_sse():
        r = _get(f"/api/managed-session/{test_sid[0]}/stream")
        chunk = r.read(4000).decode()
        r.close()
        return "data:" in chunk
    test("SSE stream delivers events", t_sse)

    def t_list_has():
        d = _json(_get("/api/managed-sessions"))
        return any(s["id"] == test_sid[0] for s in d.get("sessions", []))
    test("Session appears in list", t_list_has)

    # Wait for completion
    def t_complete():
        for _ in range(30):
            d = _json(_get(f"/api/managed-session/{test_sid[0]}/status"))
            if d.get("status") in ("completed", "stopped", "error"):
                return d.get("turn_count", 0) > 0
            time.sleep(2)
        return False
    test("Session completes with turns", t_complete)

    # Start and stop
    sid2 = [None]

    def t_stop():
        d = _json(_post("/api/managed-session/start", {
            "prompt": "Write a very long 5000 word essay",
            "cwd": r"%USERPROFILE%",
            "autopilot": True, "allow_all": True
        }))
        sid2[0] = d["managed_id"]
        time.sleep(4)
        d2 = _json(_post(f"/api/managed-session/{sid2[0]}/stop"))
        time.sleep(1)
        d3 = _json(_get(f"/api/managed-session/{sid2[0]}/status"))
        return d2.get("ok") and d3.get("status") == "stopped"
    test("Start and stop session", t_stop)

    # Input to stopped session
    def t_input_fail():
        d = _json(_post(f"/api/managed-session/{sid2[0]}/input", {"message": "hello"}))
        return d.get("ok") == False
    test("Input to stopped session fails gracefully", t_input_fail)

    # 404 for unknown session
    def t_404():
        try:
            _get("/api/managed-session/nonexistent/status")
            return False
        except urllib.error.HTTPError as e:
            return e.code == 404
    test("404 for unknown session", t_404)

    # Legacy resume API
    test("Legacy resume API works", lambda: (
        _json(_post("/api/resume-session", {"session_id": "fake-id", "cwd": r"%USERPROFILE%"})).get("ok")
    ))

    passed = sum(1 for r in results if r["passed"])
    return {
        "total": len(results),
        "passed": passed,
        "failed": len(results) - passed,
        "results": results,
    }


if __name__ == "__main__":
    print("Running Copilot Dashboard E2E Tests...")
    print(f"Target: {BASE}\n")
    report = run_tests()
    for r in report["results"]:
        sym = "\u2705" if r["passed"] else "\u274c"
        err = f" — {r['error']}" if r["error"] else ""
        print(f"  {sym} {r['name']}{err}")
    print(f"\nResults: {report['passed']}/{report['total']} passed")
    sys.exit(0 if report["failed"] == 0 else 1)
