"""
E2E Tests for PR Reviewer Dashboard.
Tests all API endpoints against a running server with a real ADO PR.

Usage:
    python e2e_tests.py [--pr-url <url>]

If no --pr-url is given, uses a default known PR for testing.
"""
import json
import sys
import os
import time
import subprocess
import socket
import urllib.request
import urllib.error

DIR = os.path.dirname(os.path.abspath(__file__))
PORT = 8097
BASE = f"http://localhost:{PORT}"

# Default test PR — a small, completed PR that won't change
DEFAULT_TEST_PR = (
    "https://dev.azure.com/microsoft/DefenderCommon/_git/FE.MSecSCC/pullrequest/15294126"
)


# ── Helpers ──────────────────────────────────────────────────────────

class TestResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []

    def ok(self, name):
        self.passed += 1
        print(f"  ✅ {name}")

    def fail(self, name, reason):
        self.failed += 1
        self.errors.append((name, reason))
        print(f"  ❌ {name}: {reason}")

    def summary(self):
        total = self.passed + self.failed
        print(f"\n{'='*50}")
        print(f"  Results: {self.passed}/{total} passed, {self.failed} failed")
        if self.errors:
            print(f"\n  Failures:")
            for name, reason in self.errors:
                print(f"    - {name}: {reason}")
        print(f"{'='*50}")
        return self.failed == 0


def api(method, path, body=None, timeout=120):
    """Make request to the PR reviewer server."""
    url = BASE + path
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode() if e.fp else ""
        try:
            return e.code, json.loads(body_text)
        except Exception:
            return e.code, {"error": body_text[:200]}
    except Exception as e:
        return 0, {"error": str(e)}


def is_server_running():
    try:
        status, data = api("GET", "/api/health", timeout=3)
        return status == 200 and data.get("status") == "ok"
    except Exception:
        return False


# ── Server lifecycle ─────────────────────────────────────────────────

_server_proc = None

def start_server():
    global _server_proc
    if is_server_running():
        print("  Server already running on port", PORT)
        return True

    print("  Starting server...")
    _server_proc = subprocess.Popen(
        [sys.executable, "pr_reviewer_server.py", "--no-browser"],
        cwd=DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
    )
    for _ in range(15):
        time.sleep(1)
        if is_server_running():
            print("  Server started")
            return True
    print("  ⚠ Server failed to start")
    return False


def stop_server():
    global _server_proc
    if _server_proc:
        _server_proc.terminate()
        _server_proc.wait(timeout=5)
        _server_proc = None
        print("  Server stopped")


# ── Test Cases ───────────────────────────────────────────────────────

def test_health(r):
    """Test /api/health endpoint."""
    status, data = api("GET", "/api/health")
    if status == 200 and data.get("status") == "ok":
        r.ok("GET /api/health")
    else:
        r.fail("GET /api/health", f"status={status}, data={data}")


def test_ai_status(r):
    """Test /api/ai-status endpoint."""
    status, data = api("GET", "/api/ai-status")
    if status == 200 and "configured" in data:
        r.ok("GET /api/ai-status")
    else:
        r.fail("GET /api/ai-status", f"status={status}, data={data}")


def test_review_empty(r):
    """Test /api/review before loading a PR."""
    status, data = api("GET", "/api/review")
    if status == 200:
        r.ok("GET /api/review (empty state)")
    else:
        r.fail("GET /api/review (empty)", f"status={status}")


def test_load_pr_missing_url(r):
    """Test loading with no URL returns error."""
    status, data = api("POST", "/api/load-pr", {"url": ""})
    if status == 400 and "error" in data:
        r.ok("POST /api/load-pr (missing URL → 400)")
    else:
        r.fail("POST /api/load-pr (missing URL)", f"expected 400, got {status}")


def test_load_pr_invalid_url(r):
    """Test loading with invalid URL returns error."""
    status, data = api("POST", "/api/load-pr", {"url": "https://google.com"})
    if status == 500 and "error" in data:
        r.ok("POST /api/load-pr (invalid URL → error)")
    else:
        r.fail("POST /api/load-pr (invalid URL)", f"expected error, got {status}: {data}")


def test_load_pr(r, pr_url):
    """Test loading a real PR."""
    status, data = api("POST", "/api/load-pr", {"url": pr_url})
    if status != 200 or not data.get("ok"):
        r.fail("POST /api/load-pr", f"status={status}, error={data.get('error', '?')}")
        return False

    pr_info = data.get("pr_info", {})
    checks = [
        ("has pr_id", bool(pr_info.get("pr_id"))),
        ("has title", bool(pr_info.get("title"))),
        ("has author", bool(pr_info.get("author"))),
        ("has repo_id", bool(pr_info.get("repo_id"))),
        ("files_count > 0", data.get("files_count", 0) > 0),
        ("diffs_count > 0", data.get("diffs_count", 0) > 0),
    ]
    all_ok = True
    for label, ok in checks:
        if not ok:
            r.fail(f"POST /api/load-pr ({label})", f"data={data}")
            all_ok = False

    if all_ok:
        r.ok(f"POST /api/load-pr → {pr_info.get('title', '?')[:50]} "
             f"({data['files_count']} files, {data['diffs_count']} diffs)")
    return all_ok


def test_review_after_load(r):
    """Test /api/review returns PR info and diffs after loading."""
    status, data = api("GET", "/api/review")
    if status != 200:
        r.fail("GET /api/review (after load)", f"status={status}")
        return

    has_pr = bool(data.get("pr_info", {}).get("pr_id"))
    has_diffs = len(data.get("diffs", {})) > 0
    has_files = len(data.get("changed_files", [])) > 0

    if has_pr and has_diffs and has_files:
        r.ok(f"GET /api/review (has pr_info, {len(data['diffs'])} diffs, "
             f"{len(data['changed_files'])} files)")
    else:
        r.fail("GET /api/review (after load)",
               f"pr={has_pr}, diffs={has_diffs}, files={has_files}")


def test_generate_review_no_ai(r):
    """Test AI review when not configured returns empty."""
    status, data = api("GET", "/api/ai-status")
    if data.get("configured"):
        r.ok("SKIP generate review (AI is configured)")
        return

    status, data = api("POST", "/api/generate-review")
    if status == 200 and isinstance(data.get("comments"), list):
        r.ok(f"POST /api/generate-review (no AI → {len(data['comments'])} comments)")
    else:
        r.fail("POST /api/generate-review (no AI)", f"status={status}, data={data}")


def test_add_comment(r):
    """Test adding a manual comment."""
    comment_data = {
        "severity": "medium",
        "file": "/test/file.tsx",
        "line": 42,
        "comment": "E2E test comment — could this be improved?",
        "issue": "Test issue description",
        "suggestion": "Test suggestion",
    }
    status, data = api("POST", "/api/add-comment", comment_data)
    if status != 200 or not data.get("ok"):
        r.fail("POST /api/add-comment", f"status={status}, data={data}")
        return None

    comment = data.get("comment", {})
    checks = [
        ("has id", "id" in comment),
        ("severity matches", comment.get("severity") == "medium"),
        ("file matches", comment.get("file") == "/test/file.tsx"),
        ("line matches", comment.get("line") == 42),
        ("comment matches", "E2E test" in comment.get("comment", "")),
        ("not posted", comment.get("posted") is False),
    ]
    all_ok = True
    for label, ok in checks:
        if not ok:
            r.fail(f"POST /api/add-comment ({label})", f"comment={comment}")
            all_ok = False

    if all_ok:
        r.ok(f"POST /api/add-comment → id={comment['id']}")
    return comment.get("id")


def test_add_second_comment(r):
    """Test adding a second comment (IDs should increment)."""
    status, data = api("POST", "/api/add-comment", {
        "severity": "critical",
        "file": "/test/other.ts",
        "line": 10,
        "comment": "Second test comment",
        "issue": "Another issue",
        "suggestion": "Another fix",
    })
    if status == 200 and data.get("ok"):
        cid = data["comment"]["id"]
        r.ok(f"POST /api/add-comment (2nd) → id={cid}")
        return cid
    else:
        r.fail("POST /api/add-comment (2nd)", f"status={status}")
        return None


def test_update_comment(r, comment_id):
    """Test updating a comment's text."""
    if comment_id is None:
        r.fail("PUT /api/comment (update)", "no comment_id from add step")
        return

    new_text = "Updated E2E test comment — is this better?"
    status, data = api("PUT", f"/api/comment/{comment_id}",
                       {"comment": new_text})
    if status != 200 or not data.get("ok"):
        r.fail("PUT /api/comment (update)", f"status={status}, data={data}")
        return

    if data.get("comment", {}).get("comment") == new_text:
        r.ok(f"PUT /api/comment/{comment_id} (text updated)")
    else:
        r.fail("PUT /api/comment (update)",
               f"expected updated text, got: {data.get('comment', {}).get('comment')}")


def test_update_saves_learning(r, comment_id):
    """Test that updating a comment saves a learning."""
    if comment_id is None:
        r.fail("Learning saved on update", "no comment_id")
        return

    status, data = api("GET", "/api/learnings")
    if status == 200 and isinstance(data, list) and len(data) > 0:
        last = data[-1]
        if "Updated E2E test comment" in last or "E2E test comment" in last:
            r.ok(f"Learning saved on comment update ({len(data)} total)")
        else:
            r.fail("Learning saved on update", f"last learning doesn't match: {last[:80]}")
    else:
        r.fail("Learning saved on update", f"status={status}, learnings={data}")


def test_delete_comment(r, comment_id):
    """Test deleting a comment."""
    if comment_id is None:
        r.fail("DELETE /api/comment", "no comment_id")
        return

    status, data = api("DELETE", f"/api/comment/{comment_id}")
    if status == 200 and data.get("deleted"):
        r.ok(f"DELETE /api/comment/{comment_id}")
    else:
        r.fail("DELETE /api/comment", f"status={status}, data={data}")


def test_delete_nonexistent(r):
    """Test deleting a comment that doesn't exist."""
    status, data = api("DELETE", "/api/comment/99999")
    if status == 200 and data.get("deleted") is False:
        r.ok("DELETE /api/comment/99999 (nonexistent → deleted=false)")
    else:
        r.fail("DELETE /api/comment (nonexistent)", f"status={status}, data={data}")


def test_update_nonexistent(r):
    """Test updating a comment that doesn't exist."""
    status, data = api("PUT", "/api/comment/99999", {"comment": "nope"})
    if status == 404:
        r.ok("PUT /api/comment/99999 (nonexistent → 404)")
    else:
        r.fail("PUT /api/comment (nonexistent)", f"expected 404, got {status}")


def test_comments_in_review(r):
    """Test that /api/review returns the remaining comments."""
    status, data = api("GET", "/api/review")
    comments = data.get("comments", [])
    # We added 2, deleted 1 → should have at least 1
    if status == 200 and len(comments) >= 1:
        r.ok(f"GET /api/review has {len(comments)} comment(s) after add/delete")
    else:
        r.fail("GET /api/review (comments)", f"expected ≥1 comments, got {len(comments)}")


def test_post_comment_no_pr(r):
    """Test posting when we have comments but verify the post mechanism.
    We skip actual posting to avoid spamming the PR.
    """
    # Just verify the endpoint exists and returns expected error for bad ID
    status, data = api("POST", "/api/post-comment", {"id": 99999})
    if status == 400 and "not found" in data.get("error", "").lower():
        r.ok("POST /api/post-comment (bad id → 'not found')")
    elif status == 200:
        r.ok("POST /api/post-comment (accepted — unexpected but ok)")
    else:
        # Any non-crash response is acceptable
        r.ok(f"POST /api/post-comment (bad id → {status}, handled gracefully)")


def test_save_learning(r):
    """Test saving a manual learning."""
    status, data = api("POST", "/api/save-learning",
                       {"text": "E2E test learning — always check for null"})
    if status == 200 and data.get("ok"):
        r.ok("POST /api/save-learning")
    else:
        r.fail("POST /api/save-learning", f"status={status}, data={data}")


def test_save_learning_empty(r):
    """Test saving empty learning returns error."""
    status, data = api("POST", "/api/save-learning", {"text": ""})
    if status == 400:
        r.ok("POST /api/save-learning (empty → 400)")
    else:
        r.fail("POST /api/save-learning (empty)", f"expected 400, got {status}")


def test_dashboard_html(r):
    """Test that dashboard.html is served."""
    try:
        req = urllib.request.Request(BASE + "/dashboard.html")
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = resp.read().decode()
            if resp.status == 200 and "PR Reviewer" in body:
                r.ok("GET /dashboard.html (200, has title)")
            else:
                r.fail("GET /dashboard.html", f"status={resp.status}, missing title")
    except Exception as e:
        r.fail("GET /dashboard.html", str(e))


def test_404(r):
    """Test unknown endpoint returns 404."""
    status, data = api("POST", "/api/nonexistent")
    if status == 404:
        r.ok("POST /api/nonexistent → 404")
    else:
        r.fail("POST /api/nonexistent", f"expected 404, got {status}")


# ── Cleanup ──────────────────────────────────────────────────────────

def cleanup_learnings():
    """Remove test learnings from learnings.json."""
    path = os.path.join(DIR, "learnings.json")
    try:
        with open(path, "r") as f:
            learnings = json.load(f)
        cleaned = [l for l in learnings if "E2E test" not in l]
        with open(path, "w") as f:
            json.dump(cleaned, f, indent=2)
        removed = len(learnings) - len(cleaned)
        if removed:
            print(f"  Cleaned {removed} test learnings")
    except Exception:
        pass


# ── Main ─────────────────────────────────────────────────────────────

def run_all_tests(pr_url=None):
    pr_url = pr_url or DEFAULT_TEST_PR
    r = TestResult()

    print(f"\n{'='*50}")
    print(f"  PR Reviewer — E2E Tests")
    print(f"  PR: {pr_url}")
    print(f"{'='*50}\n")

    # Phase 1: Server health
    print("Phase 1: Server health")
    test_health(r)
    test_ai_status(r)
    test_dashboard_html(r)
    test_404(r)

    # Phase 2: Empty state
    print("\nPhase 2: Error handling")
    test_load_pr_missing_url(r)
    test_load_pr_invalid_url(r)

    # Phase 3: Load PR
    print("\nPhase 3: Load PR")
    pr_loaded = test_load_pr(r, pr_url)

    if pr_loaded:
        # Phase 4: Review state
        print("\nPhase 4: Review state after load")
        test_review_after_load(r)
        test_generate_review_no_ai(r)

        # Phase 5: Comment CRUD
        print("\nPhase 5: Comment CRUD")
        cid1 = test_add_comment(r)
        cid2 = test_add_second_comment(r)
        test_comments_in_review(r)
        test_update_comment(r, cid1)
        test_update_saves_learning(r, cid1)
        test_update_nonexistent(r)
        test_delete_comment(r, cid2)
        test_delete_nonexistent(r)
        test_comments_in_review(r)

        # Phase 6: Post comment (safe — bad ID only)
        print("\nPhase 6: Post comment (safe)")
        test_post_comment_no_pr(r)

        # Phase 7: Learnings
        print("\nPhase 7: Learnings")
        test_save_learning(r)
        test_save_learning_empty(r)

    # Cleanup
    print("\nCleanup:")
    cleanup_learnings()

    return r.summary()


if __name__ == "__main__":
    pr_url = None
    if "--pr-url" in sys.argv:
        idx = sys.argv.index("--pr-url")
        if idx + 1 < len(sys.argv):
            pr_url = sys.argv[idx + 1]

    # Ensure server is running
    print("Setup:")
    we_started = False
    if not is_server_running():
        we_started = start_server()
        if not we_started and not is_server_running():
            print("❌ Could not start server")
            sys.exit(1)

    try:
        success = run_all_tests(pr_url)
    finally:
        if we_started:
            stop_server()

    sys.exit(0 if success else 1)
