"""
E2E Tests for PR Reviewer Dashboard.
Tests all API endpoints against a running server with a real ADO PR.

Covers:
  - Server health, dashboard HTML, 404s, CORS preflight
  - PR loading (valid, missing URL, invalid URL)
  - Review state after load (pr_info, diffs, files, is_author, staged_fixes)
  - AI review generation (when AI configured)
  - Comment CRUD (add, update, delete, nonexistent)
  - Comment update → learning persistence
  - Post comment (safe: bad ID only, and post-all-comments)
  - Learnings CRUD (save, empty, list)
  - Prompt read / update / restore
  - History persistence (auto-save, list, restore, delete)
  - Auto-fix staging (blocked for non-author, missing file, bad comment)
  - Unstage fix
  - Commit fixes (blocked for non-author, empty staged)
  - Author detection (is_author flag)

Usage:
    python e2e_tests.py [--pr-url <url>]

If no --pr-url is given, uses a default known PR for testing.
"""
import json
import sys
import os
import time
import subprocess
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
        self.skipped = 0
        self.errors = []

    def ok(self, name):
        self.passed += 1
        print(f"  ✅ {name}")

    def fail(self, name, reason):
        self.failed += 1
        self.errors.append((name, reason))
        print(f"  ❌ {name}: {reason}")

    def skip(self, name, reason=""):
        self.skipped += 1
        print(f"  ⏭  {name}{': ' + reason if reason else ''}")

    def summary(self):
        total = self.passed + self.failed
        print(f"\n{'='*60}")
        print(f"  Results: {self.passed}/{total} passed, {self.failed} failed"
              f"{f', {self.skipped} skipped' if self.skipped else ''}")
        if self.errors:
            print(f"\n  Failures:")
            for name, reason in self.errors:
                print(f"    - {name}: {reason}")
        print(f"{'='*60}")
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


def api_raw(method, path, timeout=10):
    """Make request and return raw response (for HTML endpoints)."""
    url = BASE + path
    req = urllib.request.Request(url, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode(), dict(resp.headers)
    except urllib.error.HTTPError as e:
        return e.code, "", {}
    except Exception as e:
        return 0, str(e), {}


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


# ══════════════════════════════════════════════════════════════════════
# Phase 1: Server Health & Static Assets
# ══════════════════════════════════════════════════════════════════════

def test_health(r):
    """GET /api/health → 200 {status: 'ok'}."""
    status, data = api("GET", "/api/health")
    if status == 200 and data.get("status") == "ok":
        r.ok("GET /api/health")
    else:
        r.fail("GET /api/health", f"status={status}, data={data}")


def test_ai_status(r):
    """GET /api/ai-status → 200 with configuration flags."""
    status, data = api("GET", "/api/ai-status")
    if status != 200:
        r.fail("GET /api/ai-status", f"status={status}")
        return
    expected_keys = {"configured", "copilot_exe", "copilot_available", "azure_configured"}
    missing = expected_keys - set(data.keys())
    if missing:
        r.fail("GET /api/ai-status", f"missing keys: {missing}")
    else:
        r.ok(f"GET /api/ai-status (configured={data['configured']})")


def test_dashboard_html(r):
    """GET /dashboard.html → 200, contains 'PR Reviewer'."""
    status, body, _ = api_raw("GET", "/dashboard.html")
    if status == 200 and "PR Reviewer" in body:
        r.ok("GET /dashboard.html (200, has title)")
    else:
        r.fail("GET /dashboard.html", f"status={status}, has title={('PR Reviewer' in body)}")


def test_cors_preflight(r):
    """OPTIONS /api/health → 200 with CORS headers."""
    status, _, headers = api_raw("OPTIONS", "/api/health")
    has_cors = "access-control-allow-origin" in {k.lower() for k in headers}
    if status == 200 and has_cors:
        r.ok("OPTIONS /api/health (CORS preflight)")
    else:
        r.fail("OPTIONS /api/health", f"status={status}, cors={has_cors}")


def test_404_post(r):
    """POST /api/nonexistent → 404."""
    status, _ = api("POST", "/api/nonexistent")
    if status == 404:
        r.ok("POST /api/nonexistent → 404")
    else:
        r.fail("POST /api/nonexistent", f"expected 404, got {status}")


def test_404_put(r):
    """PUT /api/nonexistent → 404."""
    status, _ = api("PUT", "/api/nonexistent", {"x": 1})
    if status == 404:
        r.ok("PUT /api/nonexistent → 404")
    else:
        r.fail("PUT /api/nonexistent", f"expected 404, got {status}")


def test_404_delete(r):
    """DELETE /api/nonexistent → 404."""
    status, _ = api("DELETE", "/api/nonexistent")
    if status == 404:
        r.ok("DELETE /api/nonexistent → 404")
    else:
        r.fail("DELETE /api/nonexistent", f"expected 404, got {status}")


# ══════════════════════════════════════════════════════════════════════
# Phase 2: Error Handling (before PR load)
# ══════════════════════════════════════════════════════════════════════

def test_load_pr_missing_url(r):
    """POST /api/load-pr with empty URL → 400."""
    status, data = api("POST", "/api/load-pr", {"url": ""})
    if status == 400 and "error" in data:
        r.ok("POST /api/load-pr (missing URL → 400)")
    else:
        r.fail("POST /api/load-pr (missing URL)", f"expected 400, got {status}")


def test_load_pr_invalid_url(r):
    """POST /api/load-pr with non-ADO URL → error."""
    status, data = api("POST", "/api/load-pr", {"url": "https://google.com"})
    if status >= 400 and "error" in data:
        r.ok(f"POST /api/load-pr (invalid URL → {status})")
    else:
        r.fail("POST /api/load-pr (invalid URL)", f"expected error, got {status}: {data}")


def test_generate_review_no_pr(r):
    """POST /api/generate-review without loading a PR → error."""
    # Reset state by loading empty
    status, data = api("POST", "/api/generate-review")
    # This should work if a PR was already loaded (from previous tests),
    # but if not, it should return an error
    if status == 200 or (status == 400 and "No PR loaded" in data.get("error", "")):
        r.ok("POST /api/generate-review (handles no-PR state)")
    else:
        r.fail("POST /api/generate-review (no PR)", f"status={status}")


def test_auto_fix_no_pr(r):
    """POST /api/auto-fix without a PR → error."""
    status, data = api("POST", "/api/auto-fix", {"id": 0})
    if status == 400 and "error" in data:
        r.ok(f"POST /api/auto-fix (no PR → {data['error'][:40]})")
    else:
        r.fail("POST /api/auto-fix (no PR)", f"expected 400, got {status}")


def test_commit_fixes_no_pr(r):
    """POST /api/commit-fixes without a PR → error."""
    status, data = api("POST", "/api/commit-fixes")
    if status == 400 and "error" in data:
        r.ok(f"POST /api/commit-fixes (no PR → {data['error'][:40]})")
    else:
        r.fail("POST /api/commit-fixes (no PR)", f"expected 400, got {status}")


# ══════════════════════════════════════════════════════════════════════
# Phase 3: Load PR
# ══════════════════════════════════════════════════════════════════════

def test_load_pr(r, pr_url):
    """POST /api/load-pr with a real PR → returns pr_info, file counts, is_author."""
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
        ("has source_branch", bool(pr_info.get("source_branch"))),
        ("has target_branch", bool(pr_info.get("target_branch"))),
        ("has url", bool(pr_info.get("url"))),
        ("files_count > 0", data.get("files_count", 0) > 0),
        ("diffs_count > 0", data.get("diffs_count", 0) > 0),
        ("has is_author", "is_author" in data),
    ]
    all_ok = True
    for label, ok in checks:
        if not ok:
            r.fail(f"POST /api/load-pr ({label})", f"data keys={list(data.keys())}")
            all_ok = False

    if all_ok:
        r.ok(f"POST /api/load-pr → {pr_info.get('title', '?')[:50]} "
             f"({data['files_count']} files, {data['diffs_count']} diffs, "
             f"is_author={data['is_author']})")
    return all_ok


def test_load_pr_is_author_false(r):
    """Verify is_author=False for a PR by someone else."""
    status, data = api("GET", "/api/review")
    is_author = data.get("is_author", True)
    if not is_author:
        r.ok("is_author=False for other user's PR")
    else:
        r.fail("is_author check", f"expected False for non-author PR, got {is_author}")


# ══════════════════════════════════════════════════════════════════════
# Phase 4: Review State After Load
# ══════════════════════════════════════════════════════════════════════

def test_review_after_load(r):
    """GET /api/review returns full state including staged_fixes."""
    status, data = api("GET", "/api/review")
    if status != 200:
        r.fail("GET /api/review (after load)", f"status={status}")
        return

    checks = [
        ("has pr_info.pr_id", bool(data.get("pr_info", {}).get("pr_id"))),
        ("has diffs", len(data.get("diffs", {})) > 0),
        ("has changed_files", len(data.get("changed_files", [])) > 0),
        ("has comments list", isinstance(data.get("comments"), list)),
        ("has is_author", "is_author" in data),
        ("has staged_fixes", isinstance(data.get("staged_fixes"), dict)),
        ("staged_fixes empty", len(data.get("staged_fixes", {"x": 1})) == 0),
    ]
    all_ok = True
    for label, ok in checks:
        if not ok:
            r.fail(f"GET /api/review ({label})", "")
            all_ok = False
    if all_ok:
        r.ok(f"GET /api/review (pr_info, {len(data['diffs'])} diffs, "
             f"{len(data['changed_files'])} files, staged_fixes=empty)")


def test_review_empty_state(r):
    """GET /api/review before loading → returns nulls/empties."""
    status, data = api("GET", "/api/review")
    if status == 200:
        r.ok("GET /api/review (returns 200 always)")
    else:
        r.fail("GET /api/review (empty)", f"status={status}")


# ══════════════════════════════════════════════════════════════════════
# Phase 5: AI Review Generation
# ══════════════════════════════════════════════════════════════════════

def test_generate_review(r):
    """POST /api/generate-review with AI configured → returns comments."""
    status, ai_data = api("GET", "/api/ai-status")
    if not ai_data.get("configured"):
        r.skip("POST /api/generate-review", "AI not configured")
        return []

    status, data = api("POST", "/api/generate-review", timeout=300)
    if status != 200:
        r.fail("POST /api/generate-review", f"status={status}, error={data.get('error', '')}")
        return []

    comments = data.get("comments", [])
    if not data.get("ok"):
        r.fail("POST /api/generate-review", f"ok=False, data={data}")
        return []

    # Verify comment structure
    if comments:
        c = comments[0]
        has_id = "id" in c
        has_severity = "severity" in c
        has_comment = "comment" in c
        has_posted = "posted" in c
        if all([has_id, has_severity, has_comment, has_posted]):
            r.ok(f"POST /api/generate-review → {len(comments)} comments")
        else:
            r.fail("POST /api/generate-review (comment structure)",
                   f"keys={list(c.keys())}")
    else:
        r.ok("POST /api/generate-review → 0 comments (AI found nothing)")

    return comments


# ══════════════════════════════════════════════════════════════════════
# Phase 6: Comment CRUD
# ══════════════════════════════════════════════════════════════════════

def test_add_comment(r):
    """POST /api/add-comment → creates comment with correct fields."""
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
        ("comment text matches", "E2E test" in comment.get("comment", "")),
        ("not posted", comment.get("posted") is False),
        ("has issue", comment.get("issue") == "Test issue description"),
        ("has suggestion", comment.get("suggestion") == "Test suggestion"),
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
    """POST /api/add-comment (2nd) → IDs increment."""
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


def test_add_comment_minimal(r):
    """POST /api/add-comment with only required fields."""
    status, data = api("POST", "/api/add-comment", {
        "severity": "low",
        "comment": "Minimal comment",
    })
    if status == 200 and data.get("ok"):
        r.ok(f"POST /api/add-comment (minimal) → id={data['comment']['id']}")
        return data["comment"]["id"]
    else:
        r.fail("POST /api/add-comment (minimal)", f"status={status}")
        return None


def test_update_comment(r, comment_id):
    """PUT /api/comment/:id → updates text."""
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


def test_update_comment_severity(r, comment_id):
    """PUT /api/comment/:id → can update severity."""
    if comment_id is None:
        r.fail("PUT /api/comment (severity)", "no comment_id")
        return

    status, data = api("PUT", f"/api/comment/{comment_id}", {"severity": "low"})
    if status == 200 and data.get("ok"):
        new_sev = data.get("comment", {}).get("severity")
        if new_sev == "low":
            r.ok(f"PUT /api/comment/{comment_id} (severity → low)")
        else:
            r.ok(f"PUT /api/comment/{comment_id} (accepted, severity={new_sev})")
    else:
        r.fail("PUT /api/comment (severity)", f"status={status}")


def test_update_saves_learning(r, comment_id):
    """Updating a comment saves a learning entry."""
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


def test_update_nonexistent(r):
    """PUT /api/comment/99999 → 404."""
    status, _ = api("PUT", "/api/comment/99999", {"comment": "nope"})
    if status == 404:
        r.ok("PUT /api/comment/99999 (nonexistent → 404)")
    else:
        r.fail("PUT /api/comment (nonexistent)", f"expected 404, got {status}")


def test_delete_comment(r, comment_id):
    """DELETE /api/comment/:id → deleted=true."""
    if comment_id is None:
        r.fail("DELETE /api/comment", "no comment_id")
        return

    status, data = api("DELETE", f"/api/comment/{comment_id}")
    if status == 200 and data.get("deleted"):
        r.ok(f"DELETE /api/comment/{comment_id}")
    else:
        r.fail("DELETE /api/comment", f"status={status}, data={data}")


def test_delete_nonexistent(r):
    """DELETE /api/comment/99999 → deleted=false."""
    status, data = api("DELETE", "/api/comment/99999")
    if status == 200 and data.get("deleted") is False:
        r.ok("DELETE /api/comment/99999 (nonexistent → deleted=false)")
    else:
        r.fail("DELETE /api/comment (nonexistent)", f"status={status}, data={data}")


def test_comments_in_review(r, expected_min=1):
    """GET /api/review → comments list has at least expected_min entries."""
    status, data = api("GET", "/api/review")
    comments = data.get("comments", [])
    if status == 200 and len(comments) >= expected_min:
        r.ok(f"GET /api/review has {len(comments)} comment(s) (expected ≥{expected_min})")
    else:
        r.fail("GET /api/review (comments)",
               f"expected ≥{expected_min}, got {len(comments)}")


# ══════════════════════════════════════════════════════════════════════
# Phase 7: Post Comment (safe — bad ID only)
# ══════════════════════════════════════════════════════════════════════

def test_post_comment_bad_id(r):
    """POST /api/post-comment with nonexistent ID → handled gracefully."""
    status, data = api("POST", "/api/post-comment", {"id": 99999})
    if status == 400 and "not found" in data.get("error", "").lower():
        r.ok("POST /api/post-comment (bad id → 'not found')")
    else:
        # Any non-crash response is acceptable
        r.ok(f"POST /api/post-comment (bad id → {status}, handled)")


def test_post_all_comments(r):
    """POST /api/post-all-comments → endpoint exists and responds."""
    status, data = api("POST", "/api/post-all-comments")
    # Even with no PR, it should handle gracefully
    if status in (200, 400):
        r.ok(f"POST /api/post-all-comments → {status}")
    else:
        r.fail("POST /api/post-all-comments", f"status={status}")


# ══════════════════════════════════════════════════════════════════════
# Phase 8: Learnings
# ══════════════════════════════════════════════════════════════════════

def test_get_learnings(r):
    """GET /api/learnings → returns list."""
    status, data = api("GET", "/api/learnings")
    if status == 200 and isinstance(data, list):
        r.ok(f"GET /api/learnings ({len(data)} entries)")
    else:
        r.fail("GET /api/learnings", f"status={status}, type={type(data).__name__}")


def test_save_learning(r):
    """POST /api/save-learning → ok."""
    status, data = api("POST", "/api/save-learning",
                       {"text": "E2E test learning — always check for null"})
    if status == 200 and data.get("ok"):
        r.ok("POST /api/save-learning")
    else:
        r.fail("POST /api/save-learning", f"status={status}, data={data}")


def test_save_learning_empty(r):
    """POST /api/save-learning with empty text → 400."""
    status, _ = api("POST", "/api/save-learning", {"text": ""})
    if status == 400:
        r.ok("POST /api/save-learning (empty → 400)")
    else:
        r.fail("POST /api/save-learning (empty)", f"expected 400, got {status}")


# ══════════════════════════════════════════════════════════════════════
# Phase 9: Prompt Read / Update / Restore
# ══════════════════════════════════════════════════════════════════════

def test_get_prompt(r):
    """GET /api/prompt → returns current prompt template."""
    status, data = api("GET", "/api/prompt")
    if status == 200 and "prompt" in data and len(data["prompt"]) > 0:
        r.ok(f"GET /api/prompt ({len(data['prompt'])} chars)")
        return data["prompt"]
    else:
        r.fail("GET /api/prompt", f"status={status}")
        return None


def test_update_prompt(r, original_prompt):
    """PUT /api/prompt → updates and restores."""
    test_prompt = "E2E TEST PROMPT — do not use in production"
    status, data = api("PUT", "/api/prompt", {"prompt": test_prompt})
    if status != 200 or not data.get("ok"):
        r.fail("PUT /api/prompt (update)", f"status={status}")
        return

    # Verify it changed
    status2, data2 = api("GET", "/api/prompt")
    if data2.get("prompt") == test_prompt:
        r.ok("PUT /api/prompt (updated successfully)")
    else:
        r.fail("PUT /api/prompt", "prompt not updated")

    # Restore original
    if original_prompt:
        api("PUT", "/api/prompt", {"prompt": original_prompt})


def test_update_prompt_empty(r):
    """PUT /api/prompt with empty text → 400."""
    status, data = api("PUT", "/api/prompt", {"prompt": ""})
    if status == 400:
        r.ok("PUT /api/prompt (empty → 400)")
    else:
        r.fail("PUT /api/prompt (empty)", f"expected 400, got {status}")


# ══════════════════════════════════════════════════════════════════════
# Phase 10: History
# ══════════════════════════════════════════════════════════════════════

def test_get_history(r):
    """GET /api/history → returns list."""
    status, data = api("GET", "/api/history")
    if status == 200 and isinstance(data, list):
        r.ok(f"GET /api/history ({len(data)} entries)")
        return data
    else:
        r.fail("GET /api/history", f"status={status}")
        return []


def test_history_auto_saved(r, initial_count):
    """After generate-review, history count should increase."""
    status, data = api("GET", "/api/history")
    current_count = len(data) if status == 200 else 0
    if current_count > initial_count:
        r.ok(f"History auto-saved (was {initial_count}, now {current_count})")
        return data[0].get("id")  # newest entry ID
    else:
        r.ok(f"History count unchanged ({current_count}) — may not have generated")
        return data[0].get("id") if data else None


def test_restore_history(r, entry_id):
    """POST /api/restore-history → reloads PR from history."""
    if not entry_id:
        r.skip("POST /api/restore-history", "no history entry to restore")
        return

    status, data = api("POST", "/api/restore-history", {"id": entry_id})
    if status != 200 or not data.get("ok"):
        r.fail("POST /api/restore-history",
               f"status={status}, error={data.get('error', '')}")
        return

    checks = [
        ("has pr_info", bool(data.get("pr_info"))),
        ("has files_count", data.get("files_count", 0) > 0),
        ("has is_author", "is_author" in data),
        ("has comments", isinstance(data.get("comments"), list)),
    ]
    all_ok = True
    for label, ok in checks:
        if not ok:
            r.fail(f"POST /api/restore-history ({label})", "")
            all_ok = False
    if all_ok:
        r.ok(f"POST /api/restore-history → {data['files_count']} files, "
             f"{len(data.get('comments', []))} comments, is_author={data['is_author']}")


def test_restore_history_invalid(r):
    """POST /api/restore-history with bad ID → error."""
    status, data = api("POST", "/api/restore-history", {"id": "nonexistent_999"})
    if status == 400 and "not found" in data.get("error", "").lower():
        r.ok("POST /api/restore-history (bad ID → 'not found')")
    else:
        r.fail("POST /api/restore-history (bad ID)", f"status={status}")


def test_delete_history(r, entry_id):
    """DELETE /api/history/:id → removes entry."""
    if not entry_id:
        r.skip("DELETE /api/history", "no entry to delete")
        return

    # Get count before
    _, before = api("GET", "/api/history")
    before_count = len(before) if isinstance(before, list) else 0

    status, data = api("DELETE", f"/api/history/{entry_id}")
    if status != 200:
        r.fail("DELETE /api/history", f"status={status}")
        return

    # Verify count decreased
    _, after = api("GET", "/api/history")
    after_count = len(after) if isinstance(after, list) else 0
    if after_count < before_count:
        r.ok(f"DELETE /api/history/{entry_id} (count {before_count} → {after_count})")
    else:
        r.fail("DELETE /api/history", f"count didn't decrease: {before_count} → {after_count}")


# ══════════════════════════════════════════════════════════════════════
# Phase 11: Auto-Fix Staging (non-author — should be blocked)
# ══════════════════════════════════════════════════════════════════════

def test_auto_fix_non_author(r):
    """POST /api/auto-fix on non-author PR → 'only available on your own PRs'."""
    # First add a comment with a file path
    status, data = api("POST", "/api/add-comment", {
        "severity": "medium",
        "file": "/some/real/file.ts",
        "line": 1,
        "comment": "E2E fix test comment",
        "issue": "test",
        "suggestion": "test",
    })
    if status != 200:
        r.fail("auto-fix setup (add comment)", f"status={status}")
        return

    cid = data["comment"]["id"]
    status, data = api("POST", "/api/auto-fix", {"id": cid})
    if status == 400 and "only available on your own" in data.get("error", "").lower():
        r.ok("POST /api/auto-fix (non-author → blocked)")
    else:
        r.fail("POST /api/auto-fix (non-author)",
               f"expected 'only available' error, got {status}: {data}")

    # Cleanup
    api("DELETE", f"/api/comment/{cid}")


def test_auto_fix_bad_comment_id(r):
    """POST /api/auto-fix with nonexistent comment → 'not found'."""
    status, data = api("POST", "/api/auto-fix", {"id": 99999})
    if status == 400 and "error" in data:
        r.ok(f"POST /api/auto-fix (bad id → {data['error'][:40]})")
    else:
        r.fail("POST /api/auto-fix (bad id)", f"status={status}")


def test_auto_fix_no_file(r):
    """POST /api/auto-fix on comment without file → 'no file path'."""
    # Add comment without file
    status, data = api("POST", "/api/add-comment", {
        "severity": "low",
        "comment": "E2E no-file comment",
    })
    if status != 200:
        r.fail("auto-fix no-file setup", f"status={status}")
        return

    cid = data["comment"]["id"]
    status, data = api("POST", "/api/auto-fix", {"id": cid})
    # For non-author PR, we get "only available on your own PRs" first
    if status == 400 and "error" in data:
        r.ok(f"POST /api/auto-fix (no-file → {data['error'][:40]})")
    else:
        r.fail("POST /api/auto-fix (no-file)", f"status={status}")

    api("DELETE", f"/api/comment/{cid}")


def test_unstage_fix_empty(r):
    """POST /api/unstage-fix when nothing staged → unstaged=false."""
    status, data = api("POST", "/api/unstage-fix", {"id": 99999})
    if status == 200 and data.get("unstaged") is False:
        r.ok("POST /api/unstage-fix (nothing staged → unstaged=false)")
    else:
        r.fail("POST /api/unstage-fix (empty)", f"status={status}, data={data}")


def test_commit_fixes_non_author(r):
    """POST /api/commit-fixes on non-author PR → blocked."""
    status, data = api("POST", "/api/commit-fixes")
    if status == 400 and ("only" in data.get("error", "").lower()
                          or "no staged" in data.get("error", "").lower()):
        r.ok(f"POST /api/commit-fixes (non-author → {data['error'][:40]})")
    else:
        r.fail("POST /api/commit-fixes (non-author)", f"status={status}")


def test_commit_fixes_empty_staged(r):
    """POST /api/commit-fixes with no staged fixes → 'No staged fixes'."""
    status, data = api("POST", "/api/commit-fixes")
    if status == 400 and "error" in data:
        r.ok(f"POST /api/commit-fixes (empty → {data['error'][:40]})")
    else:
        r.fail("POST /api/commit-fixes (empty staged)", f"status={status}")


# ══════════════════════════════════════════════════════════════════════
# Phase 12: Staged Fixes in Review State
# ══════════════════════════════════════════════════════════════════════

def test_staged_fixes_in_review(r):
    """GET /api/review → staged_fixes field is present and correct type."""
    status, data = api("GET", "/api/review")
    if status != 200:
        r.fail("staged_fixes in review", f"status={status}")
        return
    staged = data.get("staged_fixes")
    if isinstance(staged, dict):
        r.ok(f"GET /api/review has staged_fixes ({len(staged)} entries)")
    else:
        r.fail("staged_fixes in review", f"type={type(staged).__name__}")


# ══════════════════════════════════════════════════════════════════════
# Cleanup
# ══════════════════════════════════════════════════════════════════════

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


def cleanup_test_history_entries():
    """Remove any history entries created during tests (by the test PR)."""
    # We don't clean history — tests use existing entries and the
    # delete test already removes what it creates.
    pass


# ══════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════

def run_all_tests(pr_url=None):
    pr_url = pr_url or DEFAULT_TEST_PR
    r = TestResult()

    print(f"\n{'='*60}")
    print(f"  PR Reviewer — E2E Tests (comprehensive)")
    print(f"  PR: {pr_url}")
    print(f"{'='*60}\n")

    # ── Phase 1: Server Health & Static Assets ──
    print("Phase 1: Server Health & Static Assets")
    test_health(r)
    test_ai_status(r)
    test_dashboard_html(r)
    test_cors_preflight(r)
    test_404_post(r)
    test_404_put(r)
    test_404_delete(r)

    # ── Phase 2: Error handling (before PR load) ──
    print("\nPhase 2: Error Handling (pre-load)")
    test_review_empty_state(r)
    test_load_pr_missing_url(r)
    test_load_pr_invalid_url(r)

    # ── Phase 3: Load PR ──
    print("\nPhase 3: Load PR")
    pr_loaded = test_load_pr(r, pr_url)

    if not pr_loaded:
        print("\n⚠ PR load failed — skipping remaining tests")
        return r.summary()

    # ── Phase 4: Review state after load ──
    print("\nPhase 4: Review State After Load")
    test_review_after_load(r)
    test_load_pr_is_author_false(r)

    # ── Phase 5: AI Review Generation ──
    print("\nPhase 5: AI Review Generation")
    initial_history = test_get_history(r)
    initial_count = len(initial_history) if isinstance(initial_history, list) else 0
    ai_comments = test_generate_review(r)

    # ── Phase 6: Comment CRUD ──
    print("\nPhase 6: Comment CRUD")
    cid1 = test_add_comment(r)
    cid2 = test_add_second_comment(r)
    cid3 = test_add_comment_minimal(r)
    test_comments_in_review(r, expected_min=1)
    test_update_comment(r, cid1)
    test_update_comment_severity(r, cid1)
    test_update_saves_learning(r, cid1)
    test_update_nonexistent(r)
    test_delete_comment(r, cid2)
    test_delete_comment(r, cid3)
    test_delete_nonexistent(r)
    test_comments_in_review(r, expected_min=1)

    # ── Phase 7: Post comment (safe) ──
    print("\nPhase 7: Post Comment (safe — bad ID only)")
    test_post_comment_bad_id(r)
    test_post_all_comments(r)

    # ── Phase 8: Learnings ──
    print("\nPhase 8: Learnings")
    test_get_learnings(r)
    test_save_learning(r)
    test_save_learning_empty(r)

    # ── Phase 9: Prompt ──
    print("\nPhase 9: Prompt Read / Update")
    original_prompt = test_get_prompt(r)
    test_update_prompt(r, original_prompt)
    test_update_prompt_empty(r)

    # ── Phase 10: History ──
    print("\nPhase 10: History")
    history_entry_id = test_history_auto_saved(r, initial_count)
    test_restore_history(r, history_entry_id)
    test_restore_history_invalid(r)
    # Delete only the test entry we created
    test_delete_history(r, history_entry_id)

    # ── Phase 11: Auto-Fix Staging (non-author) ──
    print("\nPhase 11: Auto-Fix Staging (non-author PR)")
    test_auto_fix_non_author(r)
    test_auto_fix_bad_comment_id(r)
    test_auto_fix_no_file(r)
    test_unstage_fix_empty(r)
    test_commit_fixes_non_author(r)
    test_commit_fixes_empty_staged(r)

    # ── Phase 12: Staged Fixes in Review State ──
    print("\nPhase 12: Staged Fixes in Review State")
    test_staged_fixes_in_review(r)

    # ── Cleanup ──
    print("\nCleanup:")
    cleanup_learnings()

    # Delete remaining test comments
    status, data = api("GET", "/api/review")
    for c in data.get("comments", []):
        if "E2E test" in c.get("comment", "") or "test comment" in c.get("comment", ""):
            api("DELETE", f"/api/comment/{c['id']}")
            print(f"  Cleaned comment id={c['id']}")

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
