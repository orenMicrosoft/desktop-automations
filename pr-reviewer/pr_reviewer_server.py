"""
PR Reviewer Dashboard Server
Review PRs with AI-powered comments, edit inline, post to ADO.
Git-style auto-fix: stage fixes per comment, then commit all to the PR branch.
"""
import http.server
import json
import os
import sys
import threading
import socket
import webbrowser
import urllib.parse
import difflib

DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, DIR)

import ado_pr_client
import ai_reviewer

PORT = 8097
LEARNINGS_FILE = os.path.join(DIR, "learnings.json")
HISTORY_FILE = os.path.join(DIR, "review_history.json")


def _load_history():
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def _save_history(history):
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2, default=str)

_BENIGN = (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, OSError)

# In-memory state for the current review session
_current_review = {
    "pr_info": None,
    "changed_files": [],
    "diffs": {},
    "comments": [],
    "is_author": False,
    "staged_fixes": {},  # comment_id (str) -> {path, original, fixed, diff_preview}
}
_review_lock = threading.Lock()


def _make_diff_preview(original, fixed, file_path):
    """Create a unified diff preview between original and fixed content."""
    old_lines = original.splitlines(keepends=True)
    new_lines = fixed.splitlines(keepends=True)
    diff = difflib.unified_diff(old_lines, new_lines,
                                fromfile=f"a/{file_path}",
                                tofile=f"b/{file_path}", lineterm="")
    return "".join(diff)


class QuietServer(http.server.ThreadingHTTPServer):
    def handle_error(self, request, client_address):
        exc_type = sys.exc_info()[0]
        if exc_type and issubclass(exc_type, _BENIGN):
            return
        super().handle_error(request, client_address)


class ReviewHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def handle(self):
        try:
            super().handle()
        except _BENIGN:
            pass

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(length)) if length else {}

    def _json(self, data, status=200):
        body = json.dumps(data, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _error(self, msg, status=400):
        self._json({"error": msg}, status)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        if self.path == "/api/health":
            self._json({"status": "ok"})
        elif self.path == "/api/review":
            with _review_lock:
                data = dict(_current_review)
                data["staged_fixes"] = {
                    str(k): {"path": v["path"], "diff_preview": v["diff_preview"]}
                    for k, v in _current_review["staged_fixes"].items()
                }
                self._json(data)
        elif self.path == "/api/ai-status":
            self._json({
                "configured": ai_reviewer.is_ai_configured(),
                "copilot_exe": ai_reviewer.COPILOT_EXE,
                "copilot_available": ai_reviewer._is_copilot_cli_available(),
                "azure_configured": ai_reviewer._is_azure_openai_configured(),
            })
        elif self.path == "/api/learnings":
            self._json(ai_reviewer._load_learnings())
        elif self.path == "/api/prompt":
            self._json({"prompt": ai_reviewer.REVIEW_PROMPT_TEMPLATE})
        elif self.path == "/api/history":
            self._json(_load_history())
        else:
            super().do_GET()

    def do_POST(self):
        try:
            if self.path == "/api/load-pr":
                self._handle_load_pr()
            elif self.path == "/api/generate-review":
                self._handle_generate_review()
            elif self.path == "/api/post-comment":
                self._handle_post_comment()
            elif self.path == "/api/post-all-comments":
                self._handle_post_all_comments()
            elif self.path == "/api/add-comment":
                self._handle_add_comment()
            elif self.path == "/api/save-learning":
                self._handle_save_learning()
            elif self.path == "/api/auto-fix":
                self._handle_auto_fix()
            elif self.path == "/api/unstage-fix":
                self._handle_unstage_fix()
            elif self.path == "/api/commit-fixes":
                self._handle_commit_fixes()
            elif self.path == "/api/restore-history":
                self._handle_restore_history()
            else:
                self.send_error(404)
        except Exception as e:
            self._error(str(e), 500)

    def do_PUT(self):
        try:
            if self.path.startswith("/api/comment/"):
                self._handle_update_comment()
            elif self.path == "/api/prompt":
                self._handle_update_prompt()
            else:
                self.send_error(404)
        except Exception as e:
            self._error(str(e), 500)

    def do_DELETE(self):
        try:
            if self.path.startswith("/api/comment/"):
                self._handle_delete_comment()
            elif self.path.startswith("/api/history/"):
                self._handle_delete_history()
            else:
                self.send_error(404)
        except Exception as e:
            self._error(str(e), 500)

    # ── Load PR (+ detect author) ──
    def _handle_load_pr(self):
        body = self._read_body()
        pr_url = body.get("url", "").strip()
        if not pr_url:
            return self._error("Missing PR URL")

        parsed = ado_pr_client.parse_pr_url(pr_url)
        org = parsed["org"]
        project = parsed["project"]
        repo_name = parsed["repo_name"]
        pr_id = parsed["pr_id"]

        pr_info = ado_pr_client.get_pr_info(org, project, repo_name, pr_id)
        changed_files = ado_pr_client.get_pr_changes(org, project, pr_info["repo_id"], pr_id)

        # Detect if current user is the PR author
        is_author = False
        try:
            me = ado_pr_client.get_current_user(org)
            is_author = me.get("id") == pr_info.get("author_id", "")
        except Exception:
            pass

        # Fetch diffs for source files only
        source_files = [f for f in changed_files
                        if not any(f["path"].endswith(ext)
                                   for ext in [".json", ".png", ".jpg", ".gif",
                                               ".ico", ".woff", ".woff2"])]
        diffs = ado_pr_client.get_all_diffs(org, project, pr_info["repo_id"],
                                             pr_id, source_files)

        with _review_lock:
            _current_review["pr_info"] = pr_info
            _current_review["changed_files"] = changed_files
            _current_review["diffs"] = diffs
            _current_review["comments"] = []
            _current_review["is_author"] = is_author
            _current_review["staged_fixes"] = {}

        self._json({"ok": True, "pr_info": pr_info,
                     "files_count": len(changed_files),
                     "diffs_count": len(diffs),
                     "is_author": is_author})

    # ── Generate AI Review ──
    def _handle_generate_review(self):
        with _review_lock:
            pr_info = _current_review.get("pr_info")
            diffs = _current_review.get("diffs", {})

        if not pr_info:
            return self._error("No PR loaded")

        comments = ai_reviewer.generate_review(pr_info, diffs)

        for i, c in enumerate(comments):
            c["id"] = i
            c["posted"] = False

        with _review_lock:
            _current_review["comments"] = comments

        self._save_to_history(pr_info, comments)
        self._json({"ok": True, "comments": comments})

    # ── Add comment ──
    def _handle_add_comment(self):
        body = self._read_body()
        with _review_lock:
            comments = _current_review.get("comments", [])
            new_id = max((c["id"] for c in comments), default=-1) + 1
            comment = {
                "id": new_id,
                "severity": body.get("severity", "medium"),
                "file": body.get("file", ""),
                "line": body.get("line", 0),
                "comment": body.get("comment", ""),
                "issue": body.get("issue", ""),
                "suggestion": body.get("suggestion", ""),
                "posted": False,
            }
            comments.append(comment)
            _current_review["comments"] = comments
        self._json({"ok": True, "comment": comment})

    # ── Update comment ──
    def _handle_update_comment(self):
        idx = int(self.path.split("/api/comment/")[1])
        body = self._read_body()
        with _review_lock:
            comments = _current_review.get("comments", [])
            for c in comments:
                if c["id"] == idx:
                    old_comment = c.get("comment", "")
                    new_comment = body.get("comment", old_comment)
                    if old_comment != new_comment and old_comment:
                        learning = (f'Changed comment from "{old_comment}" '
                                    f'to "{new_comment}" '
                                    f"(file: {c.get('file', '?')})")
                        ai_reviewer.save_learning(learning)
                    for key in ["severity", "file", "line", "comment",
                                "issue", "suggestion"]:
                        if key in body:
                            c[key] = body[key]
                    self._json({"ok": True, "comment": c})
                    return
        self._error("Comment not found", 404)

    # ── Delete comment ──
    def _handle_delete_comment(self):
        idx = int(self.path.split("/api/comment/")[1])
        with _review_lock:
            comments = _current_review.get("comments", [])
            deleted = [c for c in comments if c["id"] == idx]
            _current_review["comments"] = [c for c in comments if c["id"] != idx]
            _current_review["staged_fixes"].pop(str(idx), None)
            if deleted:
                learning = (f'Deleted comment: "{deleted[0].get("comment", "")}" '
                            f"(file: {deleted[0].get('file', '?')}) --- "
                            f"this type of comment is not useful")
                ai_reviewer.save_learning(learning)
        self._json({"ok": True, "deleted": len(deleted) > 0})

    # ── Post single comment to ADO ──
    def _handle_post_comment(self):
        body = self._read_body()
        comment_id = body.get("id")
        with _review_lock:
            pr_info = _current_review.get("pr_info")
            comments = _current_review.get("comments", [])

        if not pr_info:
            return self._error("No PR loaded")

        comment = next((c for c in comments if c["id"] == comment_id), None)
        if not comment:
            return self._error("Comment not found")

        parsed = ado_pr_client.parse_pr_url(pr_info["url"])
        ado_pr_client.post_pr_comment(
            parsed["org"], parsed["project"],
            pr_info["repo_id"], pr_info["pr_id"],
            comment.get("file") or None,
            comment.get("line") or None,
            comment["comment"]
        )

        with _review_lock:
            comment["posted"] = True
        self._json({"ok": True})

    # ── Post all comments to ADO ──
    def _handle_post_all_comments(self):
        with _review_lock:
            pr_info = _current_review.get("pr_info")
            comments = _current_review.get("comments", [])

        if not pr_info:
            return self._error("No PR loaded")

        parsed = ado_pr_client.parse_pr_url(pr_info["url"])
        posted = 0
        errors = []

        for comment in comments:
            if comment.get("posted"):
                continue
            try:
                ado_pr_client.post_pr_comment(
                    parsed["org"], parsed["project"],
                    pr_info["repo_id"], pr_info["pr_id"],
                    comment.get("file") or None,
                    comment.get("line") or None,
                    comment["comment"]
                )
                comment["posted"] = True
                posted += 1
            except Exception as e:
                errors.append(f"{comment.get('file', '?')}: {e}")

        self._json({"ok": True, "posted": posted, "errors": errors})

    # ── Save learning ──
    def _handle_save_learning(self):
        body = self._read_body()
        text = body.get("text", "").strip()
        if not text:
            return self._error("Missing learning text")
        ai_reviewer.save_learning(text)
        self._json({"ok": True})

    # ── Update prompt ──
    def _handle_update_prompt(self):
        body = self._read_body()
        prompt = body.get("prompt", "")
        if not prompt:
            return self._error("Missing prompt text")
        ai_reviewer.REVIEW_PROMPT_TEMPLATE = prompt
        prompt_file = os.path.join(DIR, "review_prompt.txt")
        with open(prompt_file, "w", encoding="utf-8") as f:
            f.write(prompt)
        self._json({"ok": True})

    # ═══════ Auto-Fix (git-style staging) ═══════

    def _handle_auto_fix(self):
        """Generate a fix for one comment -> stage it (like git add)."""
        body = self._read_body()
        comment_id = body.get("id")

        with _review_lock:
            pr_info = _current_review.get("pr_info")
            comments = _current_review.get("comments", [])
            is_author = _current_review.get("is_author", False)

        if not pr_info:
            return self._error("No PR loaded")
        if not is_author:
            return self._error("Auto-fix is only available on your own PRs")

        comment = next((c for c in comments if c["id"] == comment_id), None)
        if not comment:
            return self._error("Comment not found")

        file_path = comment.get("file", "")
        if not file_path:
            return self._error("Comment has no file path")

        parsed = ado_pr_client.parse_pr_url(pr_info["url"])
        try:
            original = ado_pr_client.get_file_at_branch(
                parsed["org"], parsed["project"],
                pr_info["repo_id"], file_path,
                pr_info["source_branch"])
        except Exception as e:
            return self._error(f"Could not fetch file: {e}")

        result = ai_reviewer.generate_fix(original, file_path, comment)
        if not result.get("ok"):
            return self._error(result.get("error", "Fix generation failed"))

        fixed = result["fixed_content"]
        diff_preview = _make_diff_preview(original, fixed, file_path)

        with _review_lock:
            _current_review["staged_fixes"][str(comment_id)] = {
                "path": file_path,
                "original": original,
                "fixed": fixed,
                "diff_preview": diff_preview,
            }

        self._json({"ok": True, "diff_preview": diff_preview,
                     "staged_count": len(_current_review["staged_fixes"])})

    def _handle_unstage_fix(self):
        """Remove a staged fix (like git reset)."""
        body = self._read_body()
        comment_id = str(body.get("id", ""))
        with _review_lock:
            removed = _current_review["staged_fixes"].pop(comment_id, None)
        self._json({"ok": True, "unstaged": removed is not None,
                     "staged_count": len(_current_review["staged_fixes"])})

    def _handle_commit_fixes(self):
        """Push all staged fixes as one commit to the PR source branch."""
        with _review_lock:
            pr_info = _current_review.get("pr_info")
            staged = dict(_current_review.get("staged_fixes", {}))
            is_author = _current_review.get("is_author", False)

        if not pr_info:
            return self._error("No PR loaded")
        if not is_author:
            return self._error("Can only commit fixes on your own PRs")
        if not staged:
            return self._error("No staged fixes to commit")

        # Merge fixes per file (multiple comments may touch same file)
        file_map = {}
        for cid, fix in staged.items():
            file_map[fix["path"]] = fix["fixed"]

        file_changes = [{"path": p, "content": c} for p, c in file_map.items()]

        parsed = ado_pr_client.parse_pr_url(pr_info["url"])
        commit_msg = f"Auto-fix: {len(staged)} review comment(s) addressed"

        try:
            ado_pr_client.push_file_changes(
                parsed["org"], parsed["project"],
                pr_info["repo_id"], pr_info["source_branch"],
                file_changes, commit_msg)
        except Exception as e:
            return self._error(f"Push failed: {e}")

        with _review_lock:
            _current_review["staged_fixes"] = {}

        self._json({"ok": True, "files_pushed": len(file_changes),
                     "commit_message": commit_msg})

    # ═══════ History ═══════

    def _save_to_history(self, pr_info, comments):
        import datetime
        history = _load_history()
        entry = {
            "id": datetime.datetime.now().strftime("%Y%m%d_%H%M%S"),
            "timestamp": datetime.datetime.now().isoformat(),
            "pr_title": pr_info.get("title", ""),
            "pr_author": pr_info.get("author", ""),
            "pr_url": pr_info.get("url", ""),
            "pr_id": pr_info.get("pr_id", ""),
            "source_branch": pr_info.get("source_branch", ""),
            "target_branch": pr_info.get("target_branch", ""),
            "comments": [
                {
                    "severity": c.get("severity", ""),
                    "file": c.get("file", ""),
                    "line": c.get("line", 0),
                    "comment": c.get("comment", ""),
                    "issue": c.get("issue", ""),
                    "suggestion": c.get("suggestion", ""),
                }
                for c in comments
            ],
        }
        history.insert(0, entry)
        _save_history(history)

    def _handle_delete_history(self):
        entry_id = self.path.split("/api/history/")[1]
        history = _load_history()
        history = [h for h in history if h.get("id") != entry_id]
        _save_history(history)
        self._json({"ok": True})

    def _handle_restore_history(self):
        """Restore a past review into the active session for posting/fixing."""
        body = self._read_body()
        entry_id = body.get("id", "")
        history = _load_history()
        entry = next((h for h in history if h.get("id") == entry_id), None)
        if not entry:
            return self._error("History entry not found")

        pr_url = entry.get("pr_url", "")
        if not pr_url:
            return self._error("History entry has no PR URL")

        parsed = ado_pr_client.parse_pr_url(pr_url)
        pr_info = ado_pr_client.get_pr_info(
            parsed["org"], parsed["project"],
            parsed["repo_name"], parsed["pr_id"])
        changed_files = ado_pr_client.get_pr_changes(
            parsed["org"], parsed["project"], pr_info["repo_id"], pr_info["pr_id"])

        is_author = False
        try:
            me = ado_pr_client.get_current_user(parsed["org"])
            is_author = me.get("id") == pr_info.get("author_id", "")
        except Exception:
            pass

        source_files = [f for f in changed_files
                        if not any(f["path"].endswith(ext)
                                   for ext in [".json", ".png", ".jpg", ".gif",
                                               ".ico", ".woff", ".woff2"])]
        diffs = ado_pr_client.get_all_diffs(
            parsed["org"], parsed["project"],
            pr_info["repo_id"], pr_info["pr_id"], source_files)

        comments = []
        for i, c in enumerate(entry.get("comments", [])):
            comments.append({
                "id": i,
                "severity": c.get("severity", "medium"),
                "file": c.get("file", ""),
                "line": c.get("line", 0),
                "comment": c.get("comment", ""),
                "issue": c.get("issue", ""),
                "suggestion": c.get("suggestion", ""),
                "posted": False,
            })

        with _review_lock:
            _current_review["pr_info"] = pr_info
            _current_review["changed_files"] = changed_files
            _current_review["diffs"] = diffs
            _current_review["comments"] = comments
            _current_review["is_author"] = is_author
            _current_review["staged_fixes"] = {}

        self._json({"ok": True, "pr_info": pr_info,
                     "files_count": len(changed_files),
                     "diffs_count": len(diffs),
                     "is_author": is_author,
                     "comments": comments})


def main():
    if any(a == "--no-browser" for a in sys.argv):
        open_browser = False
    else:
        open_browser = True

    os.chdir(DIR)
    server = QuietServer(("127.0.0.1", PORT), ReviewHandler)

    url = f"http://localhost:{PORT}/dashboard.html"
    print(f"PR Reviewer running at: {url}")

    if open_browser:
        webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
