"""
PR Reviewer Dashboard Server
Review PRs with AI-powered comments, edit inline, post to ADO.
"""
import http.server
import json
import os
import sys
import threading
import socket
import webbrowser
import urllib.parse

DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, DIR)

import ado_pr_client
import ai_reviewer

PORT = 8097
LEARNINGS_FILE = os.path.join(DIR, "learnings.json")

_BENIGN = (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, OSError)

# In-memory state for the current review session
_current_review = {
    "pr_info": None,
    "changed_files": [],
    "diffs": {},
    "comments": [],
}
_review_lock = threading.Lock()


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
                self._json(_current_review)
        elif self.path == "/api/ai-status":
            self._json({"configured": ai_reviewer.is_ai_configured()})
        elif self.path == "/api/learnings":
            self._json(ai_reviewer._load_learnings())
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
            else:
                self.send_error(404)
        except Exception as e:
            self._error(str(e), 500)

    def do_PUT(self):
        try:
            if self.path.startswith("/api/comment/"):
                self._handle_update_comment()
            else:
                self.send_error(404)
        except Exception as e:
            self._error(str(e), 500)

    def do_DELETE(self):
        try:
            if self.path.startswith("/api/comment/"):
                self._handle_delete_comment()
            else:
                self.send_error(404)
        except Exception as e:
            self._error(str(e), 500)

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

        # Fetch diffs for source files only (skip large JSON mocks, images)
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

        self._json({"ok": True, "pr_info": pr_info,
                     "files_count": len(changed_files),
                     "diffs_count": len(diffs)})

    def _handle_generate_review(self):
        with _review_lock:
            pr_info = _current_review.get("pr_info")
            diffs = _current_review.get("diffs", {})

        if not pr_info:
            return self._error("No PR loaded")

        comments = ai_reviewer.generate_review(pr_info, diffs)

        # Assign IDs
        for i, c in enumerate(comments):
            c["id"] = i
            c["posted"] = False

        with _review_lock:
            _current_review["comments"] = comments

        self._json({"ok": True, "comments": comments})

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

    def _handle_update_comment(self):
        idx = int(self.path.split("/api/comment/")[1])
        body = self._read_body()
        with _review_lock:
            comments = _current_review.get("comments", [])
            for c in comments:
                if c["id"] == idx:
                    # Track correction as a learning
                    old_comment = c.get("comment", "")
                    new_comment = body.get("comment", old_comment)
                    if old_comment != new_comment and old_comment:
                        learning = (f"Changed comment from \"{old_comment}\" "
                                    f"to \"{new_comment}\" "
                                    f"(file: {c.get('file', '?')})")
                        ai_reviewer.save_learning(learning)

                    for key in ["severity", "file", "line", "comment",
                                "issue", "suggestion"]:
                        if key in body:
                            c[key] = body[key]
                    self._json({"ok": True, "comment": c})
                    return
        self._error("Comment not found", 404)

    def _handle_delete_comment(self):
        idx = int(self.path.split("/api/comment/")[1])
        with _review_lock:
            comments = _current_review.get("comments", [])
            original_len = len(comments)
            deleted = [c for c in comments if c["id"] == idx]
            _current_review["comments"] = [c for c in comments if c["id"] != idx]
            if deleted:
                learning = (f"Deleted comment: \"{deleted[0].get('comment', '')}\" "
                            f"(file: {deleted[0].get('file', '?')}) — "
                            f"this type of comment is not useful")
                ai_reviewer.save_learning(learning)
        self._json({"ok": True, "deleted": len(deleted) > 0})

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

        org = pr_info["url"].split("/_git/")[0].rsplit("/", 1)[0]
        # Re-parse from the stored URL
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

    def _handle_save_learning(self):
        body = self._read_body()
        text = body.get("text", "").strip()
        if not text:
            return self._error("Missing learning text")
        ai_reviewer.save_learning(text)
        self._json({"ok": True})


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
