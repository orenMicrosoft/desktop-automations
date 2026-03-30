"""
Pipeline Dashboard Server
Tracks PR lifecycle stages across ADO repositories.
"""
import http.server
import json
import os
import sys
import time
import threading
import socket
import webbrowser

DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, DIR)

import ado_client

PORT = 8093
DATA_FILE = os.path.join(DIR, "pipeline_data.json")

_data_lock = threading.Lock()
_BENIGN = (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, OSError)


def _load_data():
    with open(DATA_FILE, "r") as f:
        return json.load(f)


def _save_data(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2, default=str)


def _now_iso():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


class QuietServer(http.server.ThreadingHTTPServer):
    def handle_error(self, request, client_address):
        exc_type = sys.exc_info()[0]
        if exc_type and issubclass(exc_type, _BENIGN):
            return
        super().handle_error(request, client_address)


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def handle(self):
        try:
            super().handle()
        except _BENIGN:
            pass

    def _parse_pr_id(self, segment):
        """Parse a numeric pr_id from a URL segment."""
        try:
            return int(segment)
        except ValueError:
            return None

    def _find_pr(self, pr_id, data=None):
        """Find a PR by numeric pr_id in active or archived lists."""
        if data is None:
            data = _load_data()
        pr = next((p for p in data.get("prs", []) if p["pr_id"] == pr_id), None)
        if not pr:
            pr = next((p for p in data.get("archived", []) if p["pr_id"] == pr_id), None)
        return pr

    def do_GET(self):
        path = self.path.split("?")[0]
        parts = [p for p in path.strip("/").split("/") if p]

        if path == "/" or path == "/dashboard.html":
            self._serve_file("dashboard.html", "text/html")
        elif path == "/api/prs":
            self._get_prs()
        elif path == "/api/archived":
            self._get_archived()
        elif path == "/api/config":
            self._get_config()
        elif len(parts) >= 3 and parts[0] == "api" and parts[1] == "pr":
            pr_id = self._parse_pr_id(parts[2])
            if not pr_id:
                self.send_error(400, "Invalid PR ID")
                return
            if len(parts) == 3:
                self._get_pr_full(pr_id)
            elif len(parts) >= 5 and parts[3] == "step" and parts[-1] == "details":
                self._get_pr_step_details(pr_id, parts[4])
            elif parts[-1] == "failure":
                self._get_failure_details(pr_id)
            else:
                self.send_error(404)
        else:
            super().do_GET()

    def do_POST(self):
        path = self.path.split("?")[0]
        parts = [p for p in path.strip("/").split("/") if p]

        if path == "/api/scan":
            self._scan_new_prs()
        elif len(parts) == 3 and parts[0] == "api" and parts[1] == "refresh":
            pr_id = self._parse_pr_id(parts[2])
            if pr_id:
                self._refresh_single_pr(pr_id)
            else:
                self.send_error(400, "Invalid PR ID")
        elif len(parts) == 3 and parts[0] == "api" and parts[1] == "archive":
            pr_id = self._parse_pr_id(parts[2])
            if pr_id:
                self._archive_pr(pr_id)
            else:
                self.send_error(400, "Invalid PR ID")
        elif len(parts) == 3 and parts[0] == "api" and parts[1] == "unarchive":
            pr_id = self._parse_pr_id(parts[2])
            if pr_id:
                self._unarchive_pr(pr_id)
            else:
                self.send_error(400, "Invalid PR ID")
        elif path == "/api/add-repo":
            self._add_repo()
        else:
            self.send_error(404)

    # ── GET Handlers ───────────────────────────────────────────────

    def _get_prs(self):
        """Return cached PRs instantly (no ADO calls)."""
        with _data_lock:
            data = _load_data()
        prs = [p for p in data.get("prs", []) if p.get("pr_status") != "abandoned"]
        self._json_response({"prs": prs})

    def _get_archived(self):
        with _data_lock:
            data = _load_data()
        self._json_response({"archived": data.get("archived", [])})

    def _get_config(self):
        with _data_lock:
            data = _load_data()
        cfg = data.get("config", {})
        safe = {
            "repos": list(cfg.get("repos", {}).keys()),
            "creator_email": cfg.get("creator_email"),
            "poll_interval_seconds": cfg.get("poll_interval_seconds"),
        }
        self._json_response(safe)

    def _get_pr_full(self, pr_id):
        """GET /api/pr/{pr_id} — full cached data for one PR (row-click detail view)."""
        with _data_lock:
            data = _load_data()
        pr = self._find_pr(pr_id, data)
        if not pr:
            self._json_response({"error": "PR not found"}, 404)
            return
        self._json_response(pr)

    def _get_pr_step_details(self, pr_id, step):
        """GET /api/pr/{pr_id}/step/{step}/details"""
        with _data_lock:
            data = _load_data()

        pr = self._find_pr(pr_id, data)
        if not pr:
            self._json_response({"error": "PR not found"}, 404)
            return

        stage_info = pr.get("stages", {}).get(step, {})
        details = {
            "pr_id": pr_id, "step": step, "stage": stage_info,
            "pr": {"title": pr["title"], "repo": pr["repo"],
                   "source_branch": pr["source_branch"]},
        }

        # If it's a failed build step, fetch failure logs
        if stage_info.get("status") == "failed" and stage_info.get("details", {}).get("build_id"):
            try:
                # Resolve org/project for the PR's repo
                _cfg = ado_client._load_config()
                _rcfg = ado_client._repo_config(_cfg, pr.get("repo", ""))
                _org = _rcfg["org"] if _rcfg else None
                _proj = _rcfg["project"] if _rcfg else None
                failure = ado_client.get_failure_details(
                    stage_info["details"]["build_id"], org=_org, project=_proj)
                details["failure"] = failure
            except Exception as e:
                details["failure_error"] = str(e)

        self._json_response(details)

    def _get_failure_details(self, pr_id):
        """GET /api/pr/{pr_id}/failure"""
        with _data_lock:
            data = _load_data()

        pr = self._find_pr(pr_id, data)
        if not pr:
            self._json_response({"error": "PR not found"}, 404)
            return

        for stage_name in ado_client.STAGE_ORDER:
            stage = pr.get("stages", {}).get(stage_name, {})
            if stage.get("status") == "failed":
                build_id = stage.get("details", {}).get("build_id")
                if build_id:
                    try:
                        _cfg = ado_client._load_config()
                        _rcfg = ado_client._repo_config(_cfg, pr.get("repo", ""))
                        _org = _rcfg["org"] if _rcfg else None
                        _proj = _rcfg["project"] if _rcfg else None
                        failure = ado_client.get_failure_details(
                            build_id, org=_org, project=_proj)
                        self._json_response(failure)
                        return
                    except Exception as e:
                        self._json_response({"error": str(e)})
                        return

        self._json_response({"error": "No failed stage found"})

    # ── POST Handlers ──────────────────────────────────────────────

    def _refresh_single_pr(self, pr_id):
        """POST /api/refresh/{pr_id} — refresh stages + work items for ONE PR."""
        print(f"[Refresh] PR #{pr_id}...")
        with _data_lock:
            data = _load_data()

        pr = next((p for p in data["prs"] if p["pr_id"] == pr_id), None)
        if not pr:
            self._json_response({"ok": False, "error": "PR not found"}, 404)
            return

        try:
            # Re-fetch PR metadata from ADO
            ado_client.clear_caches()
            scanned = ado_client.scan_prs_for_repo(pr["repo"], top=20)
            fresh = next((s for s in scanned if s["pr_id"] == pr_id), None)
            if fresh:
                pr["pr_status"] = fresh["pr_status"]
                pr["reviewers"] = fresh.get("reviewers", [])
                pr["has_approval"] = fresh.get("has_approval", False)
                pr["has_rejection"] = fresh.get("has_rejection", False)
                pr["closed_at"] = fresh.get("closed_at")

                # If PR was abandoned, remove it from the dashboard
                if fresh["pr_status"] == "abandoned":
                    data["prs"] = [p for p in data["prs"] if p["pr_id"] != pr_id]
                    with _data_lock:
                        _save_data(data)
                    print(f"[Refresh] PR #{pr_id} is abandoned — removed from dashboard.")
                    self._json_response({"ok": True, "removed": True, "reason": "abandoned"})
                    return

            # Compute stages
            pr["stages"] = ado_client.compute_stages(pr)

            # Enrich work items
            pr["work_items"] = ado_client.enrich_pr_work_items(pr)

            pr["last_refreshed"] = _now_iso()

            with _data_lock:
                _save_data(data)

            print(f"[Refresh] PR #{pr_id} done.")
            self._json_response({"ok": True, "pr": pr})

        except Exception as e:
            print(f"[Refresh] PR #{pr_id} failed: {e}")
            self._json_response({"ok": False, "error": str(e)})

    def _scan_new_prs(self):
        """POST /api/scan — lightweight scan for new PRs (no stage computation)."""
        print("[Scan] Starting...")
        ado_client.clear_caches()

        with _data_lock:
            data = _load_data()

        try:
            all_scanned = ado_client.scan_all_repos()
            existing_ids = {p["pr_id"] for p in data["prs"]}
            archived_ids = {p["pr_id"] for p in data["archived"]}
            new_prs = []
            for scanned in all_scanned:
                if (scanned["pr_id"] not in existing_ids and
                        scanned["pr_id"] not in archived_ids and
                        scanned["pr_status"] != "abandoned"):
                    pr_entry = _create_pr_entry_fast(scanned)
                    data["prs"].append(pr_entry)
                    new_prs.append(pr_entry)

            # Also update metadata for existing PRs from cached scan
            for pr in data["prs"]:
                cached = ado_client._scan_cache.get(pr["repo"], [])
                fresh = next((s for s in cached if s["pr_id"] == pr["pr_id"]), None)
                if fresh:
                    pr["pr_status"] = fresh["pr_status"]
                    pr["has_approval"] = fresh.get("has_approval", False)
                    pr["has_rejection"] = fresh.get("has_rejection", False)
                    pr["closed_at"] = fresh.get("closed_at")
                    pr["reviewers"] = fresh.get("reviewers", [])

            # Remove abandoned PRs
            abandoned = [p for p in data["prs"] if p.get("pr_status") == "abandoned"]
            if abandoned:
                abandoned_ids = {p["pr_id"] for p in abandoned}
                data["prs"] = [p for p in data["prs"] if p["pr_id"] not in abandoned_ids]
                print(f"[Scan] Removed {len(abandoned)} abandoned PR(s): {abandoned_ids}")

            data["prs"].sort(key=lambda p: p.get("created_at", ""), reverse=True)

            with _data_lock:
                _save_data(data)

            print(f"[Scan] Done. {len(new_prs)} new PRs.")
            self._json_response({"ok": True, "new_prs": len(new_prs), "prs": data["prs"]})
        except Exception as e:
            print(f"[Scan] Failed: {e}")
            self._json_response({"ok": False, "error": str(e)})

    def _archive_pr(self, pr_id):
        """POST /api/archive/{pr_id}"""
        with _data_lock:
            data = _load_data()
            pr = next((p for p in data["prs"] if p["pr_id"] == pr_id), None)
            if pr:
                data["prs"].remove(pr)
                pr["archived_at"] = _now_iso()
                data["archived"].append(pr)
                _save_data(data)
                self._json_response({"ok": True})
            else:
                self._json_response({"ok": False, "error": "PR not found"}, 404)

    def _unarchive_pr(self, pr_id):
        """POST /api/unarchive/{pr_id}"""
        with _data_lock:
            data = _load_data()
            pr = next((p for p in data["archived"] if p["pr_id"] == pr_id), None)
            if pr:
                data["archived"].remove(pr)
                pr.pop("archived_at", None)
                data["prs"].append(pr)
                data["prs"].sort(key=lambda p: p.get("created_at", ""), reverse=True)
                _save_data(data)
                self._json_response({"ok": True})
            else:
                self._json_response({"ok": False, "error": "PR not found"}, 404)

    def _add_repo(self):
        """POST /api/add-repo with body {"name": "...", "id": "..."}"""
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
        name = body.get("name")
        repo_id = body.get("id")
        if not name or not repo_id:
            self._json_response({"ok": False, "error": "name and id required"}, 400)
            return
        with _data_lock:
            data = _load_data()
            data["config"]["repos"][name] = repo_id
            _save_data(data)
        self._json_response({"ok": True})

    # ── Helpers ────────────────────────────────────────────────────

    def _serve_file(self, filename, content_type):
        filepath = os.path.join(DIR, filename)
        if not os.path.exists(filepath):
            self.send_error(404)
            return
        with open(filepath, "rb") as f:
            content = f.read()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", len(content))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(content)

    def _json_response(self, data, status=200):
        body = json.dumps(data, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)


def _create_pr_entry_fast(scanned):
    """Create a PR entry without computing stages (fast, for scan use)."""
    return {
        "id": scanned["id"],
        "repo": scanned["repo"],
        "repo_id": scanned["repo_id"],
        "pr_id": scanned["pr_id"],
        "title": scanned["title"],
        "source_branch": scanned["source_branch"],
        "target_branch": scanned["target_branch"],
        "created_at": scanned["created_at"],
        "closed_at": scanned.get("closed_at"),
        "pr_status": scanned["pr_status"],
        "pr_url": scanned["pr_url"],
        "reviewers": scanned.get("reviewers", []),
        "has_approval": scanned.get("has_approval", False),
        "has_rejection": scanned.get("has_rejection", False),
        "stages": {},
        "work_items": [],
        "last_refreshed": None,
    }


def is_port_open(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(2)
        return s.connect_ex(("127.0.0.1", port)) == 0


def main():
    no_browser = "--no-browser" in sys.argv

    if is_port_open(PORT):
        print(f"Pipeline Dashboard already running at http://localhost:{PORT}")
        if not no_browser:
            webbrowser.open(f"http://localhost:{PORT}")
        return

    os.chdir(DIR)
    server = QuietServer(("127.0.0.1", PORT), DashboardHandler)

    url = f"http://localhost:{PORT}"
    print(f"Pipeline Dashboard running at: {url}")

    if not no_browser:
        webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down Pipeline Dashboard.")
        server.shutdown()


if __name__ == "__main__":
    main()
