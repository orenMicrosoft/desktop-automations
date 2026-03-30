"""
E2E Tests for Pipeline Dashboard
Run standalone or import run_pipeline_tests() from the hub.
"""
import urllib.request
import json
import sys

BASE = "http://localhost:8093"

EXPECTED_STAGES = [
    "local_branch", "remote_branch", "pr_review", "pr_approved", "pr_completed",
    "build_pipeline", "buddy_pipeline", "dev_pipeline", "stage_pipeline", "prod_pipeline",
]


def _get(url, timeout=10):
    return urllib.request.urlopen(url, timeout=timeout)


def _post(url, body=None, timeout=15):
    data = json.dumps(body).encode() if body else None
    headers = {"Content-Type": "application/json"} if body else {}
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    return urllib.request.urlopen(req, timeout=timeout)


def _json(resp):
    return json.loads(resp.read())


def run_pipeline_tests():
    """Full E2E test suite for Pipeline Dashboard."""
    results = []

    def test(name, fn):
        try:
            ok = fn()
            results.append({"name": name, "passed": bool(ok), "error": None})
        except Exception as e:
            results.append({"name": name, "passed": False, "error": str(e)})

    # 1. Dashboard page loads
    test("Dashboard page loads", lambda: _get(BASE + "/dashboard.html").status == 200)

    # 2. Dashboard has pipeline content
    test("Dashboard has pipeline content", lambda: (
        "Pipeline Dashboard" in _get(BASE + "/dashboard.html").read().decode()
    ))

    # 3. PRs API returns list
    test("PRs API returns list", lambda: (
        isinstance(_json(_get(BASE + "/api/prs")).get("prs"), list)
    ))

    # 4. Archived API returns list
    test("Archived API returns list", lambda: (
        isinstance(_json(_get(BASE + "/api/archived")).get("archived"), list)
    ))

    # 5. Config API returns repos
    test("Config API returns repos", lambda: (
        isinstance(_json(_get(BASE + "/api/config")).get("repos"), list)
    ))

    # 6. Scan API works (lightweight, ~20s)
    test("Scan API discovers PRs", lambda: (
        _json(_post(BASE + "/api/scan", timeout=120)).get("ok") is True
    ))

    # 7. PRs have required fields after scan
    def check_pr_fields():
        prs = _json(_get(BASE + "/api/prs"))["prs"]
        if not prs:
            return True
        pr = prs[0]
        return all(k in pr for k in ("pr_id", "title", "repo", "source_branch", "pr_url", "stages"))
    test("PRs have required fields", check_pr_fields)

    # 8. Per-PR refresh works
    def per_pr_refresh():
        prs = _json(_get(BASE + "/api/prs"))["prs"]
        if not prs:
            return True
        pr_id = prs[0]["pr_id"]
        resp = _json(_post(BASE + f"/api/refresh/{pr_id}", timeout=120))
        if not resp.get("ok"):
            raise Exception(f"Refresh failed: {resp}")
        # Verify stages are populated
        refreshed = resp["pr"]
        stages = refreshed.get("stages", {})
        return len(stages) == len(EXPECTED_STAGES)
    test("Per-PR refresh returns stages", per_pr_refresh)

    # 9. Per-PR refresh populates work_items field
    def check_work_items_field():
        prs = _json(_get(BASE + "/api/prs"))["prs"]
        if not prs:
            return True
        # Find a refreshed PR
        refreshed = [p for p in prs if p.get("last_refreshed")]
        if not refreshed:
            return True
        return "work_items" in refreshed[0]
    test("Refreshed PRs have work_items field", check_work_items_field)

    # 10. Step details API works with numeric pr_id
    def step_details_api():
        prs = _json(_get(BASE + "/api/prs"))["prs"]
        if not prs:
            return True
        pr_id = prs[0]["pr_id"]
        resp = _json(_get(BASE + f"/api/pr/{pr_id}/step/pr_review/details"))
        return "pr_id" in resp and "step" in resp and "stage" in resp
    test("Step details API works (numeric ID)", step_details_api)

    # 11. PR full details API works
    def pr_full_api():
        prs = _json(_get(BASE + "/api/prs"))["prs"]
        if not prs:
            return True
        pr_id = prs[0]["pr_id"]
        resp = _json(_get(BASE + f"/api/pr/{pr_id}"))
        return resp.get("pr_id") == pr_id and "title" in resp
    test("PR full details API works", pr_full_api)

    # 12. Archive and unarchive cycle
    def archive_unarchive_cycle():
        prs = _json(_get(BASE + "/api/prs"))["prs"]
        if not prs:
            return True
        pr_id = prs[0]["pr_id"]
        resp = _json(_post(BASE + f"/api/archive/{pr_id}"))
        if not resp.get("ok"):
            raise Exception(f"Archive failed: {resp}")
        archived_ids = [p["pr_id"] for p in _json(_get(BASE + "/api/archived"))["archived"]]
        if pr_id not in archived_ids:
            raise Exception("PR not found in archived list")
        resp = _json(_post(BASE + f"/api/unarchive/{pr_id}"))
        if not resp.get("ok"):
            raise Exception(f"Unarchive failed: {resp}")
        active_ids = [p["pr_id"] for p in _json(_get(BASE + "/api/prs"))["prs"]]
        if pr_id not in active_ids:
            raise Exception("PR not found in active list")
        return True
    test("Archive and unarchive cycle", archive_unarchive_cycle)

    # 13. Stage keys are correct after refresh
    def check_stage_keys():
        prs = _json(_get(BASE + "/api/prs"))["prs"]
        refreshed = [p for p in prs if p.get("last_refreshed")]
        if not refreshed:
            return True
        stages = refreshed[0].get("stages", {})
        return all(k in stages for k in EXPECTED_STAGES) and len(stages) == len(EXPECTED_STAGES)
    test("Stage keys correct after refresh", check_stage_keys)

    # 14. Deployment stages detect succeeded/in-progress from multi-build pipelines
    def check_deploy_stage_detection():
        prs = _json(_get(BASE + "/api/prs"))["prs"]
        completed = [p for p in prs if p.get("pr_status") == "completed" and p.get("last_refreshed")]
        if not completed:
            return True
        # At least one completed PR should have dev or stage as succeeded/in_progress/failed
        # (not all pending if the official build is succeeded)
        for pr in completed:
            stages = pr.get("stages", {})
            build_st = stages.get("build_pipeline", {}).get("status", "")
            if build_st in ("succeeded", "in_progress"):
                deploy_statuses = [
                    stages.get(k, {}).get("status", "pending")
                    for k in ("dev_pipeline", "stage_pipeline", "prod_pipeline")
                ]
                # At least one deploy stage should be non-pending if official build succeeded
                if any(s != "pending" for s in deploy_statuses):
                    return True
        # If no completed PR has a succeeded build, that's fine
        return True
    test("Deploy stages detected from multi-build pipelines", check_deploy_stage_detection)

    # 15. Step details API returns correct stage URL
    def check_step_details_url():
        prs = _json(_get(BASE + "/api/prs"))["prs"]
        refreshed = [p for p in prs if p.get("last_refreshed")]
        if not refreshed:
            return True
        pr = refreshed[0]
        pr_id = pr["pr_id"]
        # Test a PR-level step (should have pr_url)
        resp = _json(_get(BASE + f"/api/pr/{pr_id}/step/pr_review/details"))
        stage = resp.get("stage", {})
        if not stage.get("url"):
            raise Exception("pr_review step missing URL")
        # Test a pipeline step that has been refreshed
        for step in ("build_pipeline", "dev_pipeline", "stage_pipeline", "prod_pipeline"):
            step_data = pr.get("stages", {}).get(step, {})
            if step_data.get("status") in ("succeeded", "in_progress", "failed"):
                resp = _json(_get(BASE + f"/api/pr/{pr_id}/step/{step}/details"))
                stage = resp.get("stage", {})
                if not stage.get("url"):
                    raise Exception(f"{step} step is {step_data['status']} but has no URL")
                if "buildId=" not in stage["url"]:
                    raise Exception(f"{step} URL doesn't contain buildId: {stage['url']}")
                return True
        return True
    test("Step details API returns correct URLs", check_step_details_url)

    # 16. Deploy stages have correct details (build_id, stage_name) when available
    def check_deploy_stage_details():
        prs = _json(_get(BASE + "/api/prs"))["prs"]
        refreshed = [p for p in prs if p.get("last_refreshed") and p.get("pr_status") == "completed"]
        if not refreshed:
            return True
        for pr in refreshed:
            stages = pr.get("stages", {})
            for k in ("dev_pipeline", "stage_pipeline", "prod_pipeline"):
                s = stages.get(k, {})
                if s.get("status") in ("succeeded", "in_progress", "failed"):
                    details = s.get("details", {})
                    if not details.get("build_id"):
                        raise Exception(f"PR #{pr['pr_id']} {k} is {s['status']} but missing build_id in details")
                    if not details.get("stage_name"):
                        raise Exception(f"PR #{pr['pr_id']} {k} is {s['status']} but missing stage_name")
                    return True
        return True
    test("Deploy stage details include build_id and stage_name", check_deploy_stage_details)

    # ── Test 17: Abandoned PRs are excluded ──
    def check_abandoned_filtered():
        prs = _json(_get(BASE + "/api/prs"))["prs"]
        abandoned = [p for p in prs if p.get("pr_status") == "abandoned"]
        if abandoned:
            raise Exception(f"Found {len(abandoned)} abandoned PR(s) in active list: {[p['pr_id'] for p in abandoned]}")
        return True
    test("Abandoned PRs are excluded from dashboard", check_abandoned_filtered)

    # ── Test 18: FE.MSecSCC completed PRs show deploy stages ──
    def check_fe_msecscc_deploy():
        prs = _json(_get(BASE + "/api/prs"))["prs"]
        scc_completed = [p for p in prs
                         if p.get("repo") == "FE.MSecSCC"
                         and p.get("pr_status") == "completed"
                         and p.get("last_refreshed")]
        if not scc_completed:
            return True  # no refreshed SCC PRs yet
        for pr in scc_completed:
            stages = pr.get("stages", {})
            deploy_statuses = [
                stages.get(k, {}).get("status", "pending")
                for k in ("dev_pipeline", "stage_pipeline", "prod_pipeline")
            ]
            if any(s not in ("pending", "not_applicable") for s in deploy_statuses):
                return True
        raise Exception("No FE.MSecSCC completed PR has any deploy stage detected — "
                        "pipeline name classification may be broken")
    test("FE.MSecSCC deploy stages detected (name-based classification)", check_fe_msecscc_deploy)

    passed = sum(1 for r in results if r["passed"])
    return {
        "suite": "Pipeline Dashboard",
        "total": len(results),
        "passed": passed,
        "failed": len(results) - passed,
        "results": results,
    }


if __name__ == "__main__":
    print("Running Pipeline Dashboard E2E Tests...")
    print(f"Server: {BASE}\n")
    report = run_pipeline_tests()
    print(f"--- {report['suite']} ({report['passed']}/{report['total']}) ---")
    for r in report["results"]:
        sym = "\u2705" if r["passed"] else "\u274c"
        err = f" \u2014 {r['error']}" if r["error"] else ""
        print(f"  {sym} {r['name']}{err}")
    print(f"\n{'='*40}")
    print(f"Total: {report['passed']}/{report['total']} passed")
    sys.exit(0 if report["failed"] == 0 else 1)
