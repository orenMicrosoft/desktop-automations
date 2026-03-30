"""
ADO REST API client for Pipeline Dashboard.
Uses `az` CLI for auth token, then REST API for fast queries.
"""
import subprocess
import json
import urllib.request
import urllib.parse
import urllib.error
import time
import threading
import os

DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(DIR, "pipeline_data.json")

_token_cache = {"token": None, "expires": 0}
_token_lock = threading.Lock()

# Caches that live for the duration of a single refresh cycle
_pipeline_defs_cache = {}  # repo_name -> [definitions]
_scan_cache = {}  # repo_name -> [pr_data]


def clear_caches():
    """Clear per-refresh caches."""
    _pipeline_defs_cache.clear()
    _scan_cache.clear()


def _load_config():
    with open(DATA_FILE, "r") as f:
        return json.load(f)["config"]


def _repo_config(cfg, repo_name):
    """Get per-repo org, project, and repo_id.

    Repos can be configured as:
      - simple string (repo_id) → uses default ado_org / ado_project
      - dict with {id, org, project} → uses custom org/project
    """
    entry = cfg["repos"].get(repo_name)
    if entry is None:
        return None
    if isinstance(entry, str):
        return {
            "id": entry,
            "org": cfg["ado_org"],
            "project": cfg["ado_project"],
        }
    return {
        "id": entry["id"],
        "org": entry.get("org", cfg["ado_org"]),
        "project": entry.get("project", cfg["ado_project"]),
    }


def get_token():
    """Get ADO access token via az CLI, cached for ~50 min."""
    with _token_lock:
        now = time.time()
        if _token_cache["token"] and _token_cache["expires"] > now + 60:
            return _token_cache["token"]
        try:
            result = subprocess.run(
                ["az", "account", "get-access-token",
                 "--resource", "499b84ac-1321-427f-aa17-267ca6975798",
                 "--query", "accessToken", "-o", "tsv"],
                capture_output=True, text=True, timeout=30, shell=True
            )
            if result.returncode != 0:
                raise RuntimeError(f"az auth failed: {result.stderr.strip()}")
            token = result.stdout.strip()
            _token_cache["token"] = token
            _token_cache["expires"] = now + 3000
            return token
        except Exception as e:
            raise RuntimeError(f"Failed to get ADO token: {e}")


def _api_get(url, timeout=30):
    """Make authenticated GET request to ADO REST API."""
    token = get_token()
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode() if e.fp else ""
        raise RuntimeError(f"ADO API {e.code}: {body[:200]}")


def _az_cmd(*args, timeout=60):
    """Run az CLI command and return parsed JSON."""
    cmd = ["az"] + list(args) + ["--output", "json"]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, shell=True)
    if result.returncode != 0:
        raise RuntimeError(f"az command failed: {result.stderr.strip()[:200]}")
    return json.loads(result.stdout) if result.stdout.strip() else []


# ── PR Operations ──────────────────────────────────────────────────

def scan_prs_for_repo(repo_name, top=10):
    """Scan ADO for recent PRs by the user in a specific repo (cached)."""
    if repo_name in _scan_cache:
        return _scan_cache[repo_name]
    cfg = _load_config()
    rcfg = _repo_config(cfg, repo_name)
    if not rcfg:
        print(f"  [WARN] Unknown repo: {repo_name}")
        _scan_cache[repo_name] = []
        return []
    try:
        prs = _az_cmd(
            "repos", "pr", "list",
            "--organization", rcfg["org"],
            "--project", rcfg["project"],
            "--repository", repo_name,
            "--creator", cfg["creator_email"],
            "--status", "all",
            "--top", str(top),
        )
        result = [_normalize_pr(pr, repo_name) for pr in prs]
        _scan_cache[repo_name] = result
        return result
    except Exception as e:
        print(f"  [WARN] Failed to scan {repo_name}: {e}")
        _scan_cache[repo_name] = []
        return []


def _normalize_pr(pr, repo_name):
    """Convert az CLI PR output to our data model (lightweight, no API calls)."""
    cfg = _load_config()
    rcfg = _repo_config(cfg, repo_name)
    pr_id = pr["pullRequestId"]
    repo_id = pr["repository"]["id"]
    status = pr["status"]  # active, completed, abandoned
    source = pr.get("sourceRefName", "").replace("refs/heads/", "")
    target = pr.get("targetRefName", "").replace("refs/heads/", "")

    org = rcfg["org"] if rcfg else cfg["ado_org"]
    project = rcfg["project"] if rcfg else cfg["ado_project"]
    pr_url = (f"{org}/{project}/_git/{repo_name}"
              f"/pullrequest/{pr_id}")

    # Determine review/approval status from reviewers
    reviewers = pr.get("reviewers", [])
    has_approval = any(r.get("vote", 0) >= 10 for r in reviewers)
    has_rejection = any(r.get("vote", 0) < -5 for r in reviewers)
    reviewer_names = [
        {"name": r.get("displayName", ""), "vote": r.get("vote", 0)}
        for r in reviewers if r.get("vote", 0) != 0
    ]

    return {
        "id": f"{repo_name.lower()}-{pr_id}",
        "repo": repo_name,
        "repo_id": repo_id,
        "pr_id": pr_id,
        "title": pr.get("title", ""),
        "source_branch": source,
        "target_branch": target,
        "created_at": pr.get("creationDate", ""),
        "closed_at": pr.get("closedDate"),
        "pr_status": status,
        "pr_url": pr_url,
        "merge_status": pr.get("mergeStatus"),
        "reviewers": reviewer_names,
        "has_approval": has_approval,
        "has_rejection": has_rejection,
    }


def _fetch_work_items_safe(cfg, repo_id, pr_id, org=None, project=None):
    """Fetch linked work items for a PR. Returns [] on failure."""
    try:
        return get_pr_work_items(cfg, repo_id, pr_id, org=org, project=project)
    except Exception:
        return []


def get_pr_work_items(cfg, repo_id, pr_id, org=None, project=None):
    """Get work items linked to a PR via REST API."""
    org = org or cfg["ado_org"]
    project = project or cfg["ado_project"]
    url = (f"{org}/{project}/_apis/git/repositories/{repo_id}"
           f"/pullRequests/{pr_id}/workitems?api-version=7.1")
    data = _api_get(url)
    items = []
    for wi in data.get("value", []):
        wi_id = wi.get("id")
        if wi_id:
            wi_url = f"{org}/{project}/_workitems/edit/{wi_id}"
            items.append({"id": wi_id, "url": wi_url})
    return items


def get_work_item_details(wi_id, org=None, project=None):
    """Fetch full work item details: type, title, description, parent, relations."""
    cfg = _load_config()
    org = org or cfg["ado_org"]
    project = project or cfg["ado_project"]
    url = (f"{org}/{project}/_apis/wit/workitems/{wi_id}"
           f"?$expand=relations&api-version=7.1")
    data = _api_get(url)
    fields = data.get("fields", {})

    # Strip HTML from description
    desc = fields.get("System.Description", "") or ""
    import re
    desc_clean = re.sub(r'<[^>]+>', '', desc).replace('&nbsp;', ' ').strip()

    # Find parent and related work items
    parent_id = None
    related_ids = []
    for rel in data.get("relations", []):
        rel_type = rel.get("rel", "")
        rel_url = rel.get("url", "")
        import re as _re
        m = _re.search(r'/workItems/(\d+)', rel_url)
        if m:
            linked_id = int(m.group(1))
            if rel_type == "System.LinkTypes.Hierarchy-Reverse":
                parent_id = linked_id
            elif rel_type == "System.LinkTypes.Related":
                related_ids.append(linked_id)

    # Find linked PRs in other repos (cross-repo detection)
    linked_prs = []
    for rel in data.get("relations", []):
        if rel.get("rel") == "ArtifactLink" and "pullRequest" in rel.get("url", ""):
            import urllib.parse as _up
            decoded = _up.unquote(rel["url"])
            m = _re.search(r'/([a-f0-9-]+)/(\d+)$', decoded)
            if m:
                linked_prs.append({"repo_id": m.group(1), "pr_id": int(m.group(2))})

    wi_url = f"{org}/{project}/_workitems/edit/{wi_id}"

    return {
        "id": wi_id,
        "url": wi_url,
        "type": fields.get("System.WorkItemType", ""),
        "title": fields.get("System.Title", ""),
        "state": fields.get("System.State", ""),
        "description": desc_clean[:500] if desc_clean else "",
        "area_path": fields.get("System.AreaPath", ""),
        "parent_id": parent_id,
        "related_ids": related_ids,
        "linked_prs": linked_prs,
    }


def enrich_pr_work_items(pr_data):
    """Fetch and enrich a PR's work items with full details. Called during per-PR refresh."""
    cfg = _load_config()
    repo_name = pr_data.get("repo")
    rcfg = _repo_config(cfg, repo_name) if repo_name else None
    org = rcfg["org"] if rcfg else cfg["ado_org"]
    project = rcfg["project"] if rcfg else cfg["ado_project"]
    repo_id = pr_data.get("repo_id")
    pr_id = pr_data.get("pr_id")
    if not repo_id or not pr_id:
        return []

    wi_ids = _fetch_work_items_safe(cfg, repo_id, pr_id, org=org, project=project)
    enriched = []
    for wi in wi_ids:
        try:
            details = get_work_item_details(wi["id"], org=org, project=project)
            enriched.append(details)
        except Exception as e:
            enriched.append({"id": wi["id"], "url": wi["url"], "error": str(e)})
    return enriched


def get_pr_details(repo_id, pr_id, org=None, project=None):
    """Get detailed PR info via REST API."""
    cfg = _load_config()
    org = org or cfg["ado_org"]
    project = project or cfg["ado_project"]
    url = (f"{org}/{project}/_apis/git/repositories/{repo_id}"
           f"/pullRequests/{pr_id}?api-version=7.1")
    return _api_get(url)


def get_pr_threads(repo_id, pr_id, org=None, project=None):
    """Get PR comment threads."""
    cfg = _load_config()
    org = org or cfg["ado_org"]
    project = project or cfg["ado_project"]
    url = (f"{org}/{project}/_apis/git/repositories/{repo_id}"
           f"/pullRequests/{pr_id}/threads?api-version=7.1")
    data = _api_get(url)
    return data.get("value", [])


# ── Build / Pipeline Operations ────────────────────────────────────

def get_pipeline_definitions(repo_name):
    """Get pipeline definitions for a repo (cached per refresh cycle)."""
    if repo_name in _pipeline_defs_cache:
        return _pipeline_defs_cache[repo_name]
    cfg = _load_config()
    rcfg = _repo_config(cfg, repo_name)
    if not rcfg:
        return []
    url = (f"{rcfg['org']}/{rcfg['project']}/_apis/build/definitions"
           f"?repositoryId={rcfg['id']}&repositoryType=TfsGit"
           f"&$top=50&api-version=7.1")
    data = _api_get(url)
    result = data.get("value", [])
    _pipeline_defs_cache[repo_name] = result
    return result


def get_builds_for_branch(repo_name, branch, definition_ids=None, top=5):
    """Get recent builds for a branch."""
    cfg = _load_config()
    rcfg = _repo_config(cfg, repo_name)
    if not rcfg:
        return []
    branch_ref = f"refs/heads/{branch}" if not branch.startswith("refs/") else branch
    url = (f"{rcfg['org']}/{rcfg['project']}/_apis/build/builds"
           f"?repositoryId={rcfg['id']}&repositoryType=TfsGit"
           f"&branchName={urllib.parse.quote(branch_ref)}"
           f"&$top={top}&api-version=7.1")
    if definition_ids:
        url += f"&definitions={','.join(str(d) for d in definition_ids)}"
    data = _api_get(url)
    return data.get("value", [])


def get_builds_after_date(repo_name, after_date, branch="refs/heads/develop", top=10):
    """Get builds on a branch after a specific date, including in-progress builds."""
    cfg = _load_config()
    rcfg = _repo_config(cfg, repo_name)
    if not rcfg:
        return []
    # minTime filters on finishTime — in-progress builds have no finishTime and are excluded.
    # So we query completed builds with minTime AND in-progress builds separately, then merge.
    completed_url = (
        f"{rcfg['org']}/{rcfg['project']}/_apis/build/builds"
        f"?repositoryId={rcfg['id']}&repositoryType=TfsGit"
        f"&branchName={urllib.parse.quote(branch)}"
        f"&minTime={urllib.parse.quote(after_date)}"
        f"&$top={top}&api-version=7.1"
    )
    in_progress_url = (
        f"{rcfg['org']}/{rcfg['project']}/_apis/build/builds"
        f"?repositoryId={rcfg['id']}&repositoryType=TfsGit"
        f"&branchName={urllib.parse.quote(branch)}"
        f"&statusFilter=inProgress"
        f"&$top={top}&api-version=7.1"
    )
    completed = _api_get(completed_url).get("value", [])
    try:
        in_progress = _api_get(in_progress_url).get("value", [])
    except Exception:
        in_progress = []

    # Merge and deduplicate by build ID, in-progress first
    seen = set()
    merged = []
    for b in in_progress + completed:
        if b["id"] not in seen:
            seen.add(b["id"])
            merged.append(b)
    return merged


def get_build_timeline(build_id, org=None, project=None):
    """Get build timeline (stages, jobs, tasks) for failure analysis."""
    cfg = _load_config()
    org = org or cfg["ado_org"]
    project = project or cfg["ado_project"]
    url = (f"{org}/{project}/_apis/build/builds"
           f"/{build_id}/timeline?api-version=7.1")
    return _api_get(url)


def get_build_logs(build_id, log_id=None, org=None, project=None):
    """Get build logs. If log_id given, get specific log."""
    cfg = _load_config()
    org = org or cfg["ado_org"]
    project = project or cfg["ado_project"]
    if log_id:
        url = (f"{org}/{project}/_apis/build/builds"
               f"/{build_id}/logs/{log_id}?api-version=7.1")
    else:
        url = (f"{org}/{project}/_apis/build/builds"
               f"/{build_id}/logs?api-version=7.1")
    return _api_get(url)


# ── Stage Computation ──────────────────────────────────────────────

STAGE_ORDER = [
    "local_branch", "remote_branch", "pr_review", "pr_approved",
    "pr_completed", "build_pipeline", "buddy_pipeline",
    "dev_pipeline", "stage_pipeline", "prod_pipeline",
]

STAGE_LABELS = {
    "local_branch": "Local Branch",
    "remote_branch": "Remote Branch",
    "pr_review": "PR In Review",
    "pr_approved": "PR Approved",
    "pr_completed": "PR Completed",
    "build_pipeline": "Build Pipeline",
    "buddy_pipeline": "Buddy Pipeline",
    "dev_pipeline": "Dev Pipeline",
    "stage_pipeline": "Stage Pipeline",
    "prod_pipeline": "Prod Pipeline",
}


def _classify_pipeline(definition_name):
    """Classify a pipeline definition by its type keyword."""
    name = definition_name.lower()
    # Suffix match first (Visionaries naming: Repo-Service-type)
    if name.endswith("-pr"):
        return "pr_build"
    elif name.endswith("-buddy"):
        return "buddy"
    elif name.endswith("-official"):
        return "official"
    elif name.endswith("-release"):
        return "release"
    # Contains match (FE.MSecSCC naming: Repo-Type - package)
    if "-official" in name:
        return "official"
    if "-buddy" in name:
        return "buddy"
    if "-release" in name:
        return "release"
    if name.endswith("- ci") or name.endswith("-ci"):
        return "pr_build"
    return "other"


def _extract_service_name(definition_name, repo_name):
    """Extract the service sub-name from a pipeline definition.

    Visionaries naming:  'Rome-Visionaries-Enablement-Infra-official'  → 'Infra'
    SCC naming:          'FE.MSecSCC-Official - aatp'                  → 'aatp'
    Single-service:      'Rome-Visionaries-Onboarding-official'        → None
    """
    name = definition_name

    # --- Visionaries-style suffix-typed names (Repo-Service-type) ---
    ptype = _classify_pipeline(definition_name)
    _type_suffixes = {
        "official": "-official", "release": "-release",
        "buddy": "-buddy", "pr_build": "-pr",
    }
    suffix = _type_suffixes.get(ptype, "")
    if suffix and name.lower().endswith(suffix):
        stripped = name[: len(name) - len(suffix)]
        if stripped.lower().startswith(repo_name.lower()):
            service = stripped[len(repo_name):].strip("-").strip()
            if service:
                return service
        return None

    # --- SCC-style contains-typed names (Repo-Official - package) ---
    for marker in ("-Official - ", "-Official- ", "-Official  - ", "-Buddy-", "-Buddy - "):
        idx = name.find(marker)
        if idx >= 0:
            return name[idx + len(marker):].strip()

    return None


def compute_stages(pr_data):
    """Compute pipeline stages for a single PR."""
    cfg = _load_config()
    repo = pr_data["repo"]
    rcfg = _repo_config(cfg, repo)
    org = rcfg["org"] if rcfg else cfg["ado_org"]
    project = rcfg["project"] if rcfg else cfg["ado_project"]
    status = pr_data["pr_status"]
    stages = {}

    # Stages 1-2: Always done if PR exists
    stages["local_branch"] = {"status": "completed", "url": None}
    branch_url = (f"{org}/{project}/_git/{repo}"
                  f"?version=GB{urllib.parse.quote(pr_data['source_branch'])}")
    stages["remote_branch"] = {"status": "completed", "url": branch_url}

    pr_url = pr_data["pr_url"]

    # Stage 3: PR in review
    if status == "abandoned":
        stages["pr_review"] = {"status": "abandoned", "url": pr_url}
        for s in STAGE_ORDER[3:]:
            stages[s] = {"status": "not_applicable", "url": None}
        return stages

    stages["pr_review"] = {
        "status": "completed" if status == "completed" else "in_progress",
        "url": pr_url,
    }

    # Stage 4: PR approved
    if status == "active":
        if pr_data.get("has_approval"):
            stages["pr_approved"] = {"status": "completed", "url": pr_url,
                                     "details": {"reviewers": pr_data.get("reviewers", [])}}
        elif pr_data.get("has_rejection"):
            stages["pr_approved"] = {"status": "rejected", "url": pr_url,
                                     "details": {"reviewers": pr_data.get("reviewers", [])}}
        else:
            stages["pr_approved"] = {"status": "pending", "url": pr_url}
        for s in STAGE_ORDER[4:]:
            stages[s] = {"status": "pending", "url": None}
        return stages

    stages["pr_approved"] = {"status": "completed", "url": pr_url,
                             "details": {"reviewers": pr_data.get("reviewers", [])}}

    # Stage 5: PR completed
    stages["pr_completed"] = {
        "status": "completed",
        "url": pr_url,
        "details": {"closed_at": pr_data.get("closed_at")},
    }

    # Stages 6-10: Pipeline stages — query ADO builds
    try:
        _compute_pipeline_stages(pr_data, stages, cfg, rcfg)
    except Exception as e:
        print(f"  [WARN] Pipeline query failed for PR {pr_data['pr_id']}: {e}")
        for s in STAGE_ORDER[5:]:
            if s not in stages:
                stages[s] = {"status": "unknown", "url": None,
                             "details": {"error": str(e)}}

    return stages


def _compute_pipeline_stages(pr_data, stages, cfg, rcfg=None):
    """Query ADO for build/release pipeline status."""
    repo = pr_data["repo"]
    if rcfg is None:
        rcfg = _repo_config(cfg, repo) or {"id": cfg["repos"].get(repo), "org": cfg["ado_org"], "project": cfg["ado_project"]}
    closed_at = pr_data.get("closed_at", "")

    # Get pipeline definitions for buddy detection (source branch queries need def IDs)
    try:
        defs = get_pipeline_definitions(repo)
    except Exception:
        defs = []

    def_map = {}
    for d in defs:
        ptype = _classify_pipeline(d["name"])
        if ptype not in def_map:
            def_map[ptype] = []
        def_map[ptype].append(d)

    build_base = f"{rcfg['org']}/{rcfg['project']}/_build/results?buildId="

    # Resolve target branch
    target_branch = pr_data.get("target_branch", "develop")
    branch_ref = target_branch if target_branch.startswith("refs/") else f"refs/heads/{target_branch}"

    # Fetch ALL builds on target branch after merge (official + release + buddy share the query)
    all_builds = []
    if closed_at:
        try:
            all_builds = get_builds_after_date(repo, closed_at, branch=branch_ref, top=50)
        except Exception:
            all_builds = []

    # Classify builds by their embedded definition name (more reliable than matching
    # against get_pipeline_definitions IDs, which can differ for repos with YAML pipelines)
    official_builds = [b for b in all_builds
                       if _classify_pipeline(b.get("definition", {}).get("name", "")) == "official"]
    release_builds = [b for b in all_builds
                      if _classify_pipeline(b.get("definition", {}).get("name", "")) == "release"]

    # ── Stage 6: Official build pipeline ──
    # Pick the primary official build (prefer succeeded/inProgress, skip canceled)
    active_official = [b for b in official_builds
                       if b.get("result") != "canceled" or b.get("status") in ("inProgress", "notStarted")]
    official_build = active_official[0] if active_official else (official_builds[0] if official_builds else None)

    if official_build:
        b_result = official_build.get("result", "none")
        b_status = official_build.get("status", "none")
        if b_status == "completed":
            stage_status = "succeeded" if b_result == "succeeded" else "failed"
        elif b_status in ("inProgress", "notStarted"):
            stage_status = "in_progress"
        else:
            stage_status = "unknown"
        stages["build_pipeline"] = {
            "status": stage_status,
            "url": f"{build_base}{official_build['id']}",
            "details": {
                "build_id": official_build["id"],
                "definition": official_build.get("definition", {}).get("name", ""),
                "result": b_result,
                "status": b_status,
                "start_time": official_build.get("startTime"),
                "finish_time": official_build.get("finishTime"),
            },
        }
    else:
        stages["build_pipeline"] = {"status": "pending", "url": None}

    # ── Service scoping ──
    # In multi-service repos (e.g. Enablement has Enablement, Infra, PartnersApi,
    # GlobalApi services), scope deploy-stage candidates to the same service as the
    # selected official build so all stages point to the same pipeline chain.
    build_service = None
    if official_build:
        build_service = _extract_service_name(
            official_build.get("definition", {}).get("name", ""), repo)
    if build_service:
        official_builds = [
            b for b in official_builds
            if _extract_service_name(b.get("definition", {}).get("name", ""), repo) == build_service
        ]
        release_builds = [
            b for b in release_builds
            if _extract_service_name(b.get("definition", {}).get("name", ""), repo) == build_service
        ]

    # ── Stage 7: Buddy pipeline (optional, runs on source branch) ──
    buddy_defs = def_map.get("buddy", [])
    # Also check if any builds from the main query are buddy builds (name-based)
    buddy_from_all = [b for b in all_builds
                      if _classify_pipeline(b.get("definition", {}).get("name", "")) == "buddy"]
    if buddy_defs or buddy_from_all:
        try:
            source_branch = pr_data["source_branch"]
            builds = get_builds_for_branch(repo, source_branch, top=3)
            buddy_builds = [
                b for b in builds
                if _classify_pipeline(b.get("definition", {}).get("name", "")) == "buddy"
            ]
            if buddy_builds:
                build = buddy_builds[0]
                b_result = build.get("result", "none")
                b_status = build.get("status", "none")
                if b_status == "completed":
                    stage_status = "succeeded" if b_result == "succeeded" else "failed"
                elif b_status in ("inProgress", "notStarted"):
                    stage_status = "in_progress"
                else:
                    stage_status = "unknown"
                stages["buddy_pipeline"] = {
                    "status": stage_status,
                    "url": f"{build_base}{build['id']}",
                    "details": {
                        "build_id": build["id"],
                        "definition": build.get("definition", {}).get("name", ""),
                        "result": b_result,
                    },
                }
            else:
                stages["buddy_pipeline"] = {"status": "not_triggered", "url": None}
        except Exception:
            stages["buddy_pipeline"] = {"status": "unknown", "url": None}
    else:
        stages["buddy_pipeline"] = {"status": "not_applicable", "url": None}

    # ── Stages 8-10: Dev / Stage / Prod deployment ──
    # Deployment stages can live across MULTIPLE builds (official + release).
    # Scan ALL non-canceled builds, fetch timelines, and merge deployment stage results.
    _compute_merged_deploy_stages(
        official_builds, release_builds, stages, build_base, cfg,
        org=rcfg["org"], project=rcfg["project"]
    )


# Status priority for merging deployment stages across builds.
# Higher value = better (takes precedence when merging).
_DEPLOY_STATUS_PRIORITY = {
    "succeeded": 6,
    "in_progress": 5,
    "waiting_approval": 4,
    "pending": 3,
    "failed": 2,
    "canceled": 1,
    "unknown": 0,
}

# Aliases for matching ADO stage names to logical environments.
_ENV_ALIASES = {
    "dev": ["dev"],
    "stage": ["stage", "stg", "staging", "deploy to ppe"],
    "prod": ["prod", "prd", "production", "deploy to ww", "deploy to sip", "deploy to gcchigh", "deploy to dod"],
}

_ENV_TO_STAGE_KEY = {
    "dev": "dev_pipeline",
    "stage": "stage_pipeline",
    "prod": "prod_pipeline",
}


def _compute_merged_deploy_stages(official_builds, release_builds, stages, build_base, cfg, org=None, project=None):
    """Scan all official + release build timelines and merge dev/stage/prod status."""
    # Collect all candidate builds (skip canceled unless inProgress)
    candidates = []
    for b in official_builds + release_builds:
        if b.get("result") == "canceled" and b.get("status") == "completed":
            continue
        candidates.append(b)

    if not candidates:
        for key in _ENV_TO_STAGE_KEY.values():
            stages[key] = {"status": "pending", "url": None}
        return

    # For each env, track the best status seen across all builds
    best = {}  # env -> {"status", "url", "details", "priority"}
    for key in _ENV_TO_STAGE_KEY.values():
        best[key] = {"status": "pending", "url": None, "details": {}, "priority": _DEPLOY_STATUS_PRIORITY["pending"]}

    for build in candidates:
        build_url = f"{build_base}{build['id']}"
        try:
            timeline = get_build_timeline(build["id"], org=org, project=project)
            records = timeline.get("records", [])
            stage_records = [r for r in records if r.get("type") == "Stage"]

            for rec in stage_records:
                name_lower = rec.get("name", "").lower()
                matched_env = None
                for env, aliases in _ENV_ALIASES.items():
                    if any(alias in name_lower for alias in aliases):
                        matched_env = env
                        break
                if not matched_env:
                    continue

                stage_key = _ENV_TO_STAGE_KEY[matched_env]
                status = _resolve_stage_status(rec, records)
                priority = _DEPLOY_STATUS_PRIORITY.get(status, 0)

                if priority > best[stage_key]["priority"]:
                    best[stage_key] = {
                        "status": status,
                        "url": build_url,
                        "details": {
                            "build_id": build["id"],
                            "definition": build.get("definition", {}).get("name", ""),
                            "stage_name": rec.get("name", ""),
                            "state": rec.get("state", ""),
                            "result": rec.get("result", ""),
                            "start_time": rec.get("startTime"),
                            "finish_time": rec.get("finishTime"),
                        },
                        "priority": priority,
                    }
        except Exception:
            continue

    for key in _ENV_TO_STAGE_KEY.values():
        entry = best[key]
        entry.pop("priority", None)
        stages[key] = entry


def _resolve_stage_status(rec, all_records):
    """Map an ADO stage timeline record to a dashboard status string."""
    state = rec.get("state", "")
    result = rec.get("result", "")

    if state == "completed":
        return "succeeded" if result == "succeeded" else "failed"
    elif state == "inProgress":
        return "in_progress"
    elif state == "pending":
        # Check for approval gates
        rec_name = rec.get("name", "").lower()
        for r in all_records:
            if (r.get("type") == "Checkpoint" and r.get("state") == "inProgress"):
                # Match checkpoint to this stage by checking parent or name overlap
                cp_name = r.get("name", "").lower()
                for aliases in _ENV_ALIASES.values():
                    if any(a in rec_name for a in aliases) and any(a in cp_name for a in aliases):
                        return "waiting_approval"
                # Also check if checkpoint's parentId matches stage
                if r.get("parentId") == rec.get("id"):
                    return "waiting_approval"
        return "pending"
    return "pending"


# ── Failure Details ────────────────────────────────────────────────

def get_failure_details(build_id, org=None, project=None):
    """Get detailed failure information from a build."""
    try:
        timeline = get_build_timeline(build_id, org=org, project=project)
        records = timeline.get("records", [])
        failed = [
            {
                "name": r.get("name", ""),
                "type": r.get("type", ""),
                "state": r.get("state", ""),
                "result": r.get("result", ""),
                "log_url": r.get("log", {}).get("url") if r.get("log") else None,
                "issues": r.get("issues", []),
                "error_count": r.get("errorCount", 0),
            }
            for r in records
            if r.get("result") == "failed"
        ]

        # Try to get log content for failed tasks
        for item in failed:
            if item["log_url"]:
                try:
                    log_data = _api_get(item["log_url"])
                    if isinstance(log_data, dict):
                        item["log_lines"] = log_data.get("value", [])[-50:]
                    elif isinstance(log_data, list):
                        item["log_lines"] = log_data[-50:]
                except Exception:
                    item["log_lines"] = []

        return {"build_id": build_id, "failed_items": failed}
    except Exception as e:
        return {"build_id": build_id, "error": str(e)}


# ── Scan All Repos ─────────────────────────────────────────────────

def scan_all_repos():
    """Scan all configured repos for recent PRs by the user."""
    cfg = _load_config()
    all_prs = []
    for repo_name in cfg["repos"]:
        prs = scan_prs_for_repo(repo_name, top=10)
        all_prs.extend(prs)
    return all_prs
