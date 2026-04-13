"""
ADO PR Client — fetch PR diffs and post review comments.
Reuses auth pattern from pipeline-dashboard's ado_client.
"""
import subprocess
import json
import urllib.request
import urllib.parse
import urllib.error
import time
import threading
import re

_token_cache = {"token": None, "expires": 0}
_token_lock = threading.Lock()


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


def _api(method, url, body=None, timeout=30):
    """Make authenticated request to ADO REST API."""
    token = get_token()
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                raise RuntimeError(
                    f"ADO API returned non-JSON on {method} {url}: "
                    f"{raw[:200].decode('utf-8', errors='replace')}"
                )
    except urllib.error.HTTPError as e:
        resp_body = e.read().decode() if e.fp else ""
        raise RuntimeError(f"ADO API {e.code} on {method} {url}: {resp_body[:300]}")


def _api_get(url, timeout=30):
    return _api("GET", url, timeout=timeout)


def _api_post(url, body, timeout=30):
    return _api("POST", url, body, timeout=timeout)


def parse_pr_url(url):
    """Parse an ADO PR URL into (org, project, repo_id, pr_id).

    Supports:
      https://dev.azure.com/{org}/{project}/_git/{repo}/pullrequest/{id}
      https://{org}.visualstudio.com/{project}/_git/{repo}/pullrequest/{id}
    """
    clean = url.split('?')[0]

    # Format 1: dev.azure.com
    m = re.match(
        r'https://dev\.azure\.com/([^/]+)/([^/]+)/_git/([^/]+)/pullrequest/(\d+)',
        clean
    )
    if m:
        org, project, repo_name, pr_id = m.groups()
        return {
            "org": f"https://dev.azure.com/{org}",
            "project": project,
            "repo_name": repo_name,
            "pr_id": int(pr_id),
        }

    # Format 2: {org}.visualstudio.com
    m = re.match(
        r'https://([^.]+)\.visualstudio\.com/([^/]+)/_git/([^/]+)/pullrequest/(\d+)',
        clean
    )
    if m:
        org, project, repo_name, pr_id = m.groups()
        return {
            "org": f"https://{org}.visualstudio.com",
            "project": project,
            "repo_name": repo_name,
            "pr_id": int(pr_id),
        }

    raise ValueError(f"Could not parse PR URL: {url}")


def get_pr_info(org, project, repo_name, pr_id):
    """Fetch basic PR metadata."""
    url = (f"{org}/{project}/_apis/git/repositories/{repo_name}"
           f"/pullRequests/{pr_id}?api-version=7.1")
    pr = _api_get(url)
    return {
        "pr_id": pr["pullRequestId"],
        "title": pr["title"],
        "description": pr.get("description", ""),
        "author": pr["createdBy"]["displayName"],
        "author_id": pr["createdBy"]["id"],
        "source_branch": pr["sourceRefName"].replace("refs/heads/", ""),
        "target_branch": pr["targetRefName"].replace("refs/heads/", ""),
        "status": pr["status"],
        "repo_id": pr["repository"]["id"],
        "repo_name": pr["repository"]["name"],
        "url": f"{org}/{project}/_git/{repo_name}/pullrequest/{pr_id}",
    }


def get_pr_iterations(org, project, repo_id, pr_id):
    """Fetch PR iterations (each push is an iteration)."""
    url = (f"{org}/{project}/_apis/git/repositories/{repo_id}"
           f"/pullRequests/{pr_id}/iterations?api-version=7.1")
    data = _api_get(url)
    return data.get("value", [])


def get_pr_changes(org, project, repo_id, pr_id):
    """Fetch the full file diff for the PR (comparing all iterations)."""
    iterations = get_pr_iterations(org, project, repo_id, pr_id)
    if not iterations:
        return []

    last_iter = iterations[-1]["id"]
    url = (f"{org}/{project}/_apis/git/repositories/{repo_id}"
           f"/pullRequests/{pr_id}/iterations/{last_iter}/changes?api-version=7.1")
    data = _api_get(url)
    changes = data.get("changeEntries", [])

    result = []
    for c in changes:
        item = c.get("item", {})
        path = item.get("path", "")
        change_type = c.get("changeType", 0)
        # Skip directories and deletes
        if item.get("isFolder") or not path:
            continue
        result.append({
            "path": path,
            "changeType": _change_type_name(change_type),
        })
    return result


def _change_type_name(ct):
    # ADO may return changeType as string or int
    if isinstance(ct, str):
        return ct.lower()
    if not isinstance(ct, int):
        return "unknown"
    mapping = {1: "add", 2: "edit", 4: "encoding", 8: "rename",
               16: "delete", 32: "undelete", 64: "branch",
               128: "merge", 256: "lock", 512: "rollback",
               1024: "sourceRename", 2048: "targetRename"}
    names = [v for k, v in mapping.items() if ct & k]
    return ", ".join(names) if names else "unknown"


def get_file_diff(org, project, repo_id, file_path,
                  source_commit, target_commit):
    """Fetch the text diff for a single file given source/target commits."""
    if not source_commit or not target_commit:
        return ""

    # Fetch both versions of the file
    try:
        old_content = _get_file_at_commit(org, project, repo_id, target_commit, file_path)
    except Exception:
        old_content = ""  # new file

    try:
        new_content = _get_file_at_commit(org, project, repo_id, source_commit, file_path)
    except Exception:
        new_content = ""  # deleted file

    return _make_unified_diff(old_content, new_content, file_path)


def _get_file_at_commit(org, project, repo_id, commit_id, file_path):
    """Fetch file content at a specific commit."""
    encoded_path = urllib.parse.quote(file_path, safe="/")
    url = (f"{org}/{project}/_apis/git/repositories/{repo_id}"
           f"/items?path={encoded_path}&versionType=commit"
           f"&version={commit_id}&api-version=7.1")
    token = get_token()
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _make_unified_diff(old_content, new_content, file_path):
    """Create a simplified unified diff."""
    import difflib
    old_lines = old_content.splitlines(keepends=True)
    new_lines = new_content.splitlines(keepends=True)
    diff = difflib.unified_diff(
        old_lines, new_lines,
        fromfile=f"a/{file_path}",
        tofile=f"b/{file_path}",
        lineterm=""
    )
    return "".join(diff)


def get_all_diffs(org, project, repo_id, pr_id, changed_files):
    """Fetch diffs for all changed files (non-delete, source files only).

    Pre-fetches PR commit info once to avoid redundant API calls.
    """
    # Fetch source/target commits once
    pr_url = (f"{org}/{project}/_apis/git/repositories/{repo_id}"
              f"/pullRequests/{pr_id}?api-version=7.1")
    pr = _api_get(pr_url)
    source_commit = pr.get("lastMergeSourceCommit", {}).get("commitId", "")
    target_commit = pr.get("lastMergeTargetCommit", {}).get("commitId", "")

    if not source_commit or not target_commit:
        return {}

    diffs = {}
    for f in changed_files:
        path = f["path"]
        if f["changeType"] == "delete":
            continue
        # Skip binary-looking files
        if any(path.endswith(ext) for ext in [".png", ".jpg", ".gif", ".ico", ".woff", ".woff2"]):
            continue
        try:
            diff = get_file_diff(org, project, repo_id, path,
                                 source_commit, target_commit)
            if diff:
                diffs[path] = diff
        except Exception as e:
            diffs[path] = f"[Error fetching diff: {e}]"
    return diffs


def get_current_user(org):
    """Get the authenticated user's display name and ID."""
    url = f"{org}/_apis/connectionData?api-version=7.1"
    data = _api_get(url)
    user = data.get("authenticatedUser", {})
    return {
        "id": user.get("id", ""),
        "display_name": user.get("providerDisplayName", ""),
    }


def get_file_at_branch(org, project, repo_id, file_path, branch):
    """Fetch file content from a specific branch."""
    encoded_path = urllib.parse.quote(file_path, safe="/")
    url = (f"{org}/{project}/_apis/git/repositories/{repo_id}"
           f"/items?path={encoded_path}"
           f"&versionDescriptor.version={branch}"
           f"&versionDescriptor.versionType=branch"
           f"&api-version=7.1")
    token = get_token()
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", errors="replace")


def get_branch_head(org, project, repo_id, branch):
    """Get the latest commit SHA on a branch."""
    url = (f"{org}/{project}/_apis/git/repositories/{repo_id}"
           f"/refs?filter=heads/{branch}&api-version=7.1")
    data = _api_get(url)
    refs = data.get("value", [])
    if refs:
        return refs[0].get("objectId", "")
    raise RuntimeError(f"Branch '{branch}' not found")


def push_file_changes(org, project, repo_id, branch, file_changes, commit_message):
    """Push a commit with file changes to a branch.

    Args:
        file_changes: list of {"path": str, "content": str}
        commit_message: str
    """
    old_object_id = get_branch_head(org, project, repo_id, branch)
    changes = []
    for fc in file_changes:
        changes.append({
            "changeType": "edit",
            "item": {"path": fc["path"]},
            "newContent": {
                "content": fc["content"],
                "contentType": "rawtext",
            },
        })

    body = {
        "refUpdates": [{
            "name": f"refs/heads/{branch}",
            "oldObjectId": old_object_id,
        }],
        "commits": [{
            "comment": commit_message,
            "changes": changes,
        }],
    }
    url = (f"{org}/{project}/_apis/git/repositories/{repo_id}"
           f"/pushes?api-version=7.1")
    return _api_post(url, body)


def post_pr_comment(org, project, repo_id, pr_id, file_path, line, comment_text):
    """Post a comment thread on a PR at a specific file + line.

    If file_path is None, posts a general PR-level comment.
    """
    url = (f"{org}/{project}/_apis/git/repositories/{repo_id}"
           f"/pullRequests/{pr_id}/threads?api-version=7.1")

    thread = {
        "comments": [
            {
                "parentCommentId": 0,
                "content": comment_text,
                "commentType": 1,  # text
            }
        ],
        "status": 1,  # active
    }

    if file_path and line:
        thread["threadContext"] = {
            "filePath": file_path,
            "rightFileStart": {"line": line, "offset": 1},
            "rightFileEnd": {"line": line, "offset": 1},
        }

    return _api_post(url, thread)


def post_pr_comment_general(org, project, repo_id, pr_id, comment_text):
    """Post a general (non-file-specific) comment on a PR."""
    return post_pr_comment(org, project, repo_id, pr_id, None, None, comment_text)
