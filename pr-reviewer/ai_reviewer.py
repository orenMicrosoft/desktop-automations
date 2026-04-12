"""
AI Reviewer — generates PR review comments using Azure OpenAI.
Falls back to a no-op if no AI config is set.
"""
import json
import os
import urllib.request
import urllib.error

DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(DIR, "review_config.json")
LEARNINGS_FILE = os.path.join(DIR, "learnings.json")

SYSTEM_PROMPT = """You are a senior code reviewer. You review pull requests and produce a JSON array of review comments.

STYLE RULES:
- Each comment must be a **friendly question** — concise, simple, no lecturing.
- Do NOT provide the fix inside the comment itself. The comment is a question that nudges the author.
- Only produce **critical** and **medium** severity comments. Skip nitpicks, style-only, formatting.
- Focus on: bugs, logic errors, missing edge cases, maintainability, accessibility, security.

OUTPUT FORMAT — return ONLY a JSON array, no markdown fences, no extra text:
[
  {
    "severity": "medium" or "critical",
    "file": "path/to/file.tsx",
    "line": 42,
    "comment": "Friendly question for the PR author (concise, no solution)",
    "issue": "Brief description of the actual issue",
    "suggestion": "What the fix should be"
  }
]

ADDITIONAL LEARNINGS FROM PAST REVIEWS:
{learnings}
"""


def _load_config():
    if not os.path.exists(CONFIG_FILE):
        return {}
    with open(CONFIG_FILE, "r") as f:
        return json.load(f)


def _load_learnings():
    if not os.path.exists(LEARNINGS_FILE):
        return []
    with open(LEARNINGS_FILE, "r") as f:
        return json.load(f)


def save_learning(learning_text):
    """Append a new learning from user corrections."""
    learnings = _load_learnings()
    learnings.append(learning_text)
    with open(LEARNINGS_FILE, "w") as f:
        json.dump(learnings, f, indent=2)


def get_learnings_text():
    learnings = _load_learnings()
    if not learnings:
        return "No additional learnings yet."
    return "\n".join(f"- {l}" for l in learnings)


def is_ai_configured():
    cfg = _load_config()
    ai = cfg.get("azure_openai", {})
    return bool(ai.get("endpoint") and ai.get("api_key") and ai.get("deployment"))


def generate_review(pr_info, diffs):
    """Generate review comments using Azure OpenAI.

    Args:
        pr_info: dict with PR metadata (title, author, etc.)
        diffs: dict mapping file_path -> unified diff string

    Returns:
        list of comment dicts, or empty list if AI not configured.
    """
    cfg = _load_config()
    ai = cfg.get("azure_openai", {})

    if not ai.get("endpoint") or not ai.get("api_key") or not ai.get("deployment"):
        return []

    endpoint = ai["endpoint"].rstrip("/")
    api_key = ai["api_key"]
    deployment = ai["deployment"]
    api_version = ai.get("api_version", "2024-08-01-preview")

    # Build the user message with PR context and diffs
    diff_text = ""
    for path, diff in diffs.items():
        # Truncate very large diffs
        truncated = diff[:8000] if len(diff) > 8000 else diff
        diff_text += f"\n### {path}\n```diff\n{truncated}\n```\n"

    user_msg = f"""PR: {pr_info.get('title', 'Untitled')}
Author: {pr_info.get('author', 'Unknown')}
Branch: {pr_info.get('source_branch', '')} → {pr_info.get('target_branch', '')}

Files changed ({len(diffs)}):
{diff_text}
"""
    # Trim to ~120k chars to stay within token limits
    if len(user_msg) > 120000:
        user_msg = user_msg[:120000] + "\n\n[... truncated ...]"

    system = SYSTEM_PROMPT.replace("{learnings}", get_learnings_text())

    url = (f"{endpoint}/openai/deployments/{deployment}"
           f"/chat/completions?api-version={api_version}")

    body = {
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user_msg},
        ],
        "temperature": 0.3,
        "max_tokens": 4000,
    }

    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("api-key", api_key)

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read())
        content = result["choices"][0]["message"]["content"].strip()

        # Strip markdown code fences if present
        if content.startswith("```"):
            content = content.split("\n", 1)[1] if "\n" in content else content[3:]
        if content.endswith("```"):
            content = content[:-3]
        content = content.strip()

        comments = json.loads(content)
        if not isinstance(comments, list):
            return []
        return comments
    except Exception as e:
        return [{"severity": "error", "file": "", "line": 0,
                 "comment": f"AI review failed: {e}",
                 "issue": "AI error", "suggestion": "Check AI config"}]
