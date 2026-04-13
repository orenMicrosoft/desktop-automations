"""
AI Reviewer — generates PR review comments.

Primary backend: Copilot CLI (launched as subprocess with JSON output).
Fallback: Azure OpenAI (if configured in review_config.json).
"""
import json
import os
import re
import subprocess
import threading
import time
import urllib.request
import urllib.error

DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(DIR, "review_config.json")
LEARNINGS_FILE = os.path.join(DIR, "learnings.json")

# Resolve copilot CLI path at import time
def _find_copilot_exe():
    """Find the full path to copilot.exe."""
    # Check common WinGet install location
    winget_path = os.path.expandvars(
        r"%LOCALAPPDATA%\Microsoft\WinGet\Packages"
    )
    if os.path.isdir(winget_path):
        for entry in os.listdir(winget_path):
            if "GitHub.Copilot" in entry:
                candidate = os.path.join(winget_path, entry, "copilot.exe")
                if os.path.isfile(candidate):
                    return candidate
    # Fallback: try PATH via where
    try:
        result = subprocess.run(
            ["where", "copilot"], capture_output=True, text=True,
            timeout=5, shell=True)
        if result.returncode == 0:
            return result.stdout.strip().splitlines()[0]
    except Exception:
        pass
    return None

COPILOT_EXE = _find_copilot_exe()

_DEFAULT_PROMPT_TEMPLATE = """You are reviewing a pull request. Produce a JSON array of review comments.

STYLE RULES:
- Each comment must be a **friendly question** — concise, simple, no lecturing.
- Do NOT provide the fix inside the comment itself. The comment is a question that nudges the author.
- Only produce **critical** and **medium** severity comments. Skip nitpicks, style-only, formatting.
- Focus on: bugs, logic errors, missing edge cases, maintainability, accessibility, security.
- Do NOT flag auto-generated files (i18n/en/resources.json, i18n/asterisk/resources.json, i18n/strings.ts).

OUTPUT FORMAT — return ONLY a JSON array, no markdown fences, no extra text:
[
  {{
    "severity": "medium" or "critical",
    "file": "path/to/file.tsx",
    "line": 42,
    "comment": "Friendly question for the PR author (concise, no solution)",
    "issue": "Brief description of the actual issue",
    "suggestion": "What the fix should be"
  }}
]

ADDITIONAL LEARNINGS FROM PAST REVIEWS:
{learnings}

PR: {title}
Author: {author}
Branch: {source_branch} → {target_branch}

Files changed ({file_count}):
{diff_text}

IMPORTANT: Return ONLY the JSON array. No explanation, no markdown fences."""

# Load user-customized prompt if saved, otherwise use default
_SAVED_PROMPT_FILE = os.path.join(DIR, "review_prompt.txt")
if os.path.isfile(_SAVED_PROMPT_FILE):
    with open(_SAVED_PROMPT_FILE, "r", encoding="utf-8") as _f:
        REVIEW_PROMPT_TEMPLATE = _f.read()
else:
    REVIEW_PROMPT_TEMPLATE = _DEFAULT_PROMPT_TEMPLATE


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


def _is_azure_openai_configured():
    cfg = _load_config()
    ai = cfg.get("azure_openai", {})
    return bool(ai.get("endpoint") and ai.get("api_key") and ai.get("deployment"))


def _is_copilot_cli_available():
    """Check if the copilot CLI is installed and accessible."""
    return COPILOT_EXE is not None and os.path.isfile(COPILOT_EXE)


def is_ai_configured():
    """Return True if any AI backend is available."""
    return _is_copilot_cli_available() or _is_azure_openai_configured()


def _build_diff_text(diffs):
    """Build the diff section of the prompt, truncating large diffs."""
    diff_text = ""
    for path, diff in diffs.items():
        truncated = diff[:8000] if len(diff) > 8000 else diff
        diff_text += f"\n### {path}\n```diff\n{truncated}\n```\n"
    return diff_text


def _build_prompt(pr_info, diffs):
    """Build the full review prompt."""
    diff_text = _build_diff_text(diffs)
    prompt = REVIEW_PROMPT_TEMPLATE.format(
        learnings=get_learnings_text(),
        title=pr_info.get("title", "Untitled"),
        author=pr_info.get("author", "Unknown"),
        source_branch=pr_info.get("source_branch", ""),
        target_branch=pr_info.get("target_branch", ""),
        file_count=len(diffs),
        diff_text=diff_text,
    )
    # Trim to stay within token limits
    if len(prompt) > 120000:
        prompt = prompt[:120000] + "\n\n[... truncated ...]"
    return prompt


def _extract_json_array(text):
    """Extract a JSON array from text that may contain markdown fences or prose."""
    # Strip markdown code fences
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    cleaned = cleaned.strip()

    # Try direct parse first
    try:
        result = json.loads(cleaned)
        if isinstance(result, list):
            return result
    except json.JSONDecodeError:
        pass

    # Find the first [...] block in the text
    match = re.search(r'\[[\s\S]*\]', cleaned)
    if match:
        try:
            result = json.loads(match.group())
            if isinstance(result, list):
                return result
        except json.JSONDecodeError:
            pass

    return None


def _generate_via_copilot_cli(pr_info, diffs):
    """Generate review using Copilot CLI as a subprocess.
    
    Writes the PR diffs to a temp file and gives Copilot CLI a short prompt
    pointing to that file, avoiding Windows command-line length limits.
    """
    prompt = _build_prompt(pr_info, diffs)

    # Write full prompt to a temp file
    import tempfile
    prompt_file = os.path.join(tempfile.gettempdir(), "pr_review_prompt.txt")
    with open(prompt_file, "w", encoding="utf-8") as f:
        f.write(prompt)

    short_prompt = (
        f"Read the file at {prompt_file} and follow the instructions in it exactly. "
        f"It contains a PR review request with diffs. Return ONLY the JSON array as specified."
    )

    cmd = [
        COPILOT_EXE,
        "--output-format", "json",
        "--allow-all",
        "--no-ask-user",
        "-p", short_prompt,
    ]

    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        cwd=DIR,
    )

    # Collect all assistant message content from JSON events
    assistant_content = []
    result_event = None

    def read_stdout():
        nonlocal result_event
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
                evt_type = event.get("type", "")
                data = event.get("data", {})
                if evt_type == "assistant.message":
                    content = data.get("content", "")
                    if content:
                        assistant_content.append(content)
                elif evt_type == "result":
                    result_event = event
            except json.JSONDecodeError:
                pass

    reader = threading.Thread(target=read_stdout, daemon=True)
    reader.start()

    # Wait for completion (timeout 5 minutes)
    try:
        proc.wait(timeout=300)
    except subprocess.TimeoutExpired:
        proc.kill()
        return [{"severity": "error", "file": "", "line": 0,
                 "comment": "Copilot CLI timed out after 5 minutes",
                 "issue": "AI timeout", "suggestion": "Try again or check Copilot CLI"}]

    reader.join(timeout=10)

    if not assistant_content:
        stderr_output = proc.stderr.read() if proc.stderr else ""
        return [{"severity": "error", "file": "", "line": 0,
                 "comment": f"Copilot CLI produced no output. stderr: {stderr_output[:300]}",
                 "issue": "AI error", "suggestion": "Check Copilot CLI installation"}]

    # Join all assistant content and extract JSON
    full_response = "\n".join(assistant_content)
    comments = _extract_json_array(full_response)
    if comments is None:
        return [{"severity": "error", "file": "", "line": 0,
                 "comment": f"Could not parse Copilot response as JSON. Raw: {full_response[:500]}",
                 "issue": "Parse error", "suggestion": "Try regenerating"}]

    return comments


def _generate_via_azure_openai(pr_info, diffs):
    """Generate review using Azure OpenAI API."""
    cfg = _load_config()
    ai = cfg.get("azure_openai", {})
    endpoint = ai["endpoint"].rstrip("/")
    api_key = ai["api_key"]
    deployment = ai["deployment"]
    api_version = ai.get("api_version", "2024-08-01-preview")

    prompt = _build_prompt(pr_info, diffs)

    url = (f"{endpoint}/openai/deployments/{deployment}"
           f"/chat/completions?api-version={api_version}")

    body = {
        "messages": [
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.3,
        "max_tokens": 4000,
    }

    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("api-key", api_key)

    with urllib.request.urlopen(req, timeout=120) as resp:
        result = json.loads(resp.read())
    content = result["choices"][0]["message"]["content"].strip()

    comments = _extract_json_array(content)
    if comments is None:
        return [{"severity": "error", "file": "", "line": 0,
                 "comment": f"Could not parse Azure OpenAI response. Raw: {content[:500]}",
                 "issue": "Parse error", "suggestion": "Try regenerating"}]
    return comments


def generate_review(pr_info, diffs):
    """Generate review comments using best available AI backend.

    Priority: Copilot CLI > Azure OpenAI.

    Args:
        pr_info: dict with PR metadata (title, author, etc.)
        diffs: dict mapping file_path -> unified diff string

    Returns:
        list of comment dicts.
    """
    # Try Copilot CLI first
    copilot_error = None
    if _is_copilot_cli_available():
        try:
            return _generate_via_copilot_cli(pr_info, diffs)
        except Exception as e:
            copilot_error = str(e)
            # Fall through to Azure OpenAI
        
    # Try Azure OpenAI
    if _is_azure_openai_configured():
        try:
            return _generate_via_azure_openai(pr_info, diffs)
        except Exception as e:
            return [{"severity": "error", "file": "", "line": 0,
                     "comment": f"Azure OpenAI failed: {e}",
                     "issue": "AI error", "suggestion": "Check AI config"}]

    # Neither available or all failed
    error_detail = f" Copilot CLI error: {copilot_error}" if copilot_error else ""
    return [{"severity": "error", "file": "", "line": 0,
             "comment": f"No AI backend available.{error_detail}",
             "issue": "No AI configured",
             "suggestion": "Install Copilot CLI or fill review_config.json"}]


_FIX_PROMPT_TEMPLATE = """You are fixing a code file based on a review comment.

FILE: {file_path}
LINE: {line}

REVIEW COMMENT: {comment}
ISSUE: {issue}
SUGGESTED FIX: {suggestion}

CURRENT FILE CONTENT:
```
{file_content}
```

Return ONLY the complete fixed file content — no markdown fences, no explanation, no extra text.
Keep the fix minimal and surgical — only change what the review comment asks for.
Preserve all existing formatting, indentation, and line endings."""


def generate_fix(file_content, file_path, comment_info):
    """Generate a code fix for a single review comment using Copilot CLI.

    Args:
        file_content: current file content (string)
        file_path: path of the file
        comment_info: dict with keys: comment, issue, suggestion, line

    Returns:
        dict with "fixed_content" (str) and "ok" (bool)
    """
    if not _is_copilot_cli_available():
        return {"ok": False, "error": "Copilot CLI not available"}

    prompt = _FIX_PROMPT_TEMPLATE.format(
        file_path=file_path,
        line=comment_info.get("line", "?"),
        comment=comment_info.get("comment", ""),
        issue=comment_info.get("issue", ""),
        suggestion=comment_info.get("suggestion", ""),
        file_content=file_content,
    )

    # Write to temp file (same pattern as review generation)
    import tempfile
    prompt_file = os.path.join(tempfile.gettempdir(), "pr_fix_prompt.txt")
    with open(prompt_file, "w", encoding="utf-8") as f:
        f.write(prompt)

    short_prompt = (
        f"Read the file at {prompt_file} and follow the instructions in it exactly. "
        f"It contains a code fix request. Return ONLY the complete fixed file content."
    )

    cmd = [
        COPILOT_EXE,
        "--output-format", "json",
        "--allow-all",
        "--no-ask-user",
        "-p", short_prompt,
    ]

    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        cwd=DIR,
    )

    assistant_content = []

    def read_stdout():
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
                if event.get("type") == "assistant.message":
                    content = event.get("data", {}).get("content", "")
                    if content:
                        assistant_content.append(content)
            except json.JSONDecodeError:
                pass

    reader = threading.Thread(target=read_stdout, daemon=True)
    reader.start()

    try:
        proc.wait(timeout=180)
    except subprocess.TimeoutExpired:
        proc.kill()
        return {"ok": False, "error": "Copilot CLI timed out"}

    reader.join(timeout=10)

    if not assistant_content:
        return {"ok": False, "error": "Copilot CLI produced no output"}

    fixed = "".join(assistant_content)
    # Strip markdown fences if Copilot wrapped it
    if fixed.startswith("```"):
        lines = fixed.split("\n")
        # Remove first line (```lang) and last line (```)
        if lines[-1].strip() == "```":
            lines = lines[1:-1]
        else:
            lines = lines[1:]
        fixed = "\n".join(lines)

    return {"ok": True, "fixed_content": fixed}
