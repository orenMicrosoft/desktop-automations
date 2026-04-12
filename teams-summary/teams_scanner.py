"""
Teams cache scanner — extracts recent messages from the local Teams IndexedDB cache.

The Microsoft tenant's token protection policy blocks Graph API access from
non-native apps.  As a workaround this module copies the Teams WebView2
IndexedDB (LevelDB) files to a temp directory and does a raw binary scan to
extract message fragments: timestamps, sender names, subjects, content, and
shared links.

Limitations:
  - Cannot reliably distinguish unread vs. read messages.  We surface the
    *most recent* cached messages and let the user decide.
  - Content is extracted heuristically — some fragments may be truncated.
"""

import os
import re
import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── Where the new Teams app stores its IndexedDB ──────────────────────────────
_TEAMS_IDB_REL = (
    r"Packages\MSTeams_8wekyb3d8bbwe\LocalCache\Microsoft\MSTeams"
    r"\EBWebView\WV2Profile_tfw\IndexedDB"
    r"\https_teams.microsoft.com_0.indexeddb.leveldb"
)
TEAMS_IDB_DIR = Path(os.environ.get("LOCALAPPDATA", "")) / _TEAMS_IDB_REL

# Names to ignore (bots, services, generic labels)
_SKIP_NAMES = {
    "ES Chat", "Email Connector", "Support", "Microsoft Teams",
    "Unknown User", "Deleted User", "Teams Bot", "Power Automate",
    "Forms", "Planner", "Wiki", "Channel", "General",
    "Radius Requests", "Power Virtual Agents",
}


def _copy_to_temp(src: Path) -> Path:
    """Copy the LevelDB dir to a temp folder, skipping locked files."""
    dst = Path(tempfile.mkdtemp(prefix="teams_idb_"))
    target = dst / "idb"
    target.mkdir(parents=True, exist_ok=True)
    errors = []
    for item in src.iterdir():
        dest_item = target / item.name
        try:
            if item.is_file():
                shutil.copy2(item, dest_item)
            elif item.is_dir():
                shutil.copytree(item, dest_item, dirs_exist_ok=True)
        except PermissionError:
            # LOCK file and other locked files — skip them, they're not needed
            errors.append(item.name)
        except Exception as exc:
            errors.append(f"{item.name}: {exc}")
    if errors:
        # Only warn, don't fail — the data files (.ldb, .log) are what matter
        print(f"  Skipped locked files: {', '.join(errors)}")
    return target


def _is_person_name(name: str) -> bool:
    """Check if a string looks like a real person name (First Last)."""
    if not name or len(name) < 4 or name in _SKIP_NAMES:
        return False
    # Must match "Firstname Lastname" pattern (Latin or accented chars)
    return bool(re.match(
        r"^[A-Z\u00C0-\u024F][a-z\u00C0-\u024F]+\s+[A-Z\u00C0-\u024F]", name
    ))


def _extract_people(text: str) -> list[str]:
    """Pull display-names from JSON mention blocks and imdisplayname fields."""
    seen = set()
    out = []

    # Pattern 1: JSON "displayName":"..." blocks (mentions in message HTML)
    for n in re.findall(r'"displayName"\s*:\s*"([^"]{2,100})"', text):
        n = n.strip()
        if _is_person_name(n) and n not in seen:
            seen.add(n)
            out.append(n)

    # Pattern 2: imdisplayname fields
    for n in re.findall(r'imdisplayname["\s:]+([A-Z][^"\x00\x01]{2,60})', text):
        n = n.strip().rstrip('\\')
        if _is_person_name(n) and n not in seen:
            seen.add(n)
            out.append(n)

    # Pattern 3: "creator"/"from" with displayName in nearby JSON
    for n in re.findall(r'"(?:creator|from)"[^}]{0,200}"displayName"\s*:\s*"([^"]{2,80})"', text):
        n = n.strip()
        if _is_person_name(n) and n not in seen:
            seen.add(n)
            out.append(n)

    return out


def _extract_subjects(text: str) -> list[str]:
    """Extract conversation subjects from JSON and binary fields."""
    seen = set()
    out = []

    # Pattern 1: JSON "subject":"..." (clean JSON)
    for s in re.findall(r'"subject"\s*:\s*"([^"]{4,200})"', text):
        s = re.sub(r"[\x00-\x1f]", "", s).strip()
        if _is_valid_subject(s) and s not in seen:
            seen.add(s)
            out.append(s)

    # Pattern 2: Binary length-prefixed subjects
    for s in re.findall(r'"subject".{0,5}"?\d?([A-Za-z][^"\x00]{5,200})', text):
        s = re.sub(r"[\x00-\x1f]", "", s).strip()
        if _is_valid_subject(s) and s not in seen:
            seen.add(s)
            out.append(s)

    # Pattern 3: threadtopic field
    for s in re.findall(r'threadtopic["\s:]+([A-Z][^"\x00]{4,200})', text):
        s = re.sub(r"[\x00-\x1f]", "", s).strip()
        if _is_valid_subject(s) and s not in seen:
            seen.add(s)
            out.append(s)

    return out


def _is_valid_subject(s: str) -> bool:
    """Check if a string looks like a real conversation subject."""
    if len(s) < 5:
        return False
    # Must start with uppercase letter and contain at least one space
    if not s[0].isupper():
        return False
    if not re.search(r"[a-zA-Z]{2,}\s+[a-zA-Z]{2,}", s):
        return False
    # Skip garbled/binary-looking strings
    if re.match(r"^(starta|start[a-z])", s, re.IGNORECASE):
        return False
    # Skip strings that are mostly non-alpha
    alpha_ratio = sum(c.isalpha() or c.isspace() for c in s) / len(s)
    return alpha_ratio > 0.7


def _extract_links(text: str) -> list[str]:
    """Extract work-related URLs (ADO PRs, builds, GitHub, etc.)."""
    urls = []

    # ADO / GitHub URLs in JSON
    for u in re.findall(
        r'"(?:url|href|link)"\s*:\s*"(https://[^"]+)"', text
    ):
        if any(kw in u for kw in (
            "pullrequest", "build", "_git", "workitems", "_work",
            "github.com", "dev.azure.com", "visualstudio.com"
        )):
            urls.append(u)

    # Bare URLs in content (HTML href)
    for u in re.findall(r'href="(https://[^"]+)"', text):
        if any(kw in u for kw in (
            "pullrequest", "build", "_git", "workitems", "_work",
            "github.com", "dev.azure.com", "visualstudio.com"
        )):
            urls.append(u)

    return list(dict.fromkeys(urls))


def _clean_html(s: str) -> str:
    """Strip HTML tags, entities, and IndexedDB binary artifacts."""
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"&[a-z]+;", " ", s)
    s = re.sub(r"&#\d+;", " ", s)
    s = re.sub(r"\\[nrt]", " ", s)
    # Remove leading binary field names that leak into content
    s = re.sub(r'^(?:topic|messagePreview|subject|threadtopic)["\s#:]+', '', s)
    return re.sub(r"\s+", " ", s).strip()


def _extract_message_content(window_raw: bytes, window_utf8: str) -> list[str]:
    """Extract readable message content from a binary window using multiple strategies."""
    contents = []
    seen_content = set()

    def _add_content(text: str, min_len: int = 15):
        clean = _clean_html(text)
        if len(clean) < min_len:
            return
        # Skip technical/internal strings
        if any(skip in clean for skip in (
            "19:", "8:orgid", "@thread", "schema.skype",
            "Microsoft Teams", "urn:", "cid:", "blob:",
            "indexeddb://", "data:image", "emoticon",
            "avatarUrl", "mailhookservice",
        )):
            return
        # Skip base64-like strings (long runs of alphanumeric with +/= chars)
        if re.match(r'^[A-Za-z0-9+/=]{40,}$', clean):
            return
        # Skip strings that look like JSON or technical data
        if clean.startswith('{') or clean.startswith('[') or clean.startswith('"'):
            return
        # Skip strings with too few spaces relative to length (garbled binary)
        if len(clean) > 30 and clean.count(' ') < len(clean) / 20:
            return
        key = clean[:100].lower()
        if key not in seen_content:
            seen_content.add(key)
            contents.append(clean[:500])

    # Strategy 1: JSON "content":"<html>..." fields
    for m in re.finditer(r'"content"\s*:\s*"((?:[^"\\]|\\.)+")', window_utf8):
        decoded = m.group(1).rstrip('"').replace('\\"', '"')
        _add_content(decoded)

    # Strategy 2: HTML body fragments (<p>..., <div>..., etc.)
    for m in re.finditer(r'<(?:p|div|span)[^>]*>([^<]{10,500})', window_utf8):
        _add_content(m.group(1))

    # Strategy 3: UTF-16LE encoded text (common in IndexedDB binary format)
    for u16 in re.finditer(rb"(?:[\x20-\x7e]\x00){10,}", window_raw):
        try:
            decoded = u16.group().decode("utf-16-le").strip()
            if len(decoded) > 15 and re.search(r"[a-zA-Z]{3,}\s+[a-zA-Z]{2,}", decoded):
                _add_content(decoded)
        except Exception:
            pass

    # Strategy 4: Plain text runs in UTF-8 (longer readable sequences)
    for m in re.finditer(r'[\x20-\x7e]{30,}', window_utf8):
        text = m.group().strip()
        if re.search(r"[a-zA-Z]{3,}\s+[a-zA-Z]{3,}", text):
            _add_content(text, min_len=25)

    return contents[:5]


def scan(days_back: int = 3) -> dict:
    """
    Scan the local Teams cache and return a dict with:
      people   – list[str]          names mentioned
      subjects – list[str]          chat / channel subjects
      links    – list[str]          ADO / GitHub URLs shared
      messages – list[dict]         per-message dicts (time, senders, content, subject)
      scanned_at – str              ISO timestamp of the scan
      error    – str | None         error message if scan failed
    """
    now = datetime.now(timezone.utc)
    result = {
        "people": [],
        "subjects": [],
        "links": [],
        "messages": [],
        "scanned_at": now.astimezone().isoformat(),
        "error": None,
    }

    if not TEAMS_IDB_DIR.exists():
        result["error"] = f"Teams IndexedDB not found at {TEAMS_IDB_DIR}"
        return result

    try:
        tmp_dir = _copy_to_temp(TEAMS_IDB_DIR)
    except Exception as exc:
        result["error"] = f"Failed to copy IndexedDB: {exc}"
        return result

    try:
        return _do_scan(tmp_dir, days_back, result)
    finally:
        shutil.rmtree(tmp_dir.parent, ignore_errors=True)


def _do_scan(db_dir: Path, days_back: int, result: dict) -> dict:
    files = sorted(
        [f for f in db_dir.iterdir() if f.suffix in (".ldb", ".log")],
        key=lambda f: f.stat().st_mtime,
        reverse=True,
    )
    if not files:
        result["error"] = "No .ldb/.log files found in Teams IndexedDB"
        return result

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days_back)

    # Build set of recent date prefixes for fast filtering
    date_prefixes = set()
    for d in range(days_back + 1):
        dt = now - timedelta(days=d)
        date_prefixes.add(dt.strftime("%Y-%m-%d"))

    all_people: set[str] = set()
    all_subjects: list[str] = []
    all_links: list[str] = []
    messages: list[dict] = []
    seen_keys: set[str] = set()

    for fpath in files:
        try:
            raw = fpath.read_bytes()
        except Exception:
            continue

        text_utf8 = raw.decode("utf-8", errors="ignore")

        # ── Global extraction: people, subjects, links ──
        all_people.update(_extract_people(text_utf8))
        all_subjects.extend(_extract_subjects(text_utf8))
        all_links.extend(_extract_links(text_utf8))

        # ── Per-message extraction keyed by composetime ──
        for m in re.finditer(
            rb"composetime.{0,8}(\d{4}-\d{2}-\d{2}T[\d:.]+Z?)", raw
        ):
            ts = m.group(1).decode("ascii")
            if ts[:10] not in date_prefixes:
                continue

            # Wider extraction window for better content capture
            start = max(0, m.start() - 6000)
            end = min(len(raw), m.end() + 4000)
            window_raw = raw[start:end]
            window_utf8 = window_raw.decode("utf-8", errors="ignore")

            senders = _extract_people(window_utf8)
            subjects = _extract_subjects(window_utf8)
            contents = _extract_message_content(window_raw, window_utf8)

            # De-duplicate by timestamp + first content fragment
            content_key = contents[0][:80] if contents else ""
            unique_key = f"{ts}|{content_key}"
            if unique_key in seen_keys:
                continue
            # Accept messages with content OR subjects (not empty)
            if not contents and not subjects and not senders:
                continue
            seen_keys.add(unique_key)

            messages.append({
                "time": ts,
                "senders": senders[:5],
                "content": contents[:3],
                "subject": subjects[0] if subjects else None,
            })

    # De-duplicate and finalize
    result["people"] = sorted(all_people)
    result["subjects"] = list(dict.fromkeys(all_subjects))[:50]
    result["links"] = list(dict.fromkeys(all_links))[:30]
    result["messages"] = sorted(messages, key=lambda m: m["time"], reverse=True)
    return result


# ── CLI helper ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import json as _json
    import sys as _sys

    days = int(_sys.argv[1]) if len(_sys.argv) > 1 else 3
    print(f"Scanning Teams cache (last {days} days)...")
    data = scan(days_back=days)
    if data["error"]:
        print(f"ERROR: {data['error']}")
    else:
        print(f"People ({len(data['people'])}): {', '.join(data['people'][:20])}")
        print(f"Subjects ({len(data['subjects'])}): {len(data['subjects'])}")
        print(f"Links ({len(data['links'])}): {len(data['links'])}")
        print(f"Messages: {len(data['messages'])}")
        for m in data["messages"][:10]:
            print(f"  [{m['time'][:19]}] {', '.join(m['senders'][:2]) or '?'}: "
                  f"{(m['content'][0][:100] if m['content'] else m.get('subject', ''))}")
