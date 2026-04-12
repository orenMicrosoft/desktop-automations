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
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── Where the new Teams app stores its IndexedDB ──────────────────────────────
_TEAMS_IDB_REL = (
    r"Packages\MSTeams_8wekyb3d8bbwe\LocalCache\Microsoft\MSTeams"
    r"\EBWebView\WV2Profile_tfw\IndexedDB"
    r"\https_teams.microsoft.com_0.indexeddb.leveldb"
)
TEAMS_IDB_DIR = Path(os.environ.get("LOCALAPPDATA", "")) / _TEAMS_IDB_REL

# The current user's name for @mention detection
_CURRENT_USER = os.environ.get("TEAMS_USER_NAME", "Oren Horowitz")

# Names to ignore (bots, services, generic labels)
_SKIP_NAMES = {
    "ES Chat", "Email Connector", "Support", "Microsoft Teams",
    "Unknown User", "Deleted User", "Teams Bot", "Power Automate",
    "Forms", "Planner", "Wiki", "Channel", "General",
    "Radius Requests", "Power Virtual Agents",
}

# Keywords for relevance scoring — split into direct requests vs. mere mentions
# Direct requests: phrased as asking someone to do something
_DIRECT_REQUEST_RE = re.compile(
    r"\b(please review|please approve|can you review|could you review|"
    r"can you approve|could you approve|would you mind|need you to|"
    r"needs your review|needs your approval|need your help|"
    r"waiting on you|assigned to you|action required|"
    r"please take a look|can you check|could you check|"
    r"please merge|can you merge)\b",
    re.IGNORECASE,
)
# Urgency markers — only "critical" when combined with a direct request or @mention
_URGENCY_RE = re.compile(
    r"\b(urgent|asap|blocked|blocking|breaking|critical|p0|p1|hotfix|"
    r"production issue|prod issue|outage|incident|sev[- ]?[012])\b",
    re.IGNORECASE,
)
_PR_KEYWORDS = re.compile(
    r"\b(Pull [Rr]equest \d|PR #?\d|review request|code review needed)\b",
)
_QUESTION_RE = re.compile(r"\?\s*$", re.MULTILINE)


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
            errors.append(item.name)
        except Exception as exc:
            errors.append(f"{item.name}: {exc}")
    if errors:
        print(f"  Skipped locked files: {', '.join(errors)}")
    return target


def _is_person_name(name: str) -> bool:
    """Check if a string looks like a real person name (First Last)."""
    if not name or len(name) < 4 or name in _SKIP_NAMES:
        return False
    return bool(re.match(
        r"^[A-Z\u00C0-\u024F][a-z\u00C0-\u024F]+\s+[A-Z\u00C0-\u024F]", name
    ))


def _extract_people(text: str) -> list[str]:
    """Pull display-names from JSON mention blocks and imdisplayname fields."""
    seen = set()
    out = []

    for n in re.findall(r'"displayName"\s*:\s*"([^"]{2,100})"', text):
        n = n.strip()
        if _is_person_name(n) and n not in seen:
            seen.add(n)
            out.append(n)

    for n in re.findall(r'imdisplayname["\s:]+([A-Z][^"\x00\x01]{2,60})', text):
        n = n.strip().rstrip('\\')
        if _is_person_name(n) and n not in seen:
            seen.add(n)
            out.append(n)

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

    for s in re.findall(r'"subject"\s*:\s*"([^"]{4,200})"', text):
        s = re.sub(r"[\x00-\x1f]", "", s).strip()
        if _is_valid_subject(s) and s not in seen:
            seen.add(s)
            out.append(s)

    for s in re.findall(r'"subject".{0,5}"?\d?([A-Za-z][^"\x00]{5,200})', text):
        s = re.sub(r"[\x00-\x1f]", "", s).strip()
        if _is_valid_subject(s) and s not in seen:
            seen.add(s)
            out.append(s)

    for s in re.findall(r'threadtopic["\s:]+([A-Z][^"\x00]{4,200})', text):
        s = re.sub(r"[\x00-\x1f]", "", s).strip()
        if _is_valid_subject(s) and s not in seen:
            seen.add(s)
            out.append(s)

    return out


def _is_valid_subject(s: str) -> bool:
    if len(s) < 5:
        return False
    if not s[0].isupper():
        return False
    if not re.search(r"[a-zA-Z]{2,}\s+[a-zA-Z]{2,}", s):
        return False
    if re.match(r"^(starta|start[a-z])", s, re.IGNORECASE):
        return False
    alpha_ratio = sum(c.isalpha() or c.isspace() for c in s) / len(s)
    return alpha_ratio > 0.7


def _extract_links(text: str) -> list[str]:
    """Extract work-related URLs (ADO PRs, builds, GitHub, etc.)."""
    urls = []
    for u in re.findall(r'"(?:url|href|link)"\s*:\s*"(https://[^"]+)"', text):
        if any(kw in u for kw in (
            "pullrequest", "build", "_git", "workitems", "_work",
            "github.com", "dev.azure.com", "visualstudio.com"
        )):
            urls.append(u)
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
    s = re.sub(r'^(?:topic|messagePreview|subject|threadtopic)["\s#:]+', '', s)
    return re.sub(r"\s+", " ", s).strip()


def _extract_message_content(window_raw: bytes, window_utf8: str) -> list[str]:
    """Extract readable message content from a binary window."""
    contents = []
    seen_content = set()

    def _add_content(text: str, min_len: int = 15):
        clean = _clean_html(text)
        if len(clean) < min_len:
            return
        if any(skip in clean for skip in (
            "19:", "8:orgid", "@thread", "schema.skype",
            "Microsoft Teams", "urn:", "cid:", "blob:",
            "indexeddb://", "data:image", "emoticon",
            "avatarUrl", "mailhookservice",
        )):
            return
        if re.match(r'^[A-Za-z0-9+/=]{40,}$', clean):
            return
        if clean.startswith('{') or clean.startswith('[') or clean.startswith('"'):
            return
        if len(clean) > 30 and clean.count(' ') < len(clean) / 20:
            return
        key = clean[:100].lower()
        if key not in seen_content:
            seen_content.add(key)
            contents.append(clean[:500])

    for m in re.finditer(r'"content"\s*:\s*"((?:[^"\\]|\\.)+")', window_utf8):
        decoded = m.group(1).rstrip('"').replace('\\"', '"')
        _add_content(decoded)

    for m in re.finditer(r'<(?:p|div|span)[^>]*>([^<]{10,500})', window_utf8):
        _add_content(m.group(1))

    for u16 in re.finditer(rb"(?:[\x20-\x7e]\x00){10,}", window_raw):
        try:
            decoded = u16.group().decode("utf-16-le").strip()
            if len(decoded) > 15 and re.search(r"[a-zA-Z]{3,}\s+[a-zA-Z]{2,}", decoded):
                _add_content(decoded)
        except Exception:
            pass

    for m in re.finditer(r'[\x20-\x7e]{30,}', window_utf8):
        text = m.group().strip()
        if re.search(r"[a-zA-Z]{3,}\s+[a-zA-Z]{3,}", text):
            _add_content(text, min_len=25)

    return contents[:5]


# ── Thread / message ID extraction for deep links ────────────────────────────

_THREAD_SUFFIXES = r"(?:thread\.tacv2|unq\.gbl\.spaces|thread\.skype|thread\.v2)"


def _extract_thread_id(window_utf8: str) -> str | None:
    """Extract the conversation thread ID (19:xxx@thread.tacv2 etc.)."""
    m = re.search(
        r'conversation/(19:[a-zA-Z0-9_-]+@' + _THREAD_SUFFIXES + r')',
        window_utf8,
    )
    if m:
        return m.group(1)
    m = re.search(
        r'(19:[a-zA-Z0-9_-]{20,}@' + _THREAD_SUFFIXES + r')',
        window_utf8,
    )
    return m.group(1) if m else None


def _extract_message_id(window_utf8: str) -> str | None:
    """Extract the message ID (clientmessageid or from conversationLink)."""
    # conversationLink: "conversation/19:xxx@thread.tacv2;messageid=12345"
    m = re.search(r'messageid=(\d{10,20})', window_utf8)
    if m:
        return m.group(1)
    # clientmessageid field
    m = re.search(r'clientmessageid[^\d]{0,5}(\d{10,20})', window_utf8)
    return m.group(1) if m else None


_MICROSOFT_TENANT_ID = "72f988bf-86f1-41af-91ab-2d7cd011db47"


def _build_thread_group_map(files: list[Path]) -> dict[str, str]:
    """Scan all files to build a mapping from channel thread IDs to groupIds."""
    mapping: dict[str, str] = {}
    gid_re = re.compile(
        r'groupId[^0-9a-f]{0,10}([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})'
    )
    thread_re = re.compile(r'19:[a-zA-Z0-9_-]{10,}@thread\.(?:tacv2|skype)')
    for fpath in files:
        try:
            text = fpath.read_bytes().decode("ascii", errors="ignore")
        except Exception:
            continue
        for gm in gid_re.finditer(text):
            gid = gm.group(1)
            start = max(0, gm.start() - 2000)
            end = min(len(text), gm.end() + 2000)
            window = text[start:end]
            for tm in thread_re.finditer(window):
                mapping.setdefault(tm.group(), gid)
    return mapping


def _build_teams_link(
    thread_id: str | None,
    message_id: str | None,
    group_map: dict[str, str] | None = None,
) -> str | None:
    """Build a Teams deep link that opens directly in the desktop app."""
    if not thread_id:
        return None
    encoded_thread = urllib.parse.quote(thread_id, safe='')

    # Chat threads (DMs, group chats, meetings) — use /l/chat/ path
    if thread_id.endswith(("@thread.v2", "@unq.gbl.spaces")):
        return f"msteams://teams.microsoft.com/l/chat/{encoded_thread}/0"

    # Channel threads — need groupId and tenantId
    group_id = (group_map or {}).get(thread_id)
    base = f"msteams://teams.microsoft.com/l/message/{encoded_thread}/{message_id or '0'}"
    params = [f"tenantId={_MICROSOFT_TENANT_ID}"]
    if group_id:
        params.append(f"groupId={group_id}")
    if message_id:
        params.append(f"parentMessageId={message_id}")
    return f"{base}?{'&'.join(params)}"


def _detect_chat_type(window_utf8: str) -> str:
    """Detect whether a message is a direct message, group chat, or channel post.
    Returns: "dm", "group", or "channel".
    """
    # isGroup=false + chatType=chat → 1:1 DM
    has_chat = bool(re.search(r'chatType[^\w]{0,5}chat\b', window_utf8))
    is_not_group = bool(re.search(r'isGroup[^\w]{0,5}false', window_utf8))
    if has_chat and is_not_group:
        return "dm"
    # chatType=chat without isGroup info → could be group chat
    if has_chat:
        return "group"
    # thread.skype or postType → channel
    if re.search(r'@thread\.skype|postType', window_utf8):
        return "channel"
    # thread.v2 or unq.gbl.spaces without postType → likely DM/group
    if re.search(r'@(?:thread\.v2|unq\.gbl\.spaces)', window_utf8):
        return "dm"
    return "channel"


# ── Relevance scoring ────────────────────────────────────────────────────────

def _score_relevance(msg: dict) -> tuple[int, str]:
    """
    Score a message's relevance. Returns (score, label).
    Higher score = more important.
    Labels: "critical", "action", "review", "mention", "question", "info"

    Scoring philosophy:
    - "critical" = urgent + directed at you (mention + urgency, or direct request + urgency)
    - "action"   = someone directly asked you/the group to do something
    - "review"   = PR review request (explicit "Pull Request NNN" or "review request")
    - "mention"  = you were @mentioned but no specific action requested
    - "question" = someone asked a question (ends with ?)
    - "info"     = everything else
    """
    text_parts = (msg.get("content") or []) + [msg.get("subject") or ""]
    full_text = " ".join(text_parts)
    senders = msg.get("senders") or []
    score = 0
    label = "info"

    has_mention = _CURRENT_USER.lower() in full_text.lower()
    has_direct_request = bool(_DIRECT_REQUEST_RE.search(full_text))
    has_urgency = bool(_URGENCY_RE.search(full_text))
    has_pr = bool(_PR_KEYWORDS.search(full_text))
    has_question = bool(_QUESTION_RE.search(full_text))

    # Critical: urgency + (mention OR direct request) — something urgent directed at you
    if has_urgency and (has_mention or has_direct_request):
        score += 80
        label = "critical"
    # Action: direct request (with or without mention)
    elif has_direct_request:
        score += 50
        label = "action"
        if has_mention:
            score += 15  # even more important if directed at you specifically
    # Mention: you were @mentioned but no specific action
    elif has_mention:
        score += 40
        label = "mention"
    # Review: PR review request
    elif has_pr:
        score += 35
        label = "review"
    # Question: someone asked something
    elif has_question:
        score += 15
        label = "question"

    # Small bonuses (don't change the label)
    if senders:
        score += 5
    if msg.get("subject"):
        score += 3

    # DM boost: direct messages are inherently more personal/important
    chat_type = msg.get("chat_type", "channel")
    if chat_type == "dm":
        score += 25
    elif chat_type == "group":
        score += 10

    # Time decay: newer messages get a small boost (max +8)
    try:
        ts = msg.get("time", "")
        if ts:
            msg_time = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            hours_ago = (datetime.now(timezone.utc) - msg_time).total_seconds() / 3600
            score += max(0, int(8 - hours_ago))
    except Exception:
        pass

    return score, label


def scan(days_back: int = 3) -> dict:
    """
    Scan the local Teams cache and return a dict with:
      people   – list[str]          names mentioned
      subjects – list[str]          chat / channel subjects
      links    – list[str]          ADO / GitHub URLs shared
      messages – list[dict]         per-message dicts with relevance scoring
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

    # Build thread→groupId map for channel deep links
    group_map = _build_thread_group_map(files)

    now = datetime.now(timezone.utc)

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

        all_people.update(_extract_people(text_utf8))
        all_subjects.extend(_extract_subjects(text_utf8))
        all_links.extend(_extract_links(text_utf8))

        for m in re.finditer(
            rb"composetime.{0,8}(\d{4}-\d{2}-\d{2}T[\d:.]+Z?)", raw
        ):
            ts = m.group(1).decode("ascii")
            if ts[:10] not in date_prefixes:
                continue

            start = max(0, m.start() - 6000)
            end = min(len(raw), m.end() + 4000)
            window_raw = raw[start:end]
            window_utf8 = window_raw.decode("utf-8", errors="ignore")

            senders = _extract_people(window_utf8)
            subjects = _extract_subjects(window_utf8)
            contents = _extract_message_content(window_raw, window_utf8)

            # Extract IDs for Teams deep link
            thread_id = _extract_thread_id(window_utf8)
            message_id = _extract_message_id(window_utf8)
            teams_link = _build_teams_link(thread_id, message_id, group_map)
            chat_type = _detect_chat_type(window_utf8)

            content_key = contents[0][:80] if contents else ""
            unique_key = f"{ts}|{content_key}"
            if unique_key in seen_keys:
                continue
            if not contents and not subjects and not senders:
                continue
            seen_keys.add(unique_key)

            msg = {
                "time": ts,
                "senders": senders[:5],
                "content": contents[:3],
                "subject": subjects[0] if subjects else None,
                "teams_link": teams_link,
                "chat_type": chat_type,
            }

            # Score relevance
            score, label = _score_relevance(msg)
            msg["relevance_score"] = score
            msg["relevance_label"] = label

            messages.append(msg)

    # Sort by relevance score (highest first), then by time (newest first)
    messages.sort(key=lambda m: (-m["relevance_score"], m["time"]), reverse=False)

    result["people"] = sorted(all_people)
    result["subjects"] = list(dict.fromkeys(all_subjects))[:50]
    result["links"] = list(dict.fromkeys(all_links))[:30]
    result["messages"] = messages
    return result


# ── CLI helper ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import json as _json
    import sys as _sys
    _sys.stdout.reconfigure(encoding="utf-8", errors="replace")

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
        labels = {"critical": "🔴", "action": "🟠", "review": "🟡",
                  "mention": "🔵", "question": "❓", "info": "⚪"}
        types = {"dm": "💬", "group": "👥", "channel": "📢"}
        for m in data["messages"][:15]:
            icon = labels.get(m.get("relevance_label", "info"), "⚪")
            ct = types.get(m.get("chat_type", "channel"), "📢")
            score = m.get("relevance_score", 0)
            link = " 🔗" if m.get("teams_link") else ""
            print(f"  {icon} {ct} [{score:3d}] [{m['time'][:16]}] "
                  f"{', '.join(m['senders'][:2]) or '?'}: "
                  f"{(m['content'][0][:80] if m['content'] else m.get('subject', ''))}"
                  f"{link}")
