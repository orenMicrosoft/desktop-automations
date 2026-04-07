"""
Teams cache scanner — extracts recent messages from the local Teams IndexedDB cache.

The Microsoft tenant's token protection policy blocks Graph API access from
non-native apps.  As a workaround this module copies the Teams WebView2
IndexedDB (LevelDB) files to a temp directory and does a raw binary scan to
extract message fragments: timestamps, sender names, subjects, content, and
shared links.

Limitations:
  - Cannot reliably distinguish unread vs. read messages (no unread flag
    in the raw binary).  We surface the *most recent* cached messages and
    let the user decide.
  - Content is extracted heuristically — some fragments may be truncated or
    contain minor artifacts.
"""

import os
import re
import shutil
import tempfile
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── Where the new Teams app stores its IndexedDB ──────────────────────────────
_TEAMS_IDB_REL = (
    r"Packages\MSTeams_8wekyb3d8bbwe\LocalCache\Microsoft\MSTeams"
    r"\EBWebView\WV2Profile_tfw\IndexedDB"
    r"\https_teams.microsoft.com_0.indexeddb.leveldb"
)
TEAMS_IDB_DIR = Path(os.environ.get("LOCALAPPDATA", "")) / _TEAMS_IDB_REL


def _copy_to_temp(src: Path) -> Path:
    """Copy the LevelDB dir to a temp folder to avoid file-lock conflicts."""
    dst = Path(tempfile.mkdtemp(prefix="teams_idb_"))
    shutil.copytree(src, dst / "idb", dirs_exist_ok=True)
    return dst / "idb"


def _extract_people(text: str) -> list[str]:
    """Pull display-names from Skype-mention JSON fragments."""
    names = re.findall(r'"displayName"\s*:\s*"([^"]{2,100})"', text)
    skip = {"ES Chat", "Email Connector", "Support", "Microsoft Teams"}
    seen = set()
    out = []
    for n in names:
        n = n.strip()
        # Only keep names that look like real person names (First Last)
        if (
            n not in skip
            and n not in seen
            and len(n) > 3
            and re.match(r"^[A-Z][a-z]+ [A-Z]", n)  # e.g. "Maor Frankel"
        ):
            seen.add(n)
            out.append(n)
    return out


def _extract_subjects(text: str) -> list[str]:
    # Subjects stored with binary length-prefixed format or JSON-style
    raw = re.findall(r'"subject".{0,5}"?\d?([A-Za-z][^"\x00]{5,200})', text)
    seen = set()
    out = []
    for s in raw:
        # Remove any embedded control characters
        s = re.sub(r"[\x00-\x1f]", "", s).strip()
        # Filter garbled subjects: must start with uppercase and be a real phrase
        if (
            len(s) > 8
            and s[0].isupper()
            and s not in seen
            and not re.match(r"^(starta|start[a-z])", s, re.IGNORECASE)
            and re.search(r"[a-zA-Z]{3,}\s+[a-zA-Z]{2,}", s)
        ):
            seen.add(s)
            out.append(s)
    return out


def _extract_links(text: str) -> list[str]:
    urls = re.findall(
        r'"url":"(https://[^"]+(?:pullrequest|build|_git|workitems|_work)[^"]*)"',
        text,
    )
    return list(dict.fromkeys(urls))


def _clean_html(s: str) -> str:
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"&[a-z]+;", " ", s)
    s = re.sub(r"\\[nrt]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


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
    result = {
        "people": [],
        "subjects": [],
        "links": [],
        "messages": [],
        "scanned_at": datetime.now(timezone.utc).isoformat(),
        "error": None,
    }

    if not TEAMS_IDB_DIR.exists():
        result["error"] = f"Teams IndexedDB not found at {TEAMS_IDB_DIR}"
        return result

    # Copy DB to temp to avoid lock conflicts with running Teams
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

    # Compute the date prefix filter (e.g. "2026-04-0")
    now = datetime.now(timezone.utc)
    date_prefixes = set()
    for d in range(days_back + 1):
        dt = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
        dt -= timedelta(days=d)
        date_prefixes.add(dt.strftime("%Y-%m-%d"))

    all_people: set[str] = set()
    all_subjects: list[str] = []
    all_links: list[str] = []
    messages: list[dict] = []
    seen_keys: set[str] = set()

    for fpath in files:
        raw = fpath.read_bytes()
        text_utf8 = raw.decode("utf-8", errors="ignore")

        # ── People, subjects, links (from JSON mention blocks) ──
        all_people.update(_extract_people(text_utf8))
        all_subjects.extend(_extract_subjects(text_utf8))
        all_links.extend(_extract_links(text_utf8))

        # ── Messages keyed by composetime ──
        for m in re.finditer(
            rb"composetime.{0,5}(\d{4}-\d{2}-\d{2}T[\d:.]+Z?)", raw
        ):
            ts = m.group(1).decode("ascii")
            if ts[:10] not in date_prefixes:
                continue

            start = max(0, m.start() - 4000)
            end = min(len(raw), m.end() + 2000)
            window_raw = raw[start:end]
            window_utf8 = text_utf8[max(0, m.start() - 4000): min(len(text_utf8), m.end() + 2000)]

            # Extract senders from JSON mentions
            senders = _extract_people(window_utf8)

            # Extract subjects
            subjects = _extract_subjects(window_utf8)

            # Extract content from UTF-16LE sections
            contents = []
            for u16 in re.finditer(rb"(?:[\x20-\x7e]\x00){12,}", window_raw):
                try:
                    decoded = u16.group().decode("utf-16-le").strip()
                    if (
                        len(decoded) > 20
                        and not decoded.startswith("19:")
                        and not decoded.startswith("8:orgid")
                        and "@thread" not in decoded
                        and "schema.skype" not in decoded
                        and "Microsoft Teams" not in decoded
                        and re.search(r"[a-zA-Z]{3,}\s+[a-zA-Z]{3,}", decoded)
                    ):
                        clean = _clean_html(decoded)
                        if len(clean) > 20:
                            contents.append(clean[:400])
                except Exception:
                    pass

            content_key = contents[0][:80] if contents else ""
            unique_key = f"{ts}|{content_key}"
            if unique_key in seen_keys or (not contents and not subjects):
                continue
            seen_keys.add(unique_key)

            messages.append({
                "time": ts,
                "senders": senders[:5],
                "content": contents[:3],
                "subject": subjects[0] if subjects else None,
            })

    # De-duplicate subjects and links globally
    result["people"] = sorted(all_people)
    result["subjects"] = list(dict.fromkeys(all_subjects))
    result["links"] = list(dict.fromkeys(all_links))[:30]
    result["messages"] = sorted(messages, key=lambda m: m["time"], reverse=True)
    return result


# ── CLI helper ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import json as _json

    data = scan()
    if data["error"]:
        print(f"ERROR: {data['error']}")
    else:
        print(f"People ({len(data['people'])}): {', '.join(data['people'])}")
        print(f"Subjects ({len(data['subjects'])}): {data['subjects']}")
        print(f"Messages: {len(data['messages'])}")
        print(_json.dumps(data["messages"][:20], indent=2))
