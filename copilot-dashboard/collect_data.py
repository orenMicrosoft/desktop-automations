"""
Copilot CLI Usage Dashboard - Data Collector
Reads the session-store.db and generates dashboard-data.json
Run daily via scheduled task or manually before viewing the dashboard.
"""

import sqlite3
import json
import os
import re
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path

DB_PATH = os.environ.get("COPILOT_SESSION_DB", os.path.expanduser(r"~\.copilot\session-store.db"))
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "dashboard-data.json")


def get_connection():
    return sqlite3.connect(DB_PATH, timeout=5)


def query_sessions(conn):
    """Get all sessions with aggregated metrics."""
    cur = conn.execute("""
        SELECT 
            s.id,
            s.cwd,
            s.repository,
            s.branch,
            s.summary,
            s.created_at,
            s.updated_at,
            COUNT(DISTINCT t.turn_index) as total_turns,
            COALESCE(SUM(length(t.user_message)), 0) as total_user_chars,
            COALESCE(SUM(length(t.assistant_response)), 0) as total_assistant_chars,
            MIN(t.timestamp) as first_turn,
            MAX(t.timestamp) as last_turn
        FROM sessions s
        LEFT JOIN turns t ON t.session_id = s.id
        GROUP BY s.id
        ORDER BY s.created_at DESC
    """)
    columns = [d[0] for d in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def query_checkpoints(conn):
    """Get checkpoint (compact) counts per session."""
    cur = conn.execute("""
        SELECT session_id, COUNT(*) as checkpoint_count,
               GROUP_CONCAT(title, ' || ') as titles
        FROM checkpoints
        GROUP BY session_id
    """)
    return {row[0]: {"count": row[1], "titles": row[2]} for row in cur.fetchall()}


def query_turns_for_corrections(conn):
    """Get turns that look like user corrections."""
    correction_patterns = [
        r'\bno[,.]',
        r'\bwrong\b',
        r"that's not",
        r'\bincorrect\b',
        r'\bundo\b',
        r'\brevert\b',
        r'\bdon\'t do\b',
        r'\bstop\b.*\bdoing\b',
        r'\bnot what I\b',
        r'\binstead\b.*\bshould\b',
        r'\bplease fix\b',
        r'\bthat broke\b',
        r'\bwhy did you\b',
    ]
    
    cur = conn.execute("""
        SELECT session_id, turn_index, user_message, timestamp
        FROM turns
        WHERE user_message IS NOT NULL
    """)
    
    corrections = defaultdict(list)
    for row in cur.fetchall():
        session_id, turn_index, msg, timestamp = row
        if msg:
            msg_lower = msg.lower()
            for pattern in correction_patterns:
                if re.search(pattern, msg_lower):
                    corrections[session_id].append({
                        "turn": turn_index,
                        "timestamp": timestamp,
                        "snippet": msg[:120]
                    })
                    break
    return corrections


def query_session_files(conn):
    """Get files touched per session."""
    cur = conn.execute("""
        SELECT session_id, file_path, tool_name, COUNT(*) as edit_count
        FROM session_files
        GROUP BY session_id, file_path, tool_name
    """)
    files = defaultdict(list)
    for row in cur.fetchall():
        files[row[0]].append({
            "path": row[1],
            "tool": row[2],
            "count": row[3]
        })
    return files


def classify_session(session, turns_text):
    """Classify session into usage categories based on content signals."""
    categories = []
    
    repo = session.get("repository") or ""
    branch = session.get("branch") or ""
    summary = session.get("summary") or ""
    cwd = session.get("cwd") or ""
    text = f"{repo} {branch} {summary} {cwd} {turns_text}".lower()
    
    # PR review
    if any(k in text for k in ["pullrequest", "pull request", "pr comment", "pr review", "review this pr", "/pullrequest/"]):
        categories.append("PR Review")
    
    # ADO / Azure DevOps
    if any(k in text for k in ["dev.azure.com", "visualstudio.com", "ado ", "azure devops", "pipeline", "build definition"]):
        categories.append("ADO Tasks")
    
    # Multi-repo
    repo_names = set()
    for r in re.findall(r'Rome-Visionaries-\w+', text, re.IGNORECASE):
        repo_names.add(r.lower())
    if "nuget" in text and len(repo_names) >= 2:
        categories.append("Multi-Repo")
    
    # Web search / online research
    if any(k in text for k in ["web_search", "web search", "search online", "groq", "figma"]):
        categories.append("Online Search")
    
    # Code implementation
    if any(k in text for k in ["endpoint", "controller", "implement", "add feature", "create ", "build "]):
        categories.append("Code Implementation")
    
    # Debugging / bug fixing
    if any(k in text for k in ["bug", "debug", "fix", "error", "failing", "broken", "crash"]):
        categories.append("Debugging")
    
    # Testing
    if any(k in text for k in ["test", "e2e", "unit test", "nunit"]):
        categories.append("Testing")
    
    # Automation / tooling
    if any(k in text for k in ["automat", "schedule", "task scheduler", "playwright", "coreidentity", "renew"]):
        categories.append("Automation")
    
    # Learning / course
    if any(k in text for k in ["course", "unit ", "lecture", "psychopathology", "personality", "transcrib"]):
        categories.append("Learning/Education")
    
    # Documentation
    if any(k in text for k in ["readme", "documentation", "instructions", "release notes"]):
        categories.append("Documentation")
    
    # Configuration / DevOps
    if any(k in text for k in ["deploy", "kusto", "keel", "ev2", "config"]):
        categories.append("DevOps/Deploy")
    
    # PDF / file processing
    if any(k in text for k in ["pdf", "compress", "convert"]):
        categories.append("File Processing")
    
    # General utility
    if not categories:
        categories.append("General Utility")
    
    return categories


def compute_parallel_sessions(sessions):
    """Find overlapping session time windows."""
    intervals = []
    for s in sessions:
        if s.get("first_turn") and s.get("last_turn"):
            try:
                start = datetime.fromisoformat(s["first_turn"].replace("Z", "+00:00"))
                end = datetime.fromisoformat(s["last_turn"].replace("Z", "+00:00"))
                if (end - start).total_seconds() > 60:
                    intervals.append((start, end, s["id"], s.get("summary") or s["id"][:8]))
            except (ValueError, TypeError):
                continue
    
    intervals.sort(key=lambda x: x[0])
    
    parallel_groups = []
    for i, (s1, e1, id1, name1) in enumerate(intervals):
        for j, (s2, e2, id2, name2) in enumerate(intervals):
            if j <= i:
                continue
            overlap_start = max(s1, s2)
            overlap_end = min(e1, e2)
            if overlap_start < overlap_end:
                overlap_mins = (overlap_end - overlap_start).total_seconds() / 60
                if overlap_mins > 5:
                    parallel_groups.append({
                        "session_a": name1,
                        "session_b": name2,
                        "overlap_minutes": round(overlap_mins, 1),
                        "date": overlap_start.strftime("%Y-%m-%d")
                    })
    
    return parallel_groups


def compute_daily_stats(sessions):
    """Aggregate stats by day."""
    daily = defaultdict(lambda: {
        "sessions": 0, "turns": 0, "user_chars": 0, "assistant_chars": 0,
        "total_duration_min": 0, "categories": defaultdict(int)
    })
    
    for s in sessions:
        if not s.get("created_at"):
            continue
        try:
            day = s["created_at"][:10]
        except (TypeError, IndexError):
            continue
        
        daily[day]["sessions"] += 1
        daily[day]["turns"] += s.get("total_turns", 0) or 0
        daily[day]["user_chars"] += s.get("total_user_chars", 0) or 0
        daily[day]["assistant_chars"] += s.get("total_assistant_chars", 0) or 0
        daily[day]["total_duration_min"] += s.get("duration_minutes", 0) or 0
        
        for cat in s.get("categories", []):
            daily[day]["categories"][cat] += 1
    
    result = {}
    for day, stats in sorted(daily.items()):
        result[day] = {
            **stats,
            "categories": dict(stats["categories"])
        }
    return result


def compute_weekly_stats(daily_stats):
    """Aggregate daily stats into weeks."""
    weekly = defaultdict(lambda: {
        "sessions": 0, "turns": 0, "user_chars": 0, "assistant_chars": 0,
        "total_duration_min": 0, "categories": defaultdict(int), "days_active": 0
    })
    
    for day_str, stats in daily_stats.items():
        try:
            dt = datetime.strptime(day_str, "%Y-%m-%d")
            week_start = dt - timedelta(days=dt.weekday())
            week_key = week_start.strftime("%Y-%m-%d")
        except ValueError:
            continue
        
        weekly[week_key]["sessions"] += stats["sessions"]
        weekly[week_key]["turns"] += stats["turns"]
        weekly[week_key]["user_chars"] += stats["user_chars"]
        weekly[week_key]["assistant_chars"] += stats["assistant_chars"]
        weekly[week_key]["total_duration_min"] += stats["total_duration_min"]
        weekly[week_key]["days_active"] += 1
        for cat, cnt in stats["categories"].items():
            weekly[week_key]["categories"][cat] += cnt
    
    result = {}
    for week, stats in sorted(weekly.items()):
        result[week] = {
            **stats,
            "categories": dict(stats["categories"])
        }
    return result


def get_turns_text_by_session(conn):
    """Get concatenated user messages per session for classification."""
    cur = conn.execute("""
        SELECT session_id, GROUP_CONCAT(user_message, ' ')
        FROM turns
        WHERE user_message IS NOT NULL
        GROUP BY session_id
    """)
    return {row[0]: (row[1] or "") for row in cur.fetchall()}


def main():
    conn = get_connection()
    
    print("Collecting sessions...")
    sessions = query_sessions(conn)
    
    print("Collecting checkpoints (compacts)...")
    checkpoints = query_checkpoints(conn)
    
    print("Detecting corrections...")
    corrections = query_turns_for_corrections(conn)
    
    print("Collecting file edits...")
    session_files = query_session_files(conn)
    
    print("Getting turn text for classification...")
    turns_text = get_turns_text_by_session(conn)
    
    # Enrich sessions
    for s in sessions:
        sid = s["id"]
        
        # Duration in minutes
        if s.get("first_turn") and s.get("last_turn"):
            try:
                t1 = datetime.fromisoformat(s["first_turn"].replace("Z", "+00:00"))
                t2 = datetime.fromisoformat(s["last_turn"].replace("Z", "+00:00"))
                s["duration_minutes"] = round((t2 - t1).total_seconds() / 60, 1)
            except (ValueError, TypeError):
                s["duration_minutes"] = 0
        else:
            s["duration_minutes"] = 0
        
        # Estimated tokens (rough: 1 token ≈ 4 chars)
        user_chars = s.get("total_user_chars") or 0
        asst_chars = s.get("total_assistant_chars") or 0
        s["estimated_tokens_user"] = round(user_chars / 4)
        s["estimated_tokens_assistant"] = round(asst_chars / 4)
        s["estimated_tokens_total"] = s["estimated_tokens_user"] + s["estimated_tokens_assistant"]
        
        # Checkpoints (compacts)
        cp = checkpoints.get(sid, {"count": 0, "titles": None})
        s["compact_count"] = cp["count"]
        s["compact_titles"] = cp["titles"]
        
        # Corrections
        s["correction_count"] = len(corrections.get(sid, []))
        s["corrections"] = corrections.get(sid, [])
        
        # Files
        s["files_touched"] = len(session_files.get(sid, []))
        
        # Categories
        s["categories"] = classify_session(s, turns_text.get(sid, ""))
    
    # Parallel sessions
    print("Computing parallel sessions...")
    parallel = compute_parallel_sessions(sessions)
    
    # Daily/weekly aggregations
    daily_stats = compute_daily_stats(sessions)
    weekly_stats = compute_weekly_stats(daily_stats)
    
    # Category totals
    category_totals = defaultdict(int)
    for s in sessions:
        for cat in s.get("categories", []):
            category_totals[cat] += 1
    
    # Repos worked on
    repos = defaultdict(int)
    for s in sessions:
        repo = s.get("repository")
        if repo:
            short = repo.split("/")[-1] if "/" in repo else repo
            repos[short] += 1
    
    # Summary stats
    active_sessions = [s for s in sessions if (s.get("total_turns") or 0) > 0]
    total_tokens = sum(s.get("estimated_tokens_total", 0) for s in sessions)
    total_turns = sum(s.get("total_turns", 0) or 0 for s in sessions)
    total_corrections = sum(s.get("correction_count", 0) for s in sessions)
    total_compacts = sum(s.get("compact_count", 0) for s in sessions)
    durations = [s["duration_minutes"] for s in active_sessions if s["duration_minutes"] > 0]
    
    summary = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "total_sessions": len(sessions),
        "active_sessions": len(active_sessions),
        "total_turns": total_turns,
        "total_estimated_tokens": total_tokens,
        "avg_turns_per_session": round(total_turns / max(len(active_sessions), 1), 1),
        "avg_duration_minutes": round(sum(durations) / max(len(durations), 1), 1),
        "median_duration_minutes": round(sorted(durations)[len(durations)//2], 1) if durations else 0,
        "total_corrections": total_corrections,
        "correction_rate_per_session": round(total_corrections / max(len(active_sessions), 1), 2),
        "total_compacts": total_compacts,
        "compact_rate_per_session": round(total_compacts / max(len(active_sessions), 1), 2),
        "parallel_session_events": len(parallel),
        "unique_repos": len(repos),
        "days_active": len(daily_stats),
        "sessions_per_active_day": round(len(sessions) / max(len(daily_stats), 1), 1),
    }
    
    # Build output
    dashboard_data = {
        "summary": summary,
        "sessions": sessions,
        "parallel_sessions": parallel,
        "daily_stats": daily_stats,
        "weekly_stats": weekly_stats,
        "category_totals": dict(category_totals),
        "repos": dict(repos),
    }
    
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(dashboard_data, f, indent=2, default=str)
    
    print(f"\nDashboard data written to: {OUTPUT_PATH}")
    print(f"Sessions: {summary['total_sessions']} ({summary['active_sessions']} active)")
    print(f"Total turns: {summary['total_turns']}")
    print(f"Estimated tokens: {summary['total_estimated_tokens']:,}")
    print(f"Avg duration: {summary['avg_duration_minutes']} min")
    print(f"Corrections: {summary['total_corrections']}")
    print(f"Compacts: {summary['total_compacts']}")
    print(f"Parallel overlaps: {summary['parallel_session_events']}")
    
    conn.close()


if __name__ == "__main__":
    main()
