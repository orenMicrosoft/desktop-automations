# 💬 Teams Summary

A desktop automation dashboard that extracts and summarizes recent Microsoft Teams messages from the local Teams client cache.

## Why?

The Microsoft tenant's **token protection policy** blocks Graph API access from non-native apps (CLI tools, Python scripts, etc.). This automation works around that by reading the local Teams WebView2 IndexedDB cache directly — no API token needed.

## What It Shows

- **Recent messages** — timestamped, with sender names and content snippets
- **People** — names mentioned in recent conversations
- **Subjects** — chat and channel topics
- **Shared links** — ADO pull requests, builds, work items shared in chats
- **Search/filter** — quickly find messages by keyword

## Limitations

- Cannot distinguish **unread vs. read** messages — shows all recently cached messages
- Content may be **truncated** (extracted from binary IndexedDB format)
- Subject lines may have **minor artifacts** from binary prefix encoding
- Only shows messages cached by the local Teams client (usually last few days)

## Usage

```powershell
cd teams-summary
python dashboard_server.py            # Opens http://localhost:8095/dashboard.html
python dashboard_server.py --no-browser  # Headless (for hub integration)
```

Or launch from the **Automation Hub** at `http://localhost:8091`.

### CLI Scanner

```powershell
python teams_scanner.py    # Print summary to terminal
```

## Port

`8095`

## Files

| File | Description |
|---|---|
| `dashboard_server.py` | HTTP server — serves dashboard UI and scan API |
| `teams_scanner.py` | Core scanner — copies & parses Teams IndexedDB cache |
| `dashboard.html` | Browser-based dashboard UI |
| `.gitignore` | Excludes `last-scan.json` runtime cache |
