# CoreIdentity Entitlement Auto-Extender

Automatically checks your CoreIdentity memberships weekly and extends
any that are expiring within 30 days — no more missed renewals.

## How it works

1. Opens `https://coreidentity.microsoft.com/manage/entitlement` via Playwright + Edge
2. Clicks "My Memberships" tab
3. Parses the table — skips memberships that already have a pending renewal request
4. For each membership expiring within 30 days:
   - Checks the row checkbox (Syncfusion component — click `<td>` cell)
   - Clicks "Extend" → handles the confirmation dialog
   - Picks the best-matching radio button based on team keywords (Visionaries, MDC, ASC, etc.)
   - Fills justification with "<your justification text>"
5. Logs structured results to `run-history.json`

SSO login is remembered via a persistent browser profile (`.browser-profile/`).

## Usage

```powershell
# Extend all memberships expiring within 30 days
python renew_entitlements.py

# Dry run — show what would be extended without clicking
python renew_entitlements.py --dry-run

# Custom threshold (e.g., extend if expiring within 14 days)
python renew_entitlements.py --threshold 14

# Debug: show all memberships and page structure
python renew_entitlements.py --discover
```

## Scheduled Task

A Windows scheduled task `CoreIdentity-AutoExtend` runs **every Monday at 9:30 AM**.

```powershell
# Check task status
Get-ScheduledTask -TaskName "CoreIdentity-AutoExtend"

# Run it now
Start-ScheduledTask -TaskName "CoreIdentity-AutoExtend"

# Disable / Enable
Disable-ScheduledTask -TaskName "CoreIdentity-AutoExtend"
Enable-ScheduledTask -TaskName "CoreIdentity-AutoExtend"
```

## Monitoring

### Dashboard

Serve the dashboard locally (fetch doesn't work from `file://`):

```powershell
cd <YOUR_INSTALL_PATH>
python -m http.server 8090 --bind 127.0.0.1
# Open http://localhost:8090/dashboard.html
```

It reads `run-history.json` (auto-refreshes every 60 s) and shows:

- **Last run** — timestamp, status, count of extended memberships
- **30-day stats** — total runs, success rate, total memberships extended
- **Run history table** — every run with details and expandable membership list

### Command-line quick checks

```powershell
# Last run status
python -c "import json; d=json.load(open('run-history.json')); r=d[-1]; print(f'{r[\"timestamp\"]} — {r[\"status\"]} — {r[\"extended\"]} extended')"

# All run statuses
python -c "import json; [print(f'{r[\"timestamp\"]} {r[\"status\"]} ext={r[\"extended\"]}') for r in json.load(open('run-history.json'))]"
```

## Configuration

| Setting | Location | Default |
|---|---|---|
| Threshold days | `DEFAULT_THRESHOLD_DAYS` in script (or `--threshold` CLI) | 30 |
| Justification text | `DEFAULT_JUSTIFICATION_TEXT` in script | "<your justification text>" |
| Team keywords | `TEAM_KEYWORDS` dict in script | visionaries, dfc, rome, mdc, mtp, asc, defenders, … |
| Schedule | Windows Task `CoreIdentity-AutoExtend` | Monday 9:30 AM |
| Browser profile | `.browser-profile/` directory | Persistent Edge SSO |
| Run history | `run-history.json` | JSON array of all runs |

## Adding a Similar Automation

To create a new Playwright-based browser automation (e.g., for another internal portal):

**Prompt template for Copilot CLI:**

> I want to automate [TASK] on [PORTAL_URL]. The page has [DESCRIBE TABLE/FORM].
> I need it to [ACTION] when [CONDITION].
> Use the same pattern as CoreIdentity renewal in `<YOUR_INSTALL_PATH>\`.
> Include: persistent Edge SSO, Syncfusion component handling, dry-run mode,
> structured JSON logging (run-history.json), HTML dashboard, and a Windows Scheduled Task.
> Justification text: "[TEXT]". Team keywords: [LIST].

**Key patterns to reuse:**

1. **Persistent browser profile** — `launch_persistent_context()` with `channel="msedge"`
2. **Page load detection** — poll for loading indicator to disappear (CoreIdentity uses "LOADING COREIDENTITY")
3. **Syncfusion components** — click the `<td>` cell, not the hidden `<input>` checkbox
4. **One-at-a-time dialog flow** — check one row → click action → handle dialog → repeat
5. **Structured logging** — `run-history.json` with `save_run_result()` + HTML dashboard
6. **Skip-if-pending** — check a status column to avoid duplicate requests
7. **Scheduled Task** — `Register-ScheduledTask` with `-LogonType Interactive` for browser window

## Requirements

- Python 3.11+ with `playwright` (`pip install playwright && python -m playwright install chromium`)
- Microsoft Edge (used for corporate SSO)
- MSFT-AzVPN-Manual connection (CoreIdentity requires VPN)
- First run: browser opens visible for SSO login; subsequent runs reuse the session
