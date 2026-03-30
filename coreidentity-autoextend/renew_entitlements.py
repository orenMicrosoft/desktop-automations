"""
CoreIdentity Entitlement Auto-Extender

Checks https://coreidentity.microsoft.com/manage/entitlement weekly,
finds memberships expiring within ~30 days, and extends them automatically.

Uses Playwright with a persistent Edge profile so SSO login is remembered.

On failure, automatically launches a Copilot CLI session in autopilot mode
to diagnose and fix the script (unless --no-autofix is passed).

Usage:
    python renew_entitlements.py              # Extend all memberships expiring within 30 days
    python renew_entitlements.py --dry-run    # Show what would be extended
    python renew_entitlements.py --threshold 14  # Extend if expiring within 14 days
    python renew_entitlements.py --discover   # Debug: capture page structure
    python renew_entitlements.py --no-autofix # Disable autofix on failure
"""

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright

SCRIPT_DIR = Path(__file__).parent
PROFILE_DIR = SCRIPT_DIR / ".browser-profile"
ENTITLEMENTS_URL = "https://coreidentity.microsoft.com/manage/entitlement"
LOG_FILE = SCRIPT_DIR / "renewal-log.txt"
RUN_HISTORY_FILE = SCRIPT_DIR / "run-history.json"
SCREENSHOT_DIR = SCRIPT_DIR / "screenshots"

DEFAULT_JUSTIFICATION_TEXT = os.environ.get("CI_JUSTIFICATION", "Active team member — auto-extend")
VPN_CONNECTION_NAME = "MSFT-AzVPN-Manual"
VPN_CONNECT_TIMEOUT = 30  # seconds

# Keywords to match radio button options to entitlement names.
# Order matters — first match wins. The key is a keyword that may appear in
# the radio label text, the value is a list of entitlement name keywords that
# make it the best pick.
TEAM_KEYWORDS = [
    "visionaries", "dfc", "rome", "mdc", "mtp", "asc", "defenders",
    "security", "telemetry", "orion", "guardians",
]

DEFAULT_THRESHOLD_DAYS = 30

# Errors that are environmental (not code bugs) and should NOT trigger autofix
SKIP_AUTOFIX_PATTERNS = [
    "vpn not connected",
    "remote access required",
    "sso login",
    "page failed to load",
]


def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def save_run_result(result: dict):
    """Append a structured run result to run-history.json for dashboard monitoring."""
    history = []
    if RUN_HISTORY_FILE.exists():
        try:
            history = json.loads(RUN_HISTORY_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, IOError):
            history = []

    history.append(result)

    # Keep last 200 runs
    history = history[-200:]
    RUN_HISTORY_FILE.write_text(json.dumps(history, indent=2, default=str), encoding="utf-8")


def is_vpn_connected() -> bool:
    """Check if the Azure VPN connection is active."""
    try:
        result = subprocess.run(
            ["powershell", "-Command",
             f"(Get-VpnConnection -Name '{VPN_CONNECTION_NAME}').ConnectionStatus"],
            capture_output=True, text=True, timeout=10,
        )
        return "Connected" in result.stdout
    except Exception:
        return False


def connect_vpn() -> bool:
    """Connect to the Azure VPN using rasdial. Returns True if connected."""
    if is_vpn_connected():
        log(f"[VPN] Already connected to {VPN_CONNECTION_NAME}")
        return True

    log(f"[VPN] Connecting to {VPN_CONNECTION_NAME}...")
    try:
        result = subprocess.run(
            ["rasdial", VPN_CONNECTION_NAME],
            capture_output=True, text=True, timeout=VPN_CONNECT_TIMEOUT,
        )
        if result.returncode == 0:
            log(f"[VPN] Successfully connected to {VPN_CONNECTION_NAME}")
            return True
        else:
            log(f"[VPN] Failed to connect: {result.stdout.strip()} {result.stderr.strip()}")
            return False
    except subprocess.TimeoutExpired:
        log(f"[VPN] Connection timed out after {VPN_CONNECT_TIMEOUT}s")
        return False
    except Exception as e:
        log(f"[VPN] Error connecting: {e}")
        return False


def launch_browser(pw, headless=False):
    """Launch Edge with a persistent profile so SSO session is kept."""
    PROFILE_DIR.mkdir(exist_ok=True)
    return pw.chromium.launch_persistent_context(
        user_data_dir=str(PROFILE_DIR),
        headless=headless,
        channel="msedge",
        args=["--disable-blink-features=AutomationControlled"],
        viewport={"width": 1400, "height": 900},
        accept_downloads=False,
    )


def wait_for_page_load(page, max_wait_s=120):
    """Wait for the CoreIdentity SPA to finish loading."""
    print("  Waiting for page to load...")
    poll_ms = 3000
    elapsed = 0

    while elapsed < max_wait_s:
        page.wait_for_timeout(poll_ms)
        elapsed += poll_ms // 1000

        url = page.url
        if "login.microsoftonline" in url or "login.microsoft" in url:
            print(f"  [{elapsed}s] SSO login detected. Please sign in...")
            continue

        body = page.inner_text("body")

        if "REMOTE ACCESS REQUIRED" in body:
            print(f"  [{elapsed}s] Remote access required — VPN not connected!")
            return "vpn"

        if "LOADING COREIDENTITY" in body or len(body.strip()) < 100:
            print(f"  [{elapsed}s] Still loading...")
            continue

        print(f"  [{elapsed}s] Page loaded.")
        page.wait_for_timeout(2000)
        return True

    print(f"  [!] Page did not load within {max_wait_s}s")
    return False


def click_my_memberships_tab(page):
    """Click the 'My Memberships' tab and wait for the table to load."""
    btn = page.query_selector("button:has-text('My Memberships')")
    if btn:
        print("  Clicking 'My Memberships' tab...")
        btn.click()
        try:
            page.wait_for_selector("table tbody tr", timeout=10000)
        except Exception:
            page.wait_for_timeout(5000)
        return True
    else:
        print("  [!] 'My Memberships' tab not found")
        SCREENSHOT_DIR.mkdir(exist_ok=True)
        page.screenshot(path=str(SCREENSHOT_DIR / "debug_tab_not_found.png"), full_page=True)
        print(f"  [!] Screenshot saved. URL: {page.url}")
        body_preview = page.inner_text("body")[:300]
        print(f"  [!] Page body preview: {body_preview}")
        return False


def parse_date(text: str):
    """Parse a CoreIdentity date string like 'April 18, 2026 10:31 AM UTC'."""
    for fmt in ("%B %d, %Y %I:%M %p UTC", "%B %d, %Y %I:%M %p", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(text.strip(), fmt)
        except ValueError:
            continue
    m = re.search(r'(\w+ \d{1,2},? \d{4})', text)
    if m:
        for fmt in ("%B %d, %Y", "%B %d %Y", "%b %d, %Y"):
            try:
                return datetime.strptime(m.group(1), fmt)
            except ValueError:
                continue
    return None


def parse_memberships(page):
    """Parse the My Memberships table.

    Table columns (from discovery):
      [checkbox] | Guid | Entitlement Name | Target Account | Role | Expiration Date | Renewal Request ID
    """
    memberships = []

    rows = page.query_selector_all("table tbody tr")
    for row in rows:
        cells = row.query_selector_all("td")
        if len(cells) < 5:
            continue

        cell_texts = [c.inner_text().strip() for c in cells]

        # Find the expiration date column by scanning cells for a parseable date
        name = ""
        role = ""
        expiry_text = ""
        expiry_date = None
        expiry_col_index = -1

        for i, txt in enumerate(cell_texts):
            parsed = parse_date(txt)
            if parsed:
                expiry_text = txt
                expiry_date = parsed
                expiry_col_index = i
                # Walk backwards to find name and role.
                # Known layout: [checkbox='', guid, name, target_account, role, expiration, renewal_id]
                # Name is a non-empty cell that isn't a GUID or account
                for j in range(1, i):
                    val = cell_texts[j]
                    if not val or "\\orenhorowitz" in val:
                        continue
                    # Skip GUIDs (hex with dashes)
                    if re.match(r'^[0-9a-f]{6,}', val):
                        continue
                    if not name:
                        name = val
                    elif not role:
                        role = val
                # Role is typically the cell just before expiration
                if not role and i >= 1:
                    candidate = cell_texts[i - 1]
                    if candidate and "\\orenhorowitz" not in candidate:
                        role = candidate
                break

        if not name or not expiry_date:
            continue

        days_left = (expiry_date - datetime.now()).days
        # The checkbox cell is the first <td> — clicking it toggles the Syncfusion checkbox
        checkbox_cell = row.query_selector("td")
        checkbox_input = row.query_selector("input[type='checkbox']")

        # Renewal Request ID is the column right after the expiration date column
        renewal_request_id = ""
        if expiry_col_index >= 0 and expiry_col_index + 1 < len(cell_texts):
            renewal_request_id = cell_texts[expiry_col_index + 1].strip()

        memberships.append({
            "name": name,
            "role": role,
            "expiry_text": expiry_text,
            "expiry_date": expiry_date,
            "days_left": days_left,
            "renewal_request_id": renewal_request_id,
            "checkbox_cell": checkbox_cell,
            "checkbox_input": checkbox_input,
        })

    return memberships


def show_memberships(memberships, threshold=None):
    """Display memberships."""
    if not memberships:
        print("\n  No memberships found.")
        return

    label = "membership(s)" if not threshold else f"membership(s) expiring within {threshold} days"
    print(f"\n  {len(memberships)} {label}:")
    print(f"  {'─' * 85}")
    for m in memberships:
        icon = "🔴" if m["days_left"] <= 7 else "🟡" if m["days_left"] <= 30 else "🟢"
        pending = " ⏳ PENDING" if m.get("renewal_request_id") else ""
        print(f"  {icon} {m['name']:35s} {m['role']:25s} {m['days_left']:>3d}d{pending}")
    print()


def extend_memberships(page, to_extend, dry_run=False):
    """Extend memberships one at a time through the dialog flow.

    For each membership:
      1. Check its checkbox
      2. Click the page-level "Extend" button
      3. In the dialog: select Business Justification radio, click dialog "Extend"
      4. Wait for dialog to close
    """
    if not to_extend:
        log("Nothing to extend.")
        return 0

    if dry_run:
        for m in to_extend:
            log(f"  [DRY RUN] Would extend: {m['name']} / {m['role']} ({m['days_left']} days left)")
        return len(to_extend)

    SCREENSHOT_DIR.mkdir(exist_ok=True)
    extended = 0

    for m in to_extend:
        name = m["name"]
        role = m["role"]
        cell = m.get("checkbox_cell")
        cb_input = m.get("checkbox_input")

        if not cell:
            log(f"  [!] No checkbox cell for: {name}")
            continue

        try:
            # Step 1: Check this row's checkbox
            cell.scroll_into_view_if_needed()
            page.wait_for_timeout(500)
            # Make sure it's not already checked
            if cb_input and cb_input.is_checked():
                # Uncheck first to start clean
                cell.click(timeout=5000)
                page.wait_for_timeout(300)
            cell.click(timeout=5000)
            page.wait_for_timeout(500)

            if cb_input and not cb_input.is_checked():
                log(f"  [!] Could not check: {name}")
                continue

            log(f"  ✓ Checked: {name} / {role}")

            # Step 2: Click the page-level "Extend" button (above the table)
            extend_btn = page.query_selector("button:has-text('Extend')")
            if not extend_btn:
                log(f"  [!] No Extend button found for: {name}")
                continue

            extend_btn.click()
            page.wait_for_timeout(2000)

            # Step 3: Handle the "Extend Membership" dialog
            # Wait for dialog to appear — look for "Business Justification" text
            try:
                page.wait_for_selector("text=Business Justification", timeout=5000)
            except Exception:
                log(f"  [!] Dialog did not appear for: {name}")
                page.screenshot(path=str(SCREENSHOT_DIR / f"debug_no_dialog_{name}.png"))
                continue

            # Select the Business Justification radio button.
            # If multiple options, pick the most relevant based on entitlement name.
            radios = page.query_selector_all("input[type='radio']")
            if radios:
                if len(radios) == 1:
                    # Only one option — select it
                    try:
                        radios[0].check(force=True, timeout=3000)
                    except Exception:
                        radios[0].click(force=True)
                    page.wait_for_timeout(300)
                    log(f"  ✓ Selected justification for: {name}")
                else:
                    # Multiple options — pick the best match for this entitlement
                    best_radio = None
                    best_score = -1
                    name_lower = name.lower()

                    for radio in radios:
                        # Get the label text near this radio
                        label_el = radio.evaluate_handle(
                            "el => el.closest('label') || el.parentElement"
                        )
                        label_text = label_el.inner_text().lower() if label_el else ""

                        # Score: how many team keywords match between the label and entitlement name
                        score = 0
                        for kw in TEAM_KEYWORDS:
                            if kw in label_text and kw in name_lower:
                                score += 2  # strong match: keyword in both
                            elif kw in label_text:
                                score += 1  # keyword at least in the label

                        if score > best_score:
                            best_score = score
                            best_radio = radio

                    # Fallback to first radio if no good match
                    if not best_radio:
                        best_radio = radios[0]

                    try:
                        best_radio.check(force=True, timeout=3000)
                    except Exception:
                        best_radio.click(force=True)
                    page.wait_for_timeout(300)

                    label_el = best_radio.evaluate_handle(
                        "el => el.closest('label') || el.parentElement"
                    )
                    chosen = label_el.inner_text().strip()[:60] if label_el else "?"
                    log(f"  ✓ Selected justification for {name}: '{chosen}'")
            else:
                log(f"  [!] No justification radio found for: {name}, proceeding anyway")

            # Fill in any text input/textarea for justification if present
            justification_input = page.query_selector(
                "textarea:near(:text('Justification')), "
                "input[type='text']:near(:text('Justification')), "
                "textarea:near(:text('Business Justification')), "
                "input[type='text']:near(:text('Business Justification'))"
            )
            if not justification_input:
                # Broader: any visible textarea or text input in the dialog area
                for sel in ["textarea", "input[type='text']"]:
                    candidates = page.query_selector_all(sel)
                    for c in candidates:
                        if c.is_visible() and c.bounding_box():
                            justification_input = c
                            break
                    if justification_input:
                        break

            if justification_input and justification_input.is_visible():
                justification_input.fill("part of visionaries team")
                page.wait_for_timeout(300)
                log(f"  ✓ Filled justification text for: {name}")

            # Step 3b: Check the Terms & Conditions checkbox if present
            # Some dialogs require accepting T&C before the Extend button is enabled.
            # Look for a checkbox near "terms and conditions" or "I have read" text.
            tandc_checked = False
            tandc_checkbox = page.query_selector(
                "input[type='checkbox']:near(:text('terms and conditions'))"
            )
            if not tandc_checkbox:
                tandc_checkbox = page.query_selector(
                    "input[type='checkbox']:near(:text('I have read'))"
                )
            if not tandc_checkbox:
                # Broader: look for any unchecked checkbox in the dialog area
                # (skip the table row checkboxes by looking for visible ones in dialog context)
                dialog_checkboxes = page.query_selector_all(
                    "input[type='checkbox']"
                )
                for cb in dialog_checkboxes:
                    if cb.is_visible() and not cb.is_checked():
                        # Verify it's in the dialog (near Cancel button or T&C text)
                        parent_text = cb.evaluate(
                            "el => (el.closest('div.modal') || el.closest('[role=\"dialog\"]') "
                            "|| el.parentElement?.parentElement?.parentElement)?.innerText || ''"
                        )
                        if "terms" in parent_text.lower() or "condition" in parent_text.lower():
                            tandc_checkbox = cb
                            break

            if tandc_checkbox and not tandc_checkbox.is_checked():
                try:
                    tandc_checkbox.check(force=True, timeout=3000)
                except Exception:
                    # Some Syncfusion checkboxes need a click on the label/wrapper instead
                    wrapper = tandc_checkbox.evaluate_handle(
                        "el => el.closest('label') || el.parentElement"
                    )
                    if wrapper:
                        wrapper.click()
                page.wait_for_timeout(500)
                if tandc_checkbox.is_checked():
                    tandc_checked = True
                    log(f"  ✓ Accepted Terms & Conditions for: {name}")
                else:
                    log(f"  [!] Could not check T&C checkbox for: {name}")
            elif tandc_checkbox and tandc_checkbox.is_checked():
                tandc_checked = True
                log(f"  ✓ T&C already accepted for: {name}")

            # Step 4: Click the dialog's "Extend" button (blue button inside the dialog)
            # The dialog has an "Extend" button that's different from the page-level one
            # Target: a visible Extend button that appeared after the dialog opened
            dialog_extend = None
            extend_buttons = page.query_selector_all("button:has-text('Extend')")
            for btn in extend_buttons:
                # The dialog's Extend button should be near the Cancel button
                if btn.is_visible():
                    # Check if there's a Cancel button nearby (dialog context)
                    parent = btn.evaluate_handle("el => el.parentElement")
                    cancel_nearby = parent.query_selector("button:has-text('Cancel')")
                    if cancel_nearby:
                        dialog_extend = btn
                        break

            if not dialog_extend:
                # Fallback: find the last visible Extend button (dialog one appears after page one)
                for btn in reversed(extend_buttons):
                    if btn.is_visible():
                        dialog_extend = btn
                        break

            if dialog_extend:
                # Wait for the button to become enabled (T&C checkbox may need a moment)
                is_enabled = False
                for attempt in range(10):
                    disabled = dialog_extend.get_attribute("disabled")
                    if disabled is None:
                        is_enabled = True
                        break
                    page.wait_for_timeout(500)

                if not is_enabled:
                    log(f"  [!] Extend button still disabled for: {name} — missing required fields?")
                    page.screenshot(path=str(SCREENSHOT_DIR / f"debug_btn_disabled_{name}.png"))
                    # Try clicking Cancel to close the dialog cleanly
                    cancel_btn = page.query_selector("button:has-text('Cancel')")
                    if cancel_btn and cancel_btn.is_visible():
                        cancel_btn.click()
                        page.wait_for_timeout(1000)
                else:
                    log(f"  Clicking dialog 'Extend' for: {name}")
                    dialog_extend.click()
                    page.wait_for_timeout(3000)

                    # Wait for dialog to close (Cancel button disappears)
                    try:
                        page.wait_for_selector("text=Business Justification", state="hidden", timeout=10000)
                    except Exception:
                        pass

                    extended += 1
                    log(f"  [OK] Extended: {name} / {role}")
            else:
                log(f"  [!] Could not find dialog Extend button for: {name}")
                page.screenshot(path=str(SCREENSHOT_DIR / f"debug_no_dialog_btn_{name}.png"))

        except Exception as e:
            log(f"  [!] Error extending {name}: {e}")
            page.screenshot(path=str(SCREENSHOT_DIR / f"debug_error_{name}.png"))

        page.wait_for_timeout(1000)

    page.screenshot(path=str(SCREENSHOT_DIR / "after_all_extensions.png"), full_page=True)
    log(f"  [OK] Extension completed: {extended}/{len(to_extend)} membership(s)")
    return extended


def should_autofix(run_result: dict) -> bool:
    """Determine if this failure is a code bug that autofix can help with.

    Environmental issues (VPN, SSO) are not code bugs and won't be auto-fixed.
    """
    status = run_result.get("status", "")
    if status == "success":
        return False

    error_msg = run_result.get("error", "").lower()
    for pattern in SKIP_AUTOFIX_PATTERNS:
        if pattern in error_msg:
            return False

    # "partial" (some extensions failed) or "error" (code-level exception)
    return status in ("partial", "error")


def build_autofix_prompt(run_result: dict) -> str:
    """Build a Copilot CLI prompt with full context about the failure."""
    status = run_result.get("status", "unknown")
    error = run_result.get("error", "")
    failed_count = run_result.get("failed", 0)
    details = run_result.get("details", [])

    # Read last N lines of log for context
    log_tail = ""
    if LOG_FILE.exists():
        lines = LOG_FILE.read_text(encoding="utf-8").splitlines()
        log_tail = "\n".join(lines[-40:])

    # Find relevant screenshots from this run
    screenshots = []
    if SCREENSHOT_DIR.exists():
        for f in sorted(SCREENSHOT_DIR.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
            if f.suffix == ".png":
                screenshots.append(str(f))
            if len(screenshots) >= 5:
                break

    screenshots_section = "\n".join(f"  - {s}" for s in screenshots) if screenshots else "  (none)"

    failed_details = ""
    if details:
        failed_details = "\nFailed memberships:\n" + "\n".join(
            f"  - {d['name']} / {d.get('role', '?')} ({d.get('days_left', '?')}d left)"
            for d in details
        )

    prompt = f"""In my coreIdentity desktop app (in My applications), the last automated run had a failure that needs fixing.

## Failure Summary
- Status: {status}
- Error: {error or '(no error message — partial failure)'}
- Failed extensions: {failed_count}
{failed_details}

## Recent Log Output
```
{log_tail}
```

## Relevant Screenshots (inspect these for UI state clues)
{screenshots_section}

## What to Do
1. Read the script at {SCRIPT_DIR / 'renew_entitlements.py'} and understand the failure
2. Look at the screenshots to understand the UI state at time of failure
3. Fix the script so it handles this case going forward
4. Run the script with `python renew_entitlements.py --threshold 30` to verify the fix works
5. If the fix succeeds, you're done. If it fails again with a different error, iterate.

IMPORTANT: The script uses Playwright with Edge and a persistent browser profile for SSO.
The CoreIdentity SPA at {ENTITLEMENTS_URL} may have dialogs with varying layouts:
- Some have radio buttons for justification selection
- Some have a Terms & Conditions checkbox
- Some have both or neither
- The Extend button may be disabled until all required fields are filled
"""
    return prompt


def launch_autofix(run_result: dict):
    """Launch a Copilot CLI session in autopilot mode to diagnose and fix the failure."""
    prompt = build_autofix_prompt(run_result)
    log("[AUTOFIX] Launching Copilot CLI to diagnose and fix the failure...")

    cmd = [
        "copilot",
        "-p", prompt,
        "--autopilot",
        "--yolo",
        "--no-ask-user",
        "--experimental",
        "--add-dir", str(SCRIPT_DIR),
    ]

    log(f"[AUTOFIX] Command: copilot -p <prompt> --autopilot --yolo --no-ask-user --experimental --add-dir {SCRIPT_DIR}")

    try:
        result = subprocess.run(
            cmd,
            cwd=str(SCRIPT_DIR),
            timeout=600,  # 10 minute cap
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )

        # Log the outcome
        if result.returncode == 0:
            log("[AUTOFIX] Copilot CLI session completed successfully.")
        else:
            log(f"[AUTOFIX] Copilot CLI exited with code {result.returncode}.")

        if result.stdout:
            # Save full output for review
            autofix_log = SCRIPT_DIR / "last-autofix-output.log"
            autofix_log.write_text(result.stdout, encoding="utf-8")
            log(f"[AUTOFIX] Full output saved to {autofix_log}")

            # Log a summary (last 10 lines)
            summary_lines = result.stdout.strip().splitlines()[-10:]
            log("[AUTOFIX] Output summary:\n" + "\n".join(f"  | {l}" for l in summary_lines))

        if result.stderr:
            stderr_lines = result.stderr.strip().splitlines()[-5:]
            log("[AUTOFIX] Stderr:\n" + "\n".join(f"  | {l}" for l in stderr_lines))

        # Record the autofix attempt in run history
        save_run_result({
            "timestamp": datetime.now().isoformat(),
            "automation": "CoreIdentity-AutoExtend-Autofix",
            "status": "success" if result.returncode == 0 else "error",
            "exit_code": result.returncode,
            "triggered_by": run_result.get("status", "unknown"),
        })

    except subprocess.TimeoutExpired:
        log("[AUTOFIX] Copilot CLI session timed out after 10 minutes.")
        save_run_result({
            "timestamp": datetime.now().isoformat(),
            "automation": "CoreIdentity-AutoExtend-Autofix",
            "status": "error",
            "error": "Copilot CLI timed out after 600s",
        })
    except FileNotFoundError:
        log("[AUTOFIX] 'copilot' CLI not found on PATH. Install with: winget install GitHub.Copilot")
    except Exception as e:
        log(f"[AUTOFIX] Unexpected error: {e}")


def main():
    parser = argparse.ArgumentParser(description="CoreIdentity Entitlement Auto-Extender")
    parser.add_argument("--discover", action="store_true", help="Debug: capture page structure")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be extended")
    parser.add_argument("--threshold", type=int, default=DEFAULT_THRESHOLD_DAYS,
                        help=f"Extend if expiring within N days (default: {DEFAULT_THRESHOLD_DAYS})")
    parser.add_argument("--headless", action="store_true", help="Headless mode (after first login)")
    parser.add_argument("--scheduled", action="store_true", help="Non-interactive scheduled mode")
    parser.add_argument("--no-autofix", action="store_true",
                        help="Disable autofix (don't launch Copilot CLI on failure)")
    args = parser.parse_args()

    log(f"Starting CoreIdentity check (threshold: {args.threshold} days)")

    # Step 0: Ensure VPN is connected before launching the browser
    if not connect_vpn():
        log("[!] Could not connect to VPN — aborting.")
        save_run_result({
            "timestamp": datetime.now().isoformat(),
            "automation": "CoreIdentity-AutoExtend",
            "status": "error",
            "error": f"Failed to connect to VPN ({VPN_CONNECTION_NAME})",
        })
        return

    run_result = None  # Track the result for autofix decision

    with sync_playwright() as pw:
        headless = args.headless and PROFILE_DIR.exists()
        context = launch_browser(pw, headless=headless)

        try:
            page = context.pages[0] if context.pages else context.new_page()

            print(f"  Navigating to {ENTITLEMENTS_URL} ...")
            page.goto(ENTITLEMENTS_URL, wait_until="domcontentloaded", timeout=60000)

            load_result = wait_for_page_load(page)

            # If the page says VPN is needed, try reconnecting and retry once
            if load_result == "vpn":
                log("[VPN] Page reports VPN not connected — attempting reconnect...")
                if connect_vpn():
                    log("[VPN] Reconnected — reloading page...")
                    page.goto(ENTITLEMENTS_URL, wait_until="domcontentloaded", timeout=60000)
                    load_result = wait_for_page_load(page)

            if load_result != True:
                SCREENSHOT_DIR.mkdir(exist_ok=True)
                page.screenshot(path=str(SCREENSHOT_DIR / "debug_timeout.png"), full_page=True)
                error_msg = "VPN not connected (Remote Access Required)" if load_result == "vpn" else "Page failed to load (VPN? SSO expired?)"
                log(f"[!] {error_msg}")
                run_result = {
                    "timestamp": datetime.now().isoformat(),
                    "automation": "CoreIdentity-AutoExtend",
                    "status": "error",
                    "error": error_msg,
                }
                save_run_result(run_result)
                return

            # Click 'My Memberships' tab to load the table
            if not click_my_memberships_tab(page):
                return

            # Parse the memberships table
            memberships = parse_memberships(page)

            if args.discover:
                print("\n  === All Memberships ===")
                show_memberships(memberships)
                SCREENSHOT_DIR.mkdir(exist_ok=True)
                page.screenshot(path=str(SCREENSHOT_DIR / "discover_memberships.png"), full_page=True)
                if not args.scheduled:
                    input("\n  Press Enter to close browser...")
                return

            if not memberships:
                log("No memberships found. Run with --discover to debug.")
                SCREENSHOT_DIR.mkdir(exist_ok=True)
                page.screenshot(path=str(SCREENSHOT_DIR / "debug_no_memberships.png"), full_page=True)
                return

            show_memberships(memberships)

            # Filter to expiring within threshold
            expiring = [m for m in memberships if m["days_left"] <= args.threshold]

            # Skip memberships that already have a pending renewal request
            already_pending = [m for m in expiring if m.get("renewal_request_id")]
            to_extend = [m for m in expiring if not m.get("renewal_request_id")]

            if already_pending:
                names = ", ".join(f"{m['name']}/{m['role']}" for m in already_pending)
                log(f"Skipping {len(already_pending)} with pending requests: {names}")

            if not to_extend:
                msg = f"No memberships need extending (threshold {args.threshold}d)"
                if already_pending:
                    msg += f" — {len(already_pending)} already have pending requests"
                log(msg)
                save_run_result({
                    "timestamp": datetime.now().isoformat(),
                    "automation": "CoreIdentity-AutoExtend",
                    "status": "success",
                    "total_memberships": len(memberships),
                    "expiring": len(expiring),
                    "already_pending": len(already_pending),
                    "extended": 0,
                    "failed": 0,
                    "details": [],
                })
                return

            print(f"  ⚠ {len(to_extend)} membership(s) need extending:")
            show_memberships(to_extend, threshold=args.threshold)

            # Extend
            extended = extend_memberships(page, to_extend, dry_run=args.dry_run)
            action = "Would extend" if args.dry_run else "Extended"
            log(f"Done. {action} {extended}/{len(to_extend)} membership(s).")

            # Save structured result for dashboard
            run_result = {
                "timestamp": datetime.now().isoformat(),
                "automation": "CoreIdentity-AutoExtend",
                "status": "success" if extended == len(to_extend) else "partial",
                "dry_run": args.dry_run,
                "total_memberships": len(memberships),
                "expiring": len(expiring),
                "already_pending": len(already_pending),
                "extended": extended,
                "failed": len(to_extend) - extended,
                "details": [
                    {"name": m["name"], "role": m["role"], "days_left": m["days_left"]}
                    for m in to_extend
                ],
            }
            save_run_result(run_result)

            if not args.scheduled and not args.headless:
                page.wait_for_timeout(5000)

        except Exception as e:
            log(f"[!] Unhandled error: {e}")
            run_result = {
                "timestamp": datetime.now().isoformat(),
                "automation": "CoreIdentity-AutoExtend",
                "status": "error",
                "error": str(e),
            }
            save_run_result(run_result)
        finally:
            context.close()

    # After the browser is closed, check if autofix should be triggered
    if run_result and not args.no_autofix and not args.dry_run and should_autofix(run_result):
        log("[AUTOFIX] Failure detected — triggering Copilot CLI autofix session...")
        launch_autofix(run_result)


if __name__ == "__main__":
    main()
