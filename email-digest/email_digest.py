"""
Email Digest Dashboard Server
Reads unread Outlook emails, categorizes by importance, and serves
a dashboard with click-to-open-in-Outlook capability.
"""
import http.server
import json
import os
import sys
import subprocess
import socket
import webbrowser
import threading
import time
from datetime import datetime, timezone

DIR = os.path.dirname(os.path.abspath(__file__))
PORT = 8094
CACHE_FILE = os.path.join(DIR, "email_cache.json")

_data_lock = threading.Lock()
_BENIGN = (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, OSError)

# ── Outlook COM via PowerShell ─────────────────────────────────────

FETCH_EMAILS_PS = r'''
$outlook = New-Object -ComObject Outlook.Application
$ns = $outlook.GetNamespace("MAPI")
$inbox = $ns.GetDefaultFolder(6)

$cutoff = (Get-Date).AddDays(-$DAYS).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
$filter = "@SQL=""urn:schemas:httpmail:read"" = 0 AND ""urn:schemas:httpmail:datereceived"" >= '$cutoff'"
$items = $inbox.Items.Restrict($filter)
$items.Sort("[ReceivedTime]", $true)

$count = $items.Count
$results = @()
$limit = [Math]::Min($count, $MAX_EMAILS)

for ($i = 1; $i -le $limit; $i++) {
    try {
        $mail = $items.Item($i)
        $body = $mail.Body
        if ($body.Length -gt 600) { $body = $body.Substring(0, 600) }
        $body = $body -replace "`r`n", " " -replace "`n", " " -replace "`t", " "
        $results += @{
            entry_id = $mail.EntryID
            subject = $mail.Subject
            sender = $mail.SenderName
            received = $mail.ReceivedTime.ToString("yyyy-MM-dd HH:mm")
            body_preview = $body
        }
    } catch {}
}

$results | ConvertTo-Json -Depth 3 -Compress
'''

OPEN_EMAIL_PS = r'''
$outlook = New-Object -ComObject Outlook.Application
$ns = $outlook.GetNamespace("MAPI")
try {
    $mail = $ns.GetItemFromID("$ENTRY_ID")
    $mail.Display()
    Write-Output "OK"
} catch {
    Write-Output "ERROR: $_"
}
'''


def _run_powershell(script):
    """Run a PowerShell 5.1 script and return stdout."""
    result = subprocess.run(
        ["powershell.exe", "-NoProfile", "-Command", script],
        capture_output=True, text=True, timeout=300,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
    )
    return result.stdout.strip()


def fetch_emails(days=30, max_emails=500):
    """Fetch unread emails from Outlook via PowerShell COM."""
    script = FETCH_EMAILS_PS.replace("$DAYS", str(days)).replace("$MAX_EMAILS", str(max_emails))
    raw = _run_powershell(script)
    if not raw:
        return []
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            data = [data]
        return data
    except json.JSONDecodeError:
        return []


def open_email_in_outlook(entry_id):
    """Open a specific email in Outlook by EntryID."""
    script = OPEN_EMAIL_PS.replace("$ENTRY_ID", entry_id)
    result = _run_powershell(script)
    return result.startswith("OK")


# ── Email Categorization ──────────────────────────────────────────

NOISE_SENDERS = [
    "Microsoft Azure", "SharePoint Online", "CloudBuild Build Notifications",
    "Domains Manager Mail Service Account", "caresupport@hello.springhealth.com",
    "Microsoft Defender for Cloud", "Headspace", "Microsoft Security Learning",
    "Microsoft Viva Glint", "Microsoft Loop", "MSW Weekly Buzz",
    "Employee News & Events", "Global Stock Services", "Israel Give team",
    "Cibus Pluxee", "S.E.E You", "HILANET", "Viva Engage",
    "Microsoft Invitations", "Inventory Hygiene Bot", "Service Tree Auto Responder",
    "Microsoft Viva", "Power Automate", "Microsoft 365 Message center",
    "Outlook", "Reaction Daily Digest",
]

LEADERSHIP = [
    "Rob Lefferts", "Charlie Bell", "Satya Nadella",
    "Michal Braverman", "Vlad Korsunsky", "Peter Olson", "Hayete Gallot",
]

TEAM_MEMBERS = [
    "Ola Lavi", "Erel Hansav", "Eli Koreh", "Eran Gonen",
    "Gal Libedinsky", "May Dekel", "Bar Brownshtein", "Maya Bar-Rabi",
    "Ameer Abu Zhaia", "Rayan Daher", "Arik Noyman", "Inbar Rotem",
    "Yoav Barak", "Eyal Geva", "Sasha Budnik", "Stefan Buruiana",
    "Sulaiman Abu Rashed", "Shahar Bahat", "Bar Eitan", "Erez Einav",
    "Timna Seltzer", "Yaakov Iyun", "Shani Ben Simon",
    "Saicharan Mantripragada", "Nir Sela", "Sivan Manor", "Yarin Levy",
    "Hen Sinai", "Rotem Aharoni",
]

ACTION_KEYWORDS = [
    "action required", "action item", "action needed",
    "urgent", "deadline", "mandatory", "asap",
    "approve", "approval required", "review required",
]

ADO_SENDERS = ["Azure DevOps Notifications"]
ICM_SENDERS = ["IcM Incident Management"]


def _matches_any(sender, patterns):
    sender_lower = sender.lower()
    return any(p.lower() in sender_lower for p in patterns)


def _is_action_email(subject):
    subj_lower = subject.lower()
    return any(kw in subj_lower for kw in ACTION_KEYWORDS)


def categorize_emails(emails):
    """Categorize emails into buckets and return structured summary."""
    categories = {
        "action_required": [],
        "leadership": [],
        "manager": [],
        "team": [],
        "ado_builds": [],
        "icm_incidents": [],
        "other_people": [],
        "automated": [],
    }

    for email in emails:
        sender = email.get("sender", "")
        subject = email.get("subject", "")

        # Filter noise
        if _matches_any(sender, NOISE_SENDERS):
            categories["automated"].append(email)
        elif _matches_any(sender, ADO_SENDERS):
            # Only keep build failures and PR reviews
            subj_lower = subject.lower()
            if "failed" in subj_lower or "reviewer" in subj_lower or "expired" in subj_lower:
                categories["ado_builds"].append(email)
            else:
                categories["automated"].append(email)
        elif _matches_any(sender, ICM_SENDERS):
            categories["icm_incidents"].append(email)
        elif _matches_any(sender, LEADERSHIP):
            categories["leadership"].append(email)
        elif "Nir Sela" in sender:
            categories["manager"].append(email)
        elif _matches_any(sender, TEAM_MEMBERS):
            if _is_action_email(subject):
                categories["action_required"].append(email)
            else:
                categories["team"].append(email)
        elif _is_action_email(subject):
            categories["action_required"].append(email)
        elif "MSecFE" in sender or "quarantined" in subject.lower():
            categories["action_required"].append(email)
        elif "Office365" in sender and "blocked" in subject.lower():
            categories["action_required"].append(email)
        else:
            categories["other_people"].append(email)

    return categories


def build_summary(emails, categories):
    """Build a summary dict for the dashboard."""
    return {
        "total_unread": len(emails),
        "scanned_at": datetime.now(timezone.utc).isoformat(),
        "counts": {k: len(v) for k, v in categories.items()},
        "categories": categories,
    }


def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def save_cache(data):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)


# ── HTTP Server ───────────────────────────────────────────────────

class QuietServer(http.server.ThreadingHTTPServer):
    def handle_error(self, request, client_address):
        exc_type = sys.exc_info()[0]
        if exc_type and issubclass(exc_type, _BENIGN):
            return
        super().handle_error(request, client_address)


class DigestHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def handle(self):
        try:
            super().handle()
        except _BENIGN:
            pass

    def do_GET(self):
        path = self.path.split("?")[0]
        if path in ("/", "/dashboard.html"):
            self._serve_file("dashboard.html", "text/html")
        elif path == "/api/summary":
            self._get_summary()
        elif path == "/api/health":
            self._json_response({"status": "ok"})
        else:
            super().do_GET()

    def do_POST(self):
        path = self.path.split("?")[0]
        if path == "/api/refresh":
            body = {}
            cl = int(self.headers.get("Content-Length", 0))
            if cl > 0:
                body = json.loads(self.rfile.read(cl))
            days = body.get("days", 30)
            max_emails = body.get("max_emails", 500)
            self._refresh(days, max_emails)
        elif path == "/api/open-email":
            cl = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(cl))
            entry_id = body.get("entry_id", "")
            if not entry_id:
                self._json_response({"ok": False, "error": "No entry_id"}, 400)
                return
            ok = open_email_in_outlook(entry_id)
            self._json_response({"ok": ok})
        else:
            self.send_error(404)

    def _get_summary(self):
        with _data_lock:
            cached = load_cache()
        if cached:
            self._json_response(cached)
        else:
            self._json_response({"error": "No data yet. Click Refresh to scan emails."}, 200)

    def _refresh(self, days=30, max_emails=500):
        print(f"[Refresh] Scanning last {days} days (max {max_emails} emails)...")
        try:
            emails = fetch_emails(days=days, max_emails=max_emails)
            categories = categorize_emails(emails)
            summary = build_summary(emails, categories)
            with _data_lock:
                save_cache(summary)
            print(f"[Refresh] Done. {len(emails)} emails categorized.")
            self._json_response(summary)
        except Exception as e:
            print(f"[Refresh] Error: {e}")
            self._json_response({"error": str(e)}, 500)

    def _serve_file(self, filename, content_type):
        filepath = os.path.join(DIR, filename)
        if not os.path.exists(filepath):
            self.send_error(404)
            return
        with open(filepath, "rb") as f:
            content = f.read()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", len(content))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(content)

    def _json_response(self, data, status=200):
        body = json.dumps(data, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)


def is_port_open(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(2)
        return s.connect_ex(("127.0.0.1", port)) == 0


def main():
    no_browser = "--no-browser" in sys.argv

    if is_port_open(PORT):
        print(f"Email Digest already running at http://localhost:{PORT}")
        if not no_browser:
            webbrowser.open(f"http://localhost:{PORT}")
        return

    os.chdir(DIR)
    server = QuietServer(("127.0.0.1", PORT), DigestHandler)

    url = f"http://localhost:{PORT}"
    print(f"Email Digest Dashboard running at: {url}")

    if not no_browser:
        webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down Email Digest Dashboard.")
        server.shutdown()


if __name__ == "__main__":
    main()
