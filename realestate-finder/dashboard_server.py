"""
Real Estate Finder — Dashboard Server
Searches Israeli real estate listings and compares against real transaction data.
Generates Copilot CLI prompts for deep research on demand.
"""
import http.server
import json
import os
import subprocess
import sys
import socket
import urllib.parse
import urllib.request
import re
import threading
import time
from pathlib import Path
from datetime import datetime

PORT = 8098
BASE_DIR = Path(__file__).parent
DATA_FILE = BASE_DIR / "search_results.json"
PROMPT_FILE = BASE_DIR / ".last-prompt.txt"

# Default search configuration
# Yad2 location IDs sourced from https://gw.yad2.co.il/address-autocomplete/realestate/v2
# - city_id: numeric city code Yad2 uses to filter listings (Hebrew text in the URL does NOT filter)
# - area_id / top_area_id: regional grouping; included for accuracy on the filter UI
DEFAULT_CONFIG = {
    "areas": [
        {"name": "זכרון יעקב", "name_en": "Zichron Yaakov", "enabled": True,
         "yad2_city_id": "9300", "yad2_area_id": "67", "yad2_top_area_id": "101"},
        {"name": "פרדס חנה כרכור", "name_en": "Pardes Hanna-Karkur", "enabled": True,
         "yad2_city_id": "7800", "yad2_area_id": "15", "yad2_top_area_id": "101"},
        {"name": "בנימינה גבעת עדה", "name_en": "Binyamina", "enabled": True,
         "yad2_city_id": "9800", "yad2_area_id": "15", "yad2_top_area_id": "101"},
        {"name": "אור עקיבא", "name_en": "Or Akiva", "enabled": True,
         "yad2_city_id": "1020", "yad2_area_id": "16", "yad2_top_area_id": "101"},
        {"name": "עתלית", "name_en": "Atlit", "enabled": True,
         "yad2_city_id": "0053", "yad2_area_id": "67", "yad2_top_area_id": "101"},
        {"name": "חדרה", "name_en": "Hadera", "enabled": False,
         "yad2_city_id": "6500", "yad2_area_id": "15", "yad2_top_area_id": "101"},
        {"name": "חיפה", "name_en": "Haifa", "enabled": False,
         "yad2_city_id": "4000", "yad2_area_id": "5", "yad2_top_area_id": "25"},
    ],
    "budget_min": 2000000,
    "budget_max": 3200000,
    "rooms_min": 3,
    "rooms_max": 6,
    "property_types": ["apartment", "garden_apartment", "duplex", "cottage"],
    "sort_by": "price_vs_market",  # price_vs_market, price_asc, price_desc, price_per_sqm, rooms
}


def _merge_yad2_ids(config):
    """Backfill Yad2 location IDs onto saved configs that predate them."""
    by_name = {a["name"]: a for a in DEFAULT_CONFIG["areas"]}
    by_name_en = {a["name_en"]: a for a in DEFAULT_CONFIG["areas"]}
    for area in config.get("areas", []):
        defaults = by_name.get(area.get("name")) or by_name_en.get(area.get("name_en"))
        if not defaults:
            continue
        for key in ("yad2_city_id", "yad2_area_id", "yad2_top_area_id"):
            if not area.get(key) and defaults.get(key):
                area[key] = defaults[key]
    return config


def load_config():
    cfg_file = BASE_DIR / "config.json"
    if cfg_file.exists():
        with open(cfg_file, encoding="utf-8") as f:
            return _merge_yad2_ids(json.load(f))
    return DEFAULT_CONFIG.copy()


def save_config(config):
    cfg_file = BASE_DIR / "config.json"
    with open(cfg_file, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


def load_results():
    if DATA_FILE.exists():
        with open(DATA_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {"last_refresh": None, "listings": [], "transactions": []}


def save_results(data):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def build_search_links(config):
    """Build direct search URLs for Madlan, Yad2, etc."""
    links = []
    areas = [a for a in config.get("areas", []) if a.get("enabled")]
    budget_min = config.get("budget_min", 2000000)
    budget_max = config.get("budget_max", 3200000)
    rooms_min = config.get("rooms_min", 3)
    rooms_max = config.get("rooms_max", 6)

    for area in areas:
        name_he = area["name"]
        name_en = area.get("name_en", name_he)
        name_url = urllib.parse.quote(name_he)

        # Madlan
        links.append({
            "area": name_en,
            "area_he": name_he,
            "source": "Madlan",
            "icon": "🏠",
            "url": f"https://www.madlan.co.il/for-sale/{name_url}?minPrice={budget_min}&maxPrice={budget_max}&minRooms={rooms_min}&maxRooms={rooms_max}",
            "description": f"Madlan listings in {name_en}",
        })

        # Madlan — below market value filter
        links.append({
            "area": name_en,
            "area_he": name_he,
            "source": "Madlan (Below Market)",
            "icon": "💰",
            "url": f"https://www.madlan.co.il/for-sale/{name_url}?minPrice={budget_min}&maxPrice={budget_max}&minRooms={rooms_min}&maxRooms={rooms_max}&dealType=below_market",
            "description": f"Below-market deals in {name_en}",
        })

        # Madlan — transaction history (area info)
        links.append({
            "area": name_en,
            "area_he": name_he,
            "source": "Madlan (Transactions)",
            "icon": "📊",
            "url": f"https://www.madlan.co.il/area-info/{name_url}",
            "description": f"Real transaction data for {name_en}",
        })

        # Yad2 — requires numeric city IDs (Hebrew text in `city=` does NOT filter)
        # Canonical filter params are minPrice/maxPrice/minRooms/maxRooms.
        yad2_city = area.get("yad2_city_id")
        yad2_area_id = area.get("yad2_area_id")
        yad2_top_area = area.get("yad2_top_area_id")
        if yad2_city:
            yad2_params = [
                f"topArea={yad2_top_area}" if yad2_top_area else None,
                f"area={yad2_area_id}" if yad2_area_id else None,
                f"city={yad2_city}",
                f"minPrice={budget_min}",
                f"maxPrice={budget_max}",
                f"minRooms={rooms_min}",
                f"maxRooms={rooms_max}",
                "propertyGroup=apartments,houses",
            ]
            yad2_url = "https://www.yad2.co.il/realestate/forsale?" + "&".join(p for p in yad2_params if p)
        else:
            # Fallback: text-based search via the global search box
            yad2_url = f"https://www.yad2.co.il/realestate/forsale?searchString={name_url}"
        links.append({
            "area": name_en,
            "area_he": name_he,
            "source": "Yad2",
            "icon": "🔍",
            "url": yad2_url,
            "description": f"Yad2 listings in {name_en}",
        })

        # Government real estate data
        links.append({
            "area": name_en,
            "area_he": name_he,
            "source": "Tax Authority",
            "icon": "🏛️",
            "url": "https://www.nadlan.gov.il/",
            "description": f"Official transaction records — search for {name_en}",
        })

        # Facebook group search
        links.append({
            "area": name_en,
            "area_he": name_he,
            "source": "Facebook Groups",
            "icon": "📱",
            "url": f"https://www.facebook.com/search/groups/?q={urllib.parse.quote(name_he + ' דירות למכירה')}",
            "description": f"Facebook groups for {name_en} properties",
        })

    return links


def generate_research_prompt(config):
    """Generate a Copilot CLI prompt for deep real estate research."""
    areas = [a for a in config.get("areas", []) if a.get("enabled")]
    area_names = ", ".join(a["name_en"] for a in areas)
    area_names_he = ", ".join(a["name"] for a in areas)
    budget_min = config.get("budget_min", 2000000)
    budget_max = config.get("budget_max", 3200000)
    rooms_min = config.get("rooms_min", 3)
    rooms_max = config.get("rooms_max", 6)

    prompt = f"""You are a real estate research assistant. Perform a comprehensive search for properties in Israel.

## Search Parameters
- **Areas:** {area_names} ({area_names_he})
- **Budget:** ₪{budget_min:,} – ₪{budget_max:,}
- **Rooms:** {rooms_min}–{rooms_max}
- **Goal:** Find properties priced BELOW market value

## Instructions

1. **Search Current Listings** on:
   - Madlan (madlan.co.il) — use the "below market price" filter
   - Yad2 (yad2.co.il) — largest listing volume
   - Homeless (homeless.co.il)
   - Komo (komo.co.il)

2. **Get Real Transaction Data** for each area:
   - Search for actual sold prices from the last 6 months
   - Calculate average ₪/m² per area and room count
   - Use Tax Authority data (nadlan.gov.il) and Madlan transaction history

3. **Compare Listings vs. Transactions:**
   For each promising listing, find comparable real transactions and calculate:
   - Price difference (% above/below market)
   - ₪/m² comparison
   - Verdict: GREAT DEAL / GOOD DEAL / MARKET PRICE / OVERPRICED

4. **Output Format:**
   Create a JSON file at `C:\\Users\\orenhorowitz\\desktop-automations\\realestate-finder\\search_results.json` with this structure:
   ```json
   {{
     "last_refresh": "2026-04-30T17:00:00",
     "listings": [
       {{
         "id": "unique-id",
         "area": "Pardes Hanna-Karkur",
         "area_he": "פרדס חנה כרכור",
         "address": "רח' תחייה",
         "rooms": 5,
         "size_sqm": 125,
         "price": 2390000,
         "price_per_sqm": 19120,
         "source": "Yad2",
         "source_url": "https://...",
         "property_type": "apartment",
         "floor": 2,
         "description": "5 rooms, renovated...",
         "avg_market_price": 3300000,
         "avg_market_sqm": 25200,
         "price_vs_market_pct": -27.6,
         "verdict": "GREAT DEAL",
         "comparable_transactions": [
           {{
             "date": "2026-03-09",
             "address": "חלומות כרכור",
             "rooms": 5,
             "size_sqm": 131,
             "price": 2550000,
             "price_per_sqm": 19465
           }}
         ]
       }}
     ],
     "transactions": [
       {{
         "area": "Pardes Hanna-Karkur",
         "date": "2026-03-25",
         "address": "לב המושבה",
         "rooms": 4,
         "size_sqm": 133,
         "price": 2450000,
         "price_per_sqm": 18421,
         "source": "Tax Authority"
       }}
     ]
   }}
   ```

5. **Priority:** Focus on listings that are 10%+ below comparable transactions. These are the true deals.

After saving the JSON, print a summary table of the top 10 deals found.
"""
    return prompt


def launch_copilot(prompt_text, autopilot=True, allow_all=True, same_window=True):
    """Launch Copilot CLI in a terminal with the generated prompt."""
    PROMPT_FILE.write_text(prompt_text, encoding="utf-8")

    copilot_args = []
    if autopilot:
        copilot_args.append("--autopilot")
    if allow_all:
        copilot_args.append("--allow-all")

    args_str = " ".join(copilot_args)
    escaped_path = str(PROMPT_FILE).replace("'", "''")

    script_file = BASE_DIR / ".launch-copilot.ps1"
    script_content = (
        f"$prompt = Get-Content '{escaped_path}' -Raw\n"
        f'copilot -i "$prompt" {args_str}\n'
    )
    script_file.write_text(script_content, encoding="utf-8")

    errors = []

    # Strategy 1: Windows Terminal + pwsh
    try:
        wt_cmd = ["wt"]
        if same_window:
            wt_cmd += ["-w", "0"]
        wt_cmd += ["new-tab", "--title", "Real Estate Research",
                    "-d", str(BASE_DIR), "--",
                    "pwsh", "-NoExit", "-ExecutionPolicy", "Bypass",
                    "-File", str(script_file)]
        subprocess.Popen(wt_cmd, cwd=str(BASE_DIR))
        return True, None
    except Exception as e:
        errors.append(f"wt+pwsh: {e}")

    # Strategy 2: pwsh standalone
    try:
        subprocess.Popen(
            ["pwsh", "-NoExit", "-ExecutionPolicy", "Bypass",
             "-File", str(script_file)],
            cwd=str(BASE_DIR),
            creationflags=0x00000010,
        )
        return True, None
    except Exception as e:
        errors.append(f"pwsh standalone: {e}")

    # Strategy 3: cmd fallback
    try:
        subprocess.Popen(
            ["cmd", "/c", "start", "pwsh", "-NoExit",
             "-ExecutionPolicy", "Bypass", "-File", str(script_file)],
            cwd=str(BASE_DIR),
        )
        return True, None
    except Exception as e:
        errors.append(f"cmd fallback: {e}")

    return False, "; ".join(errors)


class RealEstateHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(BASE_DIR), **kwargs)

    def handle(self):
        try:
            super().handle()
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            pass

    def log_message(self, *args):
        pass

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        if self.path == "/api/config":
            self._json_response(load_config())
        elif self.path == "/api/results":
            self._json_response(load_results())
        elif self.path == "/api/links":
            config = load_config()
            self._json_response(build_search_links(config))
        elif self.path == "/api/health":
            self._json_response({"ok": True, "timestamp": datetime.now().isoformat()})
        elif self.path == "/api/last-prompt":
            if PROMPT_FILE.exists():
                text = PROMPT_FILE.read_text(encoding="utf-8")
                self._json_response({"prompt": text})
            else:
                self._json_response({"prompt": ""})
        else:
            super().do_GET()

    def do_POST(self):
        if self.path == "/api/config":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len))
            save_config(body)
            self._json_response({"ok": True})

        elif self.path == "/api/research":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len > 0 else {}

            config = load_config()
            prompt = generate_research_prompt(config)

            autopilot = body.get("autopilot", True)
            allow_all = body.get("allow_all", True)
            same_window = body.get("same_window", True)

            ok, err = launch_copilot(prompt, autopilot=autopilot,
                                      allow_all=allow_all,
                                      same_window=same_window)
            if ok:
                self._json_response({
                    "ok": True,
                    "message": "Copilot CLI research session launched!",
                    "prompt_preview": prompt[:500],
                })
            else:
                self._json_response({"ok": False, "error": f"Failed: {err}"})

        elif self.path == "/api/preview-prompt":
            config = load_config()
            prompt = generate_research_prompt(config)
            self._json_response({"prompt": prompt})

        elif self.path == "/api/results":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len))
            save_results(body)
            self._json_response({"ok": True})

        else:
            self.send_error(404)

    def _json_response(self, data):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    import webbrowser

    def is_port_in_use(port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(("127.0.0.1", port)) == 0

    url = f"http://localhost:{PORT}/dashboard.html"

    if is_port_in_use(PORT):
        print(f"Server already running at {url}")
        webbrowser.open(url)
        sys.exit(0)

    server = http.server.ThreadingHTTPServer(("127.0.0.1", PORT), RealEstateHandler)
    print(f"Real Estate Finder: {url}")
    print("Press Ctrl+C to stop.\n")

    if "--no-browser" not in sys.argv:
        webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
