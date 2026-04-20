"""
Course Workflow Pipeline — Dashboard Server
Serves the UI and launches Copilot CLI sessions with generated prompts.
"""
import http.server
import json
import os
import subprocess
import sys
import urllib.parse
from pathlib import Path

PORT = 8092
BASE_DIR = Path(__file__).parent
COURSES_DIR = BASE_DIR / "courses"
PROMPTS_DIR = BASE_DIR / "prompts"


def get_courses():
    """Read all course.json files and return course metadata."""
    courses = []
    if not COURSES_DIR.exists():
        return courses
    for folder in sorted(COURSES_DIR.iterdir()):
        cj = folder / "course.json"
        if cj.exists():
            with open(cj, encoding="utf-8") as f:
                data = json.load(f)
            # Normalize field names (psychopathology uses course_name vs name)
            name = data.get("name") or data.get("course_name", "")
            name_en = data.get("name_en") or data.get("course_name_en", "")
            units = data.get("units", {})
            existing_units = []
            for u_num in sorted(units.keys(), key=lambda x: int(x)):
                unit_dir = folder / "units" / f"unit_{int(u_num):02d}"
                has_output = (unit_dir / "output" / "summary.md").exists()
                existing_units.append({
                    "number": int(u_num),
                    "has_output": has_output,
                })
            courses.append({
                "folder": folder.name,
                "name": name,
                "name_en": name_en,
                "onenote_url": data.get("onenote_url", ""),
                "moodle_base": data.get("moodle_base") or data.get("moodle_base_url", "https://online.dyellin.ac.il"),
                "edge_profile": data.get("edge_profile", "Profile 5"),
                "units": existing_units,
                "next_unit": max((int(k) for k in units.keys()), default=0) + 1,
            })
    return courses


def generate_prompt(data):
    """Generate a Copilot CLI prompt from the form data."""
    is_new_course = data.get("is_new_course", False)

    if is_new_course:
        template = (PROMPTS_DIR / "new_course.md").read_text(encoding="utf-8")
        # Fill in placeholders
        prompt = template
        prompt = prompt.replace("<EDIT: שם הקורס בעברית>", data.get("name_he", ""))
        prompt = prompt.replace("<EDIT: Course Name In English>", data.get("name_en", ""))
        prompt = prompt.replace(
            "<EDIT: Right-click section in OneNote Desktop → \"Copy Link to Section\" and paste here>",
            data.get("onenote_url", "")
        )

        # Handle lecture URLs (list)
        lecture_urls = [u for u in data.get("lecture_urls", []) if u.strip()]
        if lecture_urls:
            lines = "\n".join(
                f"- Lecture page {i}: {u}" if len(lecture_urls) > 1
                else f"- Lecture page: {u}"
                for i, u in enumerate(lecture_urls, 1)
            )
            prompt = prompt.replace("- Lecture page: <LECTURE_URLS_PLACEHOLDER>", lines)
        else:
            prompt = prompt.replace("<LECTURE_URLS_PLACEHOLDER>", "null")

        # Handle PDFs
        pdf_urls = [u for u in data.get("pdf_urls", []) if u.strip()]
        if pdf_urls:
            prompt = prompt.replace(
                "<EDIT: paste direct PDF/PPTX URL, OR if slides are already downloaded, write \"pre-downloaded\" and place the files in the sources/ folder>",
                pdf_urls[0]
            )
            if len(pdf_urls) > 1:
                prompt = prompt.replace(
                    "<EDIT: paste direct PDF/PPTX URL, or delete this line if only 1 file>",
                    pdf_urls[1]
                )
            else:
                # Remove the PDF 2 line
                lines = prompt.split("\n")
                lines = [l for l in lines if "<EDIT: paste direct PDF/PPTX URL, or delete this line if only 1 file>" not in l]
                prompt = "\n".join(lines)
        else:
            prompt = prompt.replace(
                "<EDIT: paste direct PDF/PPTX URL, OR if slides are already downloaded, write \"pre-downloaded\" and place the files in the sources/ folder>",
                "pre-downloaded"
            )
            lines = prompt.split("\n")
            lines = [l for l in lines if "<EDIT: paste direct PDF/PPTX URL, or delete this line if only 1 file>" not in l]
            prompt = "\n".join(lines)

        # Handle articles
        article_urls = [u for u in data.get("article_urls", []) if u.strip()]
        if article_urls:
            prompt = prompt.replace(
                "<EDIT: paste public article URL, or delete this line if none>",
                "\n- Article: ".join(article_urls)
            )
        else:
            lines = prompt.split("\n")
            lines = [l for l in lines if "<EDIT: paste public article URL, or delete this line if none>" not in l]
            prompt = "\n".join(lines)

        # Handle videos
        video_urls = [u for u in data.get("video_urls", []) if u.strip()]
        if video_urls:
            prompt = prompt.replace(
                "<EDIT: paste YouTube or other supplementary video URL, or delete this line if none>",
                "\n- Video: ".join(video_urls)
            )
        else:
            lines = prompt.split("\n")
            lines = [l for l in lines if "<EDIT: paste YouTube or other supplementary video URL, or delete this line if none>" not in l]
            prompt = "\n".join(lines)

    else:
        template = (PROMPTS_DIR / "new_unit.md").read_text(encoding="utf-8")
        prompt = template
        prompt = prompt.replace("<EDIT: folder name, e.g. developmental_psychology>", data.get("course_folder", ""))
        prompt = prompt.replace("<EDIT: e.g. 2>", str(data.get("unit_number", "")))

        # Handle lecture URLs (list)
        lecture_urls = [u for u in data.get("lecture_urls", []) if u.strip()]
        if lecture_urls:
            lines = "\n".join(
                f"- Lecture page {i}: {u}" if len(lecture_urls) > 1
                else f"- Lecture page: {u}"
                for i, u in enumerate(lecture_urls, 1)
            )
            prompt = prompt.replace("- Lecture page: <LECTURE_URLS_PLACEHOLDER>", lines)
        else:
            prompt = prompt.replace("<LECTURE_URLS_PLACEHOLDER>", "null")

        pdf_urls = [u for u in data.get("pdf_urls", []) if u.strip()]
        if pdf_urls:
            prompt = prompt.replace(
                "<EDIT: paste direct PDF/PPTX URL, OR if slides are already downloaded, write \"pre-downloaded\" and place the files in the sources/ folder>",
                pdf_urls[0]
            )
            if len(pdf_urls) > 1:
                prompt = prompt.replace(
                    "<EDIT: paste direct PDF/PPTX URL, or delete this line if only 1 file>",
                    pdf_urls[1]
                )
            else:
                lines = prompt.split("\n")
                lines = [l for l in lines if "<EDIT: paste direct PDF/PPTX URL, or delete this line if only 1 file>" not in l]
                prompt = "\n".join(lines)
        else:
            prompt = prompt.replace(
                "<EDIT: paste direct PDF/PPTX URL, OR if slides are already downloaded, write \"pre-downloaded\" and place the files in the sources/ folder>",
                "pre-downloaded"
            )
            lines = prompt.split("\n")
            lines = [l for l in lines if "<EDIT: paste direct PDF/PPTX URL, or delete this line if only 1 file>" not in l]
            prompt = "\n".join(lines)

        article_urls = [u for u in data.get("article_urls", []) if u.strip()]
        if article_urls:
            prompt = prompt.replace(
                "<EDIT: paste public article URL, or delete this line if none>",
                "\n- Article: ".join(article_urls)
            )
        else:
            lines = prompt.split("\n")
            lines = [l for l in lines if "<EDIT: paste public article URL, or delete this line if none>" not in l]
            prompt = "\n".join(lines)

        video_urls = [u for u in data.get("video_urls", []) if u.strip()]
        if video_urls:
            prompt = prompt.replace(
                "<EDIT: paste YouTube or other supplementary video URL, or delete this line if none>",
                "\n- Video: ".join(video_urls)
            )
        else:
            lines = prompt.split("\n")
            lines = [l for l in lines if "<EDIT: paste YouTube or other supplementary video URL, or delete this line if none>" not in l]
            prompt = "\n".join(lines)

    return prompt


def generate_quiz_prompt(data):
    """Generate a Copilot CLI quiz prompt from the form data."""
    template = (PROMPTS_DIR / "quiz.md").read_text(encoding="utf-8")
    prompt = template
    prompt = prompt.replace("`<course_name>`", f"`{data.get('course_folder', '')}`")
    prompt = prompt.replace("`<unit_number>`", f"`{data.get('unit_number', '')}`")

    # Replace the placeholder questions section with actual questions
    questions = data.get("questions", "").strip()
    prompt = prompt.replace(
        "<!-- Paste quiz questions here -->\n\n1. \n\n2. \n\n3. ",
        questions
    )
    return prompt


def launch_copilot(prompt_text, autopilot=True, allow_all=True,
                   same_window=True):
    """Launch Copilot CLI in a terminal with the generated prompt.

    Args:
        same_window: If True (default), reuse an existing Windows Terminal
            window instead of spawning a brand-new one for every session.
    """
    # Save prompt to a temp file (avoids shell escaping issues with long prompts)
    prompt_file = BASE_DIR / ".last-prompt.txt"
    prompt_file.write_text(prompt_text, encoding="utf-8")

    # Build the copilot command
    copilot_args = []
    if autopilot:
        copilot_args.append("--autopilot")
    if allow_all:
        copilot_args.append("--allow-all")

    args_str = " ".join(copilot_args)
    escaped_path = str(prompt_file).replace("'", "''")

    # Write a launcher script. Must use pwsh (PowerShell 7) — PS 5.1 has a
    # known argument-quoting bug that corrupts strings with double quotes
    # when passing to native executables (copilot is a node.js CLI).
    script_file = BASE_DIR / ".launch-copilot.ps1"
    script_content = (
        f"$prompt = Get-Content '{escaped_path}' -Raw\n"
        f'copilot -i "$prompt" {args_str}\n'
    )
    script_file.write_text(script_content, encoding="utf-8")

    errors = []

    # Strategy 1: Windows Terminal + pwsh (PS 7)
    # -w 0 reuses the most-recently-used WT window (same_window mode).
    # Without -w 0, wt always opens a brand-new window when called from
    # a subprocess.
    try:
        wt_cmd = ["wt"]
        if same_window:
            wt_cmd += ["-w", "0"]
        # "--" tells wt that everything after it is the child command,
        # preventing wt from consuming flags meant for pwsh/copilot.
        wt_cmd += ["new-tab", "--title", "Course Workflow",
                    "-d", str(BASE_DIR), "--",
                    "pwsh", "-NoExit", "-ExecutionPolicy", "Bypass",
                    "-File", str(script_file)]
        subprocess.Popen(wt_cmd, cwd=str(BASE_DIR))
        return True, None
    except Exception as e:
        errors.append(f"wt+pwsh: {e}")

    # Strategy 2: pwsh standalone window
    try:
        subprocess.Popen(
            ["pwsh", "-NoExit", "-ExecutionPolicy", "Bypass",
             "-File", str(script_file)],
            cwd=str(BASE_DIR),
            creationflags=0x00000010,  # CREATE_NEW_CONSOLE
        )
        return True, None
    except Exception as e:
        errors.append(f"pwsh standalone: {e}")

    # Strategy 3: cmd fallback (uses copilot.cmd, avoids PS entirely)
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


def create_new_course(data):
    """Create course directory and course.json for a new course."""
    folder_name = data.get("name_en", "").lower().replace(" ", "_").replace("-", "_")
    folder_name = "".join(c for c in folder_name if c.isalnum() or c == "_")
    course_dir = COURSES_DIR / folder_name
    course_dir.mkdir(parents=True, exist_ok=True)

    course_json = {
        "name": data.get("name_he", ""),
        "name_en": data.get("name_en", ""),
        "moodle_base": "https://online.dyellin.ac.il",
        "edge_profile": "Profile 5",
        "onenote_url": data.get("onenote_url", ""),
        "units": {}
    }

    # Add unit 1
    unit_data = {}
    unit_data["lecture_urls"] = [u for u in data.get("lecture_urls", []) if u.strip()] or None
    unit_data["pdf_urls"] = [u for u in data.get("pdf_urls", []) if u.strip()]
    unit_data["article_urls"] = [u for u in data.get("article_urls", []) if u.strip()]
    unit_data["video_urls"] = [u for u in data.get("video_urls", []) if u.strip()]
    course_json["units"]["1"] = unit_data

    with open(course_dir / "course.json", "w", encoding="utf-8") as f:
        json.dump(course_json, f, indent=2, ensure_ascii=False)

    return folder_name


class CourseHandler(http.server.SimpleHTTPRequestHandler):
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
        if self.path == "/api/courses":
            courses = get_courses()
            self._json_response(courses)
        elif self.path == "/api/last-prompt":
            prompt_file = BASE_DIR / ".last-prompt.txt"
            if prompt_file.exists():
                text = prompt_file.read_text(encoding="utf-8")
                self._json_response({"prompt": text})
            else:
                self._json_response({"prompt": ""})
        else:
            super().do_GET()

    def do_POST(self):
        if self.path == "/api/submit":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len))

            # Create course folder if new
            if body.get("is_new_course"):
                folder = create_new_course(body)
                body["course_folder"] = folder

            # Generate prompt
            prompt = generate_prompt(body)

            # Launch Copilot CLI
            autopilot = body.get("autopilot", True)
            allow_all = body.get("allow_all", True)
            same_window = body.get("same_window", True)
            ok, err = launch_copilot(prompt, autopilot=autopilot,
                                     allow_all=allow_all,
                                     same_window=same_window)

            if ok:
                self._json_response({
                    "ok": True,
                    "prompt_preview": prompt[:500],
                    "message": "Copilot CLI session launched!",
                })
            else:
                self._json_response({
                    "ok": False,
                    "error": f"Failed to launch terminal: {err}",
                })

        elif self.path == "/api/preview-prompt":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len))
            prompt = generate_prompt(body)
            self._json_response({"prompt": prompt})

        elif self.path == "/api/preview-quiz":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len))
            missing = [f for f in ("course_folder", "unit_number", "questions") if not body.get(f)]
            if missing:
                self.send_error(400, f"Missing required fields: {', '.join(missing)}")
                return
            prompt = generate_quiz_prompt(body)
            self._json_response({"prompt": prompt})

        elif self.path == "/api/submit-quiz":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len))
            missing = [f for f in ("course_folder", "unit_number", "questions") if not body.get(f)]
            if missing:
                self.send_error(400, f"Missing required fields: {', '.join(missing)}")
                return
            prompt = generate_quiz_prompt(body)
            autopilot = body.get("autopilot", True)
            allow_all = body.get("allow_all", True)
            same_window = body.get("same_window", True)
            ok, err = launch_copilot(prompt, autopilot=autopilot,
                                     allow_all=allow_all,
                                     same_window=same_window)
            if ok:
                self._json_response({
                    "ok": True,
                    "message": f"Quiz session launched for {body.get('course_folder', '')} unit {body.get('unit_number', '')}!",
                })
            else:
                self._json_response({
                    "ok": False,
                    "error": f"Failed to launch terminal: {err}",
                })

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
    import socket

    def is_port_in_use(port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(("127.0.0.1", port)) == 0

    url = f"http://localhost:{PORT}/dashboard.html"

    if is_port_in_use(PORT):
        print(f"Server already running at {url}")
        webbrowser.open(url)
        sys.exit(0)

    server = http.server.ThreadingHTTPServer(("127.0.0.1", PORT), CourseHandler)
    print(f"Course Workflow: {url}")
    print("Press Ctrl+C to stop.\n")

    if "--no-browser" not in sys.argv:
        webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
