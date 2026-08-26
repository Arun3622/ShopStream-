"""
Dashboard Builder
=================
Injects processed aggregates into the HTML template -> dashboard/index.html
(Standalone file: just double-click to open in any browser.)

Run: python src/build_dashboard.py
"""

import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATE = BASE_DIR / "dashboard" / "template.html"
OUTPUT = BASE_DIR / "dashboard" / "index.html"
DATA_FILE = BASE_DIR / "data" / "processed" / "aggregates.json"

PLACEHOLDER = "__DATA_JSON__"


def main():
    data = json.loads(DATA_FILE.read_text(encoding="utf-8"))
    html = TEMPLATE.read_text(encoding="utf-8")
    if PLACEHOLDER not in html:
        raise SystemExit("template placeholder missing")
    html = html.replace(PLACEHOLDER, json.dumps(data, ensure_ascii=False))
    OUTPUT.write_text(html, encoding="utf-8")
    print(f"[done] dashboard -> {OUTPUT}")


if __name__ == "__main__":
    main()
