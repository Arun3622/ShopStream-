"""
One-command runner: generate data -> run ETL -> build dashboard.

Usage:  python run_all.py
"""

import subprocess
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent
STEPS = ["src/generate_data.py", "src/pipeline.py", "src/build_dashboard.py"]

for step in STEPS:
    print(f"\n{'='*60}\n>>> {step}\n{'='*60}")
    result = subprocess.run([sys.executable, str(BASE / step)])
    if result.returncode != 0:
        sys.exit(f"FAILED at {step}")

print("\nALL DONE -> open dashboard/index.html in your browser!")
