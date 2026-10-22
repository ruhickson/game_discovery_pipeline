#!/usr/bin/env python3
"""
Previous daily refresh entry point.

The daily run now lives in 001_run_daily.py. This forwards to it so existing
commands keep working.
"""

import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(ROOT, "001_run_daily.py")

if __name__ == "__main__":
    raise SystemExit(subprocess.call([sys.executable, SCRIPT, *sys.argv[1:]], cwd=ROOT))
