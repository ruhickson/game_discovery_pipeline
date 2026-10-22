#!/usr/bin/env python3
"""
Daily refresh.

Pulls new and changed Steam data, then rebuilds the tables and snapshots
that read it. Run from anywhere:

  python 001_run_daily.py
  python 001_run_daily.py 2026-09                 # month passed through to 17
  python 001_run_daily.py 2026-07 --start 5000    # resume args passed to 17
  python 001_run_daily.py --skip-reviews          # skip 02_update_recent_reviews.py

Order:
  1. 17  Ingest this month's releases, tags, and store notices
  2. 04  Recheck coming-soon games that may now be released
  3. 05  Parse release_date_actual from the release-date text
  4. 02  Refresh review scores for recent releases
  5. 20  Copy any missing genre names into the genres table
  6. 21  Rebuild daily genre, tag, and release counts for Trend Analysis
  7. 06  Rebuild the dashboard metric tables
  8. 16  Rebuild State of Steam monthly snapshots

A failed step is recorded and the rest still run. The process exits non-zero
if any step failed.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime

ROOT = os.path.dirname(os.path.abspath(__file__))

SKIP_REVIEWS_FLAG = "--skip-reviews"

# (script, argv after the script name, whether to append caller args)
DAILY_STEPS = [
    ("17_get_releases_for_month.py", ["--ingest"], True),
    ("04_check_coming_soon_updates.py", [], False),
    ("05_update_release_date_actual.py", [], False),
    ("02_update_recent_reviews.py", [], False),
    ("20_sync_genres_table.py", [], False),
    ("21_build_trend_analysis.py", [], False),
    ("06_recreate_top_line_metrics.py", [], False),
    ("16_build_state_of_steam_monthly.py", [], False),
]


def run_step(argv):
    """Run one pipeline step. Return (success, duration_seconds)."""
    print(f"\n{'=' * 60}")
    print(f"Starting: {' '.join(argv)}")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 60}")

    start_time = time.time()
    try:
        result = subprocess.run(
            [sys.executable, *argv],
            cwd=ROOT,
        )
        duration = time.time() - start_time
        if result.returncode == 0:
            print(f"\n{argv[0]} completed in {duration:.1f}s")
            return True, duration
        print(f"\n{argv[0]} failed with exit code {result.returncode} after {duration:.1f}s")
        return False, duration
    except KeyboardInterrupt:
        print(f"\n{argv[0]} interrupted")
        raise
    except Exception as exc:
        duration = time.time() - start_time
        print(f"\nError running {argv[0]}: {exc}")
        return False, duration


def build_steps(extra_for_17, skip_reviews):
    steps = []
    for script, args, forward_extra in DAILY_STEPS:
        if skip_reviews and script.startswith("02_"):
            continue
        argv = [script, *args]
        if forward_extra and extra_for_17:
            # Keep --ingest last so 17's argparse still sees it as a flag.
            argv = [script, *extra_for_17, *args]
        steps.append(argv)
    return steps


def main():
    os.chdir(ROOT)
    raw_args = sys.argv[1:]
    skip_reviews = SKIP_REVIEWS_FLAG in raw_args
    extra_for_17 = [arg for arg in raw_args if arg != SKIP_REVIEWS_FLAG]
    steps = build_steps(extra_for_17, skip_reviews)

    print("Daily refresh")
    print("=" * 60)
    print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Directory: {ROOT}")
    if extra_for_17:
        print(f"Args for 17: {' '.join(extra_for_17)}")
    else:
        print("17 ingests the current month")
    if skip_reviews:
        print("Skipping 02_update_recent_reviews.py")
    print("Steps:")
    for index, argv in enumerate(steps, 1):
        print(f"  {index}. {' '.join(argv)}")
    print("=" * 60)

    missing = [argv[0] for argv in steps if not os.path.exists(os.path.join(ROOT, argv[0]))]
    if missing:
        print("Missing scripts:")
        for name in missing:
            print(f"  - {name}")
        sys.exit(1)

    results = []
    total_start = time.time()
    interrupted = False

    for index, argv in enumerate(steps, 1):
        print(f"\nProgress: {index}/{len(steps)}")
        try:
            success, duration = run_step(argv)
        except KeyboardInterrupt:
            results.append({"argv": argv, "success": False, "duration": 0.0})
            interrupted = True
            print("\nDaily refresh interrupted")
            break
        results.append({"argv": argv, "success": success, "duration": duration})
        if index < len(steps):
            time.sleep(5)

    total_duration = time.time() - total_start
    print(f"\n{'=' * 60}")
    print("Daily refresh summary")
    print(f"{'=' * 60}")
    print(f"End: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {total_duration / 60:.1f} minutes")
    print()

    failed = 0
    for result in results:
        status = "ok" if result["success"] else "FAILED"
        print(f"{status:6} | {result['duration']:8.1f}s | {' '.join(result['argv'])}")
        if not result["success"]:
            failed += 1

    print(f"\n{len(results) - failed} succeeded, {failed} failed, out of {len(steps)} steps")
    if interrupted or failed or len(results) != len(steps):
        sys.exit(1)


if __name__ == "__main__":
    main()
