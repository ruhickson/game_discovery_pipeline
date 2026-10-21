#!/usr/bin/env python3
"""
Daily refresh runner.

Runs, in order:
  1. 17_get_releases_for_month.py --ingest  (current month by default)
  2. 05_update_release_date_actual.py
  3. 06_recreate_top_line_metrics.py
  4. 16_build_state_of_steam_monthly.py
  5. 02_update_recent_reviews.py

Usage:
  python 97_run_daily_refresh.py
  python 97_run_daily_refresh.py 2026-09          # pass month through to 17
  python 97_run_daily_refresh.py 2026-07 --start 5000
  python 97_run_daily_refresh.py --skip-reviews   # skip 02_update_recent_reviews.py
"""

import os
import subprocess
import sys
import time
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

# (label, argv after python executable)
DAILY_STEPS = [
    ("17_get_releases_for_month.py", ["17_get_releases_for_month.py", "--ingest"]),
    ("05_update_release_date_actual.py", ["05_update_release_date_actual.py"]),
    ("06_recreate_top_line_metrics.py", ["06_recreate_top_line_metrics.py"]),
    ("16_build_state_of_steam_monthly.py", ["16_build_state_of_steam_monthly.py"]),
    ("02_update_recent_reviews.py", ["02_update_recent_reviews.py"]),
]

SKIP_REVIEWS_FLAG = "--skip-reviews"


def run_step(label, argv):
    """Run one pipeline step; return (success, duration_seconds)."""
    print(f"\n{'=' * 60}")
    print(f"🚀 Starting: {' '.join(argv)}")
    print(f"⏰ Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 60}")

    start_time = time.time()
    try:
        result = subprocess.run(
            [sys.executable, *argv],
            capture_output=False,
            text=True,
            cwd=os.getcwd(),
        )
        duration = time.time() - start_time
        if result.returncode == 0:
            print(f"\n✅ {label} completed successfully!")
            print(f"⏱️  Duration: {duration:.2f} seconds")
            return True, duration

        print(f"\n❌ {label} failed with exit code: {result.returncode}")
        print(f"⏱️  Duration: {duration:.2f} seconds")
        return False, duration
    except KeyboardInterrupt:
        print(f"\n⚠️  {label} interrupted by user")
        return False, time.time() - start_time
    except Exception as e:
        print(f"\n❌ Error running {label}: {e}")
        return False, time.time() - start_time


def main():
    # Extra CLI args (e.g. month / --start) are forwarded to script 17 only
    raw_args = sys.argv[1:]
    skip_reviews = SKIP_REVIEWS_FLAG in raw_args
    extra_for_17 = [a for a in raw_args if a != SKIP_REVIEWS_FLAG]

    steps = []
    for label, argv in DAILY_STEPS:
        if skip_reviews and label.startswith("02_"):
            continue
        if label.startswith("17_"):
            # Insert month/start args before --ingest so argparse still works
            # e.g. python 17_...py 2026-09 --start 5000 --ingest
            forwarded = list(argv)
            if extra_for_17:
                # argv is [script, --ingest] → [script, *extra, --ingest]
                forwarded = [argv[0], *extra_for_17, *argv[1:]]
            steps.append((label, forwarded))
        else:
            steps.append((label, list(argv)))

    print("🎮 Daily Refresh Pipeline")
    print("=" * 60)
    print(f"📅 Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📁 Working Directory: {os.getcwd()}")
    print(f"🐍 Python Executable: {sys.executable}")
    if extra_for_17:
        print(f"↪️  Extra args for 17: {' '.join(extra_for_17)}")
    else:
        print("↪️  Script 17: current month (default) with --ingest")
    if skip_reviews:
        print("⏭️  Skipping 02_update_recent_reviews.py")
    print("=" * 60)

    missing = [label for label, argv in steps if not os.path.exists(argv[0])]
    if missing:
        print("❌ Missing scripts:")
        for name in missing:
            print(f"   - {name}")
        sys.exit(1)

    results = []
    total_start = time.time()

    for i, (label, argv) in enumerate(steps, 1):
        print(f"\n📊 Progress: {i}/{len(steps)} steps")
        success, duration = run_step(label, argv)
        results.append({"label": label, "argv": argv, "success": success, "duration": duration})

        if not success:
            print(f"\n⚠️  Step failed: {' '.join(argv)}")
            print("Press Ctrl+C to stop, or Enter to continue with remaining steps...")
            try:
                input()
            except KeyboardInterrupt:
                print("\n🛑 Daily refresh stopped by user")
                break

        if i < len(steps):
            print("\n⏳ Waiting 5 seconds before next step...")
            time.sleep(5)

    total_duration = time.time() - total_start
    print(f"\n{'=' * 60}")
    print("📊 DAILY REFRESH SUMMARY")
    print(f"{'=' * 60}")
    print(f"📅 End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏱️  Total Duration: {total_duration:.2f} seconds ({total_duration / 60:.2f} minutes)")
    print()

    successful = 0
    failed = 0
    for result in results:
        status = "✅ SUCCESS" if result["success"] else "❌ FAILED"
        print(f"{status} | {result['duration']:8.2f}s | {' '.join(result['argv'])}")
        if result["success"]:
            successful += 1
        else:
            failed += 1

    print(f"\n📈 Results: {successful} successful, {failed} failed out of {len(results)} steps")
    sys.exit(0 if failed == 0 and len(results) == len(steps) else 1)


if __name__ == "__main__":
    main()
