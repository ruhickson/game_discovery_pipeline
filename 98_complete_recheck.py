#!/usr/bin/env python3
"""
Complete recheck: walk backwards from max(app_id) to min(app_id) in steps of 10.

For each candidate app_id:
  - If it already exists in the DB, skip
  - If missing, fetch from Steam and insert games the same way as 01_steam_to_supabase_pipeline.py
"""

import argparse
import importlib.util
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Ensure stdout/stderr handle UTF-8 so emojis and non-ASCII don't crash on Windows consoles
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def load_pipeline_module():
    """Load SteamAPI / SupabasePipeline from the 01 pipeline script."""
    module_path = Path(__file__).resolve().parent / "01_steam_to_supabase_pipeline.py"
    spec = importlib.util.spec_from_file_location("steam_to_supabase_pipeline", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def get_lowest_app_id(pipeline):
    """Get the lowest app_id currently in the games table."""
    try:
        pipeline.cursor.execute("SELECT MIN(app_id) FROM games")
        result = pipeline.cursor.fetchone()
        return result[0] if result and result[0] is not None else 0
    except Exception as e:
        print(f"Error getting lowest app_id: {str(e)}")
        return 0


def complete_recheck(api_key=None, start_app_id=None, stop_app_id=None, fetch_tags=True, limit=None):
    """
    Walk app_ids downwards in increments of 10 from max (or start_app_id)
    down to min (or stop_app_id), filling any gaps missing from the DB.

    Args:
        api_key: Steam API key (optional; falls back to env)
        start_app_id: First app_id to check. Defaults to floor(max(app_id)/10)*10
        stop_app_id: Stop when app_id goes below this. Defaults to min(app_id) in DB
        fetch_tags: Whether to fetch and store tags for newly added games
        limit: Optional max number of *Steam checks* (missing IDs only) for testing
    """
    pipeline_mod = load_pipeline_module()
    steam_api = pipeline_mod.SteamAPI(api_key)
    pipeline = pipeline_mod.SupabasePipeline()

    if not pipeline.connect():
        return

    try:
        max_app_id = pipeline.get_highest_app_id()
        min_app_id = get_lowest_app_id(pipeline)

        if not max_app_id or not min_app_id:
            print("❌ Could not determine max/min app_id from database.")
            return

        if start_app_id is None:
            # Align to the nearest multiple of 10 at or below current max
            start_app_id = (max_app_id // 10) * 10

        if stop_app_id is None:
            stop_app_id = min_app_id

        print(f"📊 Highest app_id in DB: {max_app_id}")
        print(f"📊 Lowest app_id in DB:  {min_app_id}")
        print(f"🔄 Checking from {start_app_id} down to {stop_app_id} in steps of 10")
        if limit:
            print(f"🔢 Limiting to {limit} Steam checks (missing IDs only)")

        existing_app_ids = pipeline.get_existing_app_ids()
        print(f"📦 Loaded {len(existing_app_ids)} existing app_ids from database")

        checked = 0          # IDs visited (including skips)
        skipped_existing = 0
        steam_checked = 0    # Missing IDs we hit Steam for
        not_found = 0
        skipped_non_game = 0
        added = 0
        failed = 0
        tags_collected = 0

        app_id = start_app_id
        while app_id >= stop_app_id:
            checked += 1

            if app_id in existing_app_ids:
                skipped_existing += 1
                if checked % 5000 == 0:
                    print(
                        f"\n📈 Progress: visited {checked} IDs "
                        f"(now at {app_id}), skipped existing {skipped_existing}, "
                        f"Steam-checked {steam_checked}, added {added}"
                    )
                app_id -= 10
                continue

            # Missing from DB — check Steam
            steam_checked += 1
            print(f"\n🔄 [{steam_checked}] Checking missing app_id {app_id}...")

            details = steam_api.get_app_details(app_id)
            if not details:
                print(f"   ❌ App_id {app_id} not found on Steam")
                not_found += 1
                app_id -= 10
                if limit and steam_checked >= limit:
                    print(f"\n🛑 Reached Steam-check limit ({limit})")
                    break
                continue

            name = details.get("name", f"App {app_id}")
            app_type = (details.get("type") or "").lower()
            print(f"   ✅ Found: {name} (type={app_type})")

            if app_type != "game":
                print(f"   ⏭️ Skipping {name} - Type: {app_type}")
                skipped_non_game += 1
                app_id -= 10
                if limit and steam_checked >= limit:
                    print(f"\n🛑 Reached Steam-check limit ({limit})")
                    break
                continue

            review_summary = steam_api.get_review_summary(app_id)
            print(f"   📊 Review summary: {review_summary}")

            if pipeline.insert_game(app_id, name, details, review_summary):
                print(f"   ✅ Successfully added {name} to database")
                added += 1
                existing_app_ids.add(app_id)

                if fetch_tags:
                    print(f"   🏷️  Fetching tags for {name}...")
                    tags = steam_api.get_game_tags(app_id)
                    if tags:
                        if pipeline.insert_game_tags(app_id, tags):
                            tags_collected += len(tags)
                            print(f"   🏷️  Successfully collected {len(tags)} tags for {name}")
                        else:
                            print(f"   ❌ Failed to insert tags for {name}")
                    else:
                        print(f"   📭 No tags found for {name}")
            else:
                print(f"   ❌ Failed to insert {name}")
                failed += 1

            if steam_checked % 25 == 0:
                print(
                    f"\n📈 Progress: visited {checked} IDs (now at {app_id}), "
                    f"Steam-checked {steam_checked}, added {added}, "
                    f"not found {not_found}, non-game {skipped_non_game}, failed {failed}"
                )

            if limit and steam_checked >= limit:
                print(f"\n🛑 Reached Steam-check limit ({limit})")
                break

            app_id -= 10

        print("\n🎉 Complete recheck finished!")
        print(f"📊 IDs visited:          {checked}")
        print(f"⏭️ Already in DB:        {skipped_existing}")
        print(f"🔍 Steam checks:         {steam_checked}")
        print(f"✅ Games added:          {added}")
        print(f"❌ Not found on Steam:   {not_found}")
        print(f"⏭️ Skipped non-game:     {skipped_non_game}")
        print(f"❌ Insert failures:      {failed}")
        print(f"🏷️ Tags collected:       {tags_collected}")
        print(f"📍 Last app_id reached:  {app_id + 10}")

    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user — progress above is still committed for completed inserts.")
    except Exception as e:
        print(f"❌ Error during complete recheck: {str(e)}")
    finally:
        pipeline.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Walk backwards from a high app_id to a low app_id in steps of 10, "
            "filling any games missing from the DB."
        )
    )
    parser.add_argument(
        "start",
        nargs="?",
        type=int,
        default=None,
        help="App ID to start counting down from (default: floor of max app_id in DB)",
    )
    parser.add_argument(
        "end",
        nargs="?",
        type=int,
        default=None,
        help="App ID to stop at (default: min app_id in DB). Must be <= start.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of Steam checks (missing IDs only)",
    )
    parser.add_argument(
        "--no-tags",
        action="store_true",
        help="Skip fetching tags for newly added games",
    )
    args = parser.parse_args()

    if args.start is not None and args.end is None:
        parser.error("Pass two numbers: START END (e.g. python 98_complete_recheck.py 4000000 3500000)")
    if args.start is not None and args.end is not None and args.end > args.start:
        parser.error(f"end ({args.end}) must be <= start ({args.start})")

    print("Starting Complete Recheck (backwards by 10)")
    if args.start is not None:
        print(f"📍 Range: {args.start} → {args.end}")
    print("=" * 50)
    complete_recheck(
        api_key=os.getenv("STEAM_API_KEY"),
        start_app_id=args.start,
        stop_app_id=args.end,
        fetch_tags=not args.no_tags,
        limit=args.limit,
    )
