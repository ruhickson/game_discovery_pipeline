#!/usr/bin/env python3
"""
Collect Steam game app IDs released in a specific calendar month.

Uses the public store search (Released_DESC + category=games), paginates until
results fall before the target month, and writes a JSON file of matches.

With --ingest, missing app IDs are fetched from Steam and inserted into the
games table using the same helpers as 01_steam_to_supabase_pipeline.py.
Ingest also scrapes store-page notices (AI disclosure, kernel anti-cheat,
third-party DRM/Denuvo, EULA) into dedicated games columns.

Usage:
  python 17_get_releases_for_month.py                  # current month (+ prev month last day)
  python 17_get_releases_for_month.py 2026-09
  python 17_get_releases_for_month.py 2026 9 --check-db
  python 17_get_releases_for_month.py 2026-09 --ingest
  python 17_get_releases_for_month.py 2026-09 --ingest --limit 50
  python 17_get_releases_for_month.py 2026-07 --ingest --start-page 34
  python 17_get_releases_for_month.py 2026-07 --ingest --start 3300
"""

import argparse
import calendar
import importlib.util
import json
import os
import random
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/javascript,*/*;q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
}

SEARCH_URL = "https://store.steampowered.com/search/results/"
PAGE_SIZE = 100
# Stop after this many consecutive parseable results older than the target month
OLDER_STREAK_STOP = 40


def parse_month_args(args):
    """Accept 'YYYY-MM', 'YYYY' 'M', or omit for the current calendar month."""
    if args.month_or_year is None:
        if args.day_or_month is not None:
            raise ValueError("Pass YYYY-MM or YYYY MM (e.g. 2026-09 or 2026 9)")
        today = date.today()
        return today.year, today.month

    if args.month_or_year.count("-") == 1 and args.day_or_month is None:
        year_str, month_str = args.month_or_year.split("-", 1)
        year, month = int(year_str), int(month_str)
    elif args.day_or_month is not None:
        year, month = int(args.month_or_year), int(args.day_or_month)
    else:
        raise ValueError("Pass YYYY-MM or YYYY MM (e.g. 2026-09 or 2026 9)")

    if month < 1 or month > 12:
        raise ValueError(f"Month must be 1-12, got {month}")
    return year, month


def month_bounds(year, month):
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last_day)


def scan_bounds(year, month):
    """
    Collection window: last day of the previous month through the target month end.
    The overlap day catches boundary releases that can otherwise fall between runs.
    """
    month_start, month_end = month_bounds(year, month)
    overlap_day = month_start - timedelta(days=1)
    return overlap_day, month_end, month_start


def parse_steam_release_date(raw):
    """
    Parse common Steam store search date strings into a date.
    Returns None if unknown / coming soon / unparseable.
    """
    if not raw:
        return None

    text = re.sub(r"\s+", " ", raw).strip()
    lower = text.lower()

    if any(
        token in lower
        for token in (
            "coming soon",
            "to be announced",
            "tba",
            "tbd",
            "when it's done",
        )
    ):
        return None

    # "7 Oct, 2026" / "07 Oct, 2026"
    for fmt in ("%d %b, %Y", "%d %B, %Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass

    # "Oct 7, 2026" / "October 7, 2026"
    for fmt in ("%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass

    # "Oct 2026" / "October 2026" → first of month
    for fmt in ("%b %Y", "%B %Y"):
        try:
            return datetime.strptime(text, fmt).date().replace(day=1)
        except ValueError:
            pass

    # "2026"
    if re.fullmatch(r"\d{4}", text):
        return date(int(text), 1, 1)

    return None


def fetch_search_page(start, count=PAGE_SIZE, retry_count=0):
    """Fetch one page of store search results sorted by release date descending."""
    params = {
        "query": "",
        "start": start,
        "count": count,
        "sort_by": "Released_DESC",
        "category1": "998",  # Games
        "supportedlang": "english",
        "infinite": "1",
    }

    try:
        time.sleep(random.uniform(0.8, 1.4))
        response = requests.get(SEARCH_URL, params=params, headers=HEADERS, timeout=30)

        if response.status_code == 429:
            wait = 120
            print(f"⚠️  Rate limited (429). Waiting {wait}s...")
            time.sleep(wait)
            if retry_count < 5:
                return fetch_search_page(start, count, retry_count + 1)
            return None

        if response.status_code != 200:
            print(f"❌ Search request failed: HTTP {response.status_code}")
            return None

        data = response.json()
        if not data.get("success"):
            print("❌ Steam search returned success=0")
            return None
        return data
    except Exception as e:
        print(f"❌ Error fetching search page at start={start}: {e}")
        if retry_count < 3:
            time.sleep(5)
            return fetch_search_page(start, count, retry_count + 1)
        return None


def parse_results_html(html):
    """Extract app rows from search results HTML."""
    if not html:
        return []

    # Each result row is an <a ... data-ds-appid="..."> ... </a> block
    row_pattern = re.compile(
        r'<a[^>]*data-ds-appid="(\d+)"[^>]*>.*?class="title">([^<]*)</span>.*?'
        r'class="search_released[^"]*">\s*([^<]*)',
        re.DOTALL | re.IGNORECASE,
    )

    rows = []
    for match in row_pattern.finditer(html):
        app_id = int(match.group(1))
        name = re.sub(r"\s+", " ", match.group(2)).strip()
        release_raw = re.sub(r"\s+", " ", match.group(3)).strip()
        rows.append(
            {
                "app_id": app_id,
                "name": name,
                "release_date_raw": release_raw,
                "release_date": parse_steam_release_date(release_raw),
            }
        )

    # Fallback if the combined regex misses rows (layout drift)
    if not rows:
        app_ids = re.findall(r'data-ds-appid="(\d+)"', html)
        titles = re.findall(r'class="title">([^<]+)', html)
        dates = re.findall(r'class="search_released[^"]*">\s*([^<]+)', html)
        for i, app_id in enumerate(app_ids):
            release_raw = dates[i].strip() if i < len(dates) else ""
            rows.append(
                {
                    "app_id": int(app_id),
                    "name": titles[i].strip() if i < len(titles) else f"App {app_id}",
                    "release_date_raw": release_raw,
                    "release_date": parse_steam_release_date(release_raw),
                }
            )

    return rows


def collect_releases_for_month(year, month, max_pages=None, start_offset=0, max_offset=None):
    """
    Page through Released_DESC store search and collect games in
    [last day of previous month, month_end].

    start_offset: Steam search 'start' index to begin at (skip newer months to save
    requests / avoid rate limits). Use a prior run's page log to pick a safe offset
    a bit before the target month begins.

    max_offset: optional hard stop on the Steam search offset (absolute). Useful as a
    safety rail; do not use a low value when targeting older months — July can sit
    well past offset 5000 when paging from "now".
    """
    month_start, month_end = month_bounds(year, month)
    scan_start, scan_end, _ = scan_bounds(year, month)
    start = max(0, int(start_offset or 0))
    print(f"🎯 Target month: {month_start} → {month_end}")
    print(
        f"🛟 Scan window:  {scan_start} → {scan_end} "
        f"(includes last day of previous month)"
    )
    print("📋 Paging Steam store search (Released_DESC, games only)...")
    if start > 0:
        approx_page = (start // PAGE_SIZE) + 1
        print(
            f"⏭️  Starting at search offset {start} "
            f"(~page {approx_page}) — skipping newer results above that point"
        )
    if max_offset is not None:
        print(f"🛟 Max search offset: {max_offset}")

    collected = {}  # app_id -> record
    skipped_future = 0
    skipped_unparsed = 0
    older_streak = 0
    page = 0
    total_count = None
    warned_deep_scan = False

    while True:
        page += 1
        if max_pages and page > max_pages:
            print(f"🛑 Reached max_pages={max_pages}")
            break

        data = fetch_search_page(start)
        if not data:
            print("❌ Stopping — search page fetch failed")
            break

        if total_count is None:
            total_count = data.get("total_count")
            print(
                f"ℹ️  Steam's unfiltered games index is ~{total_count} titles "
                "(catalog size, not this month)."
            )
            print(
                f"   Games dated {scan_start} → {scan_end} are kept; "
                f"search stops after {OLDER_STREAK_STOP} consecutive older results."
            )

        rows = parse_results_html(data.get("results_html", ""))
        if not rows:
            print(f"📭 No rows at start={start}; stopping")
            break

        page_in_window = 0
        page_future = 0
        page_older = 0
        page_unparsed = 0
        page_dates = []
        for row in rows:
            release_date = row["release_date"]

            if release_date is None:
                skipped_unparsed += 1
                page_unparsed += 1
                continue

            page_dates.append(release_date)

            if release_date > scan_end:
                # Still above the scan window (e.g. looking for Sept, seeing Oct)
                skipped_future += 1
                page_future += 1
                older_streak = 0
                continue

            if scan_start <= release_date <= scan_end:
                older_streak = 0
                page_in_window += 1
                collected[row["app_id"]] = {
                    "app_id": row["app_id"],
                    "name": row["name"],
                    "release_date_raw": row["release_date_raw"],
                    "release_date": release_date.isoformat(),
                }
                continue

            # release_date < scan_start (before previous month's last day)
            older_streak += 1
            page_older += 1

        date_span = ""
        if page_dates:
            date_span = f" | dates {min(page_dates)} → {max(page_dates)}"

        print(
            f"   Page {page} (start={start}): "
            f"+{page_in_window} in-window | "
            f"newer {page_future} | older {page_older} | tba {page_unparsed} | "
            f"collected {len(collected)} | "
            f"older streak {older_streak}{date_span}"
        )

        if older_streak >= OLDER_STREAK_STOP:
            print(
                f"✅ Scan window complete — {older_streak} consecutive results "
                f"older than {scan_start}. Not scanning the rest of Steam."
            )
            break

        start += len(rows)
        if total_count is not None and start >= total_count:
            print("✅ Reached end of Steam search results")
            break

        if max_offset is not None and start >= max_offset:
            print(
                f"🛑 Reached --max-offset {max_offset} "
                f"(collected {len(collected)} in-window so far)"
            )
            break

        # Informational only: older months often sit past offset 5000 when paging
        # from "now". Keep going while dates are still newer than the scan window.
        if (
            not collected
            and not warned_deep_scan
            and start >= 5000
            and page_dates
            and min(page_dates) > scan_end
        ):
            warned_deep_scan = True
            print(
                f"ℹ️  Past offset {start} and still in newer dates "
                f"({min(page_dates)} → {max(page_dates)}); continuing toward {scan_start}."
            )

    games = sorted(collected.values(), key=lambda g: (g["release_date"], g["app_id"]))
    overlap_count = sum(1 for g in games if g["release_date"] == scan_start.isoformat())
    print(f"\n✅ Collected {len(games)} games for {year}-{month:02d} scan window")
    print(f"   Including previous-month last day ({scan_start}): {overlap_count}")
    print(f"   Skipped newer-than-window: {skipped_future}")
    print(f"   Skipped unparsed/TBA:      {skipped_unparsed}")
    print("   (Ingest, if enabled, only touches this collected set — not the full catalog)")
    return games


def check_against_database(games):
    """Report which collected app_ids are missing from the games table."""
    try:
        import psycopg2
    except ImportError:
        print("⚠️  psycopg2 not available; skipping DB check")
        return None

    connection_string = os.getenv("SUPABASE_CONNECTION_STRING")
    if not connection_string:
        print("⚠️  SUPABASE_CONNECTION_STRING not set; skipping DB check")
        return None

    app_ids = [g["app_id"] for g in games]
    if not app_ids:
        return {"missing": [], "present": []}

    try:
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT app_id FROM games WHERE app_id = ANY(%s)",
            (app_ids,),
        )
        present = {row[0] for row in cursor.fetchall()}
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ DB check failed: {e}")
        return None

    missing = [g for g in games if g["app_id"] not in present]
    present_games = [g for g in games if g["app_id"] in present]

    print("\n📦 Database comparison")
    print(f"   In DB already: {len(present_games)}")
    print(f"   Missing:       {len(missing)}")
    if missing:
        print("   Sample missing:")
        for game in missing[:15]:
            print(f"     - {game['app_id']}: {game['name']} ({game['release_date']})")
        if len(missing) > 15:
            print(f"     ... and {len(missing) - 15} more")

    return {"missing": missing, "present": present_games}


def load_pipeline_module():
    """Load SteamAPI / SupabasePipeline from the 01 pipeline script."""
    module_path = Path(__file__).resolve().parent / "01_steam_to_supabase_pipeline.py"
    spec = importlib.util.spec_from_file_location("steam_to_supabase_pipeline", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def ingest_missing_games(missing_games, fetch_tags=True, limit=None):
    """
    Fetch Steam details for missing app IDs and insert them via 01 helpers.
    Only inserts apps Steam reports as type == 'game'.
    """
    if not missing_games:
        print("\n✅ No missing games to ingest")
        return {
            "attempted": 0,
            "added": 0,
            "not_found": 0,
            "skipped_non_game": 0,
            "failed": 0,
            "tags_collected": 0,
            "added_app_ids": [],
        }

    to_process = missing_games[:limit] if limit else missing_games
    print(f"\n🔄 Ingesting {len(to_process)} missing games into Supabase...")
    if limit and len(missing_games) > limit:
        print(f"   (limited from {len(missing_games)} missing)")

    pipeline_mod = load_pipeline_module()
    steam_api = pipeline_mod.SteamAPI()
    pipeline = pipeline_mod.SupabasePipeline()

    stats = {
        "attempted": 0,
        "added": 0,
        "not_found": 0,
        "skipped_non_game": 0,
        "failed": 0,
        "tags_collected": 0,
        "added_app_ids": [],
    }

    if not pipeline.connect():
        stats["failed"] = len(to_process)
        return stats

    try:
        total = len(to_process)
        for idx, game in enumerate(to_process, 1):
            app_id = game["app_id"]
            listing_name = game.get("name") or f"App {app_id}"
            stats["attempted"] += 1

            print(f"\n🔄 [{idx}/{total}] Ingesting {listing_name} (ID: {app_id})...")

            details = steam_api.get_app_details(app_id)
            if not details:
                print(f"   ❌ Not found on Steam")
                stats["not_found"] += 1
                continue

            name = details.get("name", listing_name)
            app_type = (details.get("type") or "").lower()
            print(f"   ✅ Found: {name} (type={app_type})")

            if app_type != "game":
                print(f"   ⏭️ Skipping non-game")
                stats["skipped_non_game"] += 1
                continue

            review_summary = steam_api.get_review_summary(app_id)

            # One store-page fetch → tags + AI / kernel / DRM / EULA notices
            print(f"   🧾 Fetching store-page notices/tags...")
            extras = steam_api.get_store_page_extras(app_id)
            notices = extras.get("notices") or {}
            tags = extras.get("tags") or []

            if pipeline.insert_game(app_id, name, details, review_summary, store_notices=notices):
                print(f"   ✅ Added {name} to games table")
                stats["added"] += 1
                stats["added_app_ids"].append(app_id)

                if fetch_tags:
                    if tags and pipeline.insert_game_tags(app_id, tags):
                        stats["tags_collected"] += len(tags)
                        print(f"   🏷️  Collected {len(tags)} tags")
                    elif not tags:
                        print(f"   📭 No tags found")
            else:
                print(f"   ❌ Insert failed for {name}")
                stats["failed"] += 1

            if idx % 25 == 0:
                print(
                    f"\n📈 Ingest progress: {idx}/{total} | "
                    f"added {stats['added']} | not found {stats['not_found']} | "
                    f"non-game {stats['skipped_non_game']} | failed {stats['failed']}"
                )
    except KeyboardInterrupt:
        print("\n⚠️  Ingest interrupted — completed inserts remain committed")
    except Exception as e:
        print(f"❌ Error during ingest: {e}")
    finally:
        pipeline.disconnect()

    print("\n🎉 Ingest finished")
    print(f"   Attempted:     {stats['attempted']}")
    print(f"   Added:         {stats['added']}")
    print(f"   Not found:     {stats['not_found']}")
    print(f"   Non-game:      {stats['skipped_non_game']}")
    print(f"   Failed:        {stats['failed']}")
    print(f"   Tags collected:{stats['tags_collected']}")
    return stats


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Collect Steam game app IDs released in a given month via store search, "
            "optionally ingest missing ones into Supabase."
        )
    )
    parser.add_argument(
        "month_or_year",
        nargs="?",
        default=None,
        help="Month as YYYY-MM, or year as YYYY when followed by month number. "
        "Omit to use the current calendar month.",
    )
    parser.add_argument(
        "day_or_month",
        nargs="?",
        default=None,
        help="Month number 1-12 when first arg is YYYY",
    )
    parser.add_argument(
        "--check-db",
        action="store_true",
        help="Compare collected app IDs against the games table",
    )
    parser.add_argument(
        "--ingest",
        action="store_true",
        help="Insert missing month releases into the games table (implies --check-db)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="With --ingest, only process the first N missing app IDs",
    )
    parser.add_argument(
        "--no-tags",
        action="store_true",
        help="With --ingest, skip fetching tags for newly added games",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Optional cap on search pages fetched after the start offset (for testing)",
    )
    parser.add_argument(
        "--start",
        type=int,
        default=None,
        help=(
            "Steam search offset to begin at (0-based). "
            "Use this to skip newer months, e.g. --start 3300"
        ),
    )
    parser.add_argument(
        "--start-page",
        type=int,
        default=None,
        help=(
            f"1-based page number to begin at (page size {PAGE_SIZE}). "
            "Alternative to --start; e.g. --start-page 34 == --start 3300"
        ),
    )
    parser.add_argument(
        "--max-offset",
        type=int,
        default=None,
        help=(
            "Optional absolute Steam search offset to stop at. "
            "There is no default 5000 cap anymore — older months need to go deeper."
        ),
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output JSON path (default: releases_YYYY_MM.json)",
    )
    args = parser.parse_args()

    try:
        year, month = parse_month_args(args)
    except ValueError as e:
        parser.error(str(e))

    if args.start is not None and args.start_page is not None:
        parser.error("Use only one of --start or --start-page")
    if args.start is not None and args.start < 0:
        parser.error("--start must be >= 0")
    if args.start_page is not None and args.start_page < 1:
        parser.error("--start-page must be >= 1")

    start_offset = 0
    if args.start is not None:
        start_offset = args.start
    elif args.start_page is not None:
        start_offset = (args.start_page - 1) * PAGE_SIZE

    if args.ingest:
        args.check_db = True

    month_start, month_end = month_bounds(year, month)
    scan_start, scan_end, _ = scan_bounds(year, month)

    print("Starting Steam monthly release collector")
    if args.month_or_year is None:
        print(f"📅 No month given — defaulting to current month ({year}-{month:02d})")
    print("=" * 50)

    games = collect_releases_for_month(
        year,
        month,
        max_pages=args.max_pages,
        start_offset=start_offset,
        max_offset=args.max_offset,
    )

    output_path = Path(args.output) if args.output else Path(f"releases_{year}_{month:02d}.json")
    payload = {
        "year": year,
        "month": month,
        "month_start": month_start.isoformat(),
        "month_end": month_end.isoformat(),
        "scan_start": scan_start.isoformat(),
        "scan_end": scan_end.isoformat(),
        "includes_previous_month_last_day": True,
        "collected_at": datetime.now().isoformat(timespec="seconds"),
        "count": len(games),
        "games": games,
    }

    db_result = None
    ingest_stats = None
    if args.check_db:
        db_result = check_against_database(games)
        if db_result is not None:
            payload["in_db_count"] = len(db_result["present"])
            payload["missing_count"] = len(db_result["missing"])
            payload["missing_app_ids"] = [g["app_id"] for g in db_result["missing"]]

            if args.ingest:
                ingest_stats = ingest_missing_games(
                    db_result["missing"],
                    fetch_tags=not args.no_tags,
                    limit=args.limit,
                )
                payload["ingest"] = {
                    "attempted": ingest_stats["attempted"],
                    "added": ingest_stats["added"],
                    "not_found": ingest_stats["not_found"],
                    "skipped_non_game": ingest_stats["skipped_non_game"],
                    "failed": ingest_stats["failed"],
                    "tags_collected": ingest_stats["tags_collected"],
                    "added_app_ids": ingest_stats["added_app_ids"],
                }

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"\n💾 Saved results to {output_path}")
    if db_result is not None:
        print(f"📌 Missing from DB before ingest: {len(db_result['missing'])}")
    if ingest_stats is not None:
        print(f"✅ Newly added to games: {ingest_stats['added']}")


if __name__ == "__main__":
    main()

