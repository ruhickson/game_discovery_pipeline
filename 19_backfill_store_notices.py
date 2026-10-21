#!/usr/bin/env python3
"""
Backfill Steam store-page disclosure notices month-by-month.

Updates games released in the lookback window with:
  made_with_ai, requires_kernel_anticheat, has_third_party_drm / has_denuvo,
  requires_eula (+ related text fields).

Only scrapes games that already exist in Supabase (does not ingest new apps).
Skips rows that already have store_notices_updated_at unless --force.

Usage:
  python 19_backfill_store_notices.py
  python 19_backfill_store_notices.py --years 5
  python 19_backfill_store_notices.py --start-month 2024-01
  python 19_backfill_store_notices.py --month 2024-06 --limit 100
  python 19_backfill_store_notices.py --newest-first
  python 19_backfill_store_notices.py --force
"""

from __future__ import annotations

import argparse
import calendar
import importlib.util
import os
import random
import sys
import time
from datetime import date, datetime
from pathlib import Path

from dotenv import load_dotenv

from steam_store_notices import empty_store_notices, parse_store_notices

load_dotenv()

try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def load_pipeline_module():
    module_path = Path(__file__).resolve().parent / "01_steam_to_supabase_pipeline.py"
    spec = importlib.util.spec_from_file_location("steam_to_supabase_pipeline", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def month_start(d: date) -> date:
    return date(d.year, d.month, 1)


def add_months(d: date, months: int) -> date:
    year = d.year + (d.month - 1 + months) // 12
    month = (d.month - 1 + months) % 12 + 1
    return date(year, month, 1)


def month_end(d: date) -> date:
    last = calendar.monthrange(d.year, d.month)[1]
    return date(d.year, d.month, last)


def parse_yyyy_mm(value: str) -> date:
    year_str, month_str = value.split("-", 1)
    year, month = int(year_str), int(month_str)
    if month < 1 or month > 12:
        raise ValueError(f"Invalid month: {value}")
    return date(year, month, 1)


def iter_months(start: date, end: date, newest_first: bool = False):
    """Yield first-of-month dates from start through end inclusive."""
    start = month_start(start)
    end = month_start(end)
    months = []
    cur = start
    while cur <= end:
        months.append(cur)
        cur = add_months(cur, 1)
    if newest_first:
        months.reverse()
    return months


def fetch_month_games(cursor, month: date, force: bool, limit: int | None):
    start = month_start(month)
    end = month_end(month)
    sql = """
        SELECT app_id, name
        FROM games
        WHERE type = 'game'
          AND release_date_actual IS NOT NULL
          AND release_date_actual::date BETWEEN %s AND %s
    """
    params = [start, end]
    if not force:
        sql += " AND store_notices_updated_at IS NULL"
    sql += " ORDER BY release_date_actual ASC, app_id ASC"
    if limit:
        sql += " LIMIT %s"
        params.append(limit)

    cursor.execute(sql, params)
    return cursor.fetchall()


def update_store_notices(cursor, conn, app_id: int, notices: dict):
    cursor.execute(
        """
        UPDATE games SET
            made_with_ai = %s,
            made_with_ai_disclosure = %s,
            requires_kernel_anticheat = %s,
            kernel_anticheat_name = %s,
            has_third_party_drm = %s,
            third_party_drm = %s,
            has_denuvo = %s,
            requires_eula = %s,
            eula_label = %s,
            store_notices_updated_at = %s
        WHERE app_id = %s
        """,
        (
            bool(notices.get("made_with_ai")),
            notices.get("made_with_ai_disclosure"),
            bool(notices.get("requires_kernel_anticheat")),
            notices.get("kernel_anticheat_name"),
            bool(notices.get("has_third_party_drm")),
            notices.get("third_party_drm"),
            bool(notices.get("has_denuvo")),
            bool(notices.get("requires_eula")),
            notices.get("eula_label"),
            datetime.now(),
            app_id,
        ),
    )
    conn.commit()


def backfill_month(steam_api, cursor, conn, month: date, force: bool, limit: int | None):
    label = month.strftime("%Y-%m")
    games = fetch_month_games(cursor, month, force=force, limit=limit)
    total = len(games)
    print(f"\n{'=' * 60}")
    print(f"📅 Month {label}: {total} game(s) to process")
    print(f"{'=' * 60}")

    if total == 0:
        return {"month": label, "total": 0, "updated": 0, "failed": 0, "empty_page": 0}

    updated = 0
    failed = 0
    empty_page = 0
    flags = {"ai": 0, "kernel": 0, "drm": 0, "denuvo": 0, "eula": 0}

    for idx, (app_id, name) in enumerate(games, 1):
        display = name or f"App {app_id}"
        print(f"\n🔄 [{idx}/{total}] {display} ({app_id})")
        try:
            html = steam_api.get_store_page_html(app_id)
            if not html:
                empty_page += 1
                failed += 1
                # Do NOT stamp store_notices_updated_at — leave row eligible for retry
                print("   📭 No store page — left unstamped for retry")
                continue

            notices = parse_store_notices(html)
            update_store_notices(cursor, conn, app_id, notices)
            updated += 1
            if notices["made_with_ai"]:
                flags["ai"] += 1
            if notices["requires_kernel_anticheat"]:
                flags["kernel"] += 1
            if notices["has_third_party_drm"]:
                flags["drm"] += 1
            if notices["has_denuvo"]:
                flags["denuvo"] += 1
            if notices["requires_eula"]:
                flags["eula"] += 1

            print(
                f"   ✅ ai={notices['made_with_ai']} "
                f"kernel={notices['requires_kernel_anticheat']} "
                f"drm={notices['has_third_party_drm']} "
                f"denuvo={notices['has_denuvo']} "
                f"eula={notices['requires_eula']}"
            )
        except KeyboardInterrupt:
            raise
        except Exception as e:
            failed += 1
            print(f"   ❌ Error: {e}")
            try:
                conn.rollback()
            except Exception:
                pass

        if idx % 25 == 0:
            print(
                f"\n📈 {label} progress {idx}/{total} | "
                f"updated {updated} | failed {failed} | "
                f"flags ai={flags['ai']} kernel={flags['kernel']} "
                f"denuvo={flags['denuvo']} eula={flags['eula']}"
            )

        # Extra jitter beyond SteamAPI's internal delay
        time.sleep(random.uniform(0.1, 0.3))

    print(
        f"\n✅ {label} done — updated {updated}/{total}, failed {failed}, "
        f"empty_page {empty_page} | "
        f"ai={flags['ai']} kernel={flags['kernel']} drm={flags['drm']} "
        f"denuvo={flags['denuvo']} eula={flags['eula']}"
    )
    return {
        "month": label,
        "total": total,
        "updated": updated,
        "failed": failed,
        "empty_page": empty_page,
        **flags,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Backfill AI/kernel/DRM/EULA store notices month-by-month."
    )
    parser.add_argument(
        "--years",
        type=float,
        default=5,
        help="How many years back from today to cover (default: 5)",
    )
    parser.add_argument(
        "--start-month",
        type=str,
        default=None,
        help="Override start month YYYY-MM (inclusive)",
    )
    parser.add_argument(
        "--end-month",
        type=str,
        default=None,
        help="Override end month YYYY-MM (inclusive, default: current month)",
    )
    parser.add_argument(
        "--month",
        type=str,
        default=None,
        help="Process only this YYYY-MM month",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max games per month (for testing)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-scrape games that already have store_notices_updated_at",
    )
    parser.add_argument(
        "--newest-first",
        action="store_true",
        help="Process months from newest to oldest (default: oldest first)",
    )
    parser.add_argument(
        "--clear-empty-stamps",
        action="store_true",
        help=(
            "Before running, clear store_notices_updated_at for games stamped with "
            "all-false/empty notices (recovers rows wrongly marked done after 502s)"
        ),
    )
    args = parser.parse_args()

    today = date.today()
    if args.month:
        start = end = parse_yyyy_mm(args.month)
    else:
        end = parse_yyyy_mm(args.end_month) if args.end_month else month_start(today)
        if args.start_month:
            start = parse_yyyy_mm(args.start_month)
        else:
            # N years ≈ N*12 months inclusive of current month
            months_back = int(round(args.years * 12)) - 1
            start = add_months(month_start(today), -months_back)

    months = iter_months(start, end, newest_first=args.newest_first)

    print("🧾 Store Notices Backfill")
    print("=" * 60)
    print(f"📅 Range: {months[0].strftime('%Y-%m')} → {months[-1].strftime('%Y-%m')} ({len(months)} months)")
    print(f"↪️  Order: {'newest-first' if args.newest_first else 'oldest-first'}")
    print(f"↪️  Force re-scrape: {args.force}")
    if args.limit:
        print(f"↪️  Per-month limit: {args.limit}")
    print("=" * 60)

    pipeline_mod = load_pipeline_module()
    steam_api = pipeline_mod.SteamAPI()
    pipeline = pipeline_mod.SupabasePipeline()
    if not pipeline.connect():
        sys.exit(1)

    if args.clear_empty_stamps:
        print("🧹 Clearing empty notice stamps so failed 502 rows can be retried...")
        pipeline.cursor.execute(
            """
            UPDATE games
            SET store_notices_updated_at = NULL
            WHERE store_notices_updated_at IS NOT NULL
              AND COALESCE(made_with_ai, FALSE) = FALSE
              AND COALESCE(requires_kernel_anticheat, FALSE) = FALSE
              AND COALESCE(has_third_party_drm, FALSE) = FALSE
              AND COALESCE(has_denuvo, FALSE) = FALSE
              AND COALESCE(requires_eula, FALSE) = FALSE
              AND made_with_ai_disclosure IS NULL
              AND kernel_anticheat_name IS NULL
              AND third_party_drm IS NULL
              AND eula_label IS NULL
            """
        )
        cleared = pipeline.cursor.rowcount
        pipeline.conn.commit()
        print(f"   Cleared {cleared} row(s)")

    results = []
    total_start = time.time()
    try:
        for i, month in enumerate(months, 1):
            print(f"\n📊 Month progress: {i}/{len(months)}")
            result = backfill_month(
                steam_api,
                pipeline.cursor,
                pipeline.conn,
                month,
                force=args.force,
                limit=args.limit,
            )
            results.append(result)
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted — progress already committed per game")
    finally:
        pipeline.disconnect()

    elapsed = time.time() - total_start
    print(f"\n{'=' * 60}")
    print("📊 BACKFILL SUMMARY")
    print(f"{'=' * 60}")
    print(f"⏱️  Duration: {elapsed:.1f}s ({elapsed / 60:.1f} min)")
    total_games = sum(r["total"] for r in results)
    total_updated = sum(r["updated"] for r in results)
    total_failed = sum(r["failed"] for r in results)
    print(f"Games seen:    {total_games}")
    print(f"Updated:       {total_updated}")
    print(f"Failed/empty:  {total_failed}")
    for r in results:
        if r["total"] == 0:
            continue
        print(
            f"  {r['month']}: {r['updated']}/{r['total']} "
            f"(ai={r.get('ai', 0)} kernel={r.get('kernel', 0)} "
            f"denuvo={r.get('denuvo', 0)} eula={r.get('eula', 0)})"
        )


if __name__ == "__main__":
    main()
