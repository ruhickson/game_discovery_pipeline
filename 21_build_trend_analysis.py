#!/usr/bin/env python3
"""
Build trend_release_counts — daily genre, tag, and total release counts.

Trend Analysis compares a month with the month before it, and the current
month is cut off on today's day. Storing one count per release day lets that
page sum a date range instead of joining games to genres and tags live.

Usage:
  python 21_build_trend_analysis.py            # previous month through today
  python 21_build_trend_analysis.py 2024-01    # that month through today
  python 21_build_trend_analysis.py --all      # September 2003 through today
"""

from __future__ import annotations

import os
import sys
from datetime import date

import psycopg2
from dotenv import load_dotenv

load_dotenv()

STEAM_LAUNCH = date(2003, 9, 1)

BASE_WHERE = """
    g.type = 'game'
    AND g.name NOT ILIKE '%%playtest%%'
    AND g.release_date_actual IS NOT NULL
    AND (
      g.release_date::jsonb->>'coming_soon' IS NULL
      OR g.release_date::jsonb->>'coming_soon' = 'false'
    )
    AND g.release_date_actual >= %s
    AND g.release_date_actual < %s
"""

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS trend_release_counts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    release_date date NOT NULL,
    kind text NOT NULL,
    name text NOT NULL,
    game_count integer NOT NULL,
    CONSTRAINT trend_release_counts_kind_check
        CHECK (kind IN ('total', 'genre', 'tag')),
    CONSTRAINT trend_release_counts_date_kind_name_key
        UNIQUE (release_date, kind, name)
)
"""

CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS trend_release_counts_date_kind_idx
ON trend_release_counts (release_date, kind)
"""

DELETE_SQL = """
DELETE FROM trend_release_counts
WHERE release_date >= %s AND release_date < %s
"""

INSERT_TOTAL_SQL = f"""
INSERT INTO trend_release_counts (release_date, kind, name, game_count)
SELECT g.release_date_actual, 'total', '', COUNT(*)::int
FROM games g
WHERE {BASE_WHERE}
GROUP BY g.release_date_actual
"""

INSERT_GENRE_SQL = f"""
INSERT INTO trend_release_counts (release_date, kind, name, game_count)
SELECT g.release_date_actual, 'genre', btrim(gn.genre_name), COUNT(DISTINCT g.app_id)::int
FROM games g
JOIN genres gn ON gn.game_app_id = g.app_id
WHERE {BASE_WHERE}
  AND gn.genre_name IS NOT NULL
  AND btrim(gn.genre_name) <> ''
GROUP BY g.release_date_actual, btrim(gn.genre_name)
"""

INSERT_TAG_SQL = f"""
INSERT INTO trend_release_counts (release_date, kind, name, game_count)
SELECT g.release_date_actual, 'tag', btrim(gt.tag), COUNT(DISTINCT g.app_id)::int
FROM games g
JOIN game_tags gt ON gt.app_id = g.app_id
WHERE {BASE_WHERE}
  AND gt.tag IS NOT NULL
  AND btrim(gt.tag) <> ''
GROUP BY g.release_date_actual, btrim(gt.tag)
"""


def next_month(d: date) -> date:
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)


def month_start(year: int, month: int) -> date:
    return date(year, month, 1)


def parse_start(argv: list[str]) -> date:
    if not argv or argv == ["--all"]:
        if argv == ["--all"]:
            return STEAM_LAUNCH
        today = date.today()
        first = month_start(today.year, today.month)
        previous = date.fromordinal(first.toordinal() - 1)
        return month_start(previous.year, previous.month)
    if len(argv) == 1 and len(argv[0]) == 7 and argv[0][4] == "-":
        year, month = argv[0].split("-")
        return month_start(int(year), int(month))
    print("Usage: python 21_build_trend_analysis.py [--all | YYYY-MM]")
    raise SystemExit(2)


def ensure_table(cur) -> None:
    cur.execute(CREATE_SQL)
    cur.execute(CREATE_INDEX_SQL)


def rebuild_month(cur, start: date) -> int:
    end = next_month(start)
    params = (start, end)
    cur.execute(DELETE_SQL, params)
    inserted = 0
    for sql in (INSERT_TOTAL_SQL, INSERT_GENRE_SQL, INSERT_TAG_SQL):
        cur.execute(sql, params)
        inserted += cur.rowcount
    return inserted


def main() -> int:
    start = parse_start(sys.argv[1:])
    today = date.today()
    print(f"Building trend release counts from {start.isoformat()} through {today.isoformat()}")
    try:
        conn = psycopg2.connect(os.environ["SUPABASE_CONNECTION_STRING"])
    except Exception as exc:
        print(f"Error connecting to Supabase: {exc}")
        return 1

    try:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '5min'")
            ensure_table(cur)
            conn.commit()
            cursor = start
            total_inserted = 0
            while cursor <= today:
                cur.execute("SET statement_timeout = '5min'")
                inserted = rebuild_month(cur, cursor)
                conn.commit()
                total_inserted += inserted
                print(f"  {cursor.year:04d}-{cursor.month:02d}: {inserted} rows")
                cursor = next_month(cursor)
        print(f"Inserted {total_inserted} trend count rows")
        return 0
    except Exception as exc:
        conn.rollback()
        print(f"Error building trend counts: {exc}")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
