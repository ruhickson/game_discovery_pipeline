#!/usr/bin/env python3
"""
Build state_of_steam_monthly — one JSON snapshot per calendar month for Time Machine.

Mirrors the State of Steam dashboard payload so the frontend can load a month
with a single Cube/DB read instead of a live query fan-out.
"""

from __future__ import annotations

import json
import os
import sys
from calendar import monthrange
from datetime import date, datetime, timedelta
from decimal import Decimal

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

STEAM_LAUNCH = date(2003, 9, 1)
BASE_GAMES_SQL = """
    type = 'game'
    AND name NOT ILIKE '%%playtest%%'
    AND release_date_actual IS NOT NULL
    AND (
      release_date::jsonb->>'coming_soon' IS NULL
      OR release_date::jsonb->>'coming_soon' = 'false'
    )
"""


def month_label(year: int, month: int) -> str:
    return date(year, month, 1).strftime("%b %Y")


def ym(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def json_default(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


class StateOfSteamMonthlyBuilder:
    def __init__(self):
        self.connection_string = os.getenv("SUPABASE_CONNECTION_STRING")
        self.conn = None
        self.cursor = None

    def connect(self) -> bool:
        try:
            self.conn = psycopg2.connect(self.connection_string)
            self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            print("✅ Connected to Supabase database")
            return True
        except Exception as e:
            print(f"❌ Error connecting to Supabase: {e}")
            return False

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("✅ Disconnected from Supabase database")

    def ensure_table(self) -> bool:
        try:
            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS state_of_steam_monthly (
                    month_key TEXT PRIMARY KEY,
                    label TEXT NOT NULL,
                    year INT NOT NULL,
                    month_index INT NOT NULL,
                    selected_month_releases INT NOT NULL DEFAULT 0,
                    previous_month_releases INT NOT NULL DEFAULT 0,
                    selected_year_releases INT NOT NULL DEFAULT 0,
                    previous_year_releases INT NOT NULL DEFAULT 0,
                    released_to_date INT NOT NULL DEFAULT 0,
                    payload JSONB NOT NULL,
                    built_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self.cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_sos_monthly_year
                ON state_of_steam_monthly(year)
                """
            )
            self.conn.commit()
            print("✅ Ensured state_of_steam_monthly table exists")
            return True
        except Exception as e:
            self.conn.rollback()
            print(f"❌ Error creating table: {e}")
            return False

    def fetch_monthly_counts(self) -> dict[str, int]:
        self.cursor.execute(
            f"""
            SELECT to_char(date_trunc('month', release_date_actual), 'YYYY-MM') AS month_key,
                   COUNT(*)::int AS cnt
            FROM games
            WHERE {BASE_GAMES_SQL}
            GROUP BY 1
            ORDER BY 1
            """
        )
        return {row["month_key"]: int(row["cnt"]) for row in self.cursor.fetchall()}

    def derive_historical(self, monthly: dict[str, int], year: int, month: int) -> dict:
        selected = f"{year:04d}-{month:02d}"
        prev = date(year, month, 1) - timedelta(days=1)
        prev_key = ym(prev)

        selected_year = 0
        prev_year = 0
        to_date = 0
        for key, cnt in monthly.items():
            y = int(key.split("-")[0])
            if y == year and key <= selected:
                selected_year += cnt
            if y == year - 1:
                prev_year += cnt
            if key <= selected:
                to_date += cnt

        return {
            "previous_month_releases": monthly.get(prev_key, 0),
            "selected_year_releases": selected_year,
            "previous_year_releases": prev_year,
            "released_to_date": to_date,
            "selected_month_releases": monthly.get(selected, 0),
        }

    def build_month_payload(self, year: int, month: int, monthly: dict[str, int]) -> dict:
        start = date(year, month, 1)
        end = date(year, month, monthrange(year, month)[1])
        today = date.today()
        is_current = year == today.year and month == today.month
        range_end = min(end, today) if is_current else end

        hist = self.derive_historical(monthly, year, month)

        # Daily series
        self.cursor.execute(
            f"""
            SELECT release_date_actual::date AS d, COUNT(*)::int AS cnt
            FROM games
            WHERE {BASE_GAMES_SQL}
              AND release_date_actual::date BETWEEN %s AND %s
            GROUP BY 1
            ORDER BY 1
            """,
            (start, range_end),
        )
        by_day = {row["d"].isoformat(): int(row["cnt"]) for row in self.cursor.fetchall()}
        daily = []
        cursor = start
        while cursor <= range_end:
            key = cursor.isoformat()
            daily.append({"date": key, "count": by_day.get(key, 0)})
            cursor += timedelta(days=1)

        # Genres
        self.cursor.execute(
            f"""
            SELECT g.genre_name AS genre, COUNT(DISTINCT games.app_id)::int AS cnt
            FROM games
            JOIN genres g ON g.game_app_id = games.app_id
            WHERE {BASE_GAMES_SQL}
              AND games.release_date_actual::date BETWEEN %s AND %s
              AND g.genre_name IS NOT NULL
            GROUP BY 1
            ORDER BY cnt DESC
            LIMIT 30
            """,
            (start, range_end),
        )
        genres = [{"genre": row["genre"], "count": int(row["cnt"])} for row in self.cursor.fetchall()]

        # Popular tags
        self.cursor.execute(
            f"""
            SELECT gt.tag AS tag, COUNT(DISTINCT games.app_id)::int AS cnt
            FROM games
            JOIN game_tags gt ON gt.app_id = games.app_id
            WHERE {BASE_GAMES_SQL}
              AND games.release_date_actual::date BETWEEN %s AND %s
              AND gt.tag IS NOT NULL
              AND btrim(gt.tag) <> ''
            GROUP BY 1
            ORDER BY cnt DESC
            LIMIT 20
            """,
            (start, range_end),
        )
        tags = [{"tag": row["tag"], "count": int(row["cnt"])} for row in self.cursor.fetchall()]

        # Free vs paid
        self.cursor.execute(
            f"""
            SELECT
              COUNT(*) FILTER (WHERE COALESCE(is_free, false) = true)::int AS free,
              COUNT(*) FILTER (WHERE COALESCE(is_free, false) = false)::int AS paid
            FROM games
            WHERE {BASE_GAMES_SQL}
              AND release_date_actual::date BETWEEN %s AND %s
            """,
            (start, range_end),
        )
        fp = self.cursor.fetchone() or {}
        free_vs_paid = {"free": int(fp.get("free") or 0), "paid": int(fp.get("paid") or 0)}

        # Review totals
        self.cursor.execute(
            f"""
            SELECT
              COALESCE(SUM(total_positive), 0)::bigint AS positive,
              COALESCE(SUM(total_negative), 0)::bigint AS negative
            FROM games
            WHERE {BASE_GAMES_SQL}
              AND release_date_actual::date BETWEEN %s AND %s
            """,
            (start, range_end),
        )
        rt = self.cursor.fetchone() or {}
        positive = int(rt.get("positive") or 0)
        negative = int(rt.get("negative") or 0)
        total = positive + negative
        review_totals = {
            "positive": positive,
            "negative": negative,
            "total": total,
            "positiveShare": (positive / total * 100.0) if total else 0.0,
        }

        # Review score mix
        self.cursor.execute(
            f"""
            SELECT COALESCE(review_score_desc, 'Unknown') AS score, COUNT(*)::int AS cnt
            FROM games
            WHERE {BASE_GAMES_SQL}
              AND release_date_actual::date BETWEEN %s AND %s
              AND review_score_desc IS NOT NULL
            GROUP BY 1
            ORDER BY cnt DESC
            """,
            (start, range_end),
        )
        review_score_mix = [
            {"score": row["score"], "count": int(row["cnt"])} for row in self.cursor.fetchall()
        ]

        # Sentiment pool
        self.cursor.execute(
            f"""
            SELECT
              app_id,
              name,
              COALESCE(total_positive, 0)::int AS positive,
              COALESCE(total_negative, 0)::int AS negative,
              (COALESCE(total_positive, 0) + COALESCE(total_negative, 0))::int AS total_reviews
            FROM games
            WHERE {BASE_GAMES_SQL}
              AND release_date_actual::date BETWEEN %s AND %s
              AND (COALESCE(total_positive, 0) + COALESCE(total_negative, 0)) >= 1000
            ORDER BY total_reviews DESC
            LIMIT 100
            """,
            (start, range_end),
        )
        sentiment_games = []
        for row in self.cursor.fetchall():
            total_reviews = int(row["total_reviews"])
            pos = int(row["positive"])
            neg = int(row["negative"])
            love = (pos / total_reviews * 100.0) if total_reviews else 0.0
            hate = (neg / total_reviews * 100.0) if total_reviews else 0.0
            sentiment_games.append(
                {
                    "appId": row["app_id"],
                    "name": row["name"],
                    "positive": pos,
                    "negative": neg,
                    "totalReviews": total_reviews,
                    "loveRate": love,
                    "hateRate": hate,
                    "mixDistance": abs(love - 50.0),
                }
            )

        most_loved = sorted(
            sentiment_games, key=lambda g: (-g["loveRate"], -g["totalReviews"])
        )[:10]
        most_hated = sorted(
            sentiment_games, key=lambda g: (-g["hateRate"], -g["totalReviews"])
        )[:10]
        most_mixed = sorted(
            sentiment_games, key=lambda g: (g["mixDistance"], -g["totalReviews"])
        )[:10]

        def studio_board(table: str, name_col: str) -> tuple[list, list]:
            self.cursor.execute(
                f"""
                SELECT
                  s.{name_col} AS name,
                  COUNT(DISTINCT games.app_id)::int AS games,
                  COALESCE(SUM(games.total_positive), 0)::bigint AS positive,
                  COALESCE(SUM(games.total_negative), 0)::bigint AS negative
                FROM games
                JOIN {table} s ON s.game_app_id = games.app_id
                WHERE {BASE_GAMES_SQL}
                  AND games.release_date_actual::date BETWEEN %s AND %s
                  AND s.{name_col} IS NOT NULL
                GROUP BY 1
                ORDER BY games DESC
                LIMIT 40
                """,
                (start, range_end),
            )
            studios = []
            for row in self.cursor.fetchall():
                games_n = int(row["games"])
                pos = int(row["positive"])
                neg = int(row["negative"])
                reviews = pos + neg
                studios.append(
                    {
                        "name": row["name"],
                        "games": games_n,
                        "positive": pos,
                        "negative": neg,
                        "reviews": reviews,
                        "loveRate": (pos / reviews * 100.0) if reviews else 0.0,
                        "praisePerGame": (pos / games_n) if games_n else 0.0,
                    }
                )
            prolific = sorted(studios, key=lambda s: (-s["games"], -s["reviews"]))[:10]
            revered = sorted(
                [s for s in studios if s["reviews"] >= 100],
                key=lambda s: (-s["loveRate"], -s["praisePerGame"]),
            )[:10]
            return prolific, revered

        prolific_publishers, revered_publishers = studio_board("publishers", "publisher_name")
        prolific_developers, revered_developers = studio_board("developers", "developer_name")

        selected_month_releases = sum(row["count"] for row in daily) or hist["selected_month_releases"]

        return {
            "releasedInMonthDaily": daily,
            "dueNext7Days": [],
            "genresInMonth": genres,
            "tagsInMonth": tags,
            "reviewTotals": review_totals,
            "reviewScoreMix": review_score_mix,
            "freeVsPaid": free_vs_paid,
            "mostLoved": most_loved,
            "mostHated": most_hated,
            "mostMixed": most_mixed,
            "prolificPublishers": prolific_publishers,
            "reveredPublishers": revered_publishers,
            "prolificDevelopers": prolific_developers,
            "reveredDevelopers": revered_developers,
            "selectedMonthReleases": selected_month_releases,
            "previousMonthReleases": hist["previous_month_releases"],
            "selectedYearReleases": hist["selected_year_releases"],
            "previousYearReleases": hist["previous_year_releases"],
            "releasedToDate": hist["released_to_date"],
            "historicalStatus": "ready",
            "historicalError": None,
            "source": "snapshot",
        }

    def upsert_month(self, year: int, month: int, monthly: dict[str, int]) -> None:
        key = f"{year:04d}-{month:02d}"
        payload = self.build_month_payload(year, month, monthly)
        label = month_label(year, month)
        self.cursor.execute(
            """
            INSERT INTO state_of_steam_monthly (
                month_key, label, year, month_index,
                selected_month_releases, previous_month_releases,
                selected_year_releases, previous_year_releases, released_to_date,
                payload, built_at
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s::jsonb, CURRENT_TIMESTAMP
            )
            ON CONFLICT (month_key) DO UPDATE SET
                label = EXCLUDED.label,
                year = EXCLUDED.year,
                month_index = EXCLUDED.month_index,
                selected_month_releases = EXCLUDED.selected_month_releases,
                previous_month_releases = EXCLUDED.previous_month_releases,
                selected_year_releases = EXCLUDED.selected_year_releases,
                previous_year_releases = EXCLUDED.previous_year_releases,
                released_to_date = EXCLUDED.released_to_date,
                payload = EXCLUDED.payload,
                built_at = CURRENT_TIMESTAMP
            """,
            (
                key,
                label,
                year,
                month - 1,
                payload["selectedMonthReleases"],
                payload["previousMonthReleases"],
                payload["selectedYearReleases"],
                payload["previousYearReleases"],
                payload["releasedToDate"],
                json.dumps(payload, default=json_default),
            ),
        )

    def build_all(self) -> bool:
        try:
            monthly = self.fetch_monthly_counts()
            print(f"📊 Found release counts for {len(monthly)} months")

            today = date.today()
            cursor = STEAM_LAUNCH
            built = 0
            while cursor <= today:
                self.upsert_month(cursor.year, cursor.month, monthly)
                built += 1
                if built % 24 == 0:
                    self.conn.commit()
                    print(f"   … built through {ym(cursor)}")
                # advance one month
                if cursor.month == 12:
                    cursor = date(cursor.year + 1, 1, 1)
                else:
                    cursor = date(cursor.year, cursor.month + 1, 1)

            self.conn.commit()
            print(f"✅ Built/updated {built} month snapshots in state_of_steam_monthly")
            return True
        except Exception as e:
            self.conn.rollback()
            print(f"❌ Error building snapshots: {e}")
            raise


def main():
    builder = StateOfSteamMonthlyBuilder()
    if not builder.connect():
        return 1
    try:
        print("🔄 Building State of Steam monthly snapshots")
        print("=" * 50)
        if not builder.ensure_table():
            return 1
        builder.build_all()
        return 0
    finally:
        builder.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
