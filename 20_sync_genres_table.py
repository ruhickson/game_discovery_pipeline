#!/usr/bin/env python3
"""
Copy Steam genre lists from games.genres into the genres table.

The ingest pipeline stores genres on the game row. State of Steam reads the
separate genres table, which stopped receiving new rows around July 2025.
This script fills any missing rows and installs a trigger so later writes to
games.genres keep the table in step.

Safe to run repeatedly: existing (game, genre) pairs are left as they are.
"""

from __future__ import annotations

import os
import sys
from datetime import date

import psycopg2
from dotenv import load_dotenv

load_dotenv()

try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

PARSE_FUNCTION_SQL = """
CREATE OR REPLACE FUNCTION genres_from_games_text(raw text)
RETURNS TABLE (genre_name text)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  elem jsonb;
BEGIN
  IF raw IS NULL OR btrim(raw) IN ('', '[]', 'null') THEN
    RETURN;
  END IF;

  BEGIN
    FOR elem IN SELECT jsonb_array_elements(raw::jsonb)
    LOOP
      IF jsonb_typeof(elem) = 'object' THEN
        genre_name := btrim(elem->>'description');
      ELSIF jsonb_typeof(elem) = 'string' THEN
        genre_name := btrim(elem #>> '{}');
      ELSE
        genre_name := NULL;
      END IF;
      IF genre_name IS NOT NULL AND genre_name <> '' THEN
        RETURN NEXT;
      END IF;
    END LOOP;
  EXCEPTION WHEN others THEN
    RETURN;
  END;
END;
$$;
"""

TRIGGER_FUNCTION_SQL = """
CREATE OR REPLACE FUNCTION sync_game_genres_from_row()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  parsed_count integer;
BEGIN
  IF TG_OP = 'UPDATE' AND NEW.genres IS NOT DISTINCT FROM OLD.genres THEN
    RETURN NEW;
  END IF;

  IF NEW.genres IS NULL OR btrim(NEW.genres) IN ('', '[]', 'null') THEN
    DELETE FROM genres WHERE game_app_id = NEW.app_id;
    RETURN NEW;
  END IF;

  SELECT COUNT(*) INTO parsed_count FROM genres_from_games_text(NEW.genres);
  IF parsed_count = 0 THEN
    -- Leave any existing rows in place when the payload does not parse.
    RETURN NEW;
  END IF;

  DELETE FROM genres WHERE game_app_id = NEW.app_id;

  INSERT INTO genres (game_app_id, genre_name)
  SELECT DISTINCT NEW.app_id, src.genre_name
  FROM genres_from_games_text(NEW.genres) AS src
  ON CONFLICT (game_app_id, genre_name) DO NOTHING;

  RETURN NEW;
END;
$$;
"""

TRIGGER_SQL = """
DROP TRIGGER IF EXISTS games_sync_genres ON games;
CREATE TRIGGER games_sync_genres
AFTER INSERT OR UPDATE OF genres ON games
FOR EACH ROW
EXECUTE FUNCTION sync_game_genres_from_row();
"""

INSERT_SQL = """
INSERT INTO genres (game_app_id, genre_name)
SELECT DISTINCT g.app_id, src.genre_name
FROM games g
CROSS JOIN LATERAL genres_from_games_text(g.genres) AS src
WHERE g.release_date_actual >= %s
  AND g.release_date_actual < %s
ON CONFLICT (game_app_id, genre_name) DO NOTHING
"""

INSERT_NULL_DATE_SQL = """
INSERT INTO genres (game_app_id, genre_name)
SELECT DISTINCT g.app_id, src.genre_name
FROM games g
CROSS JOIN LATERAL genres_from_games_text(g.genres) AS src
WHERE g.release_date_actual IS NULL
ON CONFLICT (game_app_id, genre_name) DO NOTHING
"""


def connect():
    conn = psycopg2.connect(os.environ["SUPABASE_CONNECTION_STRING"])
    conn.autocommit = False
    return conn


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '5min'")
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS genres_game_app_id_genre_name_key
            ON genres (game_app_id, genre_name)
            """
        )
        cur.execute(PARSE_FUNCTION_SQL)
        cur.execute(TRIGGER_FUNCTION_SQL)
        cur.execute(TRIGGER_SQL)
    conn.commit()
    print("Ensured unique genre index, parse function, and games sync trigger")


def backfill(conn) -> int:
    inserted = 0
    start_year = 2003
    end_year = date.today().year
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '5min'")
        for year in range(start_year, end_year + 1):
            cur.execute(
                INSERT_SQL,
                (date(year, 1, 1), date(year + 1, 1, 1)),
            )
            inserted += cur.rowcount
            conn.commit()
            print(f"  {year}: inserted {cur.rowcount}")
        cur.execute(INSERT_NULL_DATE_SQL)
        inserted += cur.rowcount
        conn.commit()
        print(f"  no release date: inserted {cur.rowcount}")
    return inserted


def main() -> int:
    print("Syncing genres table from games.genres")
    try:
        conn = connect()
    except Exception as exc:
        print(f"Error connecting to Supabase: {exc}")
        return 1

    try:
        ensure_schema(conn)
        inserted = backfill(conn)
        print(f"Inserted {inserted} genre rows")
        return 0
    except Exception as exc:
        conn.rollback()
        print(f"Error syncing genres: {exc}")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
