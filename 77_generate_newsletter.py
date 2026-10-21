import argparse
import html
import json
import os
import re
import sys
from datetime import date
from pathlib import Path

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

# Provide 6 Steam app_ids here, or pass them on the command line:
# python 77_generate_newsletter.py 3936730 3341000 1116050 2904040 2629230 249680
NEWSLETTER_APP_IDS = []

CREATE_NEWSLETTER_RECOMMENDED_SQL = """
CREATE TABLE IF NOT EXISTS newsletter_recommended (
    app_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    insert_date DATE NOT NULL DEFAULT CURRENT_DATE
);
"""

GAMES_BY_APP_IDS_QUERY = """
    SELECT
        g.app_id,
        g.name,
        g.short_description,
        g.detailed_description,
        g.header_image,
        g.price_overview,
        g.is_free
    FROM games g
    WHERE g.app_id = ANY(%(app_ids)s)
    ORDER BY array_position(%(app_ids)s::integer[], g.app_id)
"""

TAGS_QUERY = """
    SELECT tag
    FROM game_tags
    WHERE app_id = %s
    ORDER BY tag
"""


class SupabasePipeline:
    def __init__(self):
        self.connection_string = os.getenv("SUPABASE_CONNECTION_STRING")
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(self.connection_string)
            self.cursor = self.conn.cursor()
            print("✅ Connected to Supabase database")
            return True
        except Exception as e:
            print(f"❌ Error connecting to Supabase: {str(e)}")
            return False

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("✅ Disconnected from Supabase database")


def parse_app_ids(args):
    if args.app_ids:
        return args.app_ids
    if NEWSLETTER_APP_IDS:
        return NEWSLETTER_APP_IDS
    return None


def validate_app_ids(app_ids):
    if not app_ids:
        return "Provide exactly 6 app_ids via NEWSLETTER_APP_IDS or on the command line."

    if len(app_ids) != 6:
        return f"Expected exactly 6 app_ids, got {len(app_ids)}."

    if len(set(app_ids)) != 6:
        return "All 6 app_ids must be unique."

    return None


def fetch_games_by_app_ids(cursor, app_ids):
    cursor.execute(GAMES_BY_APP_IDS_QUERY, {"app_ids": app_ids})
    rows = cursor.fetchall()

    found_ids = {row[0] for row in rows}
    missing = [app_id for app_id in app_ids if app_id not in found_ids]
    if missing:
        raise ValueError(f"No games found for app_id(s): {', '.join(map(str, missing))}")

    return rows


def strip_html(text):
    if not text:
        return ""
    return re.sub(r"<[^>]+>", "", text).strip()


def get_description(short_description, detailed_description):
    """Use only the game's own description text, without elaboration."""
    for candidate in (short_description, detailed_description):
        cleaned = strip_html(candidate)
        if cleaned:
            return cleaned
    return ""


def format_price(price_overview, is_free):
    if is_free:
        return "Free"
    if not price_overview:
        return "N/A"

    if isinstance(price_overview, str):
        try:
            price_overview = json.loads(price_overview)
        except json.JSONDecodeError:
            return "N/A"

    if isinstance(price_overview, dict):
        return price_overview.get("final_formatted") or "N/A"

    return "N/A"


def steam_url(app_id):
    return f"https://store.steampowered.com/app/{app_id}/"


def itad_url(app_id):
    return f"https://isthereanydeal.com/steam/app/{app_id}/"


def get_tags(cursor, app_id):
    cursor.execute(TAGS_QUERY, (app_id,))
    return [row[0] for row in cursor.fetchall()]


def build_game_paragraph(game_row, tags):
    (
        app_id,
        name,
        short_description,
        detailed_description,
        header_image,
        price_overview,
        is_free,
    ) = game_row

    description = html.escape(get_description(short_description, detailed_description))
    price = html.escape(format_price(price_overview, is_free))
    safe_name = html.escape(name)
    tags_text = html.escape(", ".join(tags)) if tags else "None listed"

    image_html = ""
    if header_image:
        image_html = (
            f'<br><img src="{html.escape(header_image)}" alt="{safe_name}" '
            f'style="max-width:100%;height:auto;" />'
        )

    paragraph = (
        f"<p>"
        f'<strong><a href="{steam_url(app_id)}">{safe_name}</a></strong> | '
        f'<a href="{itad_url(app_id)}">IsThereAnyDeal</a> | '
        f"Price: {price}"
        f"{image_html}"
    )

    if description:
        paragraph += f"<br>{description}"

    paragraph += "</p>"
    paragraph += f'<p class="tags"><em>Tags: {tags_text}</em></p>'

    return paragraph


def build_newsletter_html(game_rows, tags_by_app_id, run_date):
    paragraphs = []
    for index, game_row in enumerate(game_rows):
        if index == 5:
            paragraphs.append("<h2>From the archives</h2>")
        app_id = game_row[0]
        paragraphs.append(build_game_paragraph(game_row, tags_by_app_id.get(app_id, [])))

    body = "\n".join(paragraphs)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Game Discovery Newsletter - {run_date.isoformat()}</title>
</head>
<body>
  <h1>Game Discovery Newsletter</h1>
  <p><em>Generated on {run_date.isoformat()}</em></p>
  {body}
</body>
</html>
"""


def insert_recommended_games(cursor, games, run_date):
    for app_id, name in games:
        cursor.execute(
            """
            INSERT INTO newsletter_recommended (app_id, name, insert_date)
            VALUES (%s, %s, %s)
            ON CONFLICT (app_id) DO NOTHING
            """,
            (app_id, name, run_date),
        )


def main():
    parser = argparse.ArgumentParser(
        description="Generate a newsletter HTML file from 6 Steam app_ids."
    )
    parser.add_argument(
        "app_ids",
        nargs="*",
        type=int,
        help="Exactly 6 Steam app_ids (overrides NEWSLETTER_APP_IDS)",
    )
    args = parser.parse_args()

    app_ids = parse_app_ids(args)
    validation_error = validate_app_ids(app_ids)
    if validation_error:
        parser.error(validation_error)

    pipeline = SupabasePipeline()
    if not pipeline.connect():
        return 1

    run_date = date.today()
    output_dir = Path("newsletters")
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / f"newsletter_{run_date.isoformat()}.html"

    try:
        print("📋 Ensuring newsletter_recommended table exists...")
        pipeline.cursor.execute(CREATE_NEWSLETTER_RECOMMENDED_SQL)
        pipeline.conn.commit()
        print("✅ newsletter_recommended table ready")

        print(f"🎮 Fetching {len(app_ids)} games by app_id...")
        newsletter_games = fetch_games_by_app_ids(pipeline.cursor, app_ids)

        tags_by_app_id = {}
        for game_row in newsletter_games:
            app_id = game_row[0]
            tags_by_app_id[app_id] = get_tags(pipeline.cursor, app_id)
            print(f"   {game_row[1]} (app_id {app_id})")

        newsletter_html = build_newsletter_html(newsletter_games, tags_by_app_id, run_date)
        output_file.write_text(newsletter_html, encoding="utf-8")
        print(f"📄 Newsletter written to {output_file.resolve()}")

        games_to_insert = [(row[0], row[1]) for row in newsletter_games]
        insert_recommended_games(pipeline.cursor, games_to_insert, run_date)
        pipeline.conn.commit()
        print(f"✅ Inserted {len(games_to_insert)} games into newsletter_recommended")

        return 0

    except Exception as e:
        pipeline.conn.rollback()
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return 1
    finally:
        pipeline.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
