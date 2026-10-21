import os
import sys
import psycopg2
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


SQL_NORMALIZE_MAJ = (
    """
    UPDATE games
    SET release_date = jsonb_set(
        release_date::jsonb,
        '{date}',
        to_jsonb(replace(release_date::jsonb->>'date', 'maj', 'May'))
    )::text
    WHERE release_date::jsonb->>'date' LIKE '%maj%';
    """
)

SQL_NORMALIZE_OKT = (
    """
    UPDATE games
    SET release_date = jsonb_set(
        release_date::jsonb,
        '{date}',
        to_jsonb(replace(release_date::jsonb->>'date', 'okt', 'Oct'))
    )::text
    WHERE release_date::jsonb->>'date' LIKE '%okt%';
    """
)

# Additional month name normalizations for common variations
# Using multiple simpler UPDATE statements to avoid deeply nested replace() calls
SQL_NORMALIZE_MONTHS = [
    ("UPDATE games SET release_date = jsonb_set(release_date::jsonb, '{date}', to_jsonb(replace(release_date::jsonb->>'date', 'janv', 'Jan')))::text WHERE release_date::jsonb->>'date' LIKE '%janv%';"),
    ("UPDATE games SET release_date = jsonb_set(release_date::jsonb, '{date}', to_jsonb(replace(release_date::jsonb->>'date', 'févr', 'Feb')))::text WHERE release_date::jsonb->>'date' LIKE '%févr%';"),
    ("UPDATE games SET release_date = jsonb_set(release_date::jsonb, '{date}', to_jsonb(replace(release_date::jsonb->>'date', 'mars', 'Mar')))::text WHERE release_date::jsonb->>'date' LIKE '%mars%';"),
    ("UPDATE games SET release_date = jsonb_set(release_date::jsonb, '{date}', to_jsonb(replace(release_date::jsonb->>'date', 'avr', 'Apr')))::text WHERE release_date::jsonb->>'date' LIKE '%avr%';"),
    ("UPDATE games SET release_date = jsonb_set(release_date::jsonb, '{date}', to_jsonb(replace(release_date::jsonb->>'date', 'juin', 'Jun')))::text WHERE release_date::jsonb->>'date' LIKE '%juin%';"),
    ("UPDATE games SET release_date = jsonb_set(release_date::jsonb, '{date}', to_jsonb(replace(release_date::jsonb->>'date', 'juil', 'Jul')))::text WHERE release_date::jsonb->>'date' LIKE '%juil%';"),
    ("UPDATE games SET release_date = jsonb_set(release_date::jsonb, '{date}', to_jsonb(replace(release_date::jsonb->>'date', 'août', 'Aug')))::text WHERE release_date::jsonb->>'date' LIKE '%août%';"),
    ("UPDATE games SET release_date = jsonb_set(release_date::jsonb, '{date}', to_jsonb(replace(release_date::jsonb->>'date', 'sept', 'Sep')))::text WHERE release_date::jsonb->>'date' LIKE '%sept%';"),
    ("UPDATE games SET release_date = jsonb_set(release_date::jsonb, '{date}', to_jsonb(replace(release_date::jsonb->>'date', 'nov', 'Nov')))::text WHERE release_date::jsonb->>'date' LIKE '%nov%' AND release_date::jsonb->>'date' NOT LIKE '%okt%';"),
    ("UPDATE games SET release_date = jsonb_set(release_date::jsonb, '{date}', to_jsonb(replace(release_date::jsonb->>'date', 'déc', 'Dec')))::text WHERE release_date::jsonb->>'date' LIKE '%déc%';"),
]

SQL_UPDATE_RELEASE_DATE_ACTUAL = (
    """
    UPDATE games
    SET release_date_actual =
        CASE
            -- Full date formats: "DD Mon, YYYY" (e.g., "15 Jan, 2025")
            WHEN release_date::jsonb->>'date' ~ '^[0-9]+\\s[A-Za-z]+,\\s[0-9]{4}$'
            THEN to_date(release_date::jsonb->>'date', 'DD Mon, YYYY')
            
            -- Full date formats: "Mon DD, YYYY" (e.g., "Jan 15, 2025")
            WHEN release_date::jsonb->>'date' ~ '^[A-Za-z]+\\s[0-9]+,\\s[0-9]{4}$'
            THEN to_date(release_date::jsonb->>'date', 'Mon DD, YYYY')
            
            -- ISO format: "YYYY-MM-DD" (e.g., "2025-01-15")
            WHEN release_date::jsonb->>'date' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN to_date(release_date::jsonb->>'date', 'YYYY-MM-DD')
            
            -- Month and year: "Mon YYYY" (e.g., "Jan 2025") - use first day of month
            WHEN release_date::jsonb->>'date' ~ '^[A-Za-z]+\\s[0-9]{4}$'
            THEN to_date('1 ' || release_date::jsonb->>'date', 'DD Mon YYYY')
            
            -- Year only: "YYYY" (e.g., "2025") - use January 1st
            WHEN release_date::jsonb->>'date' ~ '^[0-9]{4}$'
            THEN to_date(release_date::jsonb->>'date' || '-01-01', 'YYYY-MM-DD')
            
            -- Slash formats: "DD/MM/YYYY" (assume DD/MM/YYYY format, more common internationally)
            -- Note: If this fails for some dates, we may need to add MM/DD/YYYY handling
            WHEN release_date::jsonb->>'date' ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
            THEN to_date(release_date::jsonb->>'date', 'DD/MM/YYYY')
            
            ELSE release_date_actual
        END
    WHERE
        release_date_actual IS NULL
        AND (release_date::jsonb->>'coming_soon')::boolean = false
        AND release_date::jsonb->>'date' IS NOT NULL
        AND trim(release_date::jsonb->>'date') <> ''
        AND release_date::jsonb->>'date' !~* '(coming soon|tba|tbd|to be announced)';
    """
)


def main() -> None:
    connection_string = os.getenv('SUPABASE_CONNECTION_STRING')
    if not connection_string:
        print('❌ SUPABASE_CONNECTION_STRING environment variable not set')
        sys.exit(1)

    conn = None
    try:
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()
        print('✅ Connected to Supabase database')

        # Ensure we run as a single transaction
        print('🔧 Normalizing month names (maj → May)...')
        cursor.execute(SQL_NORMALIZE_MAJ)
        maj_rows = cursor.rowcount
        print(f'   → Updated {maj_rows} rows')

        print('🔧 Normalizing month names (okt → Oct)...')
        cursor.execute(SQL_NORMALIZE_OKT)
        okt_rows = cursor.rowcount
        print(f'   → Updated {okt_rows} rows')

        print('🔧 Normalizing other month name variations...')
        total_month_rows = 0
        for sql in SQL_NORMALIZE_MONTHS:
            cursor.execute(sql)
            total_month_rows += cursor.rowcount
        print(f'   → Updated {total_month_rows} rows total')

        print('🗓️  Parsing and updating release_date_actual...')
        cursor.execute(SQL_UPDATE_RELEASE_DATE_ACTUAL)
        upd_rows = cursor.rowcount
        print(f'   → Updated {upd_rows} rows')

        # Diagnostic: Count games with unparsed dates
        cursor.execute("""
            SELECT COUNT(*) 
            FROM games 
            WHERE release_date_actual IS NULL
            AND type = 'game'
            AND (release_date::jsonb->>'coming_soon')::boolean = false
            AND release_date::jsonb->>'date' IS NOT NULL
            AND trim(release_date::jsonb->>'date') <> ''
            AND release_date::jsonb->>'date' !~* '(coming soon|tba|tbd|to be announced)'
        """)
        unparsed_count = cursor.fetchone()[0]
        print(f'📊 Games with unparsed dates (after update): {unparsed_count}')

        # Diagnostic: Count recent games (last 13 days)
        cursor.execute("""
            SELECT COUNT(*) 
            FROM games 
            WHERE release_date_actual IS NOT NULL
            AND release_date_actual >= CURRENT_DATE - INTERVAL '13 days'
            AND type = 'game'
        """)
        recent_count = cursor.fetchone()[0]
        print(f'📊 Games released in last 13 days: {recent_count}')

        conn.commit()
        print('✅ release_date_actual update completed successfully')

    except Exception as e:
        if conn:
            conn.rollback()
        print(f'❌ Error updating release_date_actual: {str(e)}')
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            print('✅ Disconnected from Supabase database')


if __name__ == '__main__':
    main()


