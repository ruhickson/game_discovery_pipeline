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

SQL_UPDATE_RELEASE_DATE_ACTUAL = (
    """
    UPDATE games
    SET release_date_actual =
        CASE
            WHEN release_date::jsonb->>'date' ~ '^[0-9]+\\s[A-Za-z]+,\\s[0-9]{4}$'
            THEN to_date(release_date::jsonb->>'date', 'DD Mon, YYYY')
            WHEN release_date::jsonb->>'date' ~ '^[A-Za-z]+\\s[0-9]+,\\s[0-9]{4}$'
            THEN to_date(release_date::jsonb->>'date', 'Mon DD, YYYY')
            ELSE release_date_actual
        END
    WHERE
        release_date_actual IS NULL
        AND (release_date::jsonb->>'coming_soon')::boolean = false
        AND release_date::jsonb->>'date' IS NOT NULL
        AND trim(release_date::jsonb->>'date') <> '';
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

        print('🗓️  Parsing and updating release_date_actual...')
        cursor.execute(SQL_UPDATE_RELEASE_DATE_ACTUAL)
        upd_rows = cursor.rowcount
        print(f'   → Updated {upd_rows} rows')

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


