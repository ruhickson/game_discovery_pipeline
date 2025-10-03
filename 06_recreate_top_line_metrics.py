import psycopg2
from datetime import datetime
import sys
import os
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

class SupabasePipeline:
    def __init__(self):
        self.connection_string = os.getenv('SUPABASE_CONNECTION_STRING')
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Connect to Supabase database."""
        try:
            self.conn = psycopg2.connect(self.connection_string)
            self.cursor = self.conn.cursor()
            print("✅ Connected to Supabase database")
            return True
        except Exception as e:
            print(f"❌ Error connecting to Supabase: {str(e)}")
            return False
    
    def disconnect(self):
        """Disconnect from Supabase database."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("✅ Disconnected from Supabase database")
    
    def drop_top_line_metrics_table(self):
        """Drop the existing top_line_metrics table if it exists."""
        try:
            self.cursor.execute("DROP TABLE IF EXISTS top_line_metrics")
            self.conn.commit()
            print("🗑️  Dropped existing top_line_metrics table")
            return True
        except Exception as e:
            print(f"Error dropping table: {str(e)}")
            return False
    
    def create_top_line_metrics_table(self):
        """Create the top_line_metrics table with proper structure."""
        try:
            self.cursor.execute("""
                CREATE TABLE top_line_metrics (
                    id SERIAL PRIMARY KEY,
                    review_score_desc TEXT NOT NULL,
                    num_games INTEGER NOT NULL,
                    num_reviews INTEGER NOT NULL,
                    avg_num_reviews INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create index for better performance
            self.cursor.execute("""
                CREATE INDEX idx_top_line_metrics_review_score_desc 
                ON top_line_metrics(review_score_desc)
            """)
            
            self.conn.commit()
            print("✅ Created top_line_metrics table with proper structure")
            return True
        except Exception as e:
            print(f"Error creating table: {str(e)}")
            return False
    
    def populate_top_line_metrics(self):
        """Populate the top_line_metrics table with aggregated data from games."""
        try:
            # Clear existing data
            self.cursor.execute("DELETE FROM top_line_metrics")
            
            # Insert aggregated data by review_score_desc
            self.cursor.execute("""
                INSERT INTO top_line_metrics (
                    review_score_desc, 
                    num_games, 
                    num_reviews, 
                    avg_num_reviews
                )
                SELECT 
                    CASE WHEN lower(COALESCE(review_score_desc, '')) LIKE '%user reviews%' THEN 'Under 11 Reviews' ELSE COALESCE(review_score_desc, 'No Reviews') END as review_score_desc,
                    COUNT(*) as num_games,
                    SUM(COALESCE(total_reviews, 0)) as num_reviews,
                    CASE WHEN COUNT(*) > 0 THEN SUM(COALESCE(total_reviews, 0)) / COUNT(*) ELSE 0 END as avg_num_reviews
                FROM games 
                WHERE type = 'game' 
                AND lower(name) NOT LIKE '%playtest%'
                GROUP BY 1
                ORDER BY 2 DESC
            """)
            
            inserted_rows = self.cursor.rowcount
            self.conn.commit()
            print(f"✅ Populated top_line_metrics table with {inserted_rows} rows")
            return True
        except Exception as e:
            print(f"Error populating table: {str(e)}")
            self.conn.rollback()
            return False
    
    def verify_data(self):
        """Verify the populated data."""
        try:
            # Get total counts
            self.cursor.execute("SELECT COUNT(*) FROM top_line_metrics")
            total_metrics = self.cursor.fetchone()[0]
            
            # Get sample data
            self.cursor.execute("""
                SELECT review_score_desc, num_games, num_reviews, avg_num_reviews
                FROM top_line_metrics
                ORDER BY num_games DESC
                LIMIT 10
            """)
            sample_data = self.cursor.fetchall()
            
            print(f"\n🔍 Data Verification:")
            print(f"📊 Total metric categories: {total_metrics}")
            print(f"\n📋 Sample data:")
            for row in sample_data:
                review_desc, num_games, num_reviews, avg_reviews = row
                print(f"   {review_desc}: {num_games:,} games, {num_reviews:,} reviews, {avg_reviews} avg")
            
            return True
        except Exception as e:
            print(f"Error verifying data: {str(e)}")
            return False

    def drop_interesting_recent_games_table(self):
        """Drop the existing interesting_recent_games table if it exists."""
        try:
            self.cursor.execute("DROP TABLE IF EXISTS interesting_recent_games")
            self.conn.commit()
            print("🗑️  Dropped existing interesting_recent_games table")
            return True
        except Exception as e:
            print(f"Error dropping interesting_recent_games: {str(e)}")
            return False

    def create_interesting_recent_games_table(self):
        """Create the interesting_recent_games table."""
        try:
            self.cursor.execute("""
                CREATE TABLE interesting_recent_games (
                    id SERIAL PRIMARY KEY,
                    app_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    review_score_desc TEXT,
                    total_positive INTEGER,
                    total_reviews INTEGER,
                    release_date_actual DATE,
                    last_updated TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Helpful indexes
            self.cursor.execute("""
                CREATE INDEX idx_irg_total_positive ON interesting_recent_games(total_positive DESC);
            """)
            self.cursor.execute("""
                CREATE INDEX idx_irg_review_desc ON interesting_recent_games(review_score_desc);
            """)

            self.conn.commit()
            print("✅ Created interesting_recent_games table")
            return True
        except Exception as e:
            print(f"Error creating interesting_recent_games: {str(e)}")
            return False

    def populate_interesting_recent_games(self):
        """Populate interesting_recent_games from high-rated games ordered by positivity."""
        try:
            # Clear existing data just in case (table is freshly created, but safe)
            self.cursor.execute("DELETE FROM interesting_recent_games")

            # Insert from games with top review descriptions, excluding coming soon
            self.cursor.execute("""
                INSERT INTO interesting_recent_games (
                    app_id, name, review_score_desc, total_positive, total_reviews,
                    release_date_actual, last_updated
                )
                SELECT 
                    g.app_id,
                    g.name,
                    g.review_score_desc,
                    COALESCE(g.total_positive, 0) AS total_positive,
                    COALESCE(g.total_reviews, 0) AS total_reviews,
                    g.release_date_actual,
                    g.last_updated
                FROM games g
                WHERE g.type = 'game'
                  AND (g.release_date::jsonb->>'coming_soon')::boolean = false
                  AND g.review_score_desc IN ('Very Positive', 'Overwhelmingly Positive')
                ORDER BY COALESCE(g.total_positive, 0) DESC
            """)

            inserted = self.cursor.rowcount
            self.conn.commit()
            print(f"✅ Populated interesting_recent_games with {inserted} rows")
            return True
        except Exception as e:
            print(f"Error populating interesting_recent_games: {str(e)}")
            self.conn.rollback()
            return False

    def drop_recent_top_games_table(self):
        """Drop the existing recent_top_games table if it exists."""
        try:
            self.cursor.execute("DROP TABLE IF EXISTS recent_top_games")
            self.conn.commit()
            print("🗑️  Dropped existing recent_top_games table")
            return True
        except Exception as e:
            print(f"Error dropping recent_top_games: {str(e)}")
            return False

    def create_recent_top_games_table(self):
        """Create the recent_top_games table with identical structure to games."""
        try:
            # Clone table structure (columns, defaults, constraints, etc.)
            self.cursor.execute("""
                CREATE TABLE recent_top_games (
                    LIKE games INCLUDING ALL
                )
            """)
            self.conn.commit()
            print("✅ Created recent_top_games table (LIKE games INCLUDING ALL)")
            return True
        except Exception as e:
            print(f"Error creating recent_top_games: {str(e)}")
            return False

    def populate_recent_top_games(self):
        """Populate recent_top_games with top 100 recent highly-rated games by review count."""
        try:
            # Clear existing data to avoid duplicates
            self.cursor.execute("DELETE FROM recent_top_games")

            # Insert top 100 recent games (last 30 days), highly-rated, by total_reviews desc
            self.cursor.execute("""
                INSERT INTO recent_top_games
                SELECT *
                FROM games g
                WHERE g.type = 'game'
                  AND g.review_score_desc IN (
                      'Positive', 'Mostly Positive', 'Very Positive', 'Overwhelmingly Positive'
                  )
                  AND g.release_date_actual IS NOT NULL
                  AND g.release_date_actual >= (CURRENT_DATE - INTERVAL '15 days')
                  AND COALESCE(g.num_reviews, 0) BETWEEN 11 AND 5000
                ORDER BY COALESCE(g.total_reviews, 0) DESC
                LIMIT 100
            """)

            inserted_rows = self.cursor.rowcount
            self.conn.commit()
            print(f"✅ Populated recent_top_games with {inserted_rows} rows")
            return True
        except Exception as e:
            print(f"Error populating recent_top_games: {str(e)}")
            self.conn.rollback()
            return False

def recreate_top_line_metrics():
    """Main function to recreate the top_line_metrics table."""
    pipeline = SupabasePipeline()
    
    # Connect to Supabase
    if not pipeline.connect():
        return False
    
    try:
        print("🔄 Starting top_line_metrics table recreation")
        print("=" * 50)
        
        # Step 1: Drop existing table
        if not pipeline.drop_top_line_metrics_table():
            return False
        
        # Step 2: Create new table
        if not pipeline.create_top_line_metrics_table():
            return False
        
        # Step 3: Populate with data
        if not pipeline.populate_top_line_metrics():
            return False
        
        # Step 4: Rebuild interesting_recent_games table
        if not pipeline.drop_interesting_recent_games_table():
            return False
        if not pipeline.create_interesting_recent_games_table():
            return False
        if not pipeline.populate_interesting_recent_games():
            return False

        # Step 5: Build recent_top_games (last 30d, very/overwhelmingly positive, top 100 by reviews)
        if not pipeline.drop_recent_top_games_table():
            return False
        if not pipeline.create_recent_top_games_table():
            return False
        if not pipeline.populate_recent_top_games():
            return False

        # Step 6: Verify data
        if not pipeline.verify_data():
            return False
        
        print("\n🎉 Top line metrics table recreation completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error during table recreation: {str(e)}")
        return False
    finally:
        pipeline.disconnect()

if __name__ == "__main__":
    print("🔄 Starting Top Line Metrics Table Recreation")
    print("=" * 50)
    
    success = recreate_top_line_metrics()
    
    if success:
        print("\n✅ Top line metrics table has been recreated successfully!")
        print("📊 The table is now ready for use with your Cube.js analytics and Shiny app.")
    else:
        print("\n❌ Failed to recreate top line metrics table.")
        sys.exit(1) 