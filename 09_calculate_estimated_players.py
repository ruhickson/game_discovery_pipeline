python#!/usr/bin/env python3
"""
09_calculate_estimated_players.py

Calculates estimated player counts for games based on review count * 35.
Applies specific rounding rules:
- Round up to nearest 10,000
- If > 100,000: round to nearest 100,000  
- If > 1,000,000: round to nearest million
"""

import os
import sys
import logging
from datetime import datetime
import math
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add the current directory to Python path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    print("❌ psycopg2 not installed. Install with: pip install psycopg2-binary")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('estimated_players_calculation.log'),
        logging.StreamHandler()
    ]
)

class EstimatedPlayersCalculator:
    """Calculator for estimating player counts based on review data"""
    
    def __init__(self):
        self.connection_string = os.getenv('SUPABASE_CONNECTION_STRING')
        if not self.connection_string:
            raise ValueError("SUPABASE_CONNECTION_STRING environment variable not set")
        
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Connect to Supabase database."""
        try:
            self.conn = psycopg2.connect(self.connection_string)
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            logging.info("✅ Connected to Supabase database")
            return True
        except Exception as e:
            logging.error(f"❌ Error connecting to Supabase: {str(e)}")
            return False
    
    def disconnect(self):
        """Disconnect from Supabase database."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logging.info("✅ Disconnected from Supabase database")
    
    def calculate_estimated_players(self, review_count):
        """
        Calculate estimated players based on review count with specific rounding rules.
        
        Args:
            review_count (int): Number of reviews for the game
            
        Returns:
            int: Estimated players with applied rounding rules
        """
        if not review_count or review_count <= 0:
            return None
        
        # Base calculation: reviews * 35
        estimated = review_count * 35
        
        # Apply rounding rules
        if estimated >= 1_000_000:
            # Round to nearest million
            return round(estimated / 1_000_000) * 1_000_000
        elif estimated >= 100_000:
            # Round to nearest 100,000
            return round(estimated / 100_000) * 100_000
        else:
            # Round up to nearest 10,000
            return math.ceil(estimated / 10_000) * 10_000
    
    def get_games_with_reviews(self):
        """Get all games that have review data"""
        try:
            self.cursor.execute("""
                SELECT app_id, name, total_reviews, num_reviews
                FROM games 
                WHERE (total_reviews > 0 OR num_reviews > 0)
                AND estimated_players IS NULL
                ORDER BY app_id
            """)
            
            games = self.cursor.fetchall()
            logging.info(f"📊 Found {len(games)} games with review data to process")
            return games
            
        except Exception as e:
            logging.error(f"❌ Error fetching games: {str(e)}")
            return []
    
    def update_estimated_players(self, app_id, estimated_players):
        """Update the estimated_players column for a specific game"""
        try:
            self.cursor.execute("""
                UPDATE games 
                SET estimated_players = %s
                WHERE app_id = %s
            """, (estimated_players, app_id))
            
            return True
            
        except Exception as e:
            logging.error(f"❌ Error updating app_id {app_id}: {str(e)}")
            return False
    
    def process_all_games(self):
        """Process all games and calculate estimated players"""
        games = self.get_games_with_reviews()
        
        if not games:
            logging.info("ℹ️  No games found to process")
            return
        
        processed = 0
        updated = 0
        errors = 0
        
        logging.info(f"🚀 Starting to process {len(games)} games...")
        
        for game in games:
            try:
                app_id = game['app_id']
                name = game['name']
                
                # Use total_reviews if available, otherwise num_reviews
                review_count = game['total_reviews'] or game['num_reviews'] or 0
                
                if review_count <= 0:
                    logging.info(f"⏭️  Skipping {name} (app_id: {app_id}) - no reviews")
                    continue
                
                # Calculate estimated players
                estimated_players = self.calculate_estimated_players(review_count)
                
                if estimated_players:
                    # Update the database
                    if self.update_estimated_players(app_id, estimated_players):
                        updated += 1
                        logging.info(f"✅ {name} (app_id: {app_id}): {review_count} reviews → {estimated_players:,} estimated players")
                    else:
                        errors += 1
                        logging.error(f"❌ Failed to update {name} (app_id: {app_id})")
                else:
                    logging.warning(f"⚠️  Could not calculate for {name} (app_id: {app_id})")
                    errors += 1
                
                processed += 1
                
                # Commit every 100 updates
                if processed % 100 == 0:
                    self.conn.commit()
                    logging.info(f"📊 Processed {processed}/{len(games)} games...")
                
            except Exception as e:
                logging.error(f"❌ Error processing {game.get('name', 'Unknown')}: {str(e)}")
                errors += 1
                processed += 1
        
        # Final commit
        try:
            self.conn.commit()
            logging.info(f"✅ Final commit completed")
        except Exception as e:
            logging.error(f"❌ Error during final commit: {str(e)}")
            self.conn.rollback()
        
        # Summary
        logging.info(f"""
📊 ESTIMATION COMPLETE
====================
Games processed: {processed}
Successfully updated: {updated}
Errors: {errors}
Success rate: {(updated/processed*100):.1f}% if processed > 0 else 0
        """)
    
    def get_estimation_summary(self):
        """Get a summary of the estimated players distribution"""
        try:
            self.cursor.execute("""
                SELECT 
                    COUNT(*) as total_games,
                    COUNT(estimated_players) as games_with_estimates,
                    MIN(estimated_players) as min_players,
                    MAX(estimated_players) as max_players,
                    AVG(estimated_players) as avg_players,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY estimated_players) as median_players
                FROM games 
                WHERE estimated_players IS NOT NULL
            """)
            
            result = self.cursor.fetchone()
            
            if result:
                logging.info(f"""
📈 ESTIMATION SUMMARY
===================
Total games with estimates: {result['games_with_estimates']:,}
Min estimated players: {result['min_players']:,}
Max estimated players: {result['max_players']:,}
Average estimated players: {result['avg_players']:,.0f}
Median estimated players: {result['median_players']:,.0f}
                """)
                
                # Distribution by ranges
                self.cursor.execute("""
                    SELECT 
                        CASE 
                            WHEN estimated_players < 20000 THEN '< 20k'
                            WHEN estimated_players < 50000 THEN '20k-50k'
                            WHEN estimated_players < 100000 THEN '50k-100k'
                            WHEN estimated_players < 500000 THEN '100k-500k'
                            WHEN estimated_players < 1000000 THEN '500k-1M'
                            ELSE '1M+'
                        END as player_range,
                        COUNT(*) as count
                    FROM games 
                    WHERE estimated_players IS NOT NULL
                    GROUP BY 
                        CASE 
                            WHEN estimated_players < 20000 THEN '< 20k'
                            WHEN estimated_players < 50000 THEN '20k-50k'
                            WHEN estimated_players < 100000 THEN '50k-100k'
                            WHEN estimated_players < 500000 THEN '100k-500k'
                            WHEN estimated_players < 1000000 THEN '500k-1M'
                            ELSE '1M+'
                        END
                    ORDER BY MIN(estimated_players)
                """)
                
                ranges = self.cursor.fetchall()
                logging.info("📊 Distribution by player count ranges:")
                for range_data in ranges:
                    logging.info(f"  {range_data['player_range']}: {range_data['count']:,} games")
            
        except Exception as e:
            logging.error(f"❌ Error getting summary: {str(e)}")

def main():
    """Main execution function"""
    try:
        logging.info("🚀 Starting estimated players calculation...")
        
        calculator = EstimatedPlayersCalculator()
        
        # Connect to Supabase
        if not calculator.connect():
            logging.error("❌ Failed to connect to database")
            return
        
        # Process all games
        calculator.process_all_games()
        
        # Show summary
        calculator.get_estimation_summary()
        
        calculator.disconnect()
        
        logging.info("✅ Estimated players calculation completed successfully!")
        
    except Exception as e:
        logging.error(f"❌ Script failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
