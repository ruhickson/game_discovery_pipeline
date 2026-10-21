#!/usr/bin/env python3
"""
10_fetch_completion_times.py

Fetches game completion times from HowLongToBeat.com and stores them in the database.
Uses rate limiting to avoid overwhelming the HLTB servers.
"""

import os
import sys
import logging
import time
import json
from datetime import datetime
from difflib import SequenceMatcher

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# Add the current directory to Python path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from howlongtobeatpy import HowLongToBeat
except ImportError as e:
    print(f"❌ Missing required package: {str(e)}")
    print("Install with: pip install howlongtobeatpy psycopg2-binary")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('completion_times_fetch.log'),
        logging.StreamHandler()
    ]
)

class CompletionTimesFetcher:
    """Fetches game completion times from HowLongToBeat.com"""
    
    def __init__(self):
        self.connection_string = os.getenv('SUPABASE_CONNECTION_STRING')
        if not self.connection_string:
            raise ValueError("SUPABASE_CONNECTION_STRING environment variable not set")
        
        self.conn = None
        self.cursor = None
        self.hltb = HowLongToBeat()
        
        # Rate limiting: 2 seconds between requests (30 requests per minute)
        self.request_delay = 2.0
        
        # Statistics
        self.stats = {
            'processed': 0,
            'found': 0,
            'not_found': 0,
            'errors': 0,
            'skipped': 0
        }
    
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
    
    def similarity(self, a, b):
        """Calculate similarity between two strings (0-1 scale)"""
        return SequenceMatcher(None, a.lower(), b.lower()).ratio()
    
    def clean_game_name(self, name):
        """Clean game name for better matching"""
        # Remove common suffixes that might interfere with matching
        suffixes_to_remove = [
            ' - Steam Edition',
            ' - Deluxe Edition',
            ' - Ultimate Edition',
            ' - Game of the Year Edition',
            ' - GOTY Edition',
            ' - Complete Edition',
            ' - Definitive Edition',
            ' - Enhanced Edition',
            ' - Remastered',
            ' - Remake',
            ' - 2024',
            ' - 2023',
            ' - 2022',
            ' - 2021',
            ' - 2020'
        ]
        
        cleaned = name
        for suffix in suffixes_to_remove:
            if cleaned.endswith(suffix):
                cleaned = cleaned[:-len(suffix)].strip()
        
        return cleaned
    
    def find_best_match(self, game_name, hltb_results):
        """Find the best matching game from HLTB results"""
        if not hltb_results:
            return None
        
        cleaned_name = self.clean_game_name(game_name)
        best_match = None
        best_similarity = 0.0
        
        for result in hltb_results:
            # Calculate similarity with both original and cleaned name
            similarity_original = self.similarity(game_name, result.game_name)
            similarity_cleaned = self.similarity(cleaned_name, result.game_name)
            
            # Use the higher similarity score
            similarity = max(similarity_original, similarity_cleaned)
            
            if similarity > best_similarity:
                best_similarity = similarity
                best_match = result
        
        # Only return matches with reasonable similarity (>= 0.6)
        if best_similarity >= 0.6:
            return best_match, best_similarity
        
        return None, best_similarity
    
    def fetch_completion_times(self, game_name):
        """Fetch completion times for a game with rate limiting"""
        try:
            # Rate limiting: wait before making request
            time.sleep(self.request_delay)
            
            logging.info(f"🔍 Searching HLTB for: {game_name}")
            results = self.hltb.search(game_name)
            
            if not results:
                logging.info(f"📭 No results found for: {game_name}")
                return None
            
            # Find best match
            best_match, similarity = self.find_best_match(game_name, results)
            
            if best_match:
                logging.info(f"✅ Found match for '{game_name}' -> '{best_match.game_name}' (similarity: {similarity:.2f})")
                
                # Debug: Log what we're getting from HLTB
                logging.info(f"🔍 HLTB Data - Main: {best_match.main_story}, Main+Extra: {best_match.main_extra}, Completionist: {best_match.completionist}")
                
                return {
                    'game_name': best_match.game_name,
                    'main_story_hours': best_match.main_story,
                    'main_plus_extras_hours': best_match.main_extra,
                    'completionist_hours': best_match.completionist,
                    'similarity': similarity
                }
            else:
                logging.info(f"❌ No good match found for: {game_name} (best similarity: {similarity:.2f})")
                return None
                
        except Exception as e:
            logging.error(f"❌ Error fetching data for '{game_name}': {str(e)}")
            return None
    
    def update_completion_times(self, app_id, completion_data):
        """Update completion times in the database"""
        try:
            if completion_data:
                # Update with found data
                self.cursor.execute("""
                    UPDATE games 
                    SET 
                        main_story_hours = %s,
                        main_plus_extras_hours = %s,
                        completionist_hours = %s,
                        hltb_game_name = %s,
                        hltb_similarity = %s,
                        completion_times_updated = CURRENT_TIMESTAMP
                    WHERE app_id = %s
                """, (
                    completion_data['main_story_hours'],
                    completion_data['main_plus_extras_hours'],
                    completion_data['completionist_hours'],
                    completion_data['game_name'],
                    completion_data['similarity'],
                    app_id
                ))
            else:
                # Update with null values (mark as processed)
                self.cursor.execute("""
                    UPDATE games 
                    SET 
                        main_story_hours = NULL,
                        main_plus_extras_hours = NULL,
                        completionist_hours = NULL,
                        hltb_game_name = NULL,
                        hltb_similarity = NULL,
                        completion_times_updated = CURRENT_TIMESTAMP
                    WHERE app_id = %s
                """, (app_id,))
            
            return True
            
        except Exception as e:
            logging.error(f"❌ Error updating app_id {app_id}: {str(e)}")
            return False
    
    def get_games_to_process(self):
        """Get games that need completion time data (games before 2025)"""
        try:
            self.cursor.execute("""
                SELECT app_id, name, release_date_actual
                FROM games 
                WHERE main_story_hours IS NULL
                AND name IS NOT NULL
                AND name != ''
                AND release_date_actual < '2025-01-01'
                ORDER BY app_id
            """)
            
            games = self.cursor.fetchall()
            logging.info(f"📊 Found {len(games)} pre-2025 games needing completion time data")
            return games
            
        except Exception as e:
            logging.error(f"❌ Error fetching games: {str(e)}")
            return []
    
    def process_games(self, limit=None):
        """Process games and fetch completion times"""
        games = self.get_games_to_process()
        
        if not games:
            logging.info("ℹ️  No games found to process")
            return
        
        if limit:
            games = games[:limit]
            logging.info(f"🔢 Limited to {limit} games for this run")
        
        total_games = len(games)
        logging.info(f"🚀 Starting to process {total_games} pre-2025 games...")
        logging.info(f"⏱️  Rate limiting: {self.request_delay}s between requests (30 requests/minute)")
        
        for i, game in enumerate(games, 1):
            try:
                app_id = game['app_id']
                name = game['name']
                
                release_date = game.get('release_date_actual', 'Unknown')
                logging.info(f"🔄 Processing {i}/{total_games}: {name} (app_id: {app_id}, released: {release_date})")
                
                # Fetch completion times
                completion_data = self.fetch_completion_times(name)
                
                if completion_data:
                    # Debug: Log what we're about to insert
                    logging.info(f"🔍 About to insert - Main: {completion_data['main_story_hours']}, Main+Extra: {completion_data['main_plus_extras_hours']}, Completionist: {completion_data['completionist_hours']}")
                    
                    # Update database with found data
                    if self.update_completion_times(app_id, completion_data):
                        self.stats['found'] += 1
                        logging.info(f"✅ Updated {name}: Main={completion_data['main_story_hours']}h, "
                                   f"Main+Extras={completion_data['main_plus_extras_hours']}h, "
                                   f"Completionist={completion_data['completionist_hours']}h")
                    else:
                        self.stats['errors'] += 1
                        logging.error(f"❌ Failed to update database for {name}")
                else:
                    # Update database with null values (mark as processed)
                    if self.update_completion_times(app_id, None):
                        self.stats['not_found'] += 1
                        logging.info(f"📭 No completion data found for {name} (marked as processed)")
                    else:
                        self.stats['errors'] += 1
                        logging.error(f"❌ Failed to update database for {name}")
                
                self.stats['processed'] += 1
                
                # Commit every 10 updates
                if self.stats['processed'] % 10 == 0:
                    self.conn.commit()
                    logging.info(f"📊 Progress: {self.stats['processed']}/{total_games} processed, "
                               f"{self.stats['found']} found, {self.stats['not_found']} not found, "
                               f"{self.stats['errors']} errors")
                
            except Exception as e:
                logging.error(f"❌ Error processing {game.get('name', 'Unknown')}: {str(e)}")
                self.stats['errors'] += 1
                self.stats['processed'] += 1
        
        # Final commit
        try:
            self.conn.commit()
            logging.info("✅ Final commit completed")
        except Exception as e:
            logging.error(f"❌ Error during final commit: {str(e)}")
            self.conn.rollback()
    
    def get_completion_summary(self):
        """Get a summary of completion time data"""
        try:
            self.cursor.execute("""
                SELECT 
                    COUNT(*) as total_games,
                    COUNT(main_story_hours) as games_with_completion_data,
                    AVG(main_story_hours) as avg_main_story,
                    AVG(main_plus_extras_hours) as avg_main_plus_extras,
                    AVG(completionist_hours) as avg_completionist,
                    MIN(main_story_hours) as min_main_story,
                    MAX(main_story_hours) as max_main_story
                FROM games 
                WHERE main_story_hours IS NOT NULL
            """)
            
            result = self.cursor.fetchone()
            
            if result:
                logging.info(f"""
📈 COMPLETION TIMES SUMMARY
=========================
Total games with completion data: {result['games_with_completion_data']:,}
Average main story: {result['avg_main_story']:.1f} hours
Average main + extras: {result['avg_main_plus_extras']:.1f} hours
Average completionist: {result['avg_completionist']:.1f} hours
Shortest main story: {result['min_main_story']:.1f} hours
Longest main story: {result['max_main_story']:.1f} hours
                """)
                
                # Distribution by completion time ranges
                self.cursor.execute("""
                    SELECT 
                        CASE 
                            WHEN main_story_hours < 5 THEN '< 5 hours'
                            WHEN main_story_hours < 10 THEN '5-10 hours'
                            WHEN main_story_hours < 20 THEN '10-20 hours'
                            WHEN main_story_hours < 40 THEN '20-40 hours'
                            WHEN main_story_hours < 80 THEN '40-80 hours'
                            ELSE '80+ hours'
                        END as completion_range,
                        COUNT(*) as count
                    FROM games 
                    WHERE main_story_hours IS NOT NULL
                    GROUP BY 
                        CASE 
                            WHEN main_story_hours < 5 THEN '< 5 hours'
                            WHEN main_story_hours < 10 THEN '5-10 hours'
                            WHEN main_story_hours < 20 THEN '10-20 hours'
                            WHEN main_story_hours < 40 THEN '20-40 hours'
                            WHEN main_story_hours < 80 THEN '40-80 hours'
                            ELSE '80+ hours'
                        END
                    ORDER BY MIN(main_story_hours)
                """)
                
                ranges = self.cursor.fetchall()
                logging.info("📊 Distribution by completion time ranges:")
                for range_data in ranges:
                    logging.info(f"  {range_data['completion_range']}: {range_data['count']:,} games")
            
        except Exception as e:
            logging.error(f"❌ Error getting summary: {str(e)}")

def main():
    """Main execution function"""
    try:
        logging.info("🚀 Starting completion times fetch...")
        
        fetcher = CompletionTimesFetcher()
        
        # Connect to Supabase
        if not fetcher.connect():
            logging.error("❌ Failed to connect to database")
            return
        
        # Process games (all 2025 games)
        fetcher.process_games()  # Process all 2025 games
        # fetcher.process_games(limit=100)  # Uncomment to test with 100 games
        
        # Show summary
        fetcher.get_completion_summary()
        
        # Final statistics
        logging.info(f"""
📊 FETCH COMPLETE
================
Games processed: {fetcher.stats['processed']}
Completion data found: {fetcher.stats['found']}
Not found: {fetcher.stats['not_found']}
Errors: {fetcher.stats['errors']}
Success rate: {(fetcher.stats['found']/fetcher.stats['processed']*100):.1f}% if fetcher.stats['processed'] > 0 else 0
        """)
        
        fetcher.disconnect()
        
        logging.info("✅ Completion times fetch completed successfully!")
        
    except Exception as e:
        logging.error(f"❌ Script failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
