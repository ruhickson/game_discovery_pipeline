import requests
import time
import json
import random
import psycopg2
import re
from datetime import datetime
import os
import sys
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

class SteamAPI:
    def __init__(self, api_key=None):
        if api_key is None:
            api_key = os.getenv('STEAM_API_KEY')
        self.api_key = api_key
        self.base_url = "https://api.steampowered.com"
        self.store_url = "https://store.steampowered.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
    def get_app_details(self, app_id, retry_count=0):
        """Get detailed information about a specific app."""
        try:
            # Add delay between requests (0.3-0.6 second)
            time.sleep(random.uniform(0.3, 0.6))
            url = f"https://store.steampowered.com/api/appdetails?appids={app_id}"
            response = requests.get(url, headers=self.headers)
            if response.status_code == 429:
                print(f"Received 429 Too Many Requests for app_id {app_id}. Waiting 2 minutes before retrying...")
                time.sleep(120)
                return self.get_app_details(app_id, retry_count)
            if response.status_code != 200:
                print(f"Failed to get details for app_id {app_id}: {response.status_code}")
                return None
            data = response.json()
            if str(app_id) in data and data[str(app_id)]['success']:
                return data[str(app_id)]['data']
            return None
        except Exception as e:
            print(f"Error getting details for app_id {app_id}: {str(e)}")
            return None
    
    def get_review_summary(self, app_id, retry_count=0):
        """Get review summary for a specific app."""
        try:
            # Add delay between requests (0.3-0.6 second)
            time.sleep(random.uniform(0.3, 0.6))
            url = f"https://store.steampowered.com/appreviews/{app_id}?json=1&language=all&filter=all&review_type=all&purchase_type=all"
            response = requests.get(url, headers=self.headers)
            if response.status_code == 429:
                print(f"Received 429 Too Many Requests for app_id {app_id}. Waiting 2 minutes before retrying...")
                time.sleep(120)
                return self.get_review_summary(app_id, retry_count)
            if response.status_code != 200:
                print(f"Failed to get review summary for app_id {app_id}: {response.status_code}")
                return {}
            data = response.json()
            if data['success'] == 1:
                return {
                    'num_reviews': data['query_summary']['total_reviews'],
                    'review_score': data['query_summary']['review_score'],
                    'review_score_desc': data['query_summary']['review_score_desc'],
                    'total_positive': data['query_summary']['total_positive'],
                    'total_negative': data['query_summary']['total_negative'],
                    'total_reviews': data['query_summary']['total_reviews']
                }
            return {}
        except Exception as e:
            print(f"Error getting review summary for app_id {app_id}: {str(e)}")
            return {}
    
    def get_game_tags(self, app_id, retry_count=0):
        """Get user tags for a specific game from Steam store page."""
        try:
            # Add delay between requests (0.3-0.6 second)
            time.sleep(random.uniform(0.3, 0.6))
            url = f'https://store.steampowered.com/app/{app_id}/'
            
            headers = {
                'User-Agent': self.headers['User-Agent'],
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 429:
                print(f"Rate limited (429) for app_id {app_id}. Waiting 2 minutes before retrying...")
                time.sleep(120)
                return self.get_game_tags(app_id, retry_count)
            
            if response.status_code == 200:
                # Look for tags in the HTML - focus on the most common pattern
                tag_pattern = r'"tagid":\s*(\d+),\s*"name":\s*"([^"]+)"'
                matches = re.findall(tag_pattern, response.text)
                if matches:
                    tags = [match[1] for match in matches]
                    print(f"🏷️  Found {len(tags)} tags for app_id {app_id}")
                    return tags
            
            print(f"📭 No tags found for app_id {app_id}")
            return []
            
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, 
                requests.exceptions.TooManyRedirects) as e:
            print(f"Network error getting tags for app_id {app_id}: {str(e)}")
            return []
        except Exception as e:
            print(f"Error getting tags for app_id {app_id}: {str(e)}")
            return []

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
    
    def create_games_checking_table(self):
        """Create the games_checking_for_updates table."""
        try:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS games_checking_for_updates (
                    app_id INTEGER PRIMARY KEY,
                    name TEXT,
                    type TEXT,
                    required_age INTEGER,
                    is_free BOOLEAN,
                    detailed_description TEXT,
                    short_description TEXT,
                    supported_languages TEXT,
                    header_image TEXT,
                    website TEXT,
                    developers TEXT,
                    publishers TEXT,
                    price_overview TEXT,
                    platforms TEXT,
                    metacritic INTEGER,
                    categories TEXT,
                    genres TEXT,
                    screenshots TEXT,
                    movies TEXT,
                    recommendations INTEGER,
                    release_date TEXT,
                    support_info TEXT,
                    background TEXT,
                    content_descriptors TEXT,
                    minimum_requirements TEXT,
                    recommended_requirements TEXT,
                    num_reviews INTEGER,
                    review_score INTEGER,
                    review_score_desc TEXT,
                    total_positive INTEGER,
                    total_negative INTEGER,
                    total_reviews INTEGER,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.conn.commit()
            print("✅ Created games_checking_for_updates table")
            return True
        except Exception as e:
            print(f"❌ Error creating games_checking_for_updates table: {str(e)}")
            return False
    
    def move_coming_soon_games_to_checking_table(self):
        """Move games that are coming soon and haven't been updated today to the checking table."""
        try:
            # First, get the app_ids that need to be moved
            self.cursor.execute("""
                SELECT app_id FROM games
                WHERE (release_date::jsonb->>'coming_soon')::boolean = true
                AND last_updated < (current_date - interval '7 days')
                ORDER BY last_updated ASC
                LIMIT 1000
            """)
            app_ids_to_move = [row[0] for row in self.cursor.fetchall()]
            
            if not app_ids_to_move:
                print("📋 No games need to be moved")
                return 0
            
            print(f"📋 Found {len(app_ids_to_move)} games to move")
            
            # Temporarily disable foreign key constraint checking
            self.cursor.execute("SET session_replication_role = replica")
            print("🔓 Temporarily disabled foreign key constraints")
            
            # Build placeholders for IN clause
            placeholders = ','.join(['%s'] * len(app_ids_to_move))

            # Insert games into the checking table with explicit column mapping
            self.cursor.execute(
                f"""
                INSERT INTO games_checking_for_updates (
                    app_id, name, type, required_age, is_free, detailed_description,
                    short_description, supported_languages, header_image, website,
                    developers, publishers, price_overview, platforms, metacritic,
                    categories, genres, screenshots, movies, recommendations,
                    release_date, support_info, background, content_descriptors,
                    minimum_requirements, recommended_requirements, num_reviews,
                    review_score, review_score_desc, total_positive, total_negative,
                    total_reviews, last_updated
                )
                SELECT 
                    app_id, name, type, required_age, is_free, detailed_description,
                    short_description, supported_languages, header_image, website,
                    developers, publishers, price_overview, platforms, metacritic,
                    categories, genres, screenshots, movies, recommendations,
                    release_date, support_info, background, content_descriptors,
                    minimum_requirements, recommended_requirements, num_reviews,
                    review_score, review_score_desc, total_positive, total_negative,
                    total_reviews, last_updated
                FROM games
                WHERE app_id IN ({placeholders})
                """,
                tuple(app_ids_to_move)
            )
            
            moved_count = self.cursor.rowcount
            print(f"📋 Moved {moved_count} games to checking table")
            
            # Delete them from the main games table (same set)
            self.cursor.execute(
                f"""
                DELETE FROM games
                WHERE app_id IN ({placeholders})
                """,
                tuple(app_ids_to_move)
            )
            
            deleted_count = self.cursor.rowcount
            print(f"🗑️  Removed {deleted_count} games from main games table")
            
            # Re-enable foreign key constraint checking
            self.cursor.execute("SET session_replication_role = DEFAULT")
            print("🔒 Re-enabled foreign key constraints")
            
            self.conn.commit()
            return moved_count
        except Exception as e:
            print(f"❌ Error moving games to checking table: {str(e)}")
            # Make sure to re-enable constraints even if there's an error
            try:
                self.cursor.execute("SET session_replication_role = DEFAULT")
                print("🔒 Re-enabled foreign key constraints after error")
            except:
                pass
            self.conn.rollback()
            return 0
    
    def get_games_to_check(self):
        """Get all app_ids from the games_checking_for_updates table."""
        try:
            self.cursor.execute("SELECT app_id, name FROM games_checking_for_updates ORDER BY app_id")
            return self.cursor.fetchall()
        except Exception as e:
            print(f"❌ Error getting games to check: {str(e)}")
            return []
    
    def safe_int_convert(self, value):
        """Safely convert value to integer, handling text like '17+'"""
        if value is None:
            return None
        try:
            if isinstance(value, str):
                digits = ''.join(filter(str.isdigit, value))
                return int(digits) if digits else None
            return int(value)
        except (ValueError, TypeError):
            return None
    
    def insert_game(self, app_id, name, details, review_summary):
        """Insert a game into the Supabase games table."""
        try:
            # Handle pc_requirements which can be a list or dict
            pc_reqs = details.get('pc_requirements', {})
            min_reqs = ''
            rec_reqs = ''
            
            if isinstance(pc_reqs, dict):
                min_reqs = pc_reqs.get('minimum', '')
                rec_reqs = pc_reqs.get('recommended', '')
            elif isinstance(pc_reqs, list):
                min_reqs = pc_reqs[0] if len(pc_reqs) > 0 else ''
                rec_reqs = pc_reqs[1] if len(pc_reqs) > 1 else ''
            
            # Convert data types
            is_free = details.get('is_free')
            if is_free is not None:
                is_free = bool(is_free)
            
            required_age = self.safe_int_convert(details.get('required_age'))
            
            # Insert into games table
            self.cursor.execute("""
                INSERT INTO games (
                    app_id, name, type, required_age, is_free, detailed_description,
                    short_description, supported_languages, header_image, website,
                    developers, publishers, price_overview, platforms, metacritic,
                    categories, genres, screenshots, movies, recommendations,
                    release_date, support_info, background, content_descriptors,
                    minimum_requirements, recommended_requirements, num_reviews,
                    review_score, review_score_desc, total_positive, total_negative,
                    total_reviews, last_updated
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s
                ) ON CONFLICT (app_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    type = EXCLUDED.type,
                    required_age = EXCLUDED.required_age,
                    is_free = EXCLUDED.is_free,
                    detailed_description = EXCLUDED.detailed_description,
                    short_description = EXCLUDED.short_description,
                    supported_languages = EXCLUDED.supported_languages,
                    header_image = EXCLUDED.header_image,
                    website = EXCLUDED.website,
                    developers = EXCLUDED.developers,
                    publishers = EXCLUDED.publishers,
                    price_overview = EXCLUDED.price_overview,
                    platforms = EXCLUDED.platforms,
                    metacritic = EXCLUDED.metacritic,
                    categories = EXCLUDED.categories,
                    genres = EXCLUDED.genres,
                    screenshots = EXCLUDED.screenshots,
                    movies = EXCLUDED.movies,
                    recommendations = EXCLUDED.recommendations,
                    release_date = EXCLUDED.release_date,
                    support_info = EXCLUDED.support_info,
                    background = EXCLUDED.background,
                    content_descriptors = EXCLUDED.content_descriptors,
                    minimum_requirements = EXCLUDED.minimum_requirements,
                    recommended_requirements = EXCLUDED.recommended_requirements,
                    num_reviews = EXCLUDED.num_reviews,
                    review_score = EXCLUDED.review_score,
                    review_score_desc = EXCLUDED.review_score_desc,
                    total_positive = EXCLUDED.total_positive,
                    total_negative = EXCLUDED.total_negative,
                    total_reviews = EXCLUDED.total_reviews,
                    last_updated = CURRENT_TIMESTAMP
            """, (
                app_id,
                name,
                details.get('type'),
                required_age,
                is_free,
                details.get('detailed_description'),
                details.get('short_description'),
                details.get('supported_languages'),
                details.get('header_image'),
                details.get('website'),
                json.dumps(details.get('developers', [])),
                json.dumps(details.get('publishers', [])),
                json.dumps(details.get('price_overview', {})),
                json.dumps(details.get('platforms', {})),
                details.get('metacritic', {}).get('score'),
                json.dumps(details.get('categories', [])),
                json.dumps(details.get('genres', [])),
                json.dumps(details.get('screenshots', [])),
                json.dumps(details.get('movies', [])),
                details.get('recommendations', {}).get('total'),
                json.dumps(details.get('release_date', {})),
                json.dumps(details.get('support_info', {})),
                details.get('background'),
                json.dumps(details.get('content_descriptors', {})),
                json.dumps(min_reqs),
                json.dumps(rec_reqs),
                review_summary.get('num_reviews'),
                review_summary.get('review_score'),
                review_summary.get('review_score_desc'),
                review_summary.get('total_positive'),
                review_summary.get('total_negative'),
                review_summary.get('total_reviews'),
                datetime.now()
            ))
            
            # Insert price history if available
            if 'price_overview' in details:
                price_data = details['price_overview']
                self.cursor.execute("""
                    INSERT INTO price_history (app_id, price, discount_percent, initial_price, final_price)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    app_id,
                    price_data.get('price'),
                    price_data.get('discount_percent'),
                    price_data.get('initial'),
                    price_data.get('final')
                ))
            
            self.conn.commit()
            return True
            
        except Exception as e:
            print(f"Error inserting game {name} (ID: {app_id}): {str(e)}")
            self.conn.rollback()
            return False
    
    def insert_game_tags(self, app_id, tags):
        """Insert tags for a game into the game_tags table."""
        if not tags:
            return True
        
        try:
            # Insert tags using executemany for better performance
            tag_data = [(app_id, tag) for tag in tags]
            self.cursor.executemany("""
                INSERT INTO game_tags (app_id, tag) 
                VALUES (%s, %s) 
                ON CONFLICT (app_id, tag) DO NOTHING
            """, tag_data)
            
            inserted_count = len(tag_data)
            print(f"🏷️  Inserted {inserted_count} tags for app_id {app_id}")
            return True
            
        except Exception as e:
            print(f"Error inserting tags for app_id {app_id}: {str(e)}")
            return False
    
    def remove_checked_game(self, app_id):
        """Remove a game from the games_checking_for_updates table after processing."""
        try:
            self.cursor.execute("DELETE FROM games_checking_for_updates WHERE app_id = %s", (app_id,))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error removing app_id {app_id} from checking table: {str(e)}")
            return False

def check_coming_soon_updates():
    """Main function to check for updates on games that were previously coming soon."""
    steam_api = SteamAPI()
    pipeline = SupabasePipeline()
    
    # Connect to Supabase
    if not pipeline.connect():
        return
    
    try:
        print("🚀 Starting Coming Soon Games Update Check")
        print("=" * 50)
        
        # Step 1: Create the checking table
        if not pipeline.create_games_checking_table():
            return
        
        # Step 2: Move coming soon games to checking table
        moved_count = pipeline.move_coming_soon_games_to_checking_table()
        if moved_count == 0:
            print("✅ No games need checking - all coming soon games are up to date")
            return
        
        # Step 3: Get games to check
        games_to_check = pipeline.get_games_to_check()
        total_games = len(games_to_check)
        print(f"📊 Found {total_games} games to check for updates")
        
        if total_games == 0:
            print("✅ No games to check")
            return
        
        # Step 4: Process each game
        processed = 0
        successful = 0
        skipped = 0
        tags_collected = 0
        
        for app_id, name in games_to_check:
            print(f"\n🔄 Processing {processed + 1}/{total_games}: {name} (ID: {app_id})...")
            
            # Get detailed information
            details = steam_api.get_app_details(app_id)
            if not details:
                print(f"❌ Failed to get details for {name}")
                skipped += 1
                processed += 1
                continue
            
            # Get review summary
            review_summary = steam_api.get_review_summary(app_id)
            print(f"📊 Review summary: {json.dumps(review_summary, indent=2)}")
            
            # Insert into main games table
            if pipeline.insert_game(app_id, name, details, review_summary):
                print(f"✅ Successfully updated {name}")
                successful += 1
                
                # Fetch and insert tags
                print(f"🏷️  Fetching tags for {name}...")
                tags = steam_api.get_game_tags(app_id)
                if tags:
                    if pipeline.insert_game_tags(app_id, tags):
                        tags_collected += len(tags)
                        print(f"🏷️  Successfully collected {len(tags)} tags for {name}")
                    else:
                        print(f"❌ Failed to insert tags for {name}")
                else:
                    print(f"📭 No tags found for {name}")
                
                # Remove from checking table
                pipeline.remove_checked_game(app_id)
            else:
                print(f"❌ Failed to update {name}")
                skipped += 1
            
            processed += 1
            
            # Progress update every 5 games
            if processed % 5 == 0:
                print(f"\n📈 Progress: {processed}/{total_games} processed, {successful} successful, {skipped} skipped, {tags_collected} tags collected")
        
        print(f"\n🎉 Coming Soon Update Check completed!")
        print(f"📊 Total processed: {processed}")
        print(f"✅ Successful: {successful}")
        print(f"⏭️ Skipped: {skipped}")
        print(f"🏷️ Tags collected: {tags_collected}")
        
    except Exception as e:
        print(f"❌ Error during update check: {str(e)}")
    finally:
        pipeline.disconnect()

if __name__ == "__main__":
    load_dotenv()
    STEAM_API_KEY = os.getenv('STEAM_API_KEY')
    
    if not STEAM_API_KEY:
        print("❌ STEAM_API_KEY environment variable not set")
        sys.exit(1)
    
    check_coming_soon_updates()
