#!/usr/bin/env python3
"""
Comprehensive Coming Soon Games Checker

This script checks ALL games with coming_soon = true against the Steam API
to identify games that may have been released or have updated status.
Particularly useful for finding early access games with past release dates.
"""

import requests
import time
import json
import random
import psycopg2
import re
from datetime import datetime, date
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
            # Add delay between requests (0.5-1.0 second) - conservative for large dataset
            time.sleep(random.uniform(0.5, 1.0))
            url = f"https://store.steampowered.com/api/appdetails?appids={app_id}"
            response = requests.get(url, headers=self.headers)
            if response.status_code == 429:
                print(f"Received 429 Too Many Requests for app_id {app_id}. Waiting 2 minutes before retrying...")
                time.sleep(120)
                return self.get_app_details(app_id, retry_count + 1)
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
    
    def get_all_coming_soon_games(self, batch_size=1000, offset=0):
        """Get all games with coming_soon = true in batches."""
        try:
            self.cursor.execute("""
                SELECT app_id, name, release_date::jsonb->>'date' as release_date_string,
                       release_date::jsonb->>'coming_soon' as coming_soon_status,
                       last_updated
                FROM games
                WHERE (release_date::jsonb->>'coming_soon')::boolean = true
                ORDER BY app_id
                LIMIT %s OFFSET %s
            """, (batch_size, offset))
            
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Error getting coming soon games: {str(e)}")
            return []
    
    def get_total_coming_soon_count(self):
        """Get total count of coming soon games."""
        try:
            self.cursor.execute("""
                SELECT COUNT(*) FROM games
                WHERE (release_date::jsonb->>'coming_soon')::boolean = true
            """)
            return self.cursor.fetchone()[0]
        except Exception as e:
            print(f"Error getting total count: {str(e)}")
            return 0
    
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
    
    def update_game_data(self, app_id, name, details, review_summary):
        """Update game data in the database."""
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
            
            # Update the game in the database
            self.cursor.execute("""
                UPDATE games SET
                    name = %s,
                    type = %s,
                    required_age = %s,
                    is_free = %s,
                    detailed_description = %s,
                    short_description = %s,
                    supported_languages = %s,
                    header_image = %s,
                    website = %s,
                    developers = %s,
                    publishers = %s,
                    price_overview = %s,
                    platforms = %s,
                    metacritic = %s,
                    categories = %s,
                    genres = %s,
                    screenshots = %s,
                    movies = %s,
                    recommendations = %s,
                    release_date = %s,
                    support_info = %s,
                    background = %s,
                    content_descriptors = %s,
                    minimum_requirements = %s,
                    recommended_requirements = %s,
                    num_reviews = %s,
                    review_score = %s,
                    review_score_desc = %s,
                    total_positive = %s,
                    total_negative = %s,
                    total_reviews = %s,
                    last_updated = CURRENT_TIMESTAMP
                WHERE app_id = %s
            """, (
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
                app_id
            ))
            
            if self.cursor.rowcount > 0:
                self.conn.commit()
                return True
            else:
                print(f"⚠️  No rows updated for app_id {app_id}")
                return False
                
        except Exception as e:
            print(f"Error updating game {name} (ID: {app_id}): {str(e)}")
            self.conn.rollback()
            return False

def parse_release_date(release_date_string):
    """Parse release date string and return date object if possible."""
    if not release_date_string or release_date_string.lower() in ['coming soon', 'to be announced', 'tba']:
        return None
    
    try:
        # Try parsing common date formats
        date_formats = [
            '%d %b, %Y',    # 25 Nov, 2025
            '%b %d, %Y',    # Nov 25, 2025
            '%Y-%m-%d',     # 2025-11-25
            '%d/%m/%Y',     # 25/11/2025
            '%m/%d/%Y',     # 11/25/2025
            '%Y',           # 2025
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(release_date_string.strip(), fmt).date()
            except ValueError:
                continue
        
        # If no format matches, try to extract year
        import re
        year_match = re.search(r'\b(20\d{2})\b', release_date_string)
        if year_match:
            year = int(year_match.group(1))
            if year <= datetime.now().year:
                return date(year, 1, 1)  # Use Jan 1st for year-only dates
        
        return None
    except Exception:
        return None

def is_early_access_game(categories):
    """Check if game is in early access based on categories."""
    if not categories:
        return False
    
    early_access_indicators = [
        'Early Access',
        'Early access',
        'early access',
        'EarlyAccess',
        'EARLY ACCESS'
    ]
    
    categories_str = json.dumps(categories).lower()
    return any(indicator.lower() in categories_str for indicator in early_access_indicators)

def comprehensive_coming_soon_check():
    """Main function to check all coming soon games comprehensively."""
    steam_api = SteamAPI()
    pipeline = SupabasePipeline()
    
    # Connect to Supabase
    if not pipeline.connect():
        return
    
    try:
        print("🔍 Starting Comprehensive Coming Soon Games Check")
        print("=" * 70)
        
        # Get total count
        total_games = pipeline.get_total_coming_soon_count()
        print(f"📊 Total coming soon games to check: {total_games}")
        
        if total_games == 0:
            print("✅ No coming soon games found")
            return
        
        # Processing statistics
        processed = 0
        updated = 0
        skipped = 0
        early_access_found = 0
        released_games_found = 0
        still_coming_soon = 0
        
        # Process in batches
        batch_size = 100
        offset = 0
        
        while offset < total_games:
            print(f"\n📦 Processing batch: {offset + 1}-{min(offset + batch_size, total_games)} of {total_games}")
            
            # Get batch of games
            games_batch = pipeline.get_all_coming_soon_games(batch_size, offset)
            
            if not games_batch:
                break
            
            for app_id, name, release_date_string, coming_soon_status, last_updated in games_batch:
                processed += 1
                
                print(f"\n🔄 [{processed}/{total_games}] Checking: {name} (ID: {app_id})")
                print(f"   📅 Current release date: {release_date_string}")
                print(f"   ⏰ Last updated: {last_updated}")
                
                # Get fresh data from Steam
                details = steam_api.get_app_details(app_id)
                
                if not details:
                    print(f"   ❌ Failed to get Steam data for {name}")
                    skipped += 1
                    continue
                
                # Check current Steam status
                steam_release_date = details.get('release_date', {})
                steam_coming_soon = steam_release_date.get('coming_soon', True)
                steam_date_string = steam_release_date.get('date', '')
                
                print(f"   🎮 Steam coming_soon: {steam_coming_soon}")
                print(f"   📅 Steam release date: {steam_date_string}")
                
                # Check if it's early access
                is_early_access = is_early_access_game(details.get('categories', []))
                if is_early_access:
                    print(f"   🚀 Early Access game detected!")
                    early_access_found += 1
                
                # Parse release date to check if it's in the past
                parsed_date = parse_release_date(steam_date_string)
                current_date = date.today()
                
                if parsed_date and parsed_date < current_date:
                    print(f"   ⚠️  Release date is in the past: {parsed_date}")
                    released_games_found += 1
                
                # Determine if we should update
                should_update = False
                update_reason = ""
                
                if not steam_coming_soon:
                    should_update = True
                    update_reason = "No longer coming soon"
                    released_games_found += 1
                elif is_early_access and parsed_date and parsed_date < current_date:
                    should_update = True
                    update_reason = "Early access with past release date"
                elif steam_date_string != release_date_string:
                    should_update = True
                    update_reason = "Release date updated"
                else:
                    still_coming_soon += 1
                
                if should_update:
                    print(f"   🔄 Updating game: {update_reason}")
                    
                    # Get review summary for updated data
                    review_summary = {}
                    
                    # Update the game
                    if pipeline.update_game_data(app_id, name, details, review_summary):
                        print(f"   ✅ Successfully updated {name}")
                        updated += 1
                    else:
                        print(f"   ❌ Failed to update {name}")
                        skipped += 1
                else:
                    print(f"   ✅ No update needed - still coming soon")
                
                # Progress update every 50 games
                if processed % 50 == 0:
                    print(f"\n📈 Progress Summary:")
                    print(f"   Processed: {processed}/{total_games}")
                    print(f"   Updated: {updated}")
                    print(f"   Skipped: {skipped}")
                    print(f"   Early Access: {early_access_found}")
                    print(f"   Released/Changed: {released_games_found}")
                    print(f"   Still Coming Soon: {still_coming_soon}")
            
            offset += batch_size
            
            # Brief pause between batches
            if offset < total_games:
                print(f"\n⏳ Pausing 5 seconds before next batch...")
                time.sleep(5)
        
        # Final summary
        print(f"\n🎉 Comprehensive Coming Soon Check Completed!")
        print("=" * 70)
        print(f"📊 Final Statistics:")
        print(f"   Total processed: {processed}")
        print(f"   Successfully updated: {updated}")
        print(f"   Skipped (errors): {skipped}")
        print(f"   Early access games found: {early_access_found}")
        print(f"   Games released/changed: {released_games_found}")
        print(f"   Still coming soon: {still_coming_soon}")
        
        if early_access_found > 0:
            print(f"\n🚀 Found {early_access_found} early access games that may need attention")
        
        if released_games_found > 0:
            print(f"\n🎮 Found {released_games_found} games that are no longer coming soon")
        
    except Exception as e:
        print(f"❌ Error during comprehensive check: {str(e)}")
    finally:
        pipeline.disconnect()


if __name__ == "__main__":
    load_dotenv()
    STEAM_API_KEY = os.getenv('STEAM_API_KEY')
    
    if not STEAM_API_KEY:
        print("❌ STEAM_API_KEY environment variable not set")
        sys.exit(1)
    
    print("🚀 Starting Comprehensive Coming Soon Games Check")
    print("This will check ALL games with coming_soon = true against Steam API")
    print("Estimated time: Several hours for 32,000+ games")
    print("=" * 70)
    
    comprehensive_coming_soon_check()
