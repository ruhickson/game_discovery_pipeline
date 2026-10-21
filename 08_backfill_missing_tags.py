#!/usr/bin/env python3
"""
Backfill Missing Game Tags

This script identifies games that have a release date and are of type 'game'
but don't have any tags in the game_tags table, then fetches and inserts their tags.
"""

import requests
import time
import json
import random
import psycopg2
import re
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
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    def get_game_tags(self, app_id, retry_count=0):
        """Get user tags for a specific game from Steam store page."""
        try:
            # Add delay between requests (minimum 0.6 second)
            time.sleep(0.6)
            url = f'https://store.steampowered.com/app/{app_id}/'
            
            headers = {
                'User-Agent': self.headers['User-Agent'],
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            
            # Handle rate limiting (429) or connection error (443)
            if response.status_code == 429 or response.status_code == 443:
                print(f"   ⚠️  Rate limited ({response.status_code}) for app_id {app_id}. Waiting 2 minutes before retrying...")
                time.sleep(120)
                return self.get_game_tags(app_id, retry_count + 1)
            
            if response.status_code == 200:
                # Debug: Check if we got a valid response
                if len(response.text) < 1000:
                    print(f"   ⚠️  Short response for app_id {app_id} (might be blocked or redirected)")
                    return []
                
                # Look for tags in the HTML - try multiple patterns
                tag_patterns = [
                    r'"tagid":\s*(\d+),\s*"name":\s*"([^"]+)"',  # Original pattern
                    r'"tagid":\s*(\d+),\s*"name":\s*"([^"]+)"\s*,\s*"count":\s*\d+',  # With count
                    r'data-tagid="(\d+)"[^>]*>([^<]+)<',  # HTML data attribute
                ]
                
                tags = []
                for pattern in tag_patterns:
                    matches = re.findall(pattern, response.text)
                    if matches:
                        # Extract tag names (second capture group)
                        found_tags = [match[1] for match in matches]
                        tags.extend(found_tags)
                        break
                
                if tags:
                    # Remove duplicates while preserving order
                    unique_tags = list(dict.fromkeys(tags))
                    print(f"   🏷️  Found {len(unique_tags)} tags for app_id {app_id}")
                    return unique_tags
                else:
                    # Debug: Check if we can find any tag-related content
                    if 'tag' in response.text.lower():
                        print(f"   🔍 Found 'tag' in response for app_id {app_id} but no matches")
                    else:
                        print(f"   📭 No tag content found for app_id {app_id}")
            
            else:
                print(f"   ❌ HTTP {response.status_code} for app_id {app_id}")
            
            return []
            
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, 
                requests.exceptions.TooManyRedirects) as e:
            print(f"   ❌ Network error getting tags for app_id {app_id}: {str(e)}")
            return []
        except Exception as e:
            print(f"   ❌ Error getting tags for app_id {app_id}: {str(e)}")
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
    
    def get_games_without_tags(self, limit=None, recent_only=False, skip_app_ids=None):
        """Get games that have release dates and are type 'game' but don't have tags."""
        try:
            query = """
                SELECT DISTINCT g.app_id, g.name, g.release_date_actual
                FROM games g
                WHERE g.release_date_actual IS NOT NULL
                AND g.type = 'game'
                AND g.app_id NOT IN (SELECT DISTINCT app_id FROM game_tags)
            """
            
            if recent_only:
                # Get games from 2020-2024 for better tag availability (avoiding very recent games)
                query += " AND g.release_date_actual >= '2020-01-01' AND g.release_date_actual < '2025-01-01'"
            
            if skip_app_ids:
                # Skip already processed app_ids for resume functionality
                skip_ids_str = ','.join(map(str, skip_app_ids))
                query += f" AND g.app_id NOT IN ({skip_ids_str})"
            
            query += " ORDER BY g.release_date_actual DESC, g.app_id"
            
            if limit:
                query += f" LIMIT {limit}"
            
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            
            print(f"📊 Found {len(results)} games without tags")
            return results
            
        except Exception as e:
            print(f"❌ Error getting games without tags: {str(e)}")
            return []
    
    def insert_game_tags(self, app_id, tags):
        """Insert tags for a game into the game_tags table."""
        if not tags:
            return True
        
        try:
            # Insert tags using executemany for better performance
            # Note: id is auto-generated, created_at will use default CURRENT_TIMESTAMP
            tag_data = [(app_id, tag) for tag in tags]
            self.cursor.executemany("""
                INSERT INTO game_tags (app_id, tag) 
                VALUES (%s, %s) 
                ON CONFLICT (app_id, tag) DO NOTHING
            """, tag_data)
            
            self.conn.commit()
            inserted_count = len(tag_data)
            print(f"   ✅ Inserted {inserted_count} tags for app_id {app_id}")
            return True
            
        except Exception as e:
            print(f"   ❌ Error inserting tags for app_id {app_id}: {str(e)}")
            self.conn.rollback()
            return False

def load_progress():
    """Load progress from progress file."""
    progress_file = "backfill_progress.json"
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r') as f:
                return json.load(f)
        except:
            return {'processed_app_ids': []}
    return {'processed_app_ids': []}

def save_progress(processed_app_ids):
    """Save progress to progress file."""
    progress_file = "backfill_progress.json"
    try:
        with open(progress_file, 'w') as f:
            json.dump({'processed_app_ids': processed_app_ids}, f)
    except Exception as e:
        print(f"⚠️  Warning: Could not save progress: {str(e)}")

def clear_progress():
    """Clear the progress file."""
    progress_file = "backfill_progress.json"
    if os.path.exists(progress_file):
        os.remove(progress_file)
        print("✅ Progress file cleared")

def backfill_missing_tags(limit=None, recent_only=False, resume=True):
    """Main function to backfill missing tags for games."""
    steam_api = SteamAPI()
    pipeline = SupabasePipeline()
    
    # Load progress for resume functionality
    progress = load_progress() if resume else {'processed_app_ids': []}
    processed_app_ids = progress.get('processed_app_ids', [])
    
    if processed_app_ids and resume:
        print(f"📋 Resuming from previous session - skipping {len(processed_app_ids)} already processed games")
    
    # Connect to Supabase
    if not pipeline.connect():
        return
    
    try:
        print("🏷️  Starting Missing Tags Backfill")
        print("=" * 70)
        
        # Get games without tags (excluding already processed ones)
        games_without_tags = pipeline.get_games_without_tags(limit, recent_only, processed_app_ids if resume else None)
        
        if not games_without_tags:
            print("✅ No games found that need tags backfilled")
            if resume:
                clear_progress()
            return
        
        total_games = len(games_without_tags)
        print(f"🎮 Processing {total_games} games")
        print("=" * 70)
        
        # Statistics
        processed = 0
        successful = 0
        failed = 0
        tags_collected = 0
        no_tags_found = 0
        
        for app_id, name, release_date in games_without_tags:
            processed += 1
            
            print(f"\n🔄 [{processed}/{total_games}] Processing: {name} (ID: {app_id}) - Released: {release_date}")
            
            # Get tags from Steam
            tags = steam_api.get_game_tags(app_id)
            
            if tags:
                # Insert tags into database
                if pipeline.insert_game_tags(app_id, tags):
                    successful += 1
                    tags_collected += len(tags)
                else:
                    failed += 1
            else:
                no_tags_found += 1
            
            # Track processed app_id for resume functionality
            processed_app_ids.append(app_id)
            
            # Save progress every 5 games
            if processed % 5 == 0:
                save_progress(processed_app_ids)
            
            # Progress update every 10 games
            if processed % 10 == 0:
                print(f"\n📈 Progress Summary:")
                print(f"   Processed: {processed}/{total_games}")
                print(f"   Successful: {successful}")
                print(f"   No tags found: {no_tags_found}")
                print(f"   Failed: {failed}")
                print(f"   Total tags collected: {tags_collected}")
        
        # Final summary
        print(f"\n🎉 Tag Backfill Completed!")
        print("=" * 70)
        print(f"📊 Final Statistics:")
        print(f"   Total processed: {processed}")
        print(f"   Successfully added tags: {successful}")
        print(f"   No tags found: {no_tags_found}")
        print(f"   Failed: {failed}")
        print(f"   Total tags collected: {tags_collected}")
        
        if successful > 0:
            avg_tags = tags_collected / successful
            print(f"   Average tags per game: {avg_tags:.1f}")
        
        # Clear progress file on successful completion
        if resume:
            clear_progress()
        
    except KeyboardInterrupt:
        print(f"\n⚠️  Tag backfill interrupted by user")
        print(f"   Progress: {processed}/{total_games} games processed")
        save_progress(processed_app_ids)
        print(f"   💾 Progress saved - run the script again to resume from app_id {processed_app_ids[-1] if processed_app_ids else 'start'}")
    except Exception as e:
        print(f"❌ Error during tag backfill: {str(e)}")
        save_progress(processed_app_ids)
        print(f"   💾 Progress saved for resume")
    finally:
        pipeline.disconnect()


if __name__ == "__main__":
    load_dotenv()
    STEAM_API_KEY = os.getenv('STEAM_API_KEY')
    
    if not STEAM_API_KEY:
        print("❌ STEAM_API_KEY environment variable not set")
        sys.exit(1)
    
    # You can set a limit here for testing, or None to process all games
    LIMIT = None  # Set to a number like 100 for testing, or None for all games
    
    print("🚀 Starting Game Tags Backfill")
    print("This will fetch tags from Steam for games that don't have any tags")
    print("With resume capability - progress is saved every 5 games")
    print("=" * 70)
    
    backfill_missing_tags(limit=LIMIT, recent_only=True, resume=True)

