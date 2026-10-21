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

from steam_store_notices import empty_store_notices, parse_store_notices

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
        
    def get_app_list(self):
        """Get list of all Steam apps using ISteamApps/GetAppList/v2/.
        
        Returns:
            List of apps with format: [{'appid': int, 'name': str}, ...]
        """
        url = f"{self.base_url}/ISteamApps/GetAppList/v2/"
        try:
            print("📋 Fetching Steam app list...")
            response = requests.get(url, headers=self.headers, timeout=120)
            if response.status_code == 200:
                data = response.json()
                apps = data.get('applist', {}).get('apps', [])
                print(f"✅ Fetched {len(apps)} total apps from Steam")
                return apps
            else:
                print(f"⚠️  GetAppList API returned status {response.status_code}")
                print(f"   Response: {response.text[:200]}")
        except Exception as e:
            print(f"⚠️  Error fetching app list: {str(e)}")
        return None
    
    def check_app_exists(self, app_id):
        """Check if an app_id exists on Steam by trying to get its details."""
        try:
            url = f"https://store.steampowered.com/api/appdetails?appids={app_id}"
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if str(app_id) in data and data[str(app_id)]['success']:
                    return True
            return False
        except:
            return False
    
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
    
    def get_store_page_html(self, app_id, retry_count=0, max_retries=4):
        """Fetch Steam store HTML for an app (used for tags + disclosure notices)."""
        try:
            # Back off harder after gateway failures / rate limits
            base_delay = 0.8 if retry_count else 0.4
            time.sleep(random.uniform(base_delay, base_delay + 0.5) + retry_count * 1.5)
            url = f'https://store.steampowered.com/app/{app_id}/'
            headers = {
                'User-Agent': self.headers['User-Agent'],
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            }
            response = requests.get(url, headers=headers, timeout=20)
            # Steam often returns 502/503 under load — retry with backoff
            if response.status_code in (429, 502, 503, 504):
                if retry_count >= max_retries:
                    print(
                        f"Failed to fetch store page for app_id {app_id}: "
                        f"{response.status_code} after {max_retries} retries"
                    )
                    return None
                wait = 60 if response.status_code == 429 else min(90, 15 * (retry_count + 1))
                print(
                    f"Steam {response.status_code} for app_id {app_id}. "
                    f"Waiting {wait}s before retry {retry_count + 1}/{max_retries}..."
                )
                time.sleep(wait)
                return self.get_store_page_html(app_id, retry_count + 1, max_retries)
            if response.status_code != 200:
                print(f"Failed to fetch store page for app_id {app_id}: {response.status_code}")
                return None
            # Age-gate / soft-block pages are often tiny
            if len(response.text) < 1000:
                if retry_count < max_retries:
                    wait = 20 * (retry_count + 1)
                    print(
                        f"Short store page for app_id {app_id} "
                        f"({len(response.text)} bytes). Waiting {wait}s then retry..."
                    )
                    time.sleep(wait)
                    return self.get_store_page_html(app_id, retry_count + 1, max_retries)
                print(f"Short store page for app_id {app_id} after retries — skipping")
                return None
            return response.text
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError,
                requests.exceptions.TooManyRedirects) as e:
            if retry_count < max_retries:
                wait = 15 * (retry_count + 1)
                print(
                    f"Network error for app_id {app_id}: {e}. "
                    f"Waiting {wait}s before retry {retry_count + 1}/{max_retries}..."
                )
                time.sleep(wait)
                return self.get_store_page_html(app_id, retry_count + 1, max_retries)
            print(f"Network error fetching store page for app_id {app_id}: {str(e)}")
            return None
        except Exception as e:
            print(f"Error fetching store page for app_id {app_id}: {str(e)}")
            return None

    def get_store_page_extras(self, app_id):
        """
        One store-page fetch → user tags + disclosure notices
        (AI / kernel anti-cheat / DRM / EULA).
        """
        html = self.get_store_page_html(app_id)
        if not html:
            return {"tags": [], "notices": empty_store_notices()}

        tag_pattern = r'"tagid":\s*(\d+),\s*"name":\s*"([^"]+)"'
        matches = re.findall(tag_pattern, html)
        tags = [match[1] for match in matches] if matches else []
        if tags:
            print(f"🏷️  Found {len(tags)} tags for app_id {app_id}")
        else:
            print(f"📭 No tags found for app_id {app_id}")

        notices = parse_store_notices(html)
        print(
            f"🧾 Notices for app_id {app_id}: "
            f"ai={notices['made_with_ai']} kernel={notices['requires_kernel_anticheat']} "
            f"drm={notices['has_third_party_drm']} denuvo={notices['has_denuvo']} "
            f"eula={notices['requires_eula']}"
        )
        return {"tags": tags, "notices": notices}

    def get_game_tags(self, app_id, retry_count=0):
        """Get user tags for a specific game from Steam store page."""
        extras = self.get_store_page_extras(app_id)
        return extras.get("tags") or []

    def get_store_notices(self, app_id):
        """Get AI / kernel / DRM / EULA disclosure flags from the store page."""
        extras = self.get_store_page_extras(app_id)
        return extras.get("notices") or empty_store_notices()

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
            self.ensure_store_notice_columns()
            return True
        except Exception as e:
            print(f"❌ Error connecting to Supabase: {str(e)}")
            return False

    def ensure_store_notice_columns(self):
        """Add store-notice columns to games if missing (idempotent)."""
        statements = [
            "ALTER TABLE games ADD COLUMN IF NOT EXISTS made_with_ai BOOLEAN DEFAULT FALSE",
            "ALTER TABLE games ADD COLUMN IF NOT EXISTS made_with_ai_disclosure TEXT",
            "ALTER TABLE games ADD COLUMN IF NOT EXISTS requires_kernel_anticheat BOOLEAN DEFAULT FALSE",
            "ALTER TABLE games ADD COLUMN IF NOT EXISTS kernel_anticheat_name TEXT",
            "ALTER TABLE games ADD COLUMN IF NOT EXISTS has_third_party_drm BOOLEAN DEFAULT FALSE",
            "ALTER TABLE games ADD COLUMN IF NOT EXISTS third_party_drm TEXT",
            "ALTER TABLE games ADD COLUMN IF NOT EXISTS has_denuvo BOOLEAN DEFAULT FALSE",
            "ALTER TABLE games ADD COLUMN IF NOT EXISTS requires_eula BOOLEAN DEFAULT FALSE",
            "ALTER TABLE games ADD COLUMN IF NOT EXISTS eula_label TEXT",
            "ALTER TABLE games ADD COLUMN IF NOT EXISTS store_notices_updated_at TIMESTAMP",
        ]
        try:
            for stmt in statements:
                self.cursor.execute(stmt)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"⚠️  Could not ensure store-notice columns: {e}")
    
    def disconnect(self):
        """Disconnect from Supabase database."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("✅ Disconnected from Supabase database")
    
    def get_highest_app_id(self):
        """Get the highest app_id from the games table in Supabase."""
        try:
            self.cursor.execute("SELECT MAX(app_id) FROM games")
            result = self.cursor.fetchone()
            return result[0] if result and result[0] is not None else 0
        except Exception as e:
            print(f"Error getting highest app_id: {str(e)}")
            return 0
    
    def get_existing_app_ids(self):
        """Get all app_ids currently in the database."""
        try:
            self.cursor.execute("SELECT app_id FROM games")
            result = self.cursor.fetchall()
            return set(row[0] for row in result)
        except Exception as e:
            print(f"Error getting existing app_ids: {str(e)}")
            return set()
    
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
    
    def insert_game(self, app_id, name, details, review_summary, store_notices=None):
        """Insert a game into the Supabase games table."""
        try:
            notices = store_notices or empty_store_notices()

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

            metacritic = details.get('metacritic') or {}
            metacritic_score = metacritic.get('score') if isinstance(metacritic, dict) else None
            recommendations = details.get('recommendations') or {}
            recommendations_total = (
                recommendations.get('total') if isinstance(recommendations, dict) else None
            )
            now = datetime.now()

            insert_sql = """
                INSERT INTO games (
                    app_id, name, type, required_age, is_free, detailed_description,
                    short_description, supported_languages, header_image, website,
                    developers, publishers, price_overview, platforms, metacritic,
                    categories, genres, screenshots, movies, recommendations,
                    release_date, support_info, background, content_descriptors,
                    minimum_requirements, recommended_requirements, num_reviews,
                    review_score, review_score_desc, total_positive, total_negative,
                    total_reviews, last_updated,
                    made_with_ai, made_with_ai_disclosure,
                    requires_kernel_anticheat, kernel_anticheat_name,
                    has_third_party_drm, third_party_drm, has_denuvo,
                    requires_eula, eula_label, store_notices_updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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
                    last_updated = CURRENT_TIMESTAMP,
                    made_with_ai = EXCLUDED.made_with_ai,
                    made_with_ai_disclosure = EXCLUDED.made_with_ai_disclosure,
                    requires_kernel_anticheat = EXCLUDED.requires_kernel_anticheat,
                    kernel_anticheat_name = EXCLUDED.kernel_anticheat_name,
                    has_third_party_drm = EXCLUDED.has_third_party_drm,
                    third_party_drm = EXCLUDED.third_party_drm,
                    has_denuvo = EXCLUDED.has_denuvo,
                    requires_eula = EXCLUDED.requires_eula,
                    eula_label = EXCLUDED.eula_label,
                    store_notices_updated_at = EXCLUDED.store_notices_updated_at
            """
            params = (
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
                json.dumps(details.get('developers') or []),
                json.dumps(details.get('publishers') or []),
                json.dumps(details.get('price_overview') or {}),
                json.dumps(details.get('platforms') or {}),
                metacritic_score,
                json.dumps(details.get('categories') or []),
                json.dumps(details.get('genres') or []),
                json.dumps(details.get('screenshots') or []),
                json.dumps(details.get('movies') or []),
                recommendations_total,
                json.dumps(details.get('release_date') or {}),
                json.dumps(details.get('support_info') or {}),
                details.get('background'),
                json.dumps(details.get('content_descriptors') or {}),
                json.dumps(min_reqs),
                json.dumps(rec_reqs),
                review_summary.get('num_reviews'),
                review_summary.get('review_score'),
                review_summary.get('review_score_desc'),
                review_summary.get('total_positive'),
                review_summary.get('total_negative'),
                review_summary.get('total_reviews'),
                now,
                bool(notices.get('made_with_ai')),
                notices.get('made_with_ai_disclosure'),
                bool(notices.get('requires_kernel_anticheat')),
                notices.get('kernel_anticheat_name'),
                bool(notices.get('has_third_party_drm')),
                notices.get('third_party_drm'),
                bool(notices.get('has_denuvo')),
                bool(notices.get('requires_eula')),
                notices.get('eula_label'),
                now,
            )
            placeholder_count = insert_sql.count('%s')
            if placeholder_count != len(params):
                raise ValueError(
                    f"Insert param mismatch: {placeholder_count} placeholders vs {len(params)} values"
                )

            self.cursor.execute(insert_sql, params)
            
            # Insert price history if available
            price_data = details.get('price_overview')
            if isinstance(price_data, dict) and price_data:
                self.cursor.execute("""
                    INSERT INTO price_history (app_id, price, discount_percent, initial_price, final_price)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    app_id,
                    price_data.get('final') or price_data.get('price'),
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

def update_steam_data_to_supabase(api_key, start_app_id=None, limit=None, fetch_tags=True, check_all_missing=False):
    """Update Steam games data directly to Supabase database.
    
    Args:
        api_key: Steam API key
        start_app_id: If provided, start checking from this app_id. If None, uses latest in DB + 10
        limit: Number of games to check (default 100, in increments of 10)
        fetch_tags: Whether to fetch and store game tags
        check_all_missing: Not used - kept for compatibility
    """
    steam_api = SteamAPI(api_key)
    pipeline = SupabasePipeline()
    
    # Connect to Supabase
    if not pipeline.connect():
        return
    
    try:
        # Get the latest app_id from database
        if start_app_id is None:
            latest_app_id = pipeline.get_highest_app_id()
            if latest_app_id is None or latest_app_id == 0:
                print("❌ No games found in database. Cannot determine starting point.")
                return
            # Start checking from the next increment of 10
            start_app_id = ((latest_app_id // 10) + 1) * 10
            print(f"📊 Latest app_id in database: {latest_app_id}")
            print(f"🔄 Starting check from app_id: {start_app_id}")
        else:
            print(f"🔄 Starting check from app_id: {start_app_id}")
        
        # Default to checking next 1000 games (in increments of 10, so checks 1000 app_ids)
        if limit is None:
            limit = 1000
        
        # Generate list of app_ids to check (in increments of 10)
        app_ids_to_check = []
        for i in range(limit):
            app_id = start_app_id + (i * 10)
            app_ids_to_check.append(app_id)
        
        print(f"📋 Will check {len(app_ids_to_check)} app_ids (increments of 10)")
        print(f"   Range: {app_ids_to_check[0]} to {app_ids_to_check[-1]}")
        
        # Check which app_ids already exist in database
        existing_app_ids = pipeline.get_existing_app_ids()
        app_ids_to_process = [app_id for app_id in app_ids_to_check if app_id not in existing_app_ids]
        
        print(f"📊 Found {len(app_ids_to_process)} new app_ids to process (out of {len(app_ids_to_check)} checked)")
        
        if len(app_ids_to_process) == 0:
            print("✅ No new games to process in this range!")
            return
        
        # Convert to app list format for processing
        valid_apps = [{'appid': app_id, 'name': f'App {app_id}'} for app_id in app_ids_to_process]
        
        total_apps = len(valid_apps)
        print(f"📊 Processing {total_apps} new games")
        
        # Process apps - check each app_id to see if it exists on Steam
        processed = 0
        skipped = 0
        successful = 0
        tags_collected = 0
        not_found = 0
        
        for app in valid_apps:
            app_id = app['appid']
            
            print(f"\n🔄 Processing {processed + 1}/{total_apps}: Checking app_id {app_id}...")
            
            # Get detailed information
            details = steam_api.get_app_details(app_id)
            if not details:
                print(f"   ❌ App_id {app_id} not found on Steam")
                not_found += 1
                skipped += 1
                processed += 1
                continue
            
            # Get the actual name from details
            name = details.get('name', f'App {app_id}')
            print(f"   ✅ Found: {name}")
            
            # Skip non-game content
            app_type = details.get('type', '').lower()
            if app_type not in ['game']:
                print(f"   ⏭️ Skipping {name} - Type: {app_type}")
                skipped += 1
                processed += 1
                continue
            
            # Get review summary
            review_summary = steam_api.get_review_summary(app_id)
            print(f"   📊 Review summary: {json.dumps(review_summary, indent=2)}")

            # Store page: tags + AI/kernel/DRM/EULA notices (single HTML fetch)
            print(f"   🧾 Fetching store-page extras for {name}...")
            extras = steam_api.get_store_page_extras(app_id)
            notices = extras.get("notices") or empty_store_notices()
            tags = extras.get("tags") or []
            
            # Insert into Supabase
            if pipeline.insert_game(app_id, name, details, review_summary, store_notices=notices):
                print(f"   ✅ Successfully added {name} to database")
                successful += 1
                
                # Insert tags if enabled
                if fetch_tags and tags:
                    if pipeline.insert_game_tags(app_id, tags):
                        tags_collected += len(tags)
                        print(f"   🏷️  Successfully collected {len(tags)} tags for {name}")
                    else:
                        print(f"   ❌ Failed to insert tags for {name}")
                elif fetch_tags:
                    print(f"   📭 No tags found for {name}")
            else:
                print(f"   ❌ Failed to insert {name}")
                skipped += 1
            
            processed += 1
            
            # Progress update every 10 games
            if processed % 10 == 0:
                print(f"\n📈 Progress: {processed}/{total_apps} checked, {successful} added, {skipped} skipped, {not_found} not found, {tags_collected} tags collected")
        
        print(f"\n🎉 Pipeline completed!")
        print(f"📊 Total checked: {processed}")
        print(f"✅ Games added: {successful}")
        print(f"⏭️ Skipped: {skipped}")
        print(f"❌ Not found on Steam: {not_found}")
        print(f"🏷️ Tags collected: {tags_collected}")
        
    except Exception as e:
        print(f"❌ Error during pipeline execution: {str(e)}")
    finally:
        pipeline.disconnect()

if __name__ == "__main__":
    load_dotenv()
    STEAM_API_KEY = os.getenv('STEAM_API_KEY')
    START_APP_ID = None  # If None, will use latest in DB + next increment of 10
    LIMIT = 1000  # Number of games to check (in increments of 10, so checks 1000 app_ids)
    FETCH_TAGS = True
    CHECK_ALL_MISSING = False  # Not used - kept for compatibility
    print("Starting Steam to Supabase Pipeline")
    print("=" * 50)
    update_steam_data_to_supabase(STEAM_API_KEY, START_APP_ID, LIMIT, FETCH_TAGS, CHECK_ALL_MISSING) 