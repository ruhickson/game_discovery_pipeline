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

class GOGAPI:
    def __init__(self):
        self.base_url = "https://catalog.gog.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json',
        }
    
    def get_all_games(self, limit=None):
        """Get all games from GOG catalog using pagination."""
        all_products = []
        page_size = 48
        search_after = 0
        
        print("📋 Fetching GOG catalog...")
        
        while True:
            try:
                # Add delay between requests
                time.sleep(random.uniform(0.5, 1.0))
                
                url = f"{self.base_url}/v1/catalog"
                params = {
                    'limit': page_size,
                    'order': 'asc:externalProductId',
                    'productType': 'in:game,pack',
                    'countryCode': 'US',
                    'locale': 'en-US',
                    'currencyCode': 'USD',
                    'searchAfter': search_after
                }
                
                print(f"🔄 Fetching page starting at {search_after}...")
                response = requests.get(url, headers=self.headers, params=params, timeout=30)
                
                if response.status_code == 429:
                    print(f"⚠️ Rate limited (429). Waiting 60 seconds...")
                    time.sleep(60)
                    continue
                
                if response.status_code != 200:
                    print(f"❌ Failed to fetch page: {response.status_code}")
                    print(f"Response: {response.text[:200]}")
                    break
                
                data = response.json()
                
                # Handle different response formats
                if isinstance(data, list):
                    products = data
                elif isinstance(data, dict):
                    products = data.get('products', []) or data.get('_embedded', {}).get('products', [])
                else:
                    products = []
                
                if not products:
                    print("✅ No more products to fetch")
                    break
                
                # Filter to only games (exclude DLC, extras, etc.)
                games = [p for p in products if p.get('productType') == 'game' or p.get('type') == 'game']
                all_products.extend(games)
                
                print(f"📊 Found {len(games)} games in this page (total: {len(all_products)})")
                
                # Check if we've reached the limit
                if limit and len(all_products) >= limit:
                    all_products = all_products[:limit]
                    break
                
                # Check if there are more pages
                if len(products) < page_size:
                    print("✅ Reached end of catalog")
                    break
                
                # Update search_after for next page
                # Use the last product's externalProductId or id as the next searchAfter
                if products:
                    last_product = products[-1]
                    # Try different field names for product ID
                    next_id = (last_product.get('externalProductId') or 
                              last_product.get('id') or 
                              last_product.get('productId'))
                    if next_id:
                        search_after = next_id
                    else:
                        search_after += page_size
                else:
                    break
                    
            except Exception as e:
                print(f"❌ Error fetching page: {str(e)}")
                break
        
        print(f"✅ Fetched {len(all_products)} games from GOG catalog")
        return all_products
    
    def get_game_details(self, product_id, retry_count=0):
        """Get detailed information about a specific game."""
        try:
            # Add delay between requests
            time.sleep(random.uniform(0.3, 0.6))
            
            url = f"https://api.gog.com/products/{product_id}"
            params = {
                'locale': 'en_US',
                'expand': 'downloads,screenshots,videos,changelog'
            }
            
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            
            if response.status_code == 429:
                if retry_count < 3:
                    wait_time = 60 * (retry_count + 1)
                    print(f"⚠️ Rate limited (429). Waiting {wait_time} seconds before retry {retry_count + 1}...")
                    time.sleep(wait_time)
                    return self.get_game_details(product_id, retry_count + 1)
                else:
                    print(f"❌ Too many rate limit errors for product {product_id}")
                    return None
            
            if response.status_code != 200:
                print(f"⚠️ Failed to get details for product {product_id}: {response.status_code}")
                return None
            
            return response.json()
            
        except Exception as e:
            print(f"❌ Error getting details for product {product_id}: {str(e)}")
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
    
    def create_gog_games_table(self):
        """Create the gog_games table if it doesn't exist."""
        try:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS gog_games (
                    gog_id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    slug TEXT,
                    external_product_id TEXT,
                    product_type TEXT,
                    developer TEXT,
                    publisher TEXT,
                    release_date TEXT,
                    price_final DECIMAL(10,2),
                    price_base DECIMAL(10,2),
                    discount_percentage INTEGER,
                    is_discounted BOOLEAN,
                    rating DECIMAL(3,2),
                    rating_count INTEGER,
                    description TEXT,
                    short_description TEXT,
                    genres JSONB,
                    tags JSONB,
                    screenshots JSONB,
                    videos JSONB,
                    platforms JSONB,
                    languages JSONB,
                    links JSONB,
                    gallery JSONB,
                    full_details JSONB,
                    is_on_steam BOOLEAN DEFAULT FALSE,
                    steam_app_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_gog_games_title ON gog_games(title);
                CREATE INDEX IF NOT EXISTS idx_gog_games_slug ON gog_games(slug);
                CREATE INDEX IF NOT EXISTS idx_gog_games_is_on_steam ON gog_games(is_on_steam);
            """)
            
            self.conn.commit()
            print("✅ Created/verified gog_games table")
            return True
        except Exception as e:
            print(f"❌ Error creating gog_games table: {str(e)}")
            return False
    
    def insert_or_update_game(self, game_data, full_details=None):
        """Insert or update a GOG game in the database."""
        try:
            # Try different field names for GOG ID
            gog_id = (game_data.get('id') or 
                     game_data.get('externalProductId') or
                     game_data.get('productId'))
            if not gog_id and full_details:
                gog_id = full_details.get('id') or full_details.get('productId')
            
            if not gog_id:
                print(f"⚠️ Warning: No ID found for game {game_data.get('title', 'Unknown')}")
                return False
            
            title = game_data.get('title', '') or (full_details.get('title') if full_details else '')
            slug = game_data.get('slug', '') or (full_details.get('slug') if full_details else '')
            external_product_id = game_data.get('externalProductId', '') or str(gog_id)
            product_type = game_data.get('productType', '') or game_data.get('type', '')
            
            # Extract developer and publisher
            developer = None
            publisher = None
            if full_details:
                developer = full_details.get('developer', '')
                publisher = full_details.get('publisher', '')
            
            # Extract pricing
            price_data = game_data.get('price', {})
            price_final = price_data.get('finalAmount')
            price_base = price_data.get('baseAmount')
            discount_percentage = price_data.get('discountPercentage', 0)
            is_discounted = discount_percentage > 0
            
            # Extract release date
            release_date = game_data.get('releaseDate')
            
            # Extract rating
            rating = game_data.get('rating')
            rating_count = game_data.get('reviewsCount', 0)
            
            # Extract descriptions
            description = None
            short_description = None
            if full_details:
                description = full_details.get('description', {}).get('full', '')
                short_description = full_details.get('description', {}).get('lead', '')
            
            # Extract genres, tags, screenshots, videos
            genres = game_data.get('genres', [])
            tags = game_data.get('tags', [])
            screenshots = full_details.get('screenshots', []) if full_details else []
            videos = full_details.get('videos', []) if full_details else []
            
            # Extract platforms and languages
            platforms = game_data.get('platforms', [])
            languages = game_data.get('languages', [])
            
            # Extract links
            links = game_data.get('links', {})
            
            # Extract gallery
            gallery = game_data.get('gallery', [])
            
            self.cursor.execute("""
                INSERT INTO gog_games (
                    gog_id, title, slug, external_product_id, product_type,
                    developer, publisher, release_date,
                    price_final, price_base, discount_percentage, is_discounted,
                    rating, rating_count,
                    description, short_description,
                    genres, tags, screenshots, videos,
                    platforms, languages, links, gallery,
                    full_details, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, CURRENT_TIMESTAMP
                ) ON CONFLICT (gog_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    slug = EXCLUDED.slug,
                    external_product_id = EXCLUDED.external_product_id,
                    product_type = EXCLUDED.product_type,
                    developer = EXCLUDED.developer,
                    publisher = EXCLUDED.publisher,
                    release_date = EXCLUDED.release_date,
                    price_final = EXCLUDED.price_final,
                    price_base = EXCLUDED.price_base,
                    discount_percentage = EXCLUDED.discount_percentage,
                    is_discounted = EXCLUDED.is_discounted,
                    rating = EXCLUDED.rating,
                    rating_count = EXCLUDED.rating_count,
                    description = EXCLUDED.description,
                    short_description = EXCLUDED.short_description,
                    genres = EXCLUDED.genres,
                    tags = EXCLUDED.tags,
                    screenshots = EXCLUDED.screenshots,
                    videos = EXCLUDED.videos,
                    platforms = EXCLUDED.platforms,
                    languages = EXCLUDED.languages,
                    links = EXCLUDED.links,
                    gallery = EXCLUDED.gallery,
                    full_details = EXCLUDED.full_details,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                gog_id, title, slug, external_product_id, product_type,
                developer, publisher, release_date,
                price_final, price_base, discount_percentage, is_discounted,
                rating, rating_count,
                description, short_description,
                json.dumps(genres), json.dumps(tags), json.dumps(screenshots), json.dumps(videos),
                json.dumps(platforms), json.dumps(languages), json.dumps(links), json.dumps(gallery),
                json.dumps(full_details) if full_details else None
            ))
            
            self.conn.commit()
            return True
            
        except Exception as e:
            print(f"❌ Error inserting/updating game {game_data.get('title', 'Unknown')}: {str(e)}")
            self.conn.rollback()
            return False
    
    def check_steam_match(self, gog_title, gog_developer=None):
        """Check if a GOG game exists on Steam by matching title and developer."""
        try:
            # Try exact title match first
            self.cursor.execute("""
                SELECT app_id, name, developers
                FROM games
                WHERE LOWER(TRIM(name)) = LOWER(TRIM(%s))
                AND type = 'game'
                LIMIT 1
            """, (gog_title,))
            
            result = self.cursor.fetchone()
            if result:
                return result[0]  # Return Steam app_id
            
            # Try fuzzy matching on title (contains)
            self.cursor.execute("""
                SELECT app_id, name, developers
                FROM games
                WHERE LOWER(name) LIKE LOWER(%s)
                AND type = 'game'
                LIMIT 5
            """, (f'%{gog_title}%',))
            
            results = self.cursor.fetchall()
            if results and len(results) == 1:
                # If only one match, use it
                return results[0][0]
            elif results and gog_developer:
                # If multiple matches, try to match by developer
                for app_id, name, devs_json in results:
                    if devs_json:
                        try:
                            devs = json.loads(devs_json) if isinstance(devs_json, str) else devs_json
                            if isinstance(devs, list) and gog_developer in devs:
                                return app_id
                        except:
                            pass
            
            return None
            
        except Exception as e:
            print(f"⚠️ Error checking Steam match: {str(e)}")
            return None
    
    def update_steam_matches(self):
        """Update is_on_steam and steam_app_id for all GOG games."""
        try:
            print("🔍 Checking for Steam matches...")
            self.cursor.execute("""
                SELECT gog_id, title, developer
                FROM gog_games
                WHERE is_on_steam = FALSE OR is_on_steam IS NULL
            """)
            
            games_to_check = self.cursor.fetchall()
            print(f"📊 Found {len(games_to_check)} games to check for Steam matches")
            
            matched = 0
            for gog_id, title, developer in games_to_check:
                steam_app_id = self.check_steam_match(title, developer)
                if steam_app_id:
                    self.cursor.execute("""
                        UPDATE gog_games
                        SET is_on_steam = TRUE, steam_app_id = %s
                        WHERE gog_id = %s
                    """, (steam_app_id, gog_id))
                    matched += 1
                    if matched % 100 == 0:
                        print(f"✅ Matched {matched} games so far...")
                        self.conn.commit()
            
            self.conn.commit()
            print(f"✅ Updated Steam matches: {matched} games found on Steam")
            return matched
            
        except Exception as e:
            print(f"❌ Error updating Steam matches: {str(e)}")
            self.conn.rollback()
            return 0


def collect_gog_games(limit=None, fetch_details=True, update_steam_matches=True):
    """Collect all GOG games and store them in Supabase."""
    gog_api = GOGAPI()
    pipeline = SupabasePipeline()
    
    # Connect to Supabase
    if not pipeline.connect():
        return
    
    try:
        # Create table if needed
        if not pipeline.create_gog_games_table():
            return
        
        # Get all games from GOG
        games = gog_api.get_all_games(limit=limit)
        
        if not games:
            print("❌ No games found")
            return
        
        total_games = len(games)
        print(f"📊 Processing {total_games} games...")
        
        successful = 0
        skipped = 0
        failed = 0
        
        for idx, game in enumerate(games, 1):
            title = game.get('title', 'Unknown')
            gog_id = game.get('id')
            
            print(f"\n🔄 [{idx}/{total_games}] Processing: {title} (ID: {gog_id})...")
            
            # Fetch detailed information if requested
            full_details = None
            if fetch_details:
                # Try different field names for product ID
                product_id = (game.get('externalProductId') or 
                             game.get('id') or 
                             game.get('productId') or
                             game.get('slug'))
                if product_id:
                    full_details = gog_api.get_game_details(product_id)
                if full_details:
                    print(f"✅ Fetched details for {title}")
                else:
                    print(f"⚠️ Could not fetch details for {title}, using basic data")
            
            # Insert/update game
            if pipeline.insert_or_update_game(game, full_details):
                print(f"✅ Successfully processed {title}")
                successful += 1
            else:
                print(f"❌ Failed to process {title}")
                failed += 1
            
            # Progress update every 50 games
            if idx % 50 == 0:
                print(f"\n📈 Progress: {idx}/{total_games} processed, {successful} successful, {failed} failed")
        
        print(f"\n🎉 GOG collection completed!")
        print(f"📊 Total processed: {total_games}")
        print(f"✅ Successful: {successful}")
        print(f"❌ Failed: {failed}")
        
        # Update Steam matches if requested
        if update_steam_matches:
            print("\n🔍 Checking for Steam matches...")
            pipeline.update_steam_matches()
        
        # Print summary
        pipeline.cursor.execute("""
            SELECT 
                COUNT(*) as total_gog_games,
                COUNT(*) FILTER (WHERE is_on_steam = TRUE) as on_steam,
                COUNT(*) FILTER (WHERE is_on_steam = FALSE OR is_on_steam IS NULL) as not_on_steam
            FROM gog_games
        """)
        summary = pipeline.cursor.fetchone()
        if summary:
            total, on_steam, not_on_steam = summary
            print(f"\n📊 Summary:")
            print(f"   Total GOG games: {total}")
            print(f"   On Steam: {on_steam}")
            print(f"   Not on Steam: {not_on_steam}")
            print(f"   GOG exclusives: {not_on_steam} games")
        
    except Exception as e:
        print(f"❌ Error during collection: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        pipeline.disconnect()


if __name__ == "__main__":
    load_dotenv()
    
    # Configuration
    LIMIT = None  # Set to a number to limit games processed (e.g., 100 for testing)
    FETCH_DETAILS = True  # Set to False to skip detailed API calls (faster but less data)
    UPDATE_STEAM_MATCHES = True  # Set to False to skip Steam matching
    
    print("Starting GOG Games Collection Pipeline")
    print("=" * 50)
    collect_gog_games(limit=LIMIT, fetch_details=FETCH_DETAILS, update_steam_matches=UPDATE_STEAM_MATCHES)

