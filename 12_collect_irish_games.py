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
from bs4 import BeautifulSoup

load_dotenv()

# Ensure stdout/stderr handle UTF-8 so emojis and non-ASCII don't crash on Windows consoles
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

class SteamCuratorAPI:
    """Class to fetch games from Steam Curator pages"""
    
    def __init__(self):
        self.base_url = "https://store.steampowered.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
    
    def get_curator_games(self, curator_id, max_pages=None):
        """
        Fetch all games from a Steam curator page.
        
        Args:
            curator_id: The curator ID (e.g., 41458618 for Games From Ireland)
            max_pages: Maximum number of pages to fetch (None for all)
        
        Returns:
            List of app_ids
        """
        app_ids = set()  # Use set to avoid duplicates
        page = 1
        per_page = 10  # Steam shows 10 games per page by default
        
        print(f"🍀 Fetching games from curator {curator_id}...")
        
        # First, fetch the initial page to get session info and total count
        initial_url = f"{self.base_url}/curator/{curator_id}/"
        session = requests.Session()
        session.headers.update(self.headers)
        
        try:
            print(f"📄 Fetching initial page...")
            response = session.get(initial_url, timeout=15)
            
            if response.status_code != 200:
                print(f"❌ Failed to fetch initial page: {response.status_code}")
                return []
            
            # Extract session ID from cookies or page
            session_id = self._extract_session_id(response.text)
            
            # Extract app IDs from first page
            page_app_ids = self._extract_curator_app_ids(response.text)
            app_ids.update(page_app_ids)
            print(f"✅ Found {len(page_app_ids)} games on page 1 (total: {len(app_ids)})")
            
            # Try to get total count from page
            total_games = self._extract_total_count(response.text)
            if total_games:
                print(f"📊 Total games in curator: {total_games}")
                total_pages = (total_games + per_page - 1) // per_page
                print(f"📄 Estimated pages: {total_pages}")
            else:
                total_pages = None
            
        except Exception as e:
            print(f"❌ Error fetching initial page: {str(e)}")
            return []
        
        # Now fetch remaining pages using pagination
        page = 2
        consecutive_empty = 0
        
        while True:
            if max_pages and page > max_pages:
                break
            
            try:
                # Add delay between requests
                time.sleep(random.uniform(0.5, 1.0))
                
                # Try pagination URL
                page_url = f"{self.base_url}/curator/{curator_id}/?page={page}"
                
                print(f"📄 Fetching page {page}...")
                response = session.get(page_url, timeout=15)
                
                if response.status_code == 429:
                    print(f"⚠️  Rate limited. Waiting 2 minutes...")
                    time.sleep(120)
                    continue
                
                if response.status_code != 200:
                    print(f"❌ Failed to fetch page {page}: {response.status_code}")
                    consecutive_empty += 1
                    if consecutive_empty >= 3:
                        break
                    page += 1
                    continue
                
                # Extract app IDs from page
                page_app_ids = self._extract_curator_app_ids(response.text)
                
                if not page_app_ids:
                    consecutive_empty += 1
                    print(f"⚠️  No app IDs found on page {page}")
                    if consecutive_empty >= 3:
                        print(f"📊 Reached end of curator list")
                        break
                    page += 1
                    continue
                
                consecutive_empty = 0
                
                # Add found app IDs to set
                new_ids = page_app_ids - app_ids
                app_ids.update(page_app_ids)
                
                print(f"✅ Found {len(new_ids)} new games on page {page} (total: {len(app_ids)})")
                
                # Check if we've reached the end
                if total_pages and page >= total_pages:
                    print(f"📊 Reached last page ({total_pages})")
                    break
                
                # If we got fewer than expected and have enough games, we might be done
                if len(page_app_ids) < 5 and len(app_ids) > 50:
                    print(f"📊 Reached end (only {len(page_app_ids)} games found on page {page})")
                    break
                
                page += 1
                
                # Safety limit to prevent infinite loops
                if page > 100:
                    print(f"⚠️  Reached maximum page limit (100)")
                    break
                    
            except Exception as e:
                print(f"❌ Error fetching page {page}: {str(e)}")
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    break
                page += 1
        
        return sorted(list(app_ids))
    
    def _extract_curator_app_ids(self, html):
        """Extract app IDs specifically from curator recommendations section"""
        app_ids = set()
        
        # Known non-game app IDs to exclude
        excluded_app_ids = {
            753,  # Steam
            107410,  # SteamVR
            228980,  # Steam Client Bootstrapper
        }
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Find the main content area that contains curator recommendations
            # Steam curator pages typically have the recommendations in a specific section
            content_areas = [
                soup.find('div', id='search_results'),
                soup.find('div', class_=re.compile(r'search_results|curator_recommendations', re.I)),
                soup.find('div', id='search_result_container'),
            ]
            
            # Remove None values
            content_areas = [area for area in content_areas if area is not None]
            
            # If we found specific content areas, use those
            if content_areas:
                for area in content_areas:
                    # Find all app links in this area
                    app_links = area.find_all('a', href=re.compile(r'/app/\d+/'))
                    for link in app_links:
                        href = link.get('href', '')
                        match = re.search(r'/app/(\d+)/', href)
                        if match:
                            try:
                                app_id = int(match.group(1))
                                if app_id not in excluded_app_ids:
                                    app_ids.add(app_id)
                            except ValueError:
                                pass
                    
                    # Also check for data-appid attributes
                    for element in area.find_all(attrs={'data-appid': True}):
                        try:
                            app_id = int(element.get('data-appid'))
                            if app_id not in excluded_app_ids:
                                app_ids.add(app_id)
                        except (ValueError, TypeError):
                            pass
            
            # If we didn't find enough app IDs, try a broader search but exclude navigation/footer
            if len(app_ids) < 5:
                # Find main page content
                main_content = soup.find('body')
                if main_content:
                    # Find all app links
                    all_app_links = main_content.find_all('a', href=re.compile(r'/app/\d+/'))
                    
                    for link in all_app_links:
                        # Check if link is in navigation, footer, or sidebar
                        parent = link.find_parent(['nav', 'footer', 'header', 'aside'])
                        if parent:
                            continue
                        
                        # Check parent classes for navigation indicators
                        parents = link.find_parents()
                        parent_classes = ' '.join([str(p.get('class', [])) for p in parents[:5]])
                        
                        # Skip if in navigation or footer area
                        if any(x in parent_classes.lower() for x in ['nav', 'footer', 'header', 'sidebar', 'menu']):
                            continue
                        
                        href = link.get('href', '')
                        match = re.search(r'/app/(\d+)/', href)
                        if match:
                            try:
                                app_id = int(match.group(1))
                                # Exclude known non-game apps and very small app IDs (usually system apps)
                                if app_id not in excluded_app_ids and app_id > 100:
                                    app_ids.add(app_id)
                            except ValueError:
                                pass
            
            # Also extract from JSON data in script tags
            script_tags = soup.find_all('script')
            for script in script_tags:
                if script.string:
                    json_app_ids = self._extract_app_ids_from_json(script.string)
                    app_ids.update([aid for aid in json_app_ids if aid not in excluded_app_ids and aid > 100])
            
        except Exception as e:
            print(f"⚠️  Error parsing HTML with BeautifulSoup: {str(e)}")
            # Fallback to regex - extract all app IDs but filter out excluded ones
            app_link_pattern = r'/app/(\d+)/'
            matches = re.findall(app_link_pattern, html)
            for match in matches:
                try:
                    app_id = int(match)
                    if app_id not in excluded_app_ids and app_id > 100:
                        app_ids.add(app_id)
                except ValueError:
                    pass
        
        return app_ids
    
    def _extract_session_id(self, html):
        """Extract session ID from HTML or cookies"""
        # Look for session ID in script tags or data attributes
        session_pattern = r'sessionid["\']\s*:\s*["\']([^"\']+)["\']'
        match = re.search(session_pattern, html, re.I)
        if match:
            return match.group(1)
        return None
    
    def _extract_total_count(self, html):
        """Extract total count of games from curator page"""
        # Look for patterns like "Showing 1-10 of 175 results"
        count_pattern = r'(?:of|total)\s+(\d+)\s+(?:results?|games?)'
        match = re.search(count_pattern, html, re.I)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                pass
        return None
    
    def _extract_app_ids_from_json(self, html):
        """Extract app IDs from JSON data embedded in HTML"""
        app_ids = set()
        
        # Look for JSON data in script tags or data attributes
        # Steam often embeds app data in JavaScript
        json_pattern = r'"appid":\s*(\d+)'
        matches = re.findall(json_pattern, html)
        for match in matches:
            try:
                app_ids.add(int(match))
            except ValueError:
                pass
        
        # Also look for appid in various formats
        appid_patterns = [
            r'"appid"\s*:\s*(\d+)',
            r'appid["\']\s*:\s*(\d+)',
            r'app_id["\']\s*:\s*(\d+)',
        ]
        
        for pattern in appid_patterns:
            matches = re.findall(pattern, html, re.IGNORECASE)
            for match in matches:
                try:
                    app_ids.add(int(match))
                except ValueError:
                    pass
        
        return app_ids

class SupabasePipeline:
    """Class to manage database operations"""
    
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
    
    def create_irish_games_table(self):
        """Create the games_from_ireland table."""
        try:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS games_from_ireland (
                    id SERIAL PRIMARY KEY,
                    app_id INTEGER NOT NULL UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.conn.commit()
            print("✅ Created games_from_ireland table")
            return True
        except Exception as e:
            print(f"❌ Error creating table: {str(e)}")
            self.conn.rollback()
            return False
    
    def insert_app_ids(self, app_ids):
        """Insert app IDs into the games_from_ireland table."""
        if not app_ids:
            print("⚠️  No app IDs to insert")
            return 0
        
        inserted = 0
        skipped = 0
        
        for app_id in app_ids:
            try:
                self.cursor.execute("""
                    INSERT INTO games_from_ireland (app_id)
                    VALUES (%s)
                    ON CONFLICT (app_id) DO NOTHING
                """, (app_id,))
                
                if self.cursor.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1
            except Exception as e:
                print(f"⚠️  Error inserting app_id {app_id}: {str(e)}")
                skipped += 1
        
        self.conn.commit()
        return inserted, skipped

def collect_irish_games():
    """Main function to collect Irish games from Steam curator"""
    
    curator_id = 41458618  # Games From Ireland curator ID
    curator_api = SteamCuratorAPI()
    pipeline = SupabasePipeline()
    
    # Connect to database
    if not pipeline.connect():
        return
    
    try:
        # Create table
        if not pipeline.create_irish_games_table():
            return
        
        # Fetch games from curator
        print(f"\n🍀 Starting collection of Irish games from curator {curator_id}...")
        app_ids = curator_api.get_curator_games(curator_id)
        
        if not app_ids:
            print("❌ No app IDs found")
            return
        
        print(f"\n📊 Found {len(app_ids)} unique games")
        print(f"📋 Sample app IDs: {app_ids[:10]}")
        
        # Insert into database
        print(f"\n💾 Inserting app IDs into database...")
        inserted, skipped = pipeline.insert_app_ids(app_ids)
        
        print(f"\n✅ Collection complete!")
        print(f"   📊 Total games found: {len(app_ids)}")
        print(f"   ✅ New games inserted: {inserted}")
        print(f"   ⏭️  Already existed: {skipped}")
        
        # Show current count in table
        pipeline.cursor.execute("SELECT COUNT(*) FROM games_from_ireland")
        total_count = pipeline.cursor.fetchone()[0]
        print(f"   📈 Total games in table: {total_count}")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        pipeline.disconnect()

if __name__ == "__main__":
    collect_irish_games()

