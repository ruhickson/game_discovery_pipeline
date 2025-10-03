#!/usr/bin/env python3
"""
Massive Nintendo eShop Data Collection (Updated)
Collects ALL Nintendo games from the eShop with improved table structure
"""

import requests
import json
import time
import logging
import os
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import uuid

# Load environment variables
load_dotenv('../.Renv')

# Set the Supabase connection string
os.environ['SUPABASE_CONNECTION_STRING'] = 'postgresql://postgres.ymmbpcjkhikugbcyssxn:crankyCATS_31@aws-0-eu-west-1.pooler.supabase.com:6543/postgres'

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('nintendo_collection_clean.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class MassiveNintendoScraper:
    """Scraper for Nintendo eShop data"""
    
    def __init__(self):
        self.base_url = "https://search.nintendo-europe.com/en/select"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Statistics
        self.processed_games = 0
        self.successful_games = 0
        self.failed_games = 0
    
    def get_total_games_count(self):
        """Get total number of games available"""
        try:
            params = {
                'q': '*',
                'rows': 0,
                'start': 0,
                'fq': 'type:GAME',
                'sort': 'sorting_title asc'
            }
            
            response = self.session.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            total_games = data.get('response', {}).get('numFound', 0)
            
            logging.info(f"Total games available: {total_games:,}")
            return total_games
            
        except Exception as e:
            logging.error(f"Error getting total games count: {str(e)}")
            return 0
    
    def get_games_batch(self, start=0, rows=100):
        """Get a batch of games from the API"""
        try:
            params = {
                'q': '*',
                'rows': rows,
                'start': start,
                'fq': 'type:GAME',
                'sort': 'sorting_title asc'
            }
            
            response = self.session.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            docs = data.get('response', {}).get('docs', [])
            
            return docs
            
        except Exception as e:
            logging.error(f"Error getting games batch starting at {start}: {str(e)}")
            return []
    
    def parse_game_doc(self, doc):
        """Parse a game document from the API response"""
        try:
            game_data = {
                'fs_id': str(doc.get('fs_id', '')),
                'title': doc.get('title', ''),
                'sorting_title': doc.get('sorting_title', ''),
                'title_master': doc.get('title_master', ''),
                'title_extras': doc.get('title_extras_txt', []),
                'publisher': doc.get('publisher', ''),
                'release_date': doc.get('date_from', ''),  # Use date_from as release_date
                'pretty_date': doc.get('pretty_date_s', ''),  # Use pretty_date_s
                'dates_released': doc.get('dates_released_dts', []),  # Use dates_released_dts
                'change_date': doc.get('change_date', ''),
                'price_regular': doc.get('price_regular'),
                'price_discounted': doc.get('price_discounted'),
                'price_sorting': doc.get('price_sorting'),
                'price_lowest': doc.get('price_lowest'),
                'price_has_discount': doc.get('price_has_discount_b', False),
                'price_discount_percentage': doc.get('price_discount_percentage'),
                'game_categories': doc.get('game_categories_txt', []),
                'pretty_game_categories': doc.get('pretty_game_categories', []),
                'age_rating_type': doc.get('age_rating_type', ''),
                'age_rating_value': doc.get('age_rating_value', ''),
                'pretty_agerating': doc.get('pretty_agerating', ''),
                'excerpt': doc.get('excerpt', ''),
                'product_catalog_description': doc.get('product_catalog_description_s', ''),
                'copyright': doc.get('copyright', ''),
                'url': doc.get('url', ''),
                'image_url_sq': doc.get('image_url_sq_s', ''),
                'image_url_h2x1': doc.get('image_url_h2x1_s', ''),
                'wishlist_email_square_image_url': doc.get('wishlist_email_square_image_url', ''),
                'wishlist_email_banner640w_image_url': doc.get('wishlist_email_banner640w_image_url', ''),
                'wishlist_email_banner460w_image_url': doc.get('wishlist_email_banner460w_image_url', ''),
                'players_to': doc.get('players_to'),
                'players_from': doc.get('players_from'),
                'language_availability': doc.get('language_availability', []),
                'cloud_saves': doc.get('cloud_saves_b', False),
                'digital_version': doc.get('digital_version_b', False),
                'physical_version': doc.get('physical_version_b', False),
                'demo_availability': doc.get('demo_availability', False),
                'eshop_removed': doc.get('eshop_removed', False),
                'downloads_rank': doc.get('downloads_rank'),
                'hits': doc.get('hits_i'),
                'system_type': doc.get('system_type', []),
                'system_names': doc.get('system_names', []),
                'playable_on': doc.get('playable_on_txt', []),
                'originally_for': doc.get('originally_for_t', ''),
                'compatible_controller': doc.get('compatible_controller', []),
                'play_mode_tv_mode': doc.get('play_mode_tv_mode_b', False),
                'play_mode_handheld_mode': doc.get('play_mode_handheld_mode_b', False),
                'play_mode_tabletop_mode': doc.get('play_mode_tabletop_mode_b', False),
                'paid_subscription_required': doc.get('paid_subscription_required_b', False),
                'paid_subscription_online_play': doc.get('paid_subscription_online_play_b', False),
                'club_nintendo': doc.get('club_nintendo', False),
                'switch_game_voucher': doc.get('switch_game_voucher', False),
                'nsuid': doc.get('nsuid_txt', []),
                'related_nsuids': doc.get('related_nsuids', []),
                'priority': doc.get('priority', ''),
                'deprioritise': doc.get('deprioritise_b', False),
                'pg': doc.get('pg_s', ''),
                '_version': doc.get('_version'),
                'type': doc.get('type', '')
            }
            
            return game_data
            
        except Exception as e:
            logging.error(f"Error parsing game document: {str(e)}")
            return None

class MassiveNintendoPipeline:
    """Pipeline for collecting and storing Nintendo games data"""
    
    def __init__(self):
        self.connection_string = os.getenv('SUPABASE_CONNECTION_STRING')
        self.conn = None
        self.cursor = None
        self.scraper = MassiveNintendoScraper()
        self.batch_size = 100
    
    def connect(self):
        """Connect to Supabase database"""
        try:
            self.conn = psycopg2.connect(self.connection_string)
            self.cursor = self.conn.cursor()
            
            # Set longer timeout for operations
            self.cursor.execute("SET statement_timeout = 300000")  # 5 minutes
            self.cursor.execute("SET idle_in_transaction_session_timeout = 300000")  # 5 minutes
            
            logging.info("Connected to Supabase database")
            return True
        except Exception as e:
            logging.error(f"Error connecting to database: {str(e)}")
            return False
    
    def disconnect(self):
        """Disconnect from database"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logging.info("Disconnected from database")
    
    def create_games_nintendo_table(self):
        """Create the games_nintendo table with UUID primary key"""
        try:
            # Create the main games_nintendo table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS games_nintendo (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    fs_id TEXT NOT NULL,
                    change_date TEXT NOT NULL,
                    title TEXT NOT NULL,
                    sorting_title TEXT,
                    title_master TEXT,
                    title_extras JSONB,
                    publisher TEXT,
                    release_date TEXT,
                    pretty_date TEXT,
                    dates_released JSONB,
                    price_regular DECIMAL(10,2),
                    price_discounted DECIMAL(10,2),
                    price_sorting DECIMAL(10,2),
                    price_lowest DECIMAL(10,2),
                    price_has_discount BOOLEAN,
                    price_discount_percentage DECIMAL(5,2),
                    game_categories JSONB,
                    pretty_game_categories JSONB,
                    age_rating_type TEXT,
                    age_rating_value TEXT,
                    pretty_agerating TEXT,
                    excerpt TEXT,
                    product_catalog_description TEXT,
                    copyright TEXT,
                    url TEXT,
                    image_url_sq TEXT,
                    image_url_h2x1 TEXT,
                    wishlist_email_square_image_url TEXT,
                    wishlist_email_banner640w_image_url TEXT,
                    wishlist_email_banner460w_image_url TEXT,
                    players_to INTEGER,
                    players_from INTEGER,
                    language_availability JSONB,
                    cloud_saves BOOLEAN,
                    digital_version BOOLEAN,
                    physical_version BOOLEAN,
                    demo_availability BOOLEAN,
                    eshop_removed BOOLEAN,
                    downloads_rank INTEGER,
                    hits INTEGER,
                    system_type JSONB,
                    system_names JSONB,
                    playable_on JSONB,
                    originally_for TEXT,
                    compatible_controller JSONB,
                    play_mode_tv_mode BOOLEAN,
                    play_mode_handheld_mode BOOLEAN,
                    play_mode_tabletop_mode BOOLEAN,
                    paid_subscription_required BOOLEAN,
                    paid_subscription_online_play BOOLEAN,
                    club_nintendo BOOLEAN,
                    switch_game_voucher BOOLEAN,
                    nsuid JSONB,
                    related_nsuids JSONB,
                    priority TEXT,
                    deprioritise BOOLEAN,
                    pg TEXT,
                    _version BIGINT,
                    type TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(fs_id, change_date, title)
                )
            """)
            
            # Create indexes for better performance (one at a time to avoid timeouts)
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_games_nintendo_fs_id ON games_nintendo(fs_id)",
                "CREATE INDEX IF NOT EXISTS idx_games_nintendo_title ON games_nintendo(title)",
                "CREATE INDEX IF NOT EXISTS idx_games_nintendo_publisher ON games_nintendo(publisher)",
                "CREATE INDEX IF NOT EXISTS idx_games_nintendo_release_date ON games_nintendo(release_date)",
                "CREATE INDEX IF NOT EXISTS idx_games_nintendo_price_sorting ON games_nintendo(price_sorting)",
                "CREATE INDEX IF NOT EXISTS idx_games_nintendo_downloads_rank ON games_nintendo(downloads_rank)",
                "CREATE INDEX IF NOT EXISTS idx_games_nintendo_updated_at ON games_nintendo(updated_at)"
            ]
            
            for index_sql in indexes:
                try:
                    self.cursor.execute(index_sql)
                    self.conn.commit()
                except Exception as e:
                    logging.warning(f"Could not create index: {str(e)}")
                    self.conn.rollback()
            
            self.conn.commit()
            logging.info("Created games_nintendo table with UUID primary key and indexes")
            return True
            
        except Exception as e:
            logging.error(f"Error creating games_nintendo table: {str(e)}")
            self.conn.rollback()
            return False
    
    def create_staging_table(self):
        """Create staging table for temporary data storage"""
        try:
            # Drop existing staging table and recreate
            self.cursor.execute("DROP TABLE IF EXISTS games_nintendo_staging")
            self.conn.commit()
            
            # Create staging table with same structure as main table
            # Split the creation into smaller parts to avoid timeouts
            self.cursor.execute("""
                CREATE TABLE games_nintendo_staging (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    fs_id TEXT NOT NULL,
                    change_date TEXT NOT NULL,
                    title TEXT NOT NULL,
                    sorting_title TEXT,
                    title_master TEXT,
                    title_extras JSONB,
                    publisher TEXT,
                    release_date TEXT,
                    pretty_date TEXT,
                    dates_released JSONB,
                    price_regular DECIMAL(10,2),
                    price_discounted DECIMAL(10,2),
                    price_sorting DECIMAL(10,2),
                    price_lowest DECIMAL(10,2),
                    price_has_discount BOOLEAN,
                    price_discount_percentage DECIMAL(5,2),
                    game_categories JSONB,
                    pretty_game_categories JSONB,
                    age_rating_type TEXT,
                    age_rating_value TEXT,
                    pretty_agerating TEXT,
                    excerpt TEXT,
                    product_catalog_description TEXT,
                    copyright TEXT,
                    url TEXT,
                    image_url_sq TEXT,
                    image_url_h2x1 TEXT,
                    wishlist_email_square_image_url TEXT,
                    wishlist_email_banner640w_image_url TEXT,
                    wishlist_email_banner460w_image_url TEXT,
                    players_to INTEGER,
                    players_from INTEGER,
                    language_availability JSONB,
                    cloud_saves BOOLEAN,
                    digital_version BOOLEAN,
                    physical_version BOOLEAN,
                    demo_availability BOOLEAN,
                    eshop_removed BOOLEAN,
                    downloads_rank INTEGER,
                    hits INTEGER,
                    system_type JSONB,
                    system_names JSONB,
                    playable_on JSONB,
                    originally_for TEXT,
                    compatible_controller JSONB,
                    play_mode_tv_mode BOOLEAN,
                    play_mode_handheld_mode BOOLEAN,
                    play_mode_tabletop_mode BOOLEAN,
                    paid_subscription_required BOOLEAN,
                    paid_subscription_online_play BOOLEAN,
                    club_nintendo BOOLEAN,
                    switch_game_voucher BOOLEAN,
                    nsuid JSONB,
                    related_nsuids JSONB,
                    priority TEXT,
                    deprioritise BOOLEAN,
                    pg TEXT,
                    _version BIGINT,
                    type TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            self.conn.commit()
            logging.info("Created games_nintendo_staging table")
            return True
            
        except Exception as e:
            logging.error(f"Error creating staging table: {str(e)}")
            self.conn.rollback()
            return False
    
    def insert_game_to_staging(self, game_data):
        """Insert a game into the staging table"""
        try:
            # Prepare the values tuple
            values = (
                game_data.get('fs_id'),
                game_data.get('change_date'),
                game_data.get('title'),
                game_data.get('sorting_title'),
                game_data.get('title_master'),
                json.dumps(game_data.get('title_extras', [])),
                game_data.get('publisher'),
                game_data.get('release_date'),
                game_data.get('pretty_date'),
                json.dumps(game_data.get('dates_released', [])),
                game_data.get('price_regular'),
                game_data.get('price_discounted'),
                game_data.get('price_sorting'),
                game_data.get('price_lowest'),
                game_data.get('price_has_discount'),
                game_data.get('price_discount_percentage'),
                json.dumps(game_data.get('game_categories', [])),
                json.dumps(game_data.get('pretty_game_categories', [])),
                game_data.get('age_rating_type'),
                game_data.get('age_rating_value'),
                game_data.get('pretty_agerating'),
                game_data.get('excerpt'),
                game_data.get('product_catalog_description'),
                game_data.get('copyright'),
                game_data.get('url'),
                game_data.get('image_url_sq'),
                game_data.get('image_url_h2x1'),
                game_data.get('wishlist_email_square_image_url'),
                game_data.get('wishlist_email_banner640w_image_url'),
                game_data.get('wishlist_email_banner460w_image_url'),
                game_data.get('players_to'),
                game_data.get('players_from'),
                json.dumps(game_data.get('language_availability', [])),
                game_data.get('cloud_saves'),
                game_data.get('digital_version'),
                game_data.get('physical_version'),
                game_data.get('demo_availability'),
                game_data.get('eshop_removed'),
                game_data.get('downloads_rank'),
                game_data.get('hits'),
                json.dumps(game_data.get('system_type', [])),
                json.dumps(game_data.get('system_names', [])),
                json.dumps(game_data.get('playable_on', [])),
                game_data.get('originally_for'),
                json.dumps(game_data.get('compatible_controller', [])),
                game_data.get('play_mode_tv_mode'),
                game_data.get('play_mode_handheld_mode'),
                game_data.get('play_mode_tabletop_mode'),
                game_data.get('paid_subscription_required'),
                game_data.get('paid_subscription_online_play'),
                game_data.get('club_nintendo'),
                game_data.get('switch_game_voucher'),
                json.dumps(game_data.get('nsuid', [])),
                json.dumps(game_data.get('related_nsuids', [])),
                game_data.get('priority'),
                game_data.get('deprioritise'),
                game_data.get('pg'),
                game_data.get('_version'),
                game_data.get('type')
            )
            
            # Create the correct number of placeholders
            placeholders = ', '.join(['%s'] * len(values))
            
            self.cursor.execute(f"""
                INSERT INTO games_nintendo_staging (
                    fs_id, change_date, title, sorting_title, title_master, title_extras, publisher, release_date, 
                    pretty_date, dates_released, price_regular, price_discounted, price_sorting,
                    price_lowest, price_has_discount, price_discount_percentage, game_categories, 
                    pretty_game_categories, age_rating_type, age_rating_value, pretty_agerating, excerpt,
                    product_catalog_description, copyright, url, image_url_sq, image_url_h2x1,
                    wishlist_email_square_image_url, wishlist_email_banner640w_image_url, 
                    wishlist_email_banner460w_image_url, players_to, players_from, language_availability,
                    cloud_saves, digital_version, physical_version, demo_availability, eshop_removed,
                    downloads_rank, hits, system_type, system_names, playable_on, originally_for,
                    compatible_controller, play_mode_tv_mode, play_mode_handheld_mode, play_mode_tabletop_mode,
                    paid_subscription_required, paid_subscription_online_play, club_nintendo, 
                    switch_game_voucher, nsuid, related_nsuids, priority, deprioritise, pg, _version, type
                ) VALUES ({placeholders})
            """, values)
            
            return True
            
        except Exception as e:
            logging.error(f"Error inserting game {game_data.get('title')} to staging: {str(e)}")
            return False
    
    def merge_staging_to_main(self):
        """Merge new records from staging to main table"""
        try:
            # Insert only records that don't exist in the main table
            self.cursor.execute("""
                INSERT INTO games_nintendo (
                    fs_id, change_date, title, sorting_title, title_master, title_extras, publisher, release_date, 
                    pretty_date, dates_released, price_regular, price_discounted, price_sorting,
                    price_lowest, price_has_discount, price_discount_percentage, game_categories, 
                    pretty_game_categories, age_rating_type, age_rating_value, pretty_agerating, excerpt,
                    product_catalog_description, copyright, url, image_url_sq, image_url_h2x1,
                    wishlist_email_square_image_url, wishlist_email_banner640w_image_url, 
                    wishlist_email_banner460w_image_url, players_to, players_from, language_availability,
                    cloud_saves, digital_version, physical_version, demo_availability, eshop_removed,
                    downloads_rank, hits, system_type, system_names, playable_on, originally_for,
                    compatible_controller, play_mode_tv_mode, play_mode_handheld_mode, play_mode_tabletop_mode,
                    paid_subscription_required, paid_subscription_online_play, club_nintendo, 
                    switch_game_voucher, nsuid, related_nsuids, priority, deprioritise, pg, _version, type
                )
                SELECT 
                    fs_id, change_date, title, sorting_title, title_master, title_extras, publisher, release_date, 
                    pretty_date, dates_released, price_regular, price_discounted, price_sorting,
                    price_lowest, price_has_discount, price_discount_percentage, game_categories, 
                    pretty_game_categories, age_rating_type, age_rating_value, pretty_agerating, excerpt,
                    product_catalog_description, copyright, url, image_url_sq, image_url_h2x1,
                    wishlist_email_square_image_url, wishlist_email_banner640w_image_url, 
                    wishlist_email_banner460w_image_url, players_to, players_from, language_availability,
                    cloud_saves, digital_version, physical_version, demo_availability, eshop_removed,
                    downloads_rank, hits, system_type, system_names, playable_on, originally_for,
                    compatible_controller, play_mode_tv_mode, play_mode_handheld_mode, play_mode_tabletop_mode,
                    paid_subscription_required, paid_subscription_online_play, club_nintendo, 
                    switch_game_voucher, nsuid, related_nsuids, priority, deprioritise, pg, _version, type
                FROM games_nintendo_staging s
                WHERE NOT EXISTS (
                    SELECT 1 FROM games_nintendo m 
                    WHERE m.fs_id = s.fs_id 
                    AND m.change_date = s.change_date 
                    AND m.title = s.title
                )
            """)
            
            new_records = self.cursor.rowcount
            self.conn.commit()
            
            logging.info(f"Merged {new_records} new records from staging to main table")
            return new_records
            
        except Exception as e:
            logging.error(f"Error merging staging to main: {str(e)}")
            self.conn.rollback()
            return 0
    
    def collect_all_games(self, max_games=None):
        """Collect ALL Nintendo games using staging approach"""
        if not self.connect():
            return
        
        try:
            # Create main table if it doesn't exist
            if not self.create_games_nintendo_table():
                logging.error("Failed to create games_nintendo table")
                return
            
            # Create staging table
            if not self.create_staging_table():
                logging.error("Failed to create staging table")
                return
            
            # Get total count
            total_games = self.scraper.get_total_games_count()
            if total_games == 0:
                logging.error("Could not determine total games count")
                return
            
            if max_games:
                total_games = min(total_games, max_games)
            
            logging.info(f"Starting collection of {total_games:,} games to staging table")
            logging.info(f"Batch size: {self.batch_size}")
            
            current_position = 0
            batch_count = 0
            
            while current_position < total_games:
                batch_count += 1
                batch_start = current_position
                batch_end = min(current_position + self.batch_size, total_games)
                
                logging.info(f"Batch {batch_count}: Processing games {batch_start:,} to {batch_end:,} of {total_games:,}")
                
                # Get batch of games
                docs = self.scraper.get_games_batch(current_position, self.batch_size)
                
                if not docs:
                    logging.warning(f"No games returned for batch starting at {current_position}")
                    current_position += self.batch_size
                    time.sleep(2)
                    continue
                
                # Process each game in the batch
                batch_successful = 0
                batch_failed = 0
                
                for doc in docs:
                    game_data = self.scraper.parse_game_doc(doc)
                    if game_data:
                        if self.insert_game_to_staging(game_data):
                            batch_successful += 1
                            self.scraper.successful_games += 1
                        else:
                            batch_failed += 1
                            self.scraper.failed_games += 1
                    else:
                        batch_failed += 1
                        self.scraper.failed_games += 1
                    
                    self.scraper.processed_games += 1
                
                # Log batch results
                logging.info(f"Batch {batch_count} complete: {batch_successful} successful, {batch_failed} failed")
                
                # Progress update
                progress = (current_position + len(docs)) / total_games * 100
                logging.info(f"Overall progress: {progress:.1f}% ({current_position + len(docs):,}/{total_games:,})")
                logging.info(f"Total stats: {self.scraper.successful_games:,} successful, {self.scraper.failed_games:,} failed")
                
                # Move to next batch
                current_position += self.batch_size
                
                # Be respectful with delays
                time.sleep(1)
                
                # Save progress every 10 batches
                if batch_count % 10 == 0:
                    logging.info(f"Progress checkpoint: {current_position:,} games processed")
            
            # Merge staging to main table
            logging.info("Collection complete! Merging staging data to main table...")
            new_records = self.merge_staging_to_main()
            
            # Clean up staging table
            self.cursor.execute("DROP TABLE IF EXISTS games_nintendo_staging")
            self.conn.commit()
            logging.info("Cleaned up staging table")
            
            # Final summary
            logging.info("Collection and merge complete!")
            logging.info(f"Final stats:")
            logging.info(f"  Total processed: {self.scraper.processed_games:,}")
            logging.info(f"  Successful: {self.scraper.successful_games:,}")
            logging.info(f"  Failed: {self.scraper.failed_games:,}")
            logging.info(f"  New records added: {new_records:,}")
            logging.info(f"  Success rate: {(self.scraper.successful_games / max(1, self.scraper.processed_games) * 100):.1f}%")
            
        except KeyboardInterrupt:
            logging.info("Collection interrupted by user")
            logging.info(f"Progress saved at position: {current_position:,}")
        except Exception as e:
            logging.error(f"Error during collection: {str(e)}")
        finally:
            self.disconnect()

if __name__ == "__main__":
    # Configuration
    MAX_GAMES = None  # Set to a number to limit collection, or None for ALL games
    BATCH_SIZE = 100  # Games per batch
    
    print("MASSIVE Nintendo eShop Data Collection (Updated)")
    print("Collecting ALL Nintendo games with improved table structure!")
    print("=" * 60)
    
    pipeline = MassiveNintendoPipeline()
    pipeline.batch_size = BATCH_SIZE
    pipeline.collect_all_games(max_games=MAX_GAMES) 