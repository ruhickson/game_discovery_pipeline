#!/usr/bin/env python3
"""
11_analyze_requirements.py

Analyzes minimum requirements data from Steam API for popular games
to understand the data structure and suggest database columns.
"""

import os
import sys
import logging
import json
import time
import random
from datetime import datetime

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# Add the current directory to Python path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    import requests
except ImportError as e:
    print(f"❌ Missing required package: {str(e)}")
    print("Install with: pip install psycopg2-binary requests")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('requirements_analysis.log'),
        logging.StreamHandler()
    ]
)

class RequirementsAnalyzer:
    """Analyzes Steam API requirements data structure"""
    
    def __init__(self):
        self.connection_string = os.getenv('SUPABASE_CONNECTION_STRING')
        if not self.connection_string:
            raise ValueError("SUPABASE_CONNECTION_STRING environment variable not set")
        
        self.conn = None
        self.cursor = None
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Statistics
        self.stats = {
            'processed': 0,
            'with_requirements': 0,
            'without_requirements': 0,
            'errors': 0
        }
        
        # Data structure analysis
        self.requirement_fields = set()
        self.sample_requirements = []
    
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
    
    def get_popular_games(self, limit=None):
        """Get ALL games before 2025 from the database"""
        try:
            if limit:
                query = """
                    SELECT app_id, name, total_reviews
                    FROM games 
                    WHERE release_date_actual < '2025-01-01'
                    AND name IS NOT NULL
                    AND name != ''
                    ORDER BY total_reviews DESC NULLS LAST
                    LIMIT %s
                """
                self.cursor.execute(query, (limit,))
            else:
                query = """
                    SELECT app_id, name, total_reviews
                    FROM games 
                    WHERE release_date_actual < '2025-01-01'
                    AND name IS NOT NULL
                    AND name != ''
                    ORDER BY total_reviews DESC NULLS LAST
                """
                self.cursor.execute(query)
            
            games = self.cursor.fetchall()
            logging.info(f"📊 Found {len(games)} games before 2025 to analyze")
            return games
            
        except Exception as e:
            logging.error(f"❌ Error fetching games: {str(e)}")
            return []
    
    def fetch_game_details(self, app_id):
        """Fetch game details from Steam API"""
        try:
            # Rate limiting
            time.sleep(random.uniform(0.5, 1.0))
            
            url = f"https://store.steampowered.com/api/appdetails?appids={app_id}"
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                data = response.json()
                if str(app_id) in data and data[str(app_id)]['success']:
                    return data[str(app_id)]['data']
            return None
            
        except Exception as e:
            logging.error(f"❌ Error fetching details for app_id {app_id}: {str(e)}")
            return None
    
    def analyze_requirements(self, requirements_data):
        """Analyze the structure of requirements data"""
        if not requirements_data:
            return None
        
        # Convert to string if it's not already
        if isinstance(requirements_data, str):
            try:
                requirements_data = json.loads(requirements_data)
            except:
                return requirements_data
        
        # If it's a dict, analyze its structure
        if isinstance(requirements_data, dict):
            # Extract all possible fields
            for key, value in requirements_data.items():
                self.requirement_fields.add(key)
                if isinstance(value, str) and len(value) > 10:  # Store interesting samples
                    self.sample_requirements.append({
                        'field': key,
                        'value': value[:200] + '...' if len(value) > 200 else value
                    })
        
        return requirements_data
    
    def parse_html_requirements(self, html_text):
        """Parse HTML-formatted requirements text into structured data"""
        import re
        
        logging.info(f"🔍 Raw HTML text (first 500 chars): {html_text[:500]}...")
        
        requirements = {}
        
        # Remove HTML tags and clean up the text
        clean_text = re.sub(r'<[^>]+>', '', html_text)
        clean_text = clean_text.replace('&nbsp;', ' ').replace('&amp;', '&')
        
        logging.info(f"🔍 Cleaned text (first 500 chars): {clean_text[:500]}...")
        
        # Split by sections (Minimum/Recommended)
        sections = clean_text.split('Recommended:')
        min_section = sections[0] if len(sections) > 0 else ''
        rec_section = sections[1] if len(sections) > 1 else ''
        
        logging.info(f"🔍 Min section: {min_section[:200]}...")
        logging.info(f"🔍 Rec section: {rec_section[:200]}...")
        
        # Parse minimum requirements
        if 'Minimum:' in min_section:
            min_text = min_section.split('Minimum:')[1] if 'Minimum:' in min_section else min_section
            min_parsed = self._parse_requirements_section(min_text, 'min_')
            logging.info(f"🔍 Parsed min requirements: {min_parsed}")
            requirements.update(min_parsed)
        
        # Parse recommended requirements
        if rec_section:
            rec_parsed = self._parse_requirements_section(rec_section, 'rec_')
            logging.info(f"🔍 Parsed rec requirements: {rec_parsed}")
            requirements.update(rec_parsed)
        
        logging.info(f"🔍 Final requirements: {requirements}")
        return requirements
    
    def _parse_requirements_section(self, text, prefix):
        """Parse a requirements section (minimum or recommended)"""
        import re
        
        logging.info(f"🔍 Parsing section with prefix '{prefix}': {text[:200]}...")
        
        requirements = {}
        
        # Define patterns for different requirement types - updated to match cleaned text format
        patterns = {
            f'{prefix}os': r'OS\s*\*?\s*:\s*(.*?)(?=Processor:|Memory:|Graphics:|DirectX:|Network:|Storage:|Sound Card:|$)',
            f'{prefix}processor': r'Processor\s*:\s*(.*?)(?=Memory:|Graphics:|DirectX:|Network:|Storage:|Sound Card:|$)',
            f'{prefix}memory': r'Memory\s*:\s*(.*?)(?=Graphics:|DirectX:|Network:|Storage:|Sound Card:|$)',
            f'{prefix}graphics': r'Graphics\s*:\s*(.*?)(?=DirectX:|Network:|Storage:|Sound Card:|$)',
            f'{prefix}directx': r'DirectX\s*:\s*(.*?)(?=Network:|Storage:|Sound Card:|$)',
            f'{prefix}network': r'Network\s*:\s*(.*?)(?=Storage:|Sound Card:|$)',
            f'{prefix}storage': r'Storage\s*:\s*(.*?)(?=Sound Card:|$)',
            f'{prefix}sound_card': r'Sound Card\s*:\s*(.*?)(?=$)'
        }
        
        for field, pattern in patterns.items():
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                value = match.group(1).strip()
                # Clean up the value
                value = re.sub(r'\s+', ' ', value)  # Normalize whitespace
                value = value.replace('|', ' | ')  # Add spaces around pipes
                requirements[field] = value if value else None
                logging.info(f"🔍 Found {field}: {value}")
            else:
                requirements[field] = None
                logging.info(f"🔍 No match for {field}")
        
        return requirements
    
    def extract_requirements(self, requirements_data):
        """Extract and format requirements data for database insertion"""
        if not requirements_data:
            return None
        
        # Handle different data structures
        if isinstance(requirements_data, str):
            # Try to parse as JSON first
            try:
                requirements_data = json.loads(requirements_data)
            except:
                # If it's HTML text, parse it directly
                return self.parse_html_requirements(requirements_data)
        
        if not isinstance(requirements_data, dict):
            return None
        
        # Extract minimum requirements - handle both dict and string formats
        min_reqs = {}
        rec_reqs = {}
        
        if 'minimum' in requirements_data:
            min_data = requirements_data['minimum']
            if isinstance(min_data, dict):
                min_reqs = min_data
            elif isinstance(min_data, str):
                # Try to parse as JSON
                try:
                    min_reqs = json.loads(min_data)
                except:
                    # If it's HTML, parse it
                    parsed = self.parse_html_requirements(min_data)
                    min_reqs = {k: v for k, v in parsed.items() if k.startswith('min_')}
        
        if 'recommended' in requirements_data:
            rec_data = requirements_data['recommended']
            if isinstance(rec_data, dict):
                rec_reqs = rec_data
            elif isinstance(rec_data, str):
                # Try to parse as JSON
                try:
                    rec_reqs = json.loads(rec_data)
                except:
                    # If it's HTML, parse it
                    parsed = self.parse_html_requirements(rec_data)
                    rec_reqs = {k: v for k, v in parsed.items() if k.startswith('rec_')}
        
        # Map Steam fields to our database columns
        requirements = {
            'min_os': min_reqs.get('min_os', ''),
            'min_processor': min_reqs.get('min_processor', ''),
            'min_memory': min_reqs.get('min_memory', ''),
            'min_graphics': min_reqs.get('min_graphics', ''),
            'min_directx': min_reqs.get('min_directx', ''),
            'min_storage': min_reqs.get('min_storage', ''),
            'min_sound_card': min_reqs.get('min_sound_card', ''),
            'min_network': min_reqs.get('min_network', ''),
            'min_additional_notes': min_reqs.get('min_additional_notes', ''),
            'rec_os': rec_reqs.get('rec_os', ''),
            'rec_processor': rec_reqs.get('rec_processor', ''),
            'rec_memory': rec_reqs.get('rec_memory', ''),
            'rec_graphics': rec_reqs.get('rec_graphics', ''),
            'rec_directx': rec_reqs.get('rec_directx', ''),
            'rec_storage': rec_reqs.get('rec_storage', ''),
            'rec_sound_card': rec_reqs.get('rec_sound_card', ''),
            'rec_network': rec_reqs.get('rec_network', ''),
            'rec_additional_notes': rec_reqs.get('rec_additional_notes', '')
        }
        
        # Clean up empty strings to NULL
        for key, value in requirements.items():
            if value == '' or value is None:
                requirements[key] = None
        
        return requirements
    
    def update_requirements(self, app_id, requirements_data):
        """Update requirements in the database"""
        try:
            # Debug: Log what we're about to insert
            logging.info(f"🔍 Updating requirements for app_id {app_id}:")
            logging.info(f"🔍 Min OS: {requirements_data.get('min_os')}")
            logging.info(f"🔍 Min Processor: {requirements_data.get('min_processor')}")
            logging.info(f"🔍 Min Memory: {requirements_data.get('min_memory')}")
            logging.info(f"🔍 Min Graphics: {requirements_data.get('min_graphics')}")
            logging.info(f"🔍 Rec OS: {requirements_data.get('rec_os')}")
            logging.info(f"🔍 Rec Processor: {requirements_data.get('rec_processor')}")
            
            self.cursor.execute("""
                UPDATE games 
                SET 
                    min_os = %s,
                    min_processor = %s,
                    min_memory = %s,
                    min_graphics = %s,
                    min_directx = %s,
                    min_storage = %s,
                    min_sound_card = %s,
                    min_network = %s,
                    min_additional_notes = %s,
                    rec_os = %s,
                    rec_processor = %s,
                    rec_memory = %s,
                    rec_graphics = %s,
                    rec_directx = %s,
                    rec_storage = %s,
                    rec_sound_card = %s,
                    rec_network = %s,
                    rec_additional_notes = %s,
                    requirements_updated = CURRENT_TIMESTAMP
                WHERE app_id = %s
            """, (
                requirements_data['min_os'],
                requirements_data['min_processor'],
                requirements_data['min_memory'],
                requirements_data['min_graphics'],
                requirements_data['min_directx'],
                requirements_data['min_storage'],
                requirements_data['min_sound_card'],
                requirements_data['min_network'],
                requirements_data['min_additional_notes'],
                requirements_data['rec_os'],
                requirements_data['rec_processor'],
                requirements_data['rec_memory'],
                requirements_data['rec_graphics'],
                requirements_data['rec_directx'],
                requirements_data['rec_storage'],
                requirements_data['rec_sound_card'],
                requirements_data['rec_network'],
                requirements_data['rec_additional_notes'],
                app_id
            ))
            
            logging.info(f"🔍 Rows affected: {self.cursor.rowcount}")
            return True
            
        except Exception as e:
            logging.error(f"❌ Error updating requirements for app_id {app_id}: {str(e)}")
            return False
    
    def process_games(self, limit=None):
        """Process ALL games before 2025 and analyze their requirements"""
        games = self.get_popular_games(limit)  # No limit = process ALL games
        
        if not games:
            logging.info("ℹ️  No games found to process")
            return
        
        logging.info(f"🚀 Starting to analyze {len(games)} popular games...")
        
        for i, game in enumerate(games, 1):
            try:
                app_id = game['app_id']
                name = game['name']
                reviews = game['total_reviews']
                
                logging.info(f"🔄 Processing {i}/{len(games)}: {name} (app_id: {app_id}, reviews: {reviews:,})")
                
                # Fetch game details
                details = self.fetch_game_details(app_id)
                
                if details and 'pc_requirements' in details:
                    pc_reqs = details['pc_requirements']
                    
                    # Debug: Log the structure of pc_requirements
                    logging.info(f"🔍 PC Requirements type: {type(pc_reqs)}")
                    if isinstance(pc_reqs, dict):
                        logging.info(f"🔍 PC Requirements keys: {list(pc_reqs.keys())}")
                    elif isinstance(pc_reqs, str):
                        logging.info(f"🔍 PC Requirements (first 200 chars): {pc_reqs[:200]}...")
                    
                    # Extract requirements data
                    extracted = self.extract_requirements(pc_reqs)
                    
                    if extracted:
                        # Update database with requirements
                        if self.update_requirements(app_id, extracted):
                            self.stats['with_requirements'] += 1
                            logging.info(f"✅ Updated requirements for {name}")
                            
                            # Log some key requirements
                            if extracted['min_processor']:
                                logging.info(f"  💻 Min Processor: {extracted['min_processor'][:100]}...")
                            if extracted['min_graphics']:
                                logging.info(f"  🎮 Min Graphics: {extracted['min_graphics'][:100]}...")
                            if extracted['min_memory']:
                                logging.info(f"  🧠 Min Memory: {extracted['min_memory']}")
                        else:
                            self.stats['errors'] += 1
                            logging.error(f"❌ Failed to update requirements for {name}")
                    else:
                        self.stats['without_requirements'] += 1
                        logging.info(f"📭 No extractable requirements for {name}")
                else:
                    self.stats['without_requirements'] += 1
                    logging.info(f"📭 No PC requirements for {name}")
                
                self.stats['processed'] += 1
                
                # Commit every 10 games
                if self.stats['processed'] % 10 == 0:
                    self.conn.commit()
                    logging.info(f"📊 Progress: {self.stats['processed']}/{len(games)} processed, {self.stats['with_requirements']} with requirements, {self.stats['errors']} errors")
                
            except Exception as e:
                logging.error(f"❌ Error processing {game.get('name', 'Unknown')}: {str(e)}")
                self.stats['errors'] += 1
                self.stats['processed'] += 1
    
    def generate_analysis_report(self):
        """Generate a comprehensive analysis report"""
        logging.info(f"""
📊 REQUIREMENTS ANALYSIS COMPLETE
================================
Games processed: {self.stats['processed']}
Games with requirements: {self.stats['with_requirements']}
Games without requirements: {self.stats['without_requirements']}
Errors: {self.stats['errors']}
Success rate: {(self.stats['with_requirements']/self.stats['processed']*100):.1f}% if self.stats['processed'] > 0 else 0
        """)
        
        logging.info(f"""
🔍 DISCOVERED REQUIREMENT FIELDS
===============================
Total unique fields found: {len(self.requirement_fields)}
Fields: {sorted(list(self.requirement_fields))}
        """)
        
        logging.info(f"""
📋 SAMPLE REQUIREMENT DATA
=========================
Found {len(self.sample_requirements)} sample entries:
        """)
        
        for sample in self.sample_requirements[:10]:  # Show first 10 samples
            logging.info(f"  {sample['field']}: {sample['value']}")
        
        # Suggest database columns
        self.suggest_database_columns()
    
    def suggest_database_columns(self):
        """Suggest database columns based on analysis"""
        logging.info(f"""
💡 SUGGESTED DATABASE COLUMNS
============================
Based on the analysis, here are suggested columns for the games table:

-- Core requirement fields (most common)
ALTER TABLE games ADD COLUMN min_os TEXT;
ALTER TABLE games ADD COLUMN min_processor TEXT;
ALTER TABLE games ADD COLUMN min_memory TEXT;
ALTER TABLE games ADD COLUMN min_graphics TEXT;
ALTER TABLE games ADD COLUMN min_directx TEXT;
ALTER TABLE games ADD COLUMN min_storage TEXT;
ALTER TABLE games ADD COLUMN min_sound_card TEXT;

-- Additional requirement fields (if found)
ALTER TABLE games ADD COLUMN min_network TEXT;
ALTER TABLE games ADD COLUMN min_additional_notes TEXT;

-- Recommended requirement fields (if available)
ALTER TABLE games ADD COLUMN rec_os TEXT;
ALTER TABLE games ADD COLUMN rec_processor TEXT;
ALTER TABLE games ADD COLUMN rec_memory TEXT;
ALTER TABLE games ADD COLUMN rec_graphics TEXT;
ALTER TABLE games ADD COLUMN rec_directx TEXT;
ALTER TABLE games ADD COLUMN rec_storage TEXT;
ALTER TABLE games ADD COLUMN rec_sound_card TEXT;
ALTER TABLE games ADD COLUMN rec_network TEXT;
ALTER TABLE games ADD COLUMN rec_additional_notes TEXT;

-- Metadata
ALTER TABLE games ADD COLUMN requirements_updated TIMESTAMP;
        """)

def main():
    """Main execution function"""
    try:
        logging.info("🚀 Starting requirements analysis...")
        
        analyzer = RequirementsAnalyzer()
        
        # Connect to Supabase
        if not analyzer.connect():
            logging.error("❌ Failed to connect to database")
            return
        
        # Process all games before 2025 and insert requirements
        analyzer.process_games()
        
        # Final commit
        try:
            analyzer.conn.commit()
            logging.info("✅ Final commit completed")
        except Exception as e:
            logging.error(f"❌ Error during final commit: {str(e)}")
            analyzer.conn.rollback()
        
        # Generate analysis report
        analyzer.generate_analysis_report()
        
        analyzer.disconnect()
        
        logging.info("✅ Requirements analysis completed successfully!")
        
    except Exception as e:
        logging.error(f"❌ Script failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
