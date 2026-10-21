import psycopg2
import json
import os
from dotenv import load_dotenv
import sys

load_dotenv()

# Ensure stdout/stderr handle UTF-8
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


class GOGAnalyzer:
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
    
    def get_gog_exclusives(self, limit=None, min_rating=None, sort_by='rating'):
        """Get GOG games that are not on Steam."""
        try:
            query = """
                SELECT 
                    gog_id,
                    title,
                    developer,
                    publisher,
                    release_date,
                    rating,
                    rating_count,
                    price_final,
                    discount_percentage,
                    genres,
                    tags,
                    short_description
                FROM gog_games
                WHERE (is_on_steam = FALSE OR is_on_steam IS NULL)
                AND product_type = 'game'
            """
            
            params = []
            
            if min_rating:
                query += " AND rating >= %s"
                params.append(min_rating)
            
            # Sort options
            if sort_by == 'rating':
                query += " ORDER BY rating DESC NULLS LAST, rating_count DESC NULLS LAST"
            elif sort_by == 'price':
                query += " ORDER BY price_final ASC NULLS LAST"
            elif sort_by == 'release_date':
                query += " ORDER BY release_date DESC NULLS LAST"
            elif sort_by == 'title':
                query += " ORDER BY title ASC"
            else:
                query += " ORDER BY rating DESC NULLS LAST"
            
            if limit:
                query += " LIMIT %s"
                params.append(limit)
            
            self.cursor.execute(query, params)
            results = self.cursor.fetchall()
            
            return results
            
        except Exception as e:
            print(f"❌ Error getting GOG exclusives: {str(e)}")
            return []
    
    def get_statistics(self):
        """Get statistics about GOG games."""
        try:
            stats = {}
            
            # Total GOG games
            self.cursor.execute("SELECT COUNT(*) FROM gog_games WHERE product_type = 'game'")
            stats['total_gog_games'] = self.cursor.fetchone()[0]
            
            # Games on Steam
            self.cursor.execute("""
                SELECT COUNT(*) 
                FROM gog_games 
                WHERE product_type = 'game' AND is_on_steam = TRUE
            """)
            stats['on_steam'] = self.cursor.fetchone()[0]
            
            # Games not on Steam
            self.cursor.execute("""
                SELECT COUNT(*) 
                FROM gog_games 
                WHERE product_type = 'game' AND (is_on_steam = FALSE OR is_on_steam IS NULL)
            """)
            stats['not_on_steam'] = self.cursor.fetchone()[0]
            
            # Games with ratings
            self.cursor.execute("""
                SELECT COUNT(*) 
                FROM gog_games 
                WHERE product_type = 'game' AND rating IS NOT NULL
            """)
            stats['with_ratings'] = self.cursor.fetchone()[0]
            
            # Average rating
            self.cursor.execute("""
                SELECT AVG(rating) 
                FROM gog_games 
                WHERE product_type = 'game' AND rating IS NOT NULL
            """)
            result = self.cursor.fetchone()
            stats['avg_rating'] = round(result[0], 2) if result[0] else None
            
            # Top genres
            self.cursor.execute("""
                SELECT 
                    jsonb_array_elements_text(genres) as genre,
                    COUNT(*) as count
                FROM gog_games
                WHERE product_type = 'game' AND genres IS NOT NULL
                GROUP BY genre
                ORDER BY count DESC
                LIMIT 10
            """)
            stats['top_genres'] = self.cursor.fetchall()
            
            return stats
            
        except Exception as e:
            print(f"❌ Error getting statistics: {str(e)}")
            return {}
    
    def export_exclusives_to_json(self, filename='gog_exclusives.json', limit=None, min_rating=None):
        """Export GOG exclusives to a JSON file."""
        try:
            exclusives = self.get_gog_exclusives(limit=limit, min_rating=min_rating)
            
            games_list = []
            for row in exclusives:
                gog_id, title, developer, publisher, release_date, rating, rating_count, \
                price_final, discount_percentage, genres, tags, short_description = row
                
                game_data = {
                    'gog_id': gog_id,
                    'title': title,
                    'developer': developer,
                    'publisher': publisher,
                    'release_date': release_date,
                    'rating': float(rating) if rating else None,
                    'rating_count': rating_count,
                    'price_final': float(price_final) if price_final else None,
                    'discount_percentage': discount_percentage,
                    'genres': json.loads(genres) if genres and isinstance(genres, str) else genres,
                    'tags': json.loads(tags) if tags and isinstance(tags, str) else tags,
                    'short_description': short_description
                }
                games_list.append(game_data)
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(games_list, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Exported {len(games_list)} GOG exclusives to {filename}")
            return filename
            
        except Exception as e:
            print(f"❌ Error exporting to JSON: {str(e)}")
            return None


def analyze_gog_exclusives():
    """Main function to analyze GOG exclusives."""
    analyzer = GOGAnalyzer()
    
    if not analyzer.connect():
        return
    
    try:
        # Get statistics
        print("\n📊 GOG Games Statistics")
        print("=" * 50)
        stats = analyzer.get_statistics()
        
        print(f"Total GOG games: {stats.get('total_gog_games', 0)}")
        print(f"Games on Steam: {stats.get('on_steam', 0)}")
        print(f"Games NOT on Steam (GOG exclusives): {stats.get('not_on_steam', 0)}")
        print(f"Games with ratings: {stats.get('with_ratings', 0)}")
        if stats.get('avg_rating'):
            print(f"Average rating: {stats.get('avg_rating')}")
        
        print("\n🏷️  Top Genres:")
        for genre, count in stats.get('top_genres', []):
            print(f"   {genre}: {count} games")
        
        # Get top exclusives
        print("\n🎮 Top 20 GOG Exclusives (by rating)")
        print("=" * 50)
        exclusives = analyzer.get_gog_exclusives(limit=20, min_rating=3.0, sort_by='rating')
        
        for idx, game in enumerate(exclusives, 1):
            gog_id, title, developer, publisher, release_date, rating, rating_count, \
            price_final, discount_percentage, genres, tags, short_description = game
            
            print(f"\n{idx}. {title}")
            if developer:
                print(f"   Developer: {developer}")
            if publisher:
                print(f"   Publisher: {publisher}")
            if rating:
                print(f"   Rating: {rating:.2f} ({rating_count} reviews)")
            if price_final:
                print(f"   Price: ${price_final:.2f}")
                if discount_percentage:
                    print(f"   Discount: {discount_percentage}%")
            if release_date:
                print(f"   Release: {release_date}")
            if short_description:
                desc = short_description[:150] + "..." if len(short_description) > 150 else short_description
                print(f"   {desc}")
        
        # Export to JSON
        print("\n💾 Exporting all exclusives to JSON...")
        analyzer.export_exclusives_to_json('gog_exclusives.json', min_rating=None)
        
    except Exception as e:
        print(f"❌ Error during analysis: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        analyzer.disconnect()


if __name__ == "__main__":
    print("GOG Exclusives Analyzer")
    print("=" * 50)
    analyze_gog_exclusives()

