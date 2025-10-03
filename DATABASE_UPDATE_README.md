# Database Update Pipeline - Execution Guide

This guide provides the step-by-step process for updating your Supabase database with the latest Steam and Nintendo game data.

## Overview

The database update process consists of **6 main scripts** that should be run in a specific order to ensure data consistency and completeness:

1. **Steam Data Collection** - Collects new games and tags from Steam
2. **Steam Review Updates** - Updates review data for recent releases
3. **Nintendo Data Collection** - Collects games from Nintendo eShop
4. **Coming Soon Games Check** - Updates games that were previously "coming soon" but may have been released
5. **Release Date Actual Update** - Parses and populates `release_date_actual`
6. **Analytics Table Recreation** - Recreates the top_line_metrics table for dashboard analytics

## Prerequisites

### Environment Setup

1. **Install Dependencies:**
   ```bash
   # Install Steam pipeline dependencies
   pip install -r requirements.txt
   
   # Install Nintendo pipeline dependencies
   pip install -r requirements_nintendo.txt
   ```

2. **Environment Variables:**
   Ensure these environment variables are set:
   - `SUPABASE_CONNECTION_STRING` - Your Supabase PostgreSQL connection string
   - `STEAM_API_KEY` - Your Steam API key (for Steam scripts)
   - `CUBEJS_API_URL` - Your Cube.js API URL (for analytics refresh)
   - `CUBEJS_AUTH_TOKEN` - Your Cube.js authentication token

3. **Database Access:**
   - Ensure your IP is whitelisted in Supabase
   - Verify database connection is working

## Execution Order

### Step 1: Steam Data Collection
**Script:** `steam_to_supabase_pipeline.py`
**Purpose:** Collects new Steam games and their user tags
**Duration:** 2-4 hours (depending on number of new games)

```bash
cd game_intelligence_pipeline
python steam_to_supabase_pipeline.py
```

**What it does:**
- Connects to Supabase and finds the highest existing app_id
- Fetches Steam app list to identify new games
- Collects comprehensive game details (metadata, descriptions, requirements)
- Scrapes user tags from Steam store pages
- Inserts new games and tags into the database
- Updates price history information

**Configuration Options:**
- `START_APP_ID` - Start from a specific app_id (for resuming)
- `LIMIT` - Limit number of games to process (for testing)
- `FETCH_TAGS` - Enable/disable tag collection (affects speed)

**Expected Output:**
```
✅ Connected to Supabase database
📊 Found 1,234 new games to process
🔄 Processing 1/1234: Game Name (ID: 123456)
🏷️ Found 15 tags for app_id 123456
✅ Successfully inserted game and tags
📈 Progress: 100/1234 processed, 95 successful, 5 failed
```

### Step 2: Steam Review Updates
**Script:** `update_recent_reviews.py`
**Purpose:** Updates review data for recently released games
**Duration:** 30-60 minutes

```bash
python update_recent_reviews.py
```

**What it does:**
- Queries database for games released in the past 30 days
- Fetches updated review statistics from Steam API
- Updates review scores, counts, and descriptions
- Maintains data freshness for recent releases

**Configuration Options:**
- `DAYS_BACK` - Number of days to look back (default: 30)
- `LIMIT` - Limit number of games to update (for testing)

**Expected Output:**
```
✅ Connected to Supabase database
📊 Found 45 games released in the past 30 days
🔄 Processing 1/45: Recent Game (ID: 789012)
📊 Review data: {"num_reviews": 1250, "review_score": 85, ...}
✅ Successfully updated reviews for Recent Game
📈 Progress: 45/45 processed, 42 updated, 3 failed
```

### Step 3: Nintendo Data Collection
**Script:** `collect_all_nintendo_games_clean.py`
**Purpose:** Collects comprehensive Nintendo eShop game data
**Duration:** 3-6 hours (depending on total games)

```bash
python collect_all_nintendo_games_clean.py
```

**What it does:**
- Connects to Nintendo eShop API
- Collects ALL available games from the eShop
- Extracts comprehensive metadata (prices, descriptions, ratings, etc.)
- Creates/updates the `nintendo_games` table
- Handles pagination and rate limiting automatically

**Configuration Options:**
- `max_games` - Limit total games to collect (for testing)
- `resume_from` - Resume from a specific position (for interrupted runs)

**Expected Output:**
```
Connected to Supabase database
Total games available: 5,234
📊 Processing batch 1/53 (games 1-100)
✅ Successfully inserted 95 games from batch 1
📈 Progress: 100/5234 processed, 95 successful, 5 failed
```

### Step 4: Coming Soon Games Check
**Script:** `check_coming_soon_updates.py`
**Purpose:** Updates games that were previously "coming soon" but may have been released
**Duration:** 30-90 minutes (depending on number of games to check)

```bash
python check_coming_soon_updates.py
```

**What it does:**
- Creates a temporary `games_checking_for_updates` table
- Moves games with `coming_soon: true` and `last_updated < current_date` to the checking table
- Removes those games from the main `games` table
- Fetches updated data from Steam API for each game in the checking table
- Re-inserts updated games back into the main `games` table
- Collects fresh tags and review data
- Cleans up the temporary checking table

**Configuration Options:**
- No configuration needed - automatically processes all eligible games
- Includes built-in rate limiting and error handling

**Expected Output:**
```
✅ Connected to Supabase database
✅ Created games_checking_for_updates table
📋 Moved 45 games to checking table
🗑️  Removed 45 games from main games table
📊 Found 45 games to check for updates
🔄 Processing 1/45: Game Name (ID: 123456)
📊 Review summary: {"num_reviews": 1250, "review_score": 85, ...}
✅ Successfully updated Game Name
🏷️  Successfully collected 12 tags for Game Name
📈 Progress: 45/45 processed, 42 successful, 3 skipped, 156 tags collected
🎉 Coming Soon Update Check completed!
```

### Step 5: Release Date Actual Update
**Script:** `update_release_date_actual.py`
**Purpose:** Parses and populates `release_date_actual` for games where it's NULL
**Duration:** 1-5 minutes

```bash
python update_release_date_actual.py
```

**What it does:**
- Normalizes non-standard month abbreviations in `release_date` JSON
- Parses multiple human-readable formats into a proper DATE
- Updates only when `coming_soon = false` and date string is present
- Runs in a transaction for safety

**Expected Output:**
```
✅ Connected to Supabase database
🔧 Normalizing month names (maj → May)...
   → Updated N rows
🔧 Normalizing month names (okt → Oct)...
   → Updated N rows
🗓️  Parsing and updating release_date_actual...
   → Updated N rows
✅ release_date_actual update completed successfully
✅ Disconnected from Supabase database
```

### Step 6: Analytics Table Recreation
**Script:** `recreate_top_line_metrics.py`
**Purpose:** Recreates the top_line_metrics table for dashboard analytics
**Duration:** 5-15 minutes

```bash
python recreate_top_line_metrics.py
```

**What it does:**
- Connects to Supabase database
- Drops the existing top_line_metrics table
- Creates a new table with proper structure and indexes
- Aggregates data from the games table by review score description
- Populates the table with current analytics data
- Verifies the data and shows sample results

**Expected Output:**
```
✅ Connected to Supabase database
🗑️ Dropping existing top_line_metrics table...
✅ Table dropped successfully
🏗️ Creating new top_line_metrics table...
✅ Table created with proper structure and indexes
📊 Aggregating data from games table...
✅ Successfully aggregated data for 5 review categories
📈 Sample results:
   Very Positive: 1,234 games
   Positive: 2,345 games
   Mixed: 567 games
   Negative: 89 games
   Very Negative: 23 games
✅ Analytics table recreation completed!
```

## Complete Update Process

### Quick Update (Recommended for regular runs)
```bash
# Run all scripts in sequence
cd game_intelligence_pipeline

echo "Step 1: Steam Data Collection"
python steam_to_supabase_pipeline.py

echo "Step 2: Steam Review Updates"
python update_recent_reviews.py

echo "Step 3: Nintendo Data Collection"
python collect_all_nintendo_games_clean.py

echo "Step 4: Coming Soon Games Check"
python check_coming_soon_updates.py

echo "Step 5: Release Date Actual Update"
python update_release_date_actual.py

echo "Step 6: Analytics Table Recreation"
python recreate_top_line_metrics.py

echo "✅ Database update complete!"
```

### Full Update (For comprehensive data refresh)
```bash
# Same as quick update, but with full Nintendo collection
cd game_intelligence_pipeline

echo "Step 1: Steam Data Collection"
python steam_to_supabase_pipeline.py

echo "Step 2: Steam Review Updates"
python update_recent_reviews.py

echo "Step 3: Nintendo Data Collection"
python collect_all_nintendo_games_clean.py

echo "Step 4: Coming Soon Games Check"
python check_coming_soon_updates.py

echo "Step 5: Release Date Actual Update"
python update_release_date_actual.py

echo "Step 6: Analytics Table Recreation"
python recreate_top_line_metrics.py

echo "✅ Full database update complete!"
```

## Monitoring and Verification

### Check Update Status
After running the scripts, verify the updates:

```sql
-- Check Steam data
SELECT COUNT(*) as total_steam_games,
       MAX(last_updated) as last_steam_update
FROM games;

-- Check Nintendo data
SELECT COUNT(*) as total_nintendo_games,
       MAX(last_updated) as last_nintendo_update
FROM nintendo_games;

-- Check recent additions
SELECT name, release_date_actual, last_updated
FROM games
WHERE last_updated > NOW() - INTERVAL '1 day'
ORDER BY last_updated DESC
LIMIT 10;
```

### Expected Results
- **Steam games:** Should increase by 50-200 games per update
- **Nintendo games:** Should have 5,000+ total games
- **Review updates:** Recent games should have fresh review data
- **Analytics:** Top line metrics table should reflect current data

## Troubleshooting

### Common Issues

1. **Connection Errors**
   ```bash
   # Test Supabase connection
   python -c "
   import psycopg2
   import os
   conn = psycopg2.connect(os.getenv('SUPABASE_CONNECTION_STRING'))
   print('✅ Connection successful')
   "
   ```

2. **Rate Limiting**
   - Steam scripts include automatic rate limiting
   - If you get 429 errors, the script will wait and retry
   - Nintendo script includes built-in delays

3. **Memory Issues**
   - For large datasets, consider running scripts separately
   - Monitor system resources during execution

4. **Cube.js Issues**
   - Ensure Cube.js is running before Step 4
   - Check Cube.js logs for any errors
   - Verify environment variables are set correctly

### Recovery from Interruptions

If a script is interrupted, you can resume:

```bash
# Steam pipeline - edit the script to set START_APP_ID
# Find the last processed app_id from logs and set it

# Nintendo pipeline - edit the script to set resume_from
# Find the last processed position from logs and set it

# Review updates - safe to re-run, will only update recent games
# Analytics table recreation - safe to re-run anytime
```

## Scheduling Regular Updates

### Recommended Schedule
- **Daily:** Steam review updates (Step 2) and Coming soon games check (Step 4)
- **Weekly:** Full Steam data collection (Step 1)
- **Monthly:** Full Nintendo data collection (Step 3)
- **After any data update:** Release date update (Step 5) then Analytics (Step 6)

### Automation Example (cron)
```bash
# Add to crontab for daily review updates
0 2 * * * cd /path/to/game_intelligence_pipeline && python update_recent_reviews.py

# Add to crontab for daily coming soon games check
30 2 * * * cd /path/to/game_intelligence_pipeline && python check_coming_soon_updates.py

# Add to crontab for weekly Steam updates
0 3 * * 0 cd /path/to/game_intelligence_pipeline && python steam_to_supabase_pipeline.py

# Add to crontab for monthly Nintendo updates
0 4 1 * * cd /path/to/game_intelligence_pipeline && python collect_all_nintendo_games_clean.py

# Add to crontab for release date actual update (after data updates)
0 5 * * 0 cd /path/to/game_intelligence_pipeline && python update_release_date_actual.py

# Add to crontab for analytics table recreation (after data updates)
30 5 * * 0 cd /path/to/game_intelligence_pipeline && python recreate_top_line_metrics.py
```

## Data Quality Checks

After each update, verify data quality:

```sql
-- Check for missing critical data
SELECT COUNT(*) as games_without_reviews
FROM games
WHERE num_reviews IS NULL AND type = 'game';

-- Check for recent Nintendo additions
SELECT COUNT(*) as recent_nintendo_games
FROM nintendo_games
WHERE last_updated > NOW() - INTERVAL '1 day';

-- Verify analytics data
SELECT review_score_desc, COUNT(*) as game_count
FROM games
WHERE type = 'game'
GROUP BY review_score_desc
ORDER BY game_count DESC;
```

## Support and Maintenance

### Log Files
Each script generates detailed logs:
- `nintendo_collection_clean.log` - Nintendo collection progress
- Console output for Steam scripts
- Check logs for any errors or warnings

### Performance Optimization
- Monitor database size and query performance
- Consider adding indexes based on query patterns
- Archive old data if needed

### Updates and Maintenance
- Keep dependencies updated: `pip install -r requirements.txt --upgrade`
- Monitor for API changes from Steam or Nintendo
- Update scripts as needed for new data sources

---

**Last Updated:** December 2024
**Version:** 1.0
**Maintainer:** Game Intelligence Pipeline Team 