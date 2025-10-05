#!/usr/bin/env python3
"""
Master script to run all pipeline scripts in the correct order.
This script executes all numbered pipeline scripts sequentially.
"""

import subprocess
import sys
import time
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Script execution order (excluding this script)
PIPELINE_SCRIPTS = [
    "01_steam_to_supabase_pipeline.py",
    "02_update_recent_reviews.py", 
    # "03_collect_all_nintendo_games_clean.py",  # Temporarily bypassed
    "04_check_coming_soon_updates.py",
    "05_update_release_date_actual.py",
    "06_recreate_top_line_metrics.py"
]

def run_script(script_name):
    """Run a single script and return the result."""
    print(f"\n{'='*60}")
    print(f"🚀 Starting: {script_name}")
    print(f"⏰ Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    
    start_time = time.time()
    
    try:
        # Run the script
        result = subprocess.run(
            [sys.executable, script_name],
            capture_output=False,  # Show output in real-time
            text=True,
            cwd=os.getcwd()
        )
        
        end_time = time.time()
        duration = end_time - start_time
        
        if result.returncode == 0:
            print(f"\n✅ {script_name} completed successfully!")
            print(f"⏱️  Duration: {duration:.2f} seconds")
            return True, duration
        else:
            print(f"\n❌ {script_name} failed with exit code: {result.returncode}")
            print(f"⏱️  Duration: {duration:.2f} seconds")
            return False, duration
            
    except KeyboardInterrupt:
        print(f"\n⚠️  {script_name} interrupted by user")
        return False, time.time() - start_time
    except Exception as e:
        print(f"\n❌ Error running {script_name}: {str(e)}")
        return False, time.time() - start_time

def check_script_exists(script_name):
    """Check if a script file exists."""
    return os.path.exists(script_name)

def run_all_pipeline_scripts():
    """Run all pipeline scripts in order."""
    print("🎮 Game Intelligence Pipeline - Master Execution")
    print("=" * 60)
    print(f"📅 Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📁 Working Directory: {os.getcwd()}")
    print(f"🐍 Python Executable: {sys.executable}")
    print("=" * 60)
    
    # Check if all scripts exist
    missing_scripts = []
    for script in PIPELINE_SCRIPTS:
        if not check_script_exists(script):
            missing_scripts.append(script)
    
    if missing_scripts:
        print("❌ Missing scripts:")
        for script in missing_scripts:
            print(f"   - {script}")
        print("\nPlease ensure all pipeline scripts are present before running.")
        return False
    
    print(f"✅ All {len(PIPELINE_SCRIPTS)} scripts found")
    
    # Track execution results
    results = []
    total_start_time = time.time()
    
    # Run each script
    for i, script in enumerate(PIPELINE_SCRIPTS, 1):
        print(f"\n📊 Progress: {i}/{len(PIPELINE_SCRIPTS)} scripts")
        
        success, duration = run_script(script)
        results.append({
            'script': script,
            'success': success,
            'duration': duration
        })
        
        if not success:
            print(f"\n⚠️  Script {script} failed. Do you want to continue with remaining scripts?")
            print("Press Ctrl+C to stop, or Enter to continue...")
            try:
                input()
            except KeyboardInterrupt:
                print("\n🛑 Pipeline execution stopped by user")
                break
        
        # Small delay between scripts
        if i < len(PIPELINE_SCRIPTS):
            print("\n⏳ Waiting 5 seconds before next script...")
            time.sleep(5)
    
    # Final summary
    total_end_time = time.time()
    total_duration = total_end_time - total_start_time
    
    print(f"\n{'='*60}")
    print("📊 PIPELINE EXECUTION SUMMARY")
    print(f"{'='*60}")
    print(f"📅 End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏱️  Total Duration: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
    print()
    
    successful = 0
    failed = 0
    
    for result in results:
        status = "✅ SUCCESS" if result['success'] else "❌ FAILED"
        duration_str = f"{result['duration']:.2f}s"
        print(f"{status} | {duration_str:>8} | {result['script']}")
        
        if result['success']:
            successful += 1
        else:
            failed += 1
    
    print(f"\n📈 Results: {successful} successful, {failed} failed out of {len(results)} scripts")
    
    if failed == 0:
        print("🎉 All scripts completed successfully!")
        return True
    else:
        print(f"⚠️  {failed} script(s) failed. Check the logs above for details.")
        return False

def main():
    """Main entry point."""
    try:
        success = run_all_pipeline_scripts()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n🛑 Pipeline execution interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
