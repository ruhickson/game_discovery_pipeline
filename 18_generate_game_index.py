#!/usr/bin/env python3
"""
Refresh the Game Discovery Tool "Search for a Game" index.

Queries Cube.js for game names, app IDs, and review stats, then overwrites
game_discovery_tool/public/games-index.json (the file the recommender fetches).

Cube settings are read from this pipeline's .env, then from any keys still
missing in the sibling game_discovery_tool/.env.

Usage:
  python 18_generate_game_index.py
  python 18_generate_game_index.py --limit 1000
  python 18_generate_game_index.py --output path/to/games-index.json
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import requests

try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

PIPELINE_DIR = Path(__file__).resolve().parent
TOOL_DIR = PIPELINE_DIR.parent / "game_discovery_tool"
DEFAULT_OUTPUT = TOOL_DIR / "public" / "games-index.json"


def load_env_file(path):
    """Set unset environment variables from a .env file."""
    if not path.is_file():
        print(f"Warning: .env file not found at: {path}")
        return

    print(f"Loading environment variables from: {path}")
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def load_env():
    """Load pipeline .env first, then fill gaps from the discovery tool .env."""
    try:
        from dotenv import load_dotenv

        load_dotenv(PIPELINE_DIR / ".env")
        load_dotenv(TOOL_DIR / ".env", override=False)
        print(f"Loading environment variables from: {PIPELINE_DIR / '.env'}")
        if (TOOL_DIR / ".env").is_file():
            print(f"Filling missing variables from: {TOOL_DIR / '.env'}")
    except ImportError:
        load_env_file(PIPELINE_DIR / ".env")
        load_env_file(TOOL_DIR / ".env")


def validate_cube_config():
    """Validate Cube.js configuration."""
    api_url = os.getenv("VITE_CUBEJS_API_URL", "").rstrip("/")
    auth_token = os.getenv("VITE_CUBEJS_AUTH_TOKEN", "")

    if not api_url:
        print("Error: VITE_CUBEJS_API_URL environment variable is not set")
        sys.exit(1)
    if not auth_token:
        print("Warning: VITE_CUBEJS_AUTH_TOKEN environment variable is not set; queries may be unauthorized")

    print(f"Cube.js API URL: {api_url}")
    print(f"Auth token: {'Set' if auth_token else 'Not set'}")
    return api_url, auth_token


def query_cube(api_url, auth_token, query, max_retries=3, max_continue_waits=30):
    """
    Query Cube.js API with retry logic.

    Cube often returns HTTP 200 with {"error": "Continue wait"} while a query
    is still warming. That must be polled — treating it as final data yields
    zero games (dict keys get extended into the results list).
    """
    query_json = json.dumps(query)
    query_param = quote(query_json)
    url = f"{api_url}/load?query={query_param}"

    headers = {"Content-Type": "application/json"}
    if auth_token:
        headers["Authorization"] = auth_token

    print("Querying Cube.js API...")
    print(f"Query: {json.dumps(query, indent=2)}")

    continue_waits = 0
    attempt = 0
    while attempt < max_retries + 1:
        try:
            response = requests.get(url, headers=headers, timeout=60)
            response.raise_for_status()

            data = response.json()
            error = data.get("error")
            if error == "Continue wait":
                continue_waits += 1
                if continue_waits > max_continue_waits:
                    raise Exception(
                        f"Cube.js still returning 'Continue wait' after "
                        f"{max_continue_waits} polls"
                    )
                wait_time = min(2 + continue_waits, 10)
                print(
                    f"Cube.js still preparing results "
                    f"(Continue wait {continue_waits}/{max_continue_waits}); "
                    f"retrying in {wait_time}s..."
                )
                time.sleep(wait_time)
                continue

            if error:
                raise Exception(f"Cube.js error: {error}")

            if "data" in data:
                if not isinstance(data["data"], list):
                    raise Exception(
                        f"Unexpected Cube.js data type: {type(data['data'])}"
                    )
                return data["data"]

            raise Exception(
                f"Cube.js response missing 'data' key. Keys: {list(data.keys())}"
            )

        except requests.exceptions.RequestException as e:
            attempt += 1
            print(f"Attempt {attempt} failed: {e}")
            if attempt <= max_retries:
                wait_time = 2 ** (attempt - 1)
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                raise Exception(
                    f"Failed to query Cube.js after {max_retries + 1} attempts: {e}"
                )

    raise Exception("Failed to query Cube.js")


def generate_game_index(output_path=None, limit=None):
    """
    Generate the game index used by Search for a Game.

    Args:
        output_path: Path where to save the JSON index file
        limit: Maximum number of games to include (None for all games)

    Returns:
        dict: Statistics about the generated index
    """
    print("Generating game index for client-side search...")
    load_env()
    api_url, auth_token = validate_cube_config()

    output_path = Path(output_path) if output_path else DEFAULT_OUTPUT
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        print("Querying games from Cube.js API...")

        all_results = []
        offset = 0
        # Cube accepts 10000; keep pages large but finite so offset pagination progresses.
        page_size = 5000

        while True:
            print(f"Fetching games {offset + 1} to {offset + page_size}...")

            # Measures + totalReviewsValue filter matches the deployed Cube schema.
            # Queries often return "Continue wait" first; query_cube() polls until ready.
            query = {
                "dimensions": [
                    "Games.name",
                    "Games.appId",
                    "Games.reviewScoreDesc",
                    "Games.releaseDate",
                ],
                "measures": [
                    "Games.totalReviews",
                    "Games.totalPositiveReviews",
                    "Games.totalNegativeReviews",
                ],
                "filters": [
                    {"member": "Games.type", "operator": "equals", "values": ["game"]},
                    {"member": "Games.name", "operator": "notEquals", "values": [""]},
                    {
                        "member": "Games.totalReviewsValue",
                        "operator": "gte",
                        "values": ["1"],
                    },
                ],
                "order": [
                    ["Games.totalReviews", "desc"],
                    ["Games.name", "asc"],
                ],
                "limit": page_size,
                "offset": offset,
            }

            if limit is not None and offset + page_size > limit:
                query["limit"] = limit - offset
                if query["limit"] <= 0:
                    break

            results = query_cube(api_url, auth_token, query)

            if not results:
                print("No more results found")
                break

            if not isinstance(results, list) or (
                results and not isinstance(results[0], dict)
            ):
                raise Exception(
                    f"Unexpected Cube.js row payload: {type(results)} / "
                    f"{type(results[0]) if results else 'empty'}"
                )

            all_results.extend(results)
            print(f"Retrieved {len(results)} games (total: {len(all_results)})")

            if len(results) < query["limit"]:
                print("Reached end of results")
                break

            if limit is not None and len(all_results) >= limit:
                all_results = all_results[:limit]
                break

            offset += page_size

        if not all_results:
            raise Exception("No games found in Cube.js response")

        print(f"Found {len(all_results)} games")
        print(f"Sample result: {all_results[0]}")

        def as_int(value, default=0):
            try:
                if value is None or value == "":
                    return default
                return int(float(value))
            except (TypeError, ValueError):
                return default

        games_data = []
        for row in all_results:
            if not isinstance(row, dict):
                print(f"Warning: Unexpected row format: {type(row)} - {row}")
                continue

            app_id = row.get("Games.appId", "")
            # Cube may return numeric app IDs as floats (e.g. 449960.0)
            if isinstance(app_id, float):
                app_id = str(int(app_id))
            else:
                app_id = str(app_id).split(".")[0]

            games_data.append({
                "name": str(row.get("Games.name") or ""),
                "appId": app_id,
                "reviewScore": str(row.get("Games.reviewScoreDesc") or "N/A"),
                "releaseDate": str(row.get("Games.releaseDate") or ""),
                "totalReviews": as_int(row.get("Games.totalReviews")),
                "totalPositive": as_int(row.get("Games.totalPositiveReviews")),
                "totalNegative": as_int(row.get("Games.totalNegativeReviews")),
            })

        print(f"Processed {len(games_data)} unique games")
        if not games_data:
            raise Exception(
                "Cube.js returned rows but none could be parsed into games"
            )

        index_data = {
            "games": games_data,
            "lastUpdated": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "totalGames": len(games_data),
            "generatedBy": "18_generate_game_index.py",
            "version": "1.0",
        }

        print(f"Writing index to: {output_path}")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(index_data, f, indent=2, ensure_ascii=False)

        file_size = output_path.stat().st_size
        file_size_mb = round(file_size / (1024 * 1024), 2)

        print("Index generated successfully!")
        print(f"File size: {file_size_mb} MB")
        print(f"Games included: {len(games_data)}")
        print(f"Last updated: {index_data['lastUpdated']}")

        return {
            "success": True,
            "totalGames": len(games_data),
            "fileSize": file_size_mb,
            "lastUpdated": index_data["lastUpdated"],
            "outputPath": str(output_path),
        }

    except Exception as e:
        print(f"Error generating game index: {e}")
        return {
            "success": False,
            "error": str(e),
        }


def main():
    parser = argparse.ArgumentParser(
        description="Refresh the Game Discovery Tool Search for a Game index"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of games to include (default: all)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output JSON path (default: ../game_discovery_tool/public/games-index.json)",
    )
    args = parser.parse_args()

    result = generate_game_index(args.output, args.limit)

    if result["success"]:
        print("\n=== Game Index Generation Complete ===")
        print(f"Total games: {result['totalGames']}")
        print(f"File size: {result['fileSize']} MB")
        print(f"Output: {result['outputPath']}")
        print(f"Last updated: {result['lastUpdated']}")
        sys.exit(0)

    print("\n=== Game Index Generation Failed ===")
    print(f"Error: {result['error']}")
    sys.exit(1)


if __name__ == "__main__":
    main()
