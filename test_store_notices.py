#!/usr/bin/env python3
"""
Validate Steam store-notice parsing against known positive/negative examples.

Usage:
  python test_store_notices.py
  python test_store_notices.py --ensure-schema   # also ADD COLUMN IF NOT EXISTS on games
"""

from __future__ import annotations

import argparse
import json
import sys

import requests
from dotenv import load_dotenv

from steam_store_notices import parse_store_notices

load_dotenv()

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

# Expected flags for well-known store pages (as of implementation).
CASES = [
    {
        "app_id": 1808500,
        "name": "ARC Raiders",
        "expect": {
            "made_with_ai": True,
            "requires_kernel_anticheat": True,
            "has_denuvo": True,
            "requires_eula": True,
        },
    },
    {
        "app_id": 2358720,
        "name": "Black Myth: Wukong",
        "expect": {
            "made_with_ai": False,
            "requires_kernel_anticheat": False,
            "has_third_party_drm": True,
            "has_denuvo": True,
            "requires_eula": True,
        },
    },
    {
        "app_id": 1245620,
        "name": "ELDEN RING",
        "expect": {
            "made_with_ai": False,
            "has_denuvo": False,
            "has_third_party_drm": False,
            "requires_eula": True,
        },
    },
    {
        "app_id": 413150,
        "name": "Stardew Valley",
        "expect": {
            "made_with_ai": False,
            "requires_kernel_anticheat": False,
            "has_third_party_drm": False,
            "has_denuvo": False,
            "requires_eula": False,
        },
    },
    {
        "app_id": 2379780,
        "name": "Balatro",
        "expect": {
            "made_with_ai": False,
            "requires_kernel_anticheat": False,
            "has_third_party_drm": False,
            "has_denuvo": False,
            "requires_eula": False,
        },
    },
]


def fetch_and_parse(app_id: int):
    url = f"https://store.steampowered.com/app/{app_id}/"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return parse_store_notices(resp.text)


def main():
    parser = argparse.ArgumentParser(description="Validate store-notice scraping")
    parser.add_argument(
        "--ensure-schema",
        action="store_true",
        help="Connect to Supabase and ensure notice columns exist on games",
    )
    args = parser.parse_args()

    if args.ensure_schema:
        from importlib.util import module_from_spec, spec_from_file_location
        from pathlib import Path

        path = Path(__file__).resolve().parent / "01_steam_to_supabase_pipeline.py"
        spec = spec_from_file_location("steam_to_supabase_pipeline", path)
        mod = module_from_spec(spec)
        spec.loader.exec_module(mod)
        pipeline = mod.SupabasePipeline()
        if not pipeline.connect():
            print("Failed to connect for --ensure-schema")
            sys.exit(1)
        pipeline.disconnect()
        print("Schema ensure complete.\n")

    failed = 0
    for case in CASES:
        app_id = case["app_id"]
        name = case["name"]
        expect = case["expect"]
        try:
            notices = fetch_and_parse(app_id)
        except Exception as e:
            print(f"❌ {name} ({app_id}): fetch/parse error: {e}")
            failed += 1
            continue

        mismatches = []
        for key, wanted in expect.items():
            got = notices.get(key)
            if got != wanted:
                mismatches.append(f"{key}: got {got!r}, want {wanted!r}")

        if mismatches:
            failed += 1
            print(f"❌ {name} ({app_id})")
            for line in mismatches:
                print(f"   - {line}")
            print(f"   full: {json.dumps(notices, ensure_ascii=False)}")
        else:
            print(
                f"✅ {name} ({app_id}) | "
                f"ai={notices['made_with_ai']} kernel={notices['requires_kernel_anticheat']} "
                f"drm={notices['has_third_party_drm']} denuvo={notices['has_denuvo']} "
                f"eula={notices['requires_eula']}"
            )
            if notices.get("third_party_drm"):
                print(f"   drm={notices['third_party_drm']!r}")
            if notices.get("kernel_anticheat_name"):
                print(f"   kernel_name={notices['kernel_anticheat_name']!r}")
            if notices.get("eula_label"):
                print(f"   eula={notices['eula_label']!r}")

    print()
    if failed:
        print(f"FAILED: {failed}/{len(CASES)} cases")
        sys.exit(1)
    print(f"PASSED: all {len(CASES)} cases")
    sys.exit(0)


if __name__ == "__main__":
    main()
