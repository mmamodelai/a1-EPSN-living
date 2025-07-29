#!/usr/bin/env python3
"""
Test script to scrape the 24 fighters with small HTML files
"""

import json
import sys
import os
sys.path.append('src')

from espn_data_processor import ESPNDataProcessor
import logging

def load_fighter_list():
    """Load the list of fighters to scrape"""
    try:
        with open('fighters_to_scrape.json', 'r') as f:
            fighters = json.load(f)
        return fighters
    except Exception as e:
        print(f"Error loading fighter list: {e}")
        return []

def test_small_fighter_scraping():
    """Test scraping the fighters with small HTML files"""
    print("=== Testing Small Fighter List Scraping ===")
    
    # Load fighter list
    fighters = load_fighter_list()
    if not fighters:
        print("❌ No fighters loaded!")
        return
    
    print(f"📋 Loaded {len(fighters)} fighters to scrape:")
    for i, fighter in enumerate(fighters, 1):
        print(f"   {i:2d}. {fighter}")
    
    print(f"\n🚀 Starting scrape with improved anti-detection features...")
    print("   ✅ Max 25 requests per minute")
    print("   ✅ Exponential backoff retries")
    print("   ✅ Realistic browser headers")
    print("   ✅ User agent rotation")
    
    # Initialize processor
    processor = ESPNDataProcessor()
    
    # Scrape the fighters
    print(f"\n--- Scraping {len(fighters)} fighters ---")
    scraped_htmls = processor.scrape_fighter_htmls(fighters)
    
    print(f"\n📊 Results:")
    success_count = 0
    for fighter_name, html_content in scraped_htmls.items():
        html_size = len(html_content)
        print(f"  {fighter_name}: {html_size:,} bytes ({html_size/1024:.1f} KB)")
        
        # Check if it's a real ESPN page
        if "prtlCmnApiRsp" in html_content:
            print(f"    ✅ Real ESPN data found")
            success_count += 1
        else:
            print(f"    ❌ Placeholder content")
    
    print(f"\n🎯 Summary:")
    print(f"   Total fighters: {len(fighters)}")
    print(f"   Successfully scraped: {len(scraped_htmls)}")
    print(f"   Real ESPN data: {success_count}")
    print(f"   Success rate: {len(scraped_htmls)/len(fighters)*100:.1f}%")
    
    # Check file sizes after scraping
    print(f"\n📁 Checking file sizes after scraping...")
    from pathlib import Path
    html_folder = Path("data/FighterHTMLs")
    
    small_files_after = []
    for fighter in fighters:
        html_file = html_folder / f"{fighter.replace(' ', '_')}.html"
        if html_file.exists():
            file_size = html_file.stat().st_size
            if file_size < 5000:  # Under 5KB
                small_files_after.append((fighter, file_size))
    
    if small_files_after:
        print(f"❌ Still have {len(small_files_after)} small files:")
        for fighter, size in small_files_after:
            print(f"   {fighter}: {size} bytes ({size/1024:.1f} KB)")
    else:
        print("✅ All files are now large (>5KB)!")

if __name__ == "__main__":
    test_small_fighter_scraping() 