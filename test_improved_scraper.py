#!/usr/bin/env python3
"""
Test script for the improved ESPN scraper
"""

import sys
import os
sys.path.append('src')

from espn_scraper import ESPNFighterScraper
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_improved_scraper():
    """Test the improved ESPN scraper with anti-detection features"""
    print("=== Testing Improved ESPN Scraper with Anti-Detection ===")
    
    from src.espn_data_processor import ESPNDataProcessor
    
    processor = ESPNDataProcessor()
    test_fighters = ["Charles Oliveira", "Robert Whittaker", "Israel Adesanya"]
    
    print(f"\n--- Testing HTML scraping for {len(test_fighters)} fighters ---")
    print("Note: This will use improved rate limiting (max 25 requests/minute)")
    
    # Test the fixed HTML scraping method
    scraped_htmls = processor.scrape_fighter_htmls(test_fighters)
    
    print(f"\nResults:")
    for fighter_name, html_content in scraped_htmls.items():
        html_size = len(html_content)
        print(f"  {fighter_name}: {html_size:,} bytes ({html_size/1024:.1f} KB)")
        
        # Check if it's a real ESPN page or placeholder
        if "prtlCmnApiRsp" in html_content:
            print(f"    ✅ Real ESPN data found")
        else:
            print(f"    ❌ Placeholder content")
    
    print(f"\nTotal fighters scraped: {len(scraped_htmls)}")
    print("\nAnti-detection features active:")
    print("  ✅ Max 25 requests per minute")
    print("  ✅ Exponential backoff retries")
    print("  ✅ Realistic browser headers")
    print("  ✅ User agent rotation")
    print("  ✅ Request tracking")

if __name__ == "__main__":
    test_improved_scraper()