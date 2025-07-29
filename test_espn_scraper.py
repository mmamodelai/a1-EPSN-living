#!/usr/bin/env python3
"""
Test script for ESPN Fighter Scraper
Tests the real ESPN scraping functionality
"""

import sys
import os
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from espn_scraper import ESPNFighterScraper, create_sample_fighter_data
from espn_data_processor import ESPNDataProcessor

def test_espn_scraper():
    """Test the ESPN scraper with sample fighters"""
    print("🧪 Testing ESPN Fighter Scraper...")
    
    # Initialize scraper
    scraper = ESPNFighterScraper(delay_range=(1, 2))  # Faster for testing
    
    # Test with sample fighters
    test_fighters = [
        'Robert Whittaker',
        'Israel Adesanya', 
        'Alex Pereira',
        'Sean Strickland'
    ]
    
    print(f"📊 Testing with {len(test_fighters)} fighters:")
    for fighter in test_fighters:
        print(f"  - {fighter}")
    
    # Test individual fighter scraping
    print("\n🔍 Testing individual fighter scraping...")
    for fighter in test_fighters[:2]:  # Test first 2 fighters
        print(f"\n📈 Scraping {fighter}...")
        try:
            stats = scraper.get_fighter_stats(fighter)
            if stats:
                print(f"✅ Success: Found {len(stats)} data points")
                print(f"   ESPN URL: {stats.get('espn_url', 'Not found')}")
                print(f"   Fight History: {len(stats.get('fight_history', []))} fights")
            else:
                print(f"❌ No data found for {fighter}")
        except Exception as e:
            print(f"❌ Error scraping {fighter}: {e}")
    
    # Test batch scraping
    print("\n🔄 Testing batch scraping...")
    try:
        results = scraper.scrape_fighters_batch(test_fighters[:2])
        print(f"✅ Batch scraping completed: {len(results)} results")
        
        for result in results:
            fighter_name = result.get('fighter_name', 'Unknown')
            has_error = 'error' in result
            print(f"   {fighter_name}: {'❌ Error' if has_error else '✅ Success'}")
            
    except Exception as e:
        print(f"❌ Batch scraping error: {e}")
    
    print("\n✅ ESPN Scraper test completed!")

def test_data_processor():
    """Test the data processor with real scraping"""
    print("\n🧪 Testing ESPN Data Processor...")
    
    # Initialize processor
    processor = ESPNDataProcessor()
    
    # Test with small fighter list
    test_fighters = ['Robert Whittaker', 'Israel Adesanya']
    
    print(f"📊 Testing with {len(test_fighters)} fighters")
    
    # Test scraping functionality
    try:
        scraped_data = processor.scrape_fighter_data(test_fighters)
        
        print(f"✅ Scraped data summary:")
        for data_type, df in scraped_data.items():
            print(f"   {data_type}: {len(df)} records")
        
        # Test UPSERT functionality
        print("\n🔄 Testing UPSERT functionality...")
        processor._upsert_data('clinch', scraped_data['clinch'])
        processor._upsert_data('ground', scraped_data['ground'])
        processor._upsert_data('striking', scraped_data['striking'])
        processor._upsert_data('profiles', scraped_data['profiles'])
        
        # Get summary
        summary = processor.get_data_summary()
        print(f"✅ Final data summary: {summary}")
        
    except Exception as e:
        print(f"❌ Data processor error: {e}")
    
    print("\n✅ ESPN Data Processor test completed!")

def test_sample_data():
    """Test with sample data (no real scraping)"""
    print("\n🧪 Testing with sample data...")
    
    # Create sample data
    sample_data = create_sample_fighter_data()
    print(f"📊 Created {len(sample_data)} sample fighter records")
    
    # Test data processor with sample data
    processor = ESPNDataProcessor()
    
    # Convert sample data to expected format
    # Create test DataFrames from sample data
    clinch_records = []
    ground_records = []
    striking_records = []
    profile_records = []
    
    for fighter_data in sample_data:
        fighter_name = fighter_data.get('fighter_name', '')
        
        # Create clinch record
        clinch_record = {
            'Player': fighter_name,
            'Date': fighter_data.get('scraped_at', ''),
            'Opponent': 'N/A',
            'Event': 'ESPN Scraped',
            'Result': 'N/A',
            'SCBL': fighter_data.get('SCBL', '-'),
            'SCBA': fighter_data.get('SCBA', '-'),
            'TDL': fighter_data.get('TDL', '-'),
            'TDA': fighter_data.get('TDA', '-'),
            'TDS': fighter_data.get('TDS', '-')
        }
        clinch_records.append(clinch_record)
        
        # Create ground record
        ground_record = {
            'Player': fighter_name,
            'Date': fighter_data.get('scraped_at', ''),
            'Opponent': 'N/A',
            'Event': 'ESPN Scraped',
            'Result': 'N/A',
            'SGBL': fighter_data.get('SGBL', '-'),
            'SGBA': fighter_data.get('SGBA', '-'),
            'SA': fighter_data.get('SA', '-'),
            'SL': fighter_data.get('SL', '-')
        }
        ground_records.append(ground_record)
        
        # Create striking record
        striking_record = {
            'Player': fighter_name,
            'Date': fighter_data.get('scraped_at', ''),
            'Opponent': 'N/A',
            'Event': 'ESPN Scraped',
            'Result': 'N/A',
            'TSL': fighter_data.get('TSL', '-'),
            'TSA': fighter_data.get('TSA', '-'),
            'SSL': fighter_data.get('SSL', '-'),
            'SSA': fighter_data.get('SSA', '-'),
            'KD': fighter_data.get('KD', '-')
        }
        striking_records.append(striking_record)
        
        # Create profile record
        profile_record = {
            'Fighter Name': fighter_name,
            'ESPN URL': fighter_data.get('espn_url', ''),
            'Scraped At': fighter_data.get('scraped_at', ''),
            'Total Fights': 0,
            'Last Updated': '2025-01-27T00:00:00'
        }
        profile_records.append(profile_record)
    
    # Test UPSERT with sample data
    import pandas as pd
    
    sample_clinch_df = pd.DataFrame(clinch_records)
    sample_ground_df = pd.DataFrame(ground_records)
    sample_striking_df = pd.DataFrame(striking_records)
    sample_profiles_df = pd.DataFrame(profile_records)
    
    processor._upsert_data('clinch', sample_clinch_df)
    processor._upsert_data('ground', sample_ground_df)
    processor._upsert_data('striking', sample_striking_df)
    processor._upsert_data('profiles', sample_profiles_df)
    
    # Get summary
    summary = processor.get_data_summary()
    print(f"✅ Sample data summary: {summary}")
    
    print("\n✅ Sample data test completed!")

def main():
    """Run all tests"""
    print("🚀 Starting ESPN Scraper Tests")
    print("=" * 50)
    
    # Test 1: ESPN Scraper
    test_espn_scraper()
    
    # Test 2: Data Processor
    test_data_processor()
    
    # Test 3: Sample Data
    test_sample_data()
    
    print("\n" + "=" * 50)
    print("🎉 All tests completed!")
    print("\n📝 Next steps:")
    print("1. Review the test results above")
    print("2. Check the espn_processor.log for detailed logs")
    print("3. Run the full processor: python run_espn_processor.py")
    print("4. Check the data/ folder for output files")

if __name__ == "__main__":
    main()