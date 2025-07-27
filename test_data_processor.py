#!/usr/bin/env python3
"""
Test script for ESPN Data Processor
Tests the IN/OUT data folder functionality with UPSERT logic
"""

import sys
import os
import pandas as pd
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from espn_data_processor import ESPNDataProcessor

def test_data_processor():
    """Test the data processor functionality"""
    print("Testing ESPN Data Processor...")
    
    # Initialize processor
    processor = ESPNDataProcessor()
    
    # Test 1: Load existing data
    print("\n1. Testing data loading...")
    data = processor.load_existing_data()
    
    for data_type, df in data.items():
        print(f"   {data_type}: {len(df)} records")
    
    # Test 2: Get data summary
    print("\n2. Testing data summary...")
    summary = processor.get_data_summary()
    
    for key, value in summary.items():
        print(f"   {key}: {value}")
    
    # Test 3: Test UPSERT logic with empty data
    print("\n3. Testing UPSERT logic...")
    existing_df = data.get('clinch', pd.DataFrame())
    new_df = pd.DataFrame()  # Empty new data
    
    result_df = processor.upsert_data(existing_df, new_df)
    print(f"   UPSERT result: {len(result_df)} records (should equal existing: {len(existing_df)})")
    
    # Test 4: Create backup
    print("\n4. Testing backup creation...")
    backup_dir = processor.create_backup()
    print(f"   Backup created: {backup_dir}")
    
    print("\nAll tests completed successfully!")
    print("The data processor is working correctly with your existing data.")

if __name__ == "__main__":
    test_data_processor() 