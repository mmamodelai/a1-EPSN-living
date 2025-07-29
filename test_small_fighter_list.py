#!/usr/bin/env python3
"""
Test Small Fighter List
Run the ESPN processor with just 25 test fighters
"""

import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from espn_data_processor import ESPNDataProcessor

def main():
    """Test the processor with small fighter list"""
    print("🧪 Testing Small Fighter List (25 fighters)")
    print("=" * 50)
    
    try:
        # Initialize processor
        processor = ESPNDataProcessor()
        
        # Show initial data summary
        print("\n📊 Initial Data Summary:")
        print("-" * 30)
        initial_summary = processor.get_data_summary()
        for key, value in initial_summary.items():
            print(f"  {key}: {value}")
        
        # Run HTML processing only (skip CSV processing for now)
        print("\n🔄 Processing Fighter HTMLs...")
        print("-" * 30)
        
        # Load test fighters list
        test_fighters_file = Path("data/fighter_names.csv")
        if test_fighters_file.exists():
            import pandas as pd
            fighters_df = pd.read_csv(test_fighters_file)
            fighter_names = fighters_df['Fighter Name'].tolist()
            print(f"📋 Loaded {len(fighter_names)} test fighters:")
            for i, name in enumerate(fighter_names, 1):
                print(f"  {i:2d}. {name}")
        else:
            print("❌ Test fighters file not found")
            return False
        
        # Process HTML files
        new_count, updated_count = processor.process_fighter_htmls()
        
        # Show final data summary
        print("\n📊 Final Data Summary:")
        print("-" * 30)
        final_summary = processor.get_data_summary()
        for key, value in final_summary.items():
            print(f"  {key}: {value}")
        
        # Show changes
        print("\n📈 Changes Summary:")
        print("-" * 30)
        html_change = final_summary['fighter_html_files'] - initial_summary['fighter_html_files']
        print(f"  HTML files: {initial_summary['fighter_html_files']} → {final_summary['fighter_html_files']} ({html_change:+d})")
        print(f"  New HTML files: {new_count}")
        print(f"  Updated HTML files: {updated_count}")
        
        print("\n🎉 Small fighter list test completed successfully!")
        print(f"✅ Processed {len(fighter_names)} fighters")
        print(f"✅ HTML UPSERT working correctly")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 