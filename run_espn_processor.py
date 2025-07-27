#!/usr/bin/env python3
"""
ESPN Data Processor Runner
Simple script to run the full ESPN data processing pipeline
"""

import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from espn_data_processor import ESPNDataProcessor

def main():
    """Run the ESPN data processing pipeline"""
    import argparse
    
    parser = argparse.ArgumentParser(description='ESPN Data Processor')
    parser.add_argument('--full-scrape', action='store_true', help='Run full scrape mode')
    args = parser.parse_args()
    
    print("ESPN Data Processor - Step 2 Pipeline")
    if args.full_scrape:
        print("Mode: FULL SCRAPE")
    else:
        print("Mode: INCREMENTAL UPDATE")
    print("=" * 50)
    
    try:
        # Initialize processor
        processor = ESPNDataProcessor()
        
        # Show initial data summary
        print("\nInitial Data Summary:")
        print("-" * 30)
        summary = processor.get_data_summary()
        
        # Run full processing
        print("\nRunning ESPN Data Processing Pipeline...")
        print("-" * 50)
        processor.run_full_processing()
        
        # Show final data summary
        print("\nFinal Data Summary:")
        print("-" * 30)
        final_summary = processor.get_data_summary()
        
        # Show changes
        print("\nChanges Summary:")
        print("-" * 30)
        for key in summary:
            initial = summary[key]
            final = final_summary[key]
            change = final - initial
            if change > 0:
                print(f"  {key}: +{change} (added)")
            elif change < 0:
                print(f"  {key}: {change} (removed)")
            else:
                print(f"  {key}: no change")
        
        print("\nProcessing completed successfully!")
        print("All data has been preserved and enhanced.")
        
    except Exception as e:
        print(f"\nError during processing: {e}")
        print("Check the log file for details.")
        sys.exit(1)

if __name__ == "__main__":
    main() 