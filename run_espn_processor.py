#!/usr/bin/env python3
"""
ESPN Data Processor Runner
Runs the ESPN data processing pipeline with real scraping
"""

import argparse
import sys
import logging
import pandas as pd
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from espn_data_processor import ESPNDataProcessor

def main():
    """Main function to run ESPN data processing"""
    parser = argparse.ArgumentParser(description='ESPN MMA Data Processor')
    parser.add_argument('--data-type', choices=['clinch', 'ground', 'striking', 'profiles', 'all'], 
                       default='all', help='Type of data to process')
    parser.add_argument('--test-mode', action='store_true', 
                       help='Run in test mode with sample data only')
    parser.add_argument('--fighters', nargs='+', 
                       help='Specific fighters to scrape (overrides fighters_name.csv)')
    parser.add_argument('--delay', type=float, default=3.0,
                       help='Delay between requests in seconds')
    parser.add_argument('--verbose', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('espn_processor.log'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    
    print("🚀 ESPN MMA Data Processor")
    print("=" * 50)
    print(f"📊 Data Type: {args.data_type}")
    print(f"🧪 Test Mode: {args.test_mode}")
    print(f"⏱️  Delay: {args.delay}s")
    print(f"🔍 Verbose: {args.verbose}")
    
    if args.fighters:
        print(f"👥 Custom Fighters: {', '.join(args.fighters)}")
    
    print("=" * 50)
    
    try:
        # Initialize processor
        processor = ESPNDataProcessor()
        
        if args.test_mode:
            logger.info("🧪 Running in TEST MODE with sample data")
            
            # Use sample data instead of real scraping
            from espn_scraper import create_sample_fighter_data
            sample_data = create_sample_fighter_data()
            
            # Convert sample data to DataFrames
            from datetime import datetime
            
            clinch_records = []
            ground_records = []
            striking_records = []
            profile_records = []
            
            for fighter_data in sample_data:
                fighter_name = fighter_data.get('fighter_name', '')
                
                # Create clinch record
                clinch_record = {
                    'Player': fighter_name,
                    'Date': datetime.now().isoformat(),
                    'Opponent': 'N/A',
                    'Event': 'TEST MODE',
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
                    'Date': datetime.now().isoformat(),
                    'Opponent': 'N/A',
                    'Event': 'TEST MODE',
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
                    'Date': datetime.now().isoformat(),
                    'Opponent': 'N/A',
                    'Event': 'TEST MODE',
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
                    'Scraped At': datetime.now().isoformat(),
                    'Total Fights': 0,
                    'Last Updated': datetime.now().isoformat()
                }
                profile_records.append(profile_record)
            
            # Create DataFrames
            sample_clinch_df = pd.DataFrame(clinch_records)
            sample_ground_df = pd.DataFrame(ground_records)
            sample_striking_df = pd.DataFrame(striking_records)
            sample_profiles_df = pd.DataFrame(profile_records)
            
            # Apply UPSERT
            if args.data_type in ['clinch', 'all']:
                processor._upsert_data('clinch', sample_clinch_df)
            
            if args.data_type in ['ground', 'all']:
                processor._upsert_data('ground', sample_ground_df)
            
            if args.data_type in ['striking', 'all']:
                processor._upsert_data('striking', sample_striking_df)
            
            if args.data_type in ['profiles', 'all']:
                processor._upsert_data('profiles', sample_profiles_df)
            
            logger.info("✅ Test mode processing completed")
            
        else:
            logger.info("🌐 Running with REAL ESPN scraping")
            
            # Override fighter list if specified
            if args.fighters:
                logger.info(f"Using custom fighter list: {args.fighters}")
                processor.fighters_name = pd.DataFrame({'fighters': args.fighters})
            
            # Run full processing
            summary = processor.run_full_processing()
            
            if summary:
                logger.info("✅ Real scraping processing completed")
            else:
                logger.error("❌ Real scraping processing failed")
                return 1
        
        # Save data
        processor.save_data()
        
        # Clean temp folders
        processor.clean_temp_folders()
        
        # Get final summary
        final_summary = processor.get_data_summary()
        
        print("\n" + "=" * 50)
        print("🎉 ESPN Data Processing Completed!")
        print("=" * 50)
        print("📊 Final Data Summary:")
        for key, value in final_summary.items():
            print(f"   {key}: {value}")
        
        print("\n📁 Output Files:")
        data_folder = Path("data")
        for file_path in data_folder.glob("*.csv"):
            print(f"   📄 {file_path.name}")
        
        print("\n✅ Processing completed successfully!")
        return 0
        
    except KeyboardInterrupt:
        logger.info("⏹️  Processing interrupted by user")
        return 1
        
    except Exception as e:
        logger.error(f"❌ Processing failed: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code) 