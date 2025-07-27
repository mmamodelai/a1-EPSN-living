#!/usr/bin/env python3
"""
ESPN Data Processor - Step 2 Pipeline (Simplified)
Works IN and OUT of data folder with UPSERT logic for main outputs
Overwrites HTML/temp files at folder level
"""

import pandas as pd
import os
import logging
import shutil
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('espn_processor.log'),
        logging.StreamHandler()
    ]
)

class ESPNDataProcessor:
    """Process ESPN MMA data with simplified UPSERT logic"""
    
    def __init__(self, data_folder="data"):
        self.data_folder = Path(data_folder)
        
        # Main output files (UPSERT)
        self.clinch_file = self.data_folder / "clinch_data_living.csv"
        self.ground_file = self.data_folder / "ground_data_living.csv"
        self.striking_file = self.data_folder / "striking_data_living.csv"
        self.profiles_file = self.data_folder / "fighter_profiles.csv"
        
        # Temp folders (OVERWRITE)
        self.temp_folder = Path("temp")
        self.html_folder = Path("html_cache")
        
        logging.info(f"ESPN Data Processor initialized for folder: {self.data_folder}")
    
    def clean_temp_folders(self):
        """Clean and recreate temp folders (OVERWRITE policy)"""
        folders_to_clean = [self.temp_folder, self.html_folder]
        
        for folder in folders_to_clean:
            if folder.exists():
                shutil.rmtree(folder)
                logging.info(f"Cleaned temp folder: {folder}")
            folder.mkdir(exist_ok=True)
            logging.info(f"Created temp folder: {folder}")
    
    def load_existing_data(self):
        """Load existing main output files"""
        data = {}
        
        # Load main output files (UPSERT)
        main_files = {
            'clinch': self.clinch_file,
            'ground': self.ground_file,
            'striking': self.striking_file,
            'profiles': self.profiles_file
        }
        
        for data_type, file_path in main_files.items():
            if file_path.exists():
                data[data_type] = pd.read_csv(file_path)
                logging.info(f"Loaded {data_type} data: {len(data[data_type])} records")
            else:
                data[data_type] = pd.DataFrame()
                logging.warning(f"No existing {data_type} data found")
        
        return data
    
    def upsert_data(self, existing_df, new_df, key_columns=['Player', 'Date', 'Opponent']):
        """
        UPSERT logic: Preserve existing data, add new records
        
        Args:
            existing_df: Existing DataFrame
            new_df: New DataFrame to merge
            key_columns: Columns that form unique identifier
            
        Returns:
            Updated DataFrame with UPSERT logic applied
        """
        if existing_df.empty:
            logging.info(f"UPSERT: No existing data, using {len(new_df)} new records")
            return new_df
        
        if new_df.empty:
            logging.info(f"UPSERT: No new data, keeping {len(existing_df)} existing records")
            return existing_df
        
        # Create composite key for comparison
        existing_df['_key'] = existing_df[key_columns].astype(str).agg('|'.join, axis=1)
        new_df['_key'] = new_df[key_columns].astype(str).agg('|'.join, axis=1)
        
        # Find new records (not in existing)
        existing_keys = set(existing_df['_key'])
        new_records = new_df[~new_df['_key'].isin(existing_keys)]
        
        # Combine existing + new records
        combined_df = pd.concat([existing_df, new_records], ignore_index=True)
        
        # Clean up temporary key column
        combined_df = combined_df.drop('_key', axis=1)
        
        logging.info(f"UPSERT: {len(existing_df)} existing + {len(new_records)} new = {len(combined_df)} total")
        
        return combined_df
    
    def process_fight_data(self, data_type):
        """
        Process fight data with UPSERT logic
        
        Args:
            data_type: 'clinch', 'ground', or 'striking'
        """
        logging.info(f"Processing {data_type} data...")
        
        # Load existing data
        existing_data = self.load_existing_data()
        existing_df = existing_data.get(data_type, pd.DataFrame())
        
        # TODO: Add ESPN scraping logic here
        # For now, we'll just preserve existing data
        new_df = pd.DataFrame()  # Placeholder for scraped data
        
        # Apply UPSERT logic
        updated_df = self.upsert_data(existing_df, new_df)
        
        # Save back to data folder
        if data_type == 'clinch':
            output_file = self.clinch_file
        elif data_type == 'ground':
            output_file = self.ground_file
        elif data_type == 'striking':
            output_file = self.striking_file
        
        updated_df.to_csv(output_file, index=False)
        logging.info(f"Saved {data_type} data: {len(updated_df)} records to {output_file}")
        
        return updated_df
    
    def process_fighter_profiles(self):
        """Process fighter profiles with UPSERT logic"""
        logging.info("Processing fighter profiles...")
        
        # Load existing data
        existing_data = self.load_existing_data()
        existing_df = existing_data.get('profiles', pd.DataFrame())
        
        # TODO: Add fighter profile scraping logic here
        # For now, we'll just preserve existing data
        new_df = pd.DataFrame()  # Placeholder for scraped data
        
        # Apply UPSERT logic (use fighter name as key)
        updated_df = self.upsert_data(existing_df, new_df, key_columns=['Fighter Name'])
        
        # Save back to data folder
        updated_df.to_csv(self.profiles_file, index=False)
        logging.info(f"Saved fighter profiles: {len(updated_df)} records to {self.profiles_file}")
        
        return updated_df
    
    def run_full_processing(self):
        """Run full data processing pipeline"""
        logging.info("Starting ESPN Data Processing Pipeline")
        
        try:
            # Clean temp folders (OVERWRITE policy)
            self.clean_temp_folders()
            
            # Process main outputs (UPSERT policy)
            self.process_fight_data('clinch')
            self.process_fight_data('ground')
            self.process_fight_data('striking')
            self.process_fighter_profiles()
            
            logging.info("ESPN Data Processing Pipeline completed successfully")
            
        except Exception as e:
            logging.error(f"Error in processing pipeline: {e}")
            raise
    
    def get_data_summary(self):
        """Get summary of current main output data"""
        data = self.load_existing_data()
        
        summary = {
            'clinch_records': len(data.get('clinch', pd.DataFrame())),
            'ground_records': len(data.get('ground', pd.DataFrame())),
            'striking_records': len(data.get('striking', pd.DataFrame())),
            'fighter_profiles': len(data.get('profiles', pd.DataFrame()))
        }
        
        logging.info("Data Summary:")
        for key, value in summary.items():
            logging.info(f"  {key}: {value}")
        
        return summary

def main():
    """Main execution function"""
    processor = ESPNDataProcessor()
    
    # Get current data summary
    processor.get_data_summary()
    
    # Run processing pipeline
    processor.run_full_processing()
    
    # Get updated summary
    processor.get_data_summary()

if __name__ == "__main__":
    main() 