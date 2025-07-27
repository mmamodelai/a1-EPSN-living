#!/usr/bin/env python3
"""
ESPN Data Processor - Step 2 Pipeline
Works IN and OUT of data folder with UPSERT logic

This script processes ESPN MMA data while preserving existing data.
It reads from data folder, processes with UPSERT logic, and writes back.
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
    """Process ESPN MMA data with UPSERT logic"""
    
    def __init__(self, data_folder="data"):
        self.data_folder = Path(data_folder)
        self.backup_folder = self.data_folder / "backups"
        self.backup_folder.mkdir(exist_ok=True)
        
        # Data file paths
        self.clinch_file = self.data_folder / "clinch_data_living.csv"
        self.ground_file = self.data_folder / "ground_data_living.csv"
        self.striking_file = self.data_folder / "striking_data_living.csv"
        self.fighters_file = self.data_folder / "fighters_name.csv"
        self.profiles_file = self.data_folder / "fighter_profiles.csv"
        
        logging.info(f"ESPN Data Processor initialized for folder: {self.data_folder}")
    
    def create_backup(self):
        """Create timestamped backup of all data files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = self.backup_folder / f"backup_{timestamp}"
        backup_dir.mkdir(exist_ok=True)
        
        files_to_backup = [
            self.clinch_file,
            self.ground_file, 
            self.striking_file,
            self.fighters_file,
            self.profiles_file
        ]
        
        for file_path in files_to_backup:
            if file_path.exists():
                backup_path = backup_dir / file_path.name
                shutil.copy2(file_path, backup_path)
                logging.info(f"Backed up: {file_path.name}")
        
        logging.info(f"Backup created: {backup_dir}")
        return backup_dir
    
    def load_existing_data(self):
        """Load existing data from data folder"""
        data = {}
        
        # Load clinch data
        if self.clinch_file.exists():
            data['clinch'] = pd.read_csv(self.clinch_file)
            logging.info(f"Loaded clinch data: {len(data['clinch'])} records")
        else:
            data['clinch'] = pd.DataFrame()
            logging.warning("No existing clinch data found")
        
        # Load ground data
        if self.ground_file.exists():
            data['ground'] = pd.read_csv(self.ground_file)
            logging.info(f"Loaded ground data: {len(data['ground'])} records")
        else:
            data['ground'] = pd.DataFrame()
            logging.warning("No existing ground data found")
        
        # Load striking data
        if self.striking_file.exists():
            data['striking'] = pd.read_csv(self.striking_file)
            logging.info(f"Loaded striking data: {len(data['striking'])} records")
        else:
            data['striking'] = pd.DataFrame()
            logging.warning("No existing striking data found")
        
        # Load fighters data
        if self.fighters_file.exists():
            data['fighters'] = pd.read_csv(self.fighters_file)
            logging.info(f"Loaded fighters data: {len(data['fighters'])} records")
        else:
            data['fighters'] = pd.DataFrame()
            logging.warning("No existing fighters data found")
        
        # Load profiles data
        if self.profiles_file.exists():
            data['profiles'] = pd.read_csv(self.profiles_file)
            logging.info(f"Loaded profiles data: {len(data['profiles'])} records")
        else:
            data['profiles'] = pd.DataFrame()
            logging.warning("No existing profiles data found")
        
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
    
    def process_fighters_data(self):
        """Process fighters data with UPSERT logic"""
        logging.info("Processing fighters data...")
        
        # Load existing data
        existing_data = self.load_existing_data()
        existing_df = existing_data.get('fighters', pd.DataFrame())
        
        # TODO: Add fighter scraping logic here
        # For now, we'll just preserve existing data
        new_df = pd.DataFrame()  # Placeholder for scraped data
        
        # Apply UPSERT logic
        updated_df = self.upsert_data(existing_df, new_df, key_columns=['Fighter Name'])
        
        # Save back to data folder
        updated_df.to_csv(self.fighters_file, index=False)
        logging.info(f"Saved fighters data: {len(updated_df)} records to {self.fighters_file}")
        
        return updated_df
    
    def run_full_processing(self):
        """Run full data processing pipeline"""
        logging.info("Starting ESPN Data Processing Pipeline")
        
        try:
            # Create backup before processing
            backup_dir = self.create_backup()
            logging.info(f"Backup created: {backup_dir}")
            
            # Process all data types
            self.process_fight_data('clinch')
            self.process_fight_data('ground')
            self.process_fight_data('striking')
            self.process_fighters_data()
            
            logging.info("ESPN Data Processing Pipeline completed successfully")
            
        except Exception as e:
            logging.error(f"Error in processing pipeline: {e}")
            raise
    
    def get_data_summary(self):
        """Get summary of current data"""
        data = self.load_existing_data()
        
        summary = {
            'clinch_records': len(data.get('clinch', pd.DataFrame())),
            'ground_records': len(data.get('ground', pd.DataFrame())),
            'striking_records': len(data.get('striking', pd.DataFrame())),
            'fighters': len(data.get('fighters', pd.DataFrame())),
            'profiles': len(data.get('profiles', pd.DataFrame()))
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