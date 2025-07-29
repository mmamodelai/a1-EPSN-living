#!/usr/bin/env python3
"""
ESPN Data Processor with Real Scraping Integration
Processes ESPN MMA data with UPSERT logic and real web scraping
"""

import pandas as pd
import logging
import os
from pathlib import Path
from datetime import datetime
import shutil
from typing import Dict, List, Optional

# Import the new ESPN scraper
from espn_scraper import ESPNFighterScraper, create_sample_fighter_data

class ESPNDataProcessor:
    """Processes ESPN MMA data with UPSERT logic and real scraping"""
    
    def __init__(self, data_folder: str = "data"):
        self.data_folder = Path(data_folder)
        self.data_folder.mkdir(exist_ok=True)
        
        # Initialize the ESPN scraper
        self.espn_scraper = ESPNFighterScraper(delay_range=(2, 4))
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('espn_processor.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize data containers
        self.clinch_data = pd.DataFrame()
        self.ground_data = pd.DataFrame()
        self.striking_data = pd.DataFrame()
        self.fighter_profiles = pd.DataFrame()
        self.fighters_name = pd.DataFrame()
        
        # Load existing data
        self._load_existing_data()
    
    def _load_existing_data(self):
        """Load existing CSV files from data folder"""
        try:
            # Load clinch data
            clinch_file = self.data_folder / "clinch_data_living.csv"
            if clinch_file.exists():
                self.clinch_data = pd.read_csv(clinch_file)
                self.logger.info(f"Loaded clinch data: {len(self.clinch_data)} records")
            else:
                self.logger.info("No existing clinch data found")
            
            # Load ground data
            ground_file = self.data_folder / "ground_data_living.csv"
            if ground_file.exists():
                self.ground_data = pd.read_csv(ground_file)
                self.logger.info(f"Loaded ground data: {len(self.ground_data)} records")
            else:
                self.logger.info("No existing ground data found")
            
            # Load striking data
            striking_file = self.data_folder / "striking_data_living.csv"
            if striking_file.exists():
                self.striking_data = pd.read_csv(striking_file)
                self.logger.info(f"Loaded striking data: {len(self.striking_data)} records")
            else:
                self.logger.info("No existing striking data found")
            
            # Load fighter profiles
            profiles_file = self.data_folder / "fighter_profiles.csv"
            if profiles_file.exists():
                self.fighter_profiles = pd.read_csv(profiles_file)
                self.logger.info(f"Loaded profiles data: {len(self.fighter_profiles)} records")
            else:
                self.logger.info("No existing fighter profiles found")
            
            # Load fighters list
            fighters_file = self.data_folder / "fighters_name.csv"
            if fighters_file.exists():
                self.fighters_name = pd.read_csv(fighters_file)
                self.logger.info(f"Loaded fighters data: {len(self.fighters_name)} records")
            else:
                self.logger.info("No existing fighters list found")
                
        except Exception as e:
            self.logger.error(f"Error loading existing data: {e}")
    
    def scrape_fighter_data(self, fighter_names: List[str]) -> Dict[str, pd.DataFrame]:
        """
        Scrape real fighter data from ESPN
        Returns dictionary with clinch, ground, and striking data
        """
        self.logger.info(f"Starting ESPN scraping for {len(fighter_names)} fighters")
        
        # Scrape fighter data using the ESPN scraper
        scraped_data = self.espn_scraper.scrape_fighters_batch(
            fighter_names,
            progress_callback=self._progress_callback
        )
        
        # Convert scraped data to DataFrames
        clinch_records = []
        ground_records = []
        striking_records = []
        profile_records = []
        
        for fighter_data in scraped_data:
            if not fighter_data or 'error' in fighter_data:
                self.logger.warning(f"Skipping fighter with error: {fighter_data.get('fighter_name', 'Unknown')}")
                continue
            
            fighter_name = fighter_data.get('fighter_name', '')
            espn_url = fighter_data.get('espn_url', '')
            scraped_at = fighter_data.get('scraped_at', '')
            
            # Create clinch record
            clinch_record = {
                'Player': fighter_name,
                'Date': scraped_at,
                'Opponent': 'N/A',  # Will be filled from fight history
                'Event': 'ESPN Scraped',
                'Result': 'N/A',
                'SCBL': fighter_data.get('SCBL', '-'),
                'SCBA': fighter_data.get('SCBA', '-'),
                'SCHL': fighter_data.get('SCHL', '-'),
                'SCHA': fighter_data.get('SCHA', '-'),
                'SCLL': fighter_data.get('SCLL', '-'),
                'SCLA': fighter_data.get('SCLA', '-'),
                'RV': fighter_data.get('RV', '-'),
                'SR': fighter_data.get('SR', '-'),
                'TDL': fighter_data.get('TDL', '-'),
                'TDA': fighter_data.get('TDA', '-'),
                'TDS': fighter_data.get('TDS', '-'),
                'TK ACC': fighter_data.get('TK_ACC', '-')
            }
            clinch_records.append(clinch_record)
            
            # Create ground record
            ground_record = {
                'Player': fighter_name,
                'Date': scraped_at,
                'Opponent': 'N/A',
                'Event': 'ESPN Scraped',
                'Result': 'N/A',
                'SGBL': fighter_data.get('SGBL', '-'),
                'SGBA': fighter_data.get('SGBA', '-'),
                'SGHL': fighter_data.get('SGHL', '-'),
                'SGHA': fighter_data.get('SGHA', '-'),
                'SGLL': fighter_data.get('SGLL', '-'),
                'SGLA': fighter_data.get('SGLA', '-'),
                'AD': fighter_data.get('AD', '-'),
                'ADHG': fighter_data.get('ADHG', '-'),
                'ADTB': fighter_data.get('ADTB', '-'),
                'ADTM': fighter_data.get('ADTM', '-'),
                'ADTS': fighter_data.get('ADTS', '-'),
                'SM': fighter_data.get('SM', '-')
            }
            ground_records.append(ground_record)
            
            # Create striking record
            striking_record = {
                'Player': fighter_name,
                'Date': scraped_at,
                'Opponent': 'N/A',
                'Event': 'ESPN Scraped',
                'Result': 'N/A',
                'SDBL/A': fighter_data.get('SDBL_A', '-'),
                'SDHL/A': fighter_data.get('SDHL_A', '-'),
                'SDLL/A': fighter_data.get('SDLL_A', '-'),
                'TSL': fighter_data.get('TSL', '-'),
                'TSA': fighter_data.get('TSA', '-'),
                'SSL': fighter_data.get('SSL', '-'),
                'SSA': fighter_data.get('SSA', '-'),
                'TSL-TSA': fighter_data.get('TSL_TSA', '-'),
                'KD': fighter_data.get('KD', '-'),
                '%BODY': fighter_data.get('BODY_PCT', '-'),
                '%HEAD': fighter_data.get('HEAD_PCT', '-'),
                '%LEG': fighter_data.get('LEG_PCT', '-')
            }
            striking_records.append(striking_record)
            
            # Create profile record
            profile_record = {
                'Fighter Name': fighter_name,
                'ESPN URL': espn_url,
                'Scraped At': scraped_at,
                'Total Fights': len(fighter_data.get('fight_history', [])),
                'Last Updated': datetime.now().isoformat()
            }
            profile_records.append(profile_record)
        
        # Convert to DataFrames
        new_clinch_df = pd.DataFrame(clinch_records)
        new_ground_df = pd.DataFrame(ground_records)
        new_striking_df = pd.DataFrame(striking_records)
        new_profiles_df = pd.DataFrame(profile_records)
        
        self.logger.info(f"Scraped {len(new_clinch_df)} clinch records")
        self.logger.info(f"Scraped {len(new_ground_df)} ground records")
        self.logger.info(f"Scraped {len(new_striking_df)} striking records")
        self.logger.info(f"Scraped {len(new_profiles_df)} profile records")
        
        return {
            'clinch': new_clinch_df,
            'ground': new_ground_df,
            'striking': new_striking_df,
            'profiles': new_profiles_df
        }
    
    def _progress_callback(self, current: int, total: int, fighter_name: str):
        """Progress callback for scraping"""
        self.logger.info(f"Progress: {current}/{total} - {fighter_name}")
    
    def process_fight_data(self, data_type: str = 'all'):
        """
        Process fight data with UPSERT logic
        data_type: 'clinch', 'ground', 'striking', or 'all'
        """
        try:
            # Get fighter names to scrape
            if len(self.fighters_name) == 0:
                self.logger.warning("No fighters list available, using sample data")
                fighter_names = ['Robert Whittaker', 'Israel Adesanya', 'Alex Pereira']
            else:
                fighter_names = self.fighters_name['fighters'].tolist()
            
            # Scrape new data
            scraped_data = self.scrape_fighter_data(fighter_names)
            
            # Process each data type
            if data_type in ['clinch', 'all']:
                self._upsert_data('clinch', scraped_data['clinch'])
            
            if data_type in ['ground', 'all']:
                self._upsert_data('ground', scraped_data['ground'])
            
            if data_type in ['striking', 'all']:
                self._upsert_data('striking', scraped_data['striking'])
            
            if data_type in ['profiles', 'all']:
                self._upsert_data('profiles', scraped_data['profiles'])
            
            self.logger.info(f"Completed processing {data_type} data")
            
        except Exception as e:
            self.logger.error(f"Error processing fight data: {e}")
    
    def _upsert_data(self, data_type: str, new_df: pd.DataFrame):
        """Apply UPSERT logic to merge new data with existing data"""
        try:
            if new_df.empty:
                self.logger.warning(f"No new {data_type} data to process")
                return
            
            # Get existing data
            existing_df = getattr(self, f"{data_type}_data", pd.DataFrame())
            
            if existing_df.empty:
                # No existing data, use new data as is
                setattr(self, f"{data_type}_data", new_df)
                self.logger.info(f"UPSERT: No existing {data_type} data, using {len(new_df)} new records")
            else:
                # Merge with existing data using composite key
                if data_type in ['clinch', 'ground', 'striking']:
                    # Use Player + Date + Opponent as composite key
                    composite_key = ['Player', 'Date', 'Opponent']
                else:
                    # Use Fighter Name as key for profiles
                    composite_key = ['Fighter Name']
                
                # Combine existing and new data
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                
                # Remove duplicates based on composite key
                combined_df = combined_df.drop_duplicates(subset=composite_key, keep='first')
                
                setattr(self, f"{data_type}_data", combined_df)
                self.logger.info(f"UPSERT: Merged {len(new_df)} new {data_type} records with {len(existing_df)} existing records")
            
        except Exception as e:
            self.logger.error(f"Error in UPSERT for {data_type}: {e}")
    
    def save_data(self):
        """Save all processed data to CSV files"""
        try:
            # Create backup
            self._create_backup()
            
            # Save clinch data
            if not self.clinch_data.empty:
                clinch_file = self.data_folder / "clinch_data_living.csv"
                self.clinch_data.to_csv(clinch_file, index=False)
                self.logger.info(f"Saved {len(self.clinch_data)} clinch records")
            
            # Save ground data
            if not self.ground_data.empty:
                ground_file = self.data_folder / "ground_data_living.csv"
                self.ground_data.to_csv(ground_file, index=False)
                self.logger.info(f"Saved {len(self.ground_data)} ground records")
            
            # Save striking data
            if not self.striking_data.empty:
                striking_file = self.data_folder / "striking_data_living.csv"
                self.striking_data.to_csv(striking_file, index=False)
                self.logger.info(f"Saved {len(self.striking_data)} striking records")
            
            # Save fighter profiles
            if not self.fighter_profiles.empty:
                profiles_file = self.data_folder / "fighter_profiles.csv"
                self.fighter_profiles.to_csv(profiles_file, index=False)
                self.logger.info(f"Saved {len(self.fighter_profiles)} profile records")
            
            # Save fighters list
            if not self.fighters_name.empty:
                fighters_file = self.data_folder / "fighters_name.csv"
                self.fighters_name.to_csv(fighters_file, index=False)
                self.logger.info(f"Saved {len(self.fighters_name)} fighter names")
            
        except Exception as e:
            self.logger.error(f"Error saving data: {e}")
    
    def _create_backup(self):
        """Create backup of existing data files"""
        try:
            backup_dir = self.data_folder / "backups"
            backup_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_folder = backup_dir / f"backup_{timestamp}"
            backup_folder.mkdir(exist_ok=True)
            
            # Backup existing files
            files_to_backup = [
                "clinch_data_living.csv",
                "ground_data_living.csv", 
                "striking_data_living.csv",
                "fighters_name.csv",
                "fighter_profiles.csv"
            ]
            
            for filename in files_to_backup:
                source_file = self.data_folder / filename
                if source_file.exists():
                    backup_file = backup_folder / filename
                    shutil.copy2(source_file, backup_file)
                    self.logger.info(f"Backed up: {filename}")
            
            self.logger.info(f"Backup created: {backup_folder}")
            
        except Exception as e:
            self.logger.error(f"Error creating backup: {e}")
    
    def clean_temp_folders(self):
        """Clean and recreate temp folders"""
        try:
            temp_folders = ['temp', 'html_cache']
            
            for folder_name in temp_folders:
                folder_path = Path(folder_name)
                
                if folder_path.exists():
                    shutil.rmtree(folder_path)
                    self.logger.info(f"Cleaned temp folder: {folder_name}")
                
                folder_path.mkdir(exist_ok=True)
                self.logger.info(f"Created temp folder: {folder_name}")
            
        except Exception as e:
            self.logger.error(f"Error cleaning temp folders: {e}")
    
    def get_data_summary(self):
        """Get summary of all data"""
        summary = {
            'clinch_records': len(self.clinch_data),
            'ground_records': len(self.ground_data),
            'striking_records': len(self.striking_data),
            'fighters': len(self.fighters_name),
            'profiles': len(self.fighter_profiles)
        }
        
        self.logger.info("Data Summary:")
        for key, value in summary.items():
            self.logger.info(f"  {key}: {value}")
        
        return summary
    
    def run_full_processing(self):
        """Run complete data processing pipeline"""
        try:
            self.logger.info("Starting full ESPN data processing pipeline")
            
            # Process all data types
            self.process_fight_data('all')
            
            # Save processed data
            self.save_data()
            
            # Clean temp folders
            self.clean_temp_folders()
            
            # Get summary
            summary = self.get_data_summary()
            
            self.logger.info("Full processing pipeline completed successfully")
            return summary
            
        except Exception as e:
            self.logger.error(f"Error in full processing pipeline: {e}")
            return None


def main():
    """Main function to run the ESPN data processor"""
    processor = ESPNDataProcessor()
    processor.run_full_processing()


if __name__ == "__main__":
    main() 