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
        
        # HTML storage (UPSERT) - Keep existing HTMLs in GitHub
        self.fighter_html_folder = self.data_folder / "FighterHTMLs"
        
        # Temp folders (OVERWRITE)
        self.temp_folder = Path("temp")
        self.html_folder = Path("html_cache")
        
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
        
        self.logger.info(f"ESPN Data Processor initialized for folder: {self.data_folder}")
        self.logger.info(f"Fighter HTML folder: {self.fighter_html_folder}")
        
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
    
    def clean_temp_folders(self):
        """Clean and recreate temp folders (OVERWRITE policy)"""
        folders_to_clean = [self.temp_folder, self.html_folder]
        
        for folder in folders_to_clean:
            if folder.exists():
                shutil.rmtree(folder)
                self.logger.info(f"Cleaned temp folder: {folder}")
            folder.mkdir(exist_ok=True)
            self.logger.info(f"Created temp folder: {folder}")
    
    def get_existing_html_files(self):
        """Get list of existing HTML files in FighterHTMLs folder"""
        if not self.fighter_html_folder.exists():
            return set()
        
        html_files = set()
        for file_path in self.fighter_html_folder.glob("*.html"):
            html_files.add(file_path.stem)  # filename without extension
        return html_files
    
    def upsert_html_files(self, new_html_files):
        """
        UPSERT logic for HTML files: Preserve existing, add new ones
        
        Args:
            new_html_files: Dict of {fighter_name: html_content}
        """
        self.logger.info("Processing HTML files with UPSERT logic...")
        
        # Ensure FighterHTMLs folder exists
        self.fighter_html_folder.mkdir(exist_ok=True)
        
        # Get existing HTML files
        existing_files = self.get_existing_html_files()
        self.logger.info(f"Found {len(existing_files)} existing HTML files")
        
        # Process new HTML files
        for fighter_name, html_content in new_html_files.items():
            # Clean fighter name for filename
            safe_name = fighter_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
            html_file = self.fighter_html_folder / f"{safe_name}.html"
            
            # Only write if file doesn't exist or content is different
            should_write = False
            if not html_file.exists():
                should_write = True
                self.logger.info(f"New HTML file: {safe_name}")
            else:
                # Check if content is different
                try:
                    with open(html_file, 'r', encoding='utf-8') as f:
                        existing_content = f.read()
                    if existing_content != html_content:
                        should_write = True
                        self.logger.info(f"Updated HTML file: {safe_name}")
                except Exception as e:
                    self.logger.warning(f"Error reading existing HTML for {safe_name}: {e}")
                    should_write = True
            
            if should_write:
                try:
                    with open(html_file, 'w', encoding='utf-8') as f:
                        f.write(html_content)
                    self.logger.info(f"Saved HTML for {fighter_name}")
                except Exception as e:
                    self.logger.error(f"Error saving HTML for {fighter_name}: {e}")
        
        self.logger.info(f"HTML UPSERT completed. Total files: {len(self.get_existing_html_files())}")
    
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
    
    def scrape_fighter_htmls(self, fighter_names):
        """
        Scrape HTML pages for fighters (placeholder for actual scraping logic)
        
        Args:
            fighter_names: List of fighter names to scrape
            
        Returns:
            Dict of {fighter_name: html_content}
        """
        logging.info(f"Scraping HTML for {len(fighter_names)} fighters...")
        
        # TODO: Implement actual ESPN scraping logic here
        # For now, this is a placeholder that simulates scraping
        
        scraped_htmls = {}
        
        for fighter_name in fighter_names:
            # Simulate HTML content (replace with actual scraping)
            html_content = f"""
            <html>
            <head><title>{fighter_name} - ESPN MMA</title></head>
            <body>
                <h1>{fighter_name}</h1>
                <p>ESPN MMA fighter profile</p>
                <p>Scraped at: {datetime.now().isoformat()}</p>
            </body>
            </html>
            """
            scraped_htmls[fighter_name] = html_content
        
        logging.info(f"Scraped HTML for {len(scraped_htmls)} fighters")
        return scraped_htmls
    
    def process_fighter_htmls(self):
        """Process fighter HTML files with UPSERT logic"""
        logging.info("Processing fighter HTML files...")
        
        # Load fighters list (try test file first, then fallback to full list)
        test_fighters_file = self.data_folder / "fighter_names.csv"
        full_fighters_file = self.data_folder / "fighters_name.csv"
        
        if test_fighters_file.exists():
            fighters_file = test_fighters_file
            logging.info("Using TEST fighter list (fighter_names.csv)")
        elif full_fighters_file.exists():
            fighters_file = full_fighters_file
            logging.info("Using FULL fighter list (fighters_name.csv)")
        else:
            logging.warning("No fighters list found, skipping HTML processing")
            return
        
        fighters_df = pd.read_csv(fighters_file)
        fighter_names = fighters_df['Fighter Name'].tolist()
        
        logging.info(f"Processing HTML for {len(fighter_names)} fighters")
        
        # Scrape HTML files
        new_html_files = self.scrape_fighter_htmls(fighter_names)
        
        # Apply UPSERT logic to HTML files
        new_count, updated_count = self.upsert_html_files(new_html_files)
        
        logging.info(f"HTML processing complete: {new_count} new, {updated_count} updated")
        return new_count, updated_count
    
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
            # Get fighter names to scrape
            if len(self.fighters_name) == 0:
                self.logger.warning("No fighters list available, using sample data")
                fighter_names = ['Robert Whittaker', 'Israel Adesanya', 'Alex Pereira']
            else:
                fighter_names = self.fighters_name['fighters'].tolist()
            
            # Process fighter HTMLs (UPSERT policy)
            self.process_fighter_htmls()
            
            # Process main outputs (UPSERT policy)
            self.process_fight_data('clinch')
            self.process_fight_data('ground')
            self.process_fight_data('striking')
            self.process_fighter_profiles()
            
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
        """Get summary of current main output data"""
        data = self.load_existing_data()
        
        # Count HTML files
        html_count = len(self.get_existing_html_files())
        
        summary = {
            'clinch_records': len(data.get('clinch', pd.DataFrame())),
            'ground_records': len(data.get('ground', pd.DataFrame())),
            'striking_records': len(data.get('striking', pd.DataFrame())),
            'fighter_profiles': len(data.get('profiles', pd.DataFrame())),
            'fighter_html_files': html_count
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