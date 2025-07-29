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
        
        # HTML storage (UPSERT) - Keep existing HTMLs in GitHub
        self.fighter_html_folder = self.data_folder / "FighterHTMLs"
        
        # Temp folders (OVERWRITE)
        self.temp_folder = Path("temp")
        self.html_folder = Path("html_cache")
        
        logging.info(f"ESPN Data Processor initialized for folder: {self.data_folder}")
        logging.info(f"Fighter HTML folder: {self.fighter_html_folder}")
    
    def clean_temp_folders(self):
        """Clean and recreate temp folders (OVERWRITE policy)"""
        folders_to_clean = [self.temp_folder, self.html_folder]
        
        for folder in folders_to_clean:
            if folder.exists():
                shutil.rmtree(folder)
                logging.info(f"Cleaned temp folder: {folder}")
            folder.mkdir(exist_ok=True)
            logging.info(f"Created temp folder: {folder}")
    
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
        logging.info("Processing HTML files with UPSERT logic...")
        
        # Ensure FighterHTMLs folder exists
        self.fighter_html_folder.mkdir(exist_ok=True)
        
        # Get existing HTML files
        existing_files = self.get_existing_html_files()
        logging.info(f"Found {len(existing_files)} existing HTML files")
        
        # Process new HTML files
        new_count = 0
        updated_count = 0
        
        for fighter_name, html_content in new_html_files.items():
            # Clean fighter name for filename
            safe_name = fighter_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
            html_file = self.fighter_html_folder / f"{safe_name}.html"
            
            if html_file.exists():
                # File exists - check if content is different
                with open(html_file, 'r', encoding='utf-8') as f:
                    existing_content = f.read()
                
                if existing_content != html_content:
                    # Content is different - update it
                    with open(html_file, 'w', encoding='utf-8') as f:
                        f.write(html_content)
                    updated_count += 1
                    logging.debug(f"Updated HTML for: {fighter_name}")
                else:
                    logging.debug(f"HTML unchanged for: {fighter_name}")
            else:
                # New file - create it
                with open(html_file, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                new_count += 1
                logging.debug(f"Created new HTML for: {fighter_name}")
        
        logging.info(f"HTML UPSERT complete: {new_count} new files, {updated_count} updated files")
        return new_count, updated_count
    
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
            # Clean temp folders (OVERWRITE policy)
            self.clean_temp_folders()
            
            # Process fighter HTMLs (UPSERT policy)
            self.process_fighter_htmls()
            
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
        data = self._load_existing_data()
        
        # Count HTML files
        html_count = len(self.get_existing_html_files())
        
        summary = {
            'clinch_records': len(data.get('clinch', pd.DataFrame())),
            'ground_records': len(data.get('ground', pd.DataFrame())),
            'striking_records': len(data.get('striking', pd.DataFrame())),
            'fighter_profiles': len(data.get('profiles', pd.DataFrame())),
            'fighter_html_files': html_count
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