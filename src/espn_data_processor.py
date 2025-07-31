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
from bs4 import BeautifulSoup

# Import the new ESPN scraper
from src.espn_scraper import ESPNFighterScraper, create_sample_fighter_data

class ESPNDataProcessor:
    """Processes ESPN MMA data with UPSERT logic and real scraping"""
    
    def __init__(self, data_folder: str = "data"):
        self.data_folder = Path(data_folder)
        self.data_folder.mkdir(exist_ok=True)
        
        # HTML storage (UPSERT) - Keep existing HTMLs in GitHub
        self.fighter_html_folder = self.data_folder / "FighterHTMLs"
        
        # Initialize ESPN scraper
        self.espn_scraper = ESPNFighterScraper(
            html_profile_dir=self.fighter_html_folder,
            max_workers=3,
            rate_limit=2.0,
            max_retries=5
        )
        
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
        
        # File paths
        self.profiles_file = self.data_folder / "fighter_profiles.csv"
        
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
        self.logger.info(f"Processing {data_type} data...")
        
        # Load existing data (already loaded in __init__)
        # Get existing data from instance variables
        if data_type == 'clinch':
            existing_df = self.clinch_data
            output_file = self.data_folder / "clinch_data_living.csv"
        elif data_type == 'ground':
            existing_df = self.ground_data
            output_file = self.data_folder / "ground_data_living.csv"
        elif data_type == 'striking':
            existing_df = self.striking_data
            output_file = self.data_folder / "striking_data_living.csv"
        else:
            self.logger.error(f"Unknown data type: {data_type}")
            return pd.DataFrame()
        
        # TODO: Add ESPN scraping logic here
        # For now, we'll just preserve existing data
        new_df = pd.DataFrame()  # Placeholder for scraped data
        
        # Apply UPSERT logic
        updated_df = self._upsert_data(data_type, new_df)
        
        # Save back to data folder
        updated_df.to_csv(output_file, index=False)
        self.logger.info(f"Saved {data_type} data: {len(updated_df)} records to {output_file}")
        
        return updated_df
    
    def scrape_fighter_htmls(self, fighter_names):
        """
        Scrape HTML pages for fighters using improved ESPN scraper
        
        Args:
            fighter_names: List of fighter names to scrape
            
        Returns:
            Dict of {fighter_name: html_content}
        """
        logging.info(f"Scraping HTML for {len(fighter_names)} fighters...")
        
        from espn_scraper import ESPNFighterScraper
        
        scraper = ESPNFighterScraper()
        scraped_htmls = {}
        
        for i, fighter_name in enumerate(fighter_names, 1):
            try:
                logging.info(f"Scraping {fighter_name} ({i}/{len(fighter_names)})")
                
                # Use the improved scraper to get fighter stats (which saves HTML)
                fighter_data = scraper.get_fighter_stats(fighter_name)
                
                if fighter_data and 'html_file' in fighter_data:
                    # Read the saved HTML file
                    html_file_path = Path(fighter_data['html_file'])
                    if html_file_path.exists():
                        with open(html_file_path, 'r', encoding='utf-8') as f:
                            html_content = f.read()
                        scraped_htmls[fighter_name] = html_content
                        logging.info(f"Successfully scraped HTML for {fighter_name}")
                    else:
                        logging.warning(f"HTML file not found for {fighter_name}")
                else:
                    logging.warning(f"Could not scrape {fighter_name}")
                    
            except Exception as e:
                logging.error(f"Error scraping {fighter_name}: {e}")
                continue
        
        logging.info(f"Scraped HTML for {len(scraped_htmls)} fighters")
        return scraped_htmls
    
    def process_fighter_htmls(self):
        """Process fighter HTML files with UPSERT logic"""
        logging.info("Processing fighter HTML files...")
        
        # Load fighters list (always use full list)
        full_fighters_file = self.data_folder / "fighters_name.csv"
        
        if full_fighters_file.exists():
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
        existing_df = self.fighter_profiles  # Use the already loaded data
        
        # Extract profile data from stored HTML files
        new_df = self._extract_profiles_from_html()
        
        # Apply UPSERT logic (use fighter name as key)
        updated_df = self._upsert_data('profiles', new_df)
        
        # Save back to data folder with proper format
        updated_df.to_csv(self.profiles_file, index=False)
        logging.info(f"Saved fighter profiles: {len(updated_df)} records to {self.profiles_file}")
        
        # Also save a backup with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = self.data_folder / f"fighter_profiles_backup_{timestamp}.csv"
        updated_df.to_csv(backup_file, index=False)
        logging.info(f"Backup saved to: {backup_file}")
        
        return updated_df
    
    def _extract_profiles_from_html(self):
        """Extract fighter profile data from stored HTML files"""
        import json
        import re
        
        profiles = []
        
        # Get all HTML files
        html_files = list(self.fighter_html_folder.glob("*.html"))
        logging.info(f"Found {len(html_files)} HTML files to process")
        
        for html_file in html_files:
            try:
                # Extract fighter name from filename
                fighter_name = html_file.stem.replace('_', ' ')
                
                # Read HTML content
                with open(html_file, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                
                # Skip small placeholder files
                if len(html_content) < 10000:
                    continue
                
                # Look for the ESPN embedded JSON object
                match = re.search(r'"prtlCmnApiRsp"\s*:\s*({.*?})\s*,\s*"pageType"', html_content, re.DOTALL)
                if not match:
                    logging.debug(f"No ESPN JSON found in {fighter_name}")
                    continue
                json_str = match.group(1)
                try:
                    data = json.loads(json_str)
                except Exception as e:
                    logging.debug(f"JSON parse error for {fighter_name}: {e}")
                    continue
                # Navigate to athlete data (ESPN structure uses plyrHdr)
                if 'plyrHdr' not in data:
                    logging.debug(f"No plyrHdr found in {fighter_name}")
                    continue
                athlete_data = data['plyrHdr']
                logging.info(f"Successfully extracted data for {fighter_name}")

                # Extract profile information with proper column format
                profile = {
                    'Name': fighter_name,
                    'Division Tr': athlete_data.get('ath', {}).get('wghtclss', '') if 'ath' in athlete_data else '',
                    'Division Rk': '',
                    'Wins by Kn': 0,
                    'Wins by Su': 0,
                    'First Roun': 0,
                    'Wins by De': 0,
                    'Striking ac': '',
                    'Sig. Strike': 0,
                    'Takedownr': 0,
                    'Sig. Str. La': 0,
                    'Sig. Str. At': 0,
                    'Submissio': 0,
                    'Sig. Str. De': '',
                    'Knockdow': 0,
                    'Average Sig': '',
                    'Event_1_H': '',
                    'Event_1_D': '',
                    'Event_1_R': '',
                    'Event_1_Ti': '',
                    'Event_1_M': '',
                    'Event_2_H': '',
                    'Event_2_D': '',
                    'Event_2_R': '',
                    'Event_2_Ti': '',
                    'Event_2_M': '',
                    'Event_3_H': '',
                    'Event_3_D': '',
                    'Event_3_R': '',
                    'Event_3_Ti': '',
                    'Event_3_M': '',
                    'Status': 'Active',
                    'Place_ct': '',
                    'Fighting_s': '',
                    'Age': '',
                    'Height': '',
                    'Weight': '',
                    'Octagon_Reach': '',
                    'Leg_reach': '',
                    'Reach': '',
                    'Trains_at': '',
                    'Fight': '',
                    'Win': '',
                    'Title': '',
                    'Defeat': '',
                    'Former Champion': ''
                }
                
                # Extract record from statsBlck.vals (correct ESPN structure)
                if 'statsBlck' in athlete_data and 'vals' in athlete_data['statsBlck']:
                    stats = athlete_data['statsBlck']['vals']
                    for stat in stats:
                        if stat.get('name') == 'Wins-Losses-Draws':
                            record = stat.get('val', '0-0-0')
                            profile['Division Rk'] = record
                            
                            # Parse W-L-D
                            parts = record.split('-')
                            if len(parts) >= 3:
                                wins = int(parts[0]) if parts[0].isdigit() else 0
                                losses = int(parts[1]) if parts[1].isdigit() else 0
                                draws = int(parts[2]) if parts[2].isdigit() else 0
                                profile['Wins by De'] = wins
                        elif stat.get('name') == 'Technical Knockout-Technical Knockout Losses':
                            tko_record = stat.get('val', '0-0')
                            parts = tko_record.split('-')
                            if len(parts) >= 2:
                                profile['Wins by Kn'] = int(parts[0]) if parts[0].isdigit() else 0
                        elif stat.get('name') == 'Submissions-Submission Losses':
                            sub_record = stat.get('val', '0-0')
                            parts = sub_record.split('-')
                            if len(parts) >= 2:
                                profile['Wins by Su'] = int(parts[0]) if parts[0].isdigit() else 0
                
                # Extract personal info from ath (correct ESPN structure)
                if 'ath' in athlete_data:
                    ath_data = athlete_data['ath']
                    if 'htwt' in ath_data:
                        profile['Height'] = ath_data['htwt']
                        profile['Weight'] = ath_data['htwt']
                    if 'dob' in ath_data:
                        # Extract age from dob like "12/20/1990 (34)"
                        dob_str = ath_data['dob']
                        if '(' in dob_str and ')' in dob_str:
                            age_str = dob_str.split('(')[1].split(')')[0]
                            profile['Age'] = float(age_str) if age_str.isdigit() else ''
                    if 'rch' in ath_data:
                        profile['Reach'] = ath_data['rch']
                    if 'stnc' in ath_data:
                        profile['Fighting_s'] = ath_data['stnc']
                    if 'cntry' in ath_data:
                        profile['Place_ct'] = ath_data['cntry']
                    if 'tm' in ath_data:
                        profile['Trains_at'] = ath_data['tm']
                

                
                # Extract personal info from plyrHdr.ath (correct ESPN structure)
                if 'plyrHdr' in athlete_data and 'ath' in athlete_data['plyrHdr']:
                    ath_data = athlete_data['plyrHdr']['ath']
                    if 'htwt' in ath_data:
                        profile['Height'] = ath_data['htwt']
                    if 'htwt' in ath_data:
                        profile['Weight'] = ath_data['htwt']
                    if 'dob' in ath_data:
                        # Extract age from dob like "12/20/1990 (34)"
                        dob_str = ath_data['dob']
                        if '(' in dob_str and ')' in dob_str:
                            age_str = dob_str.split('(')[1].split(')')[0]
                            profile['Age'] = float(age_str) if age_str.isdigit() else ''
                    if 'rch' in ath_data:
                        profile['Reach'] = ath_data['rch']
                    if 'stnc' in ath_data:
                        profile['Fighting_s'] = ath_data['stnc']
                    if 'cntry' in ath_data:
                        profile['Place_ct'] = ath_data['cntry']
                    if 'tm' in ath_data:
                        profile['Trains_at'] = ath_data['tm']
                    if 'wghtclss' in ath_data:
                        profile['Division Tr'] = ath_data['wghtclss']
                
                profiles.append(profile)
                
            except Exception as e:
                self.logger.warning(f"Error processing {html_file.name}: {e}")
                continue
        
        result_df = pd.DataFrame(profiles)
        logging.info(f"Extracted {len(profiles)} profiles from HTML files")
        if len(profiles) > 0:
            logging.info(f"Sample profile: {profiles[0]}")
        return result_df
    
    def _parse_fighter_profile(self, soup: BeautifulSoup, fighter_name: str) -> dict:
        """Parse fighter profile data from HTML soup"""
        try:
            profile = {
                'Name': fighter_name,
                'Division Title': '',
                'Division Record': '',
                'Wins by Knockout': 0,
                'Wins by Submission': 0,
                'First Round Finishes': 0,
                'Wins by Decision': 0,
                'Striking accuracy': '',
                'Sig. Strikes Landed': 0,
                'Sig. Strikes Attempted': 0,
                'Takedown Accuracy': '',
                'Takedowns Landed': 0,
                'Takedowns Attempted': 0,
                'Sig. Str. Landed Per Min': 0,
                'Sig. Str. Absorbed Per Min': 0,
                'Takedown avg Per 15 Min': 0,
                'Submission avg Per 15 Min': 0,
                'Sig. Str. Defense': '',
                'Takedown Defense': '',
                'Knockdown Avg': 0,
                'Average fight time': '',
                'Sig. Str. By Position - Standing': '',
                'Sig. Str. By Position - Clinch': '',
                'Sig. Str. By Position - Ground': '',
                'Win by Method - KO/TKO': '',
                'Win by Method - DEC': '',
                'Win by Method - SUB': '',
                'Sig. Str. by target - Head Strike Percentage': '',
                'Sig. Str. by target - Head Strike Count': 0,
                'Sig. Str. by target - Body Strike Percentage': '',
                'Sig. Str. by target - Body Strike Count': 0,
                'Sig. Str. by target - Leg Strike Percentage': '',
                'Sig. Str. by target - Leg Strike Count': 0,
                'Event_1_Headline': '',
                'Event_1_Date': '',
                'Event_1_Round': '',
                'Event_1_Time': '',
                'Event_1_Method': '',
                'Event_2_Headline': '',
                'Event_2_Date': '',
                'Event_2_Round': '',
                'Event_2_Time': '',
                'Event_2_Method': '',
                'Event_3_Headline': '',
                'Event_3_Date': '',
                'Event_3_Round': '',
                'Event_3_Time': '',
                'Event_3_Method': '',
                'Status': 'Active',
                'Place_of_Birth': '',
                'Fighting_style': '',
                'Age': 0,
                'Height': 0,
                'Weight': 0,
                'Octagon_Debut': '',
                'Reach': 0,
                'Leg_reach': 0,
                'Trains_at': '',
                'Fight Win Streak': '',
                'Title Defenses': '',
                'Former Champion': ''
            }
            
            # Look for record information
            record_patterns = [
                r'Record:\s*(\d+)-(\d+)-(\d+)',  # "Record: X-Y-Z"
                r'(\d+)-(\d+)-(\d+)\s*\(W-L-D\)',  # "X-Y-Z (W-L-D)"
                r'(\d+)-(\d+)-(\d+)\s*record',  # "X-Y-Z record"
                r'(\d+)\s*wins.*?(\d+)\s*losses.*?(\d+)\s*draws',  # "X wins, Y losses, Z draws"
                r'(\d+)\s*wins.*?(\d+)\s*losses',  # "X wins, Y losses"
            ]
            
            page_text = soup.get_text()
            
            # Try to find record
            for pattern in record_patterns:
                import re
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    if len(match.groups()) == 3:
                        wins, losses, draws = match.groups()
                        # Validate that these look like reasonable fight record numbers
                        if (0 <= int(wins) <= 50 and 0 <= int(losses) <= 50 and 0 <= int(draws) <= 10):
                            profile['Division Record'] = f"{wins}-{losses}-{draws} (W-L-D)"
                            break
                    elif len(match.groups()) == 2:
                        wins, losses = match.groups()
                        # Validate that these look like reasonable fight record numbers
                        if (0 <= int(wins) <= 50 and 0 <= int(losses) <= 50):
                            profile['Division Record'] = f"{wins}-{losses}-0 (W-L-D)"
                            break
            
            # Look for division information
            division_patterns = [
                r'(\w+weight)\s*Division',
                r'Division:\s*(\w+weight)',
                r'(\w+weight)\s*class'
            ]
            
            for pattern in division_patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    profile['Division Title'] = f"{match.group(1).title()} Division"
                    break
            
            # Look for fight statistics
            stats_patterns = {
                'Wins by Knockout': r'(\d+)\s*KO|(\d+)\s*knockout',
                'Wins by Submission': r'(\d+)\s*submission|(\d+)\s*SUB',
                'Wins by Decision': r'(\d+)\s*decision|(\d+)\s*DEC',
                'Age': r'Age:\s*(\d+)',
                'Height': r'Height:\s*(\d+)',
                'Weight': r'Weight:\s*(\d+)',
                'Reach': r'Reach:\s*(\d+)',
            }
            
            for field, pattern in stats_patterns.items():
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    try:
                        value = int(match.group(1) or match.group(2))
                        profile[field] = value
                    except (ValueError, IndexError):
                        continue
            
            # Look for recent fight history
            fight_history = self._extract_recent_fights(soup)
            if fight_history:
                for i, fight in enumerate(fight_history[:3], 1):
                    profile[f'Event_{i}_Headline'] = fight.get('headline', '')
                    profile[f'Event_{i}_Date'] = fight.get('date', '')
                    profile[f'Event_{i}_Round'] = fight.get('round', '')
                    profile[f'Event_{i}_Time'] = fight.get('time', '')
                    profile[f'Event_{i}_Method'] = fight.get('method', '')
            
            return profile
            
        except Exception as e:
            logging.error(f"Error parsing profile for {fighter_name}: {e}")
            return None
    
    def _extract_recent_fights(self, soup: BeautifulSoup) -> list:
        """Extract recent fight history from HTML"""
        fights = []
        try:
            # Look for fight history tables or sections
            fight_sections = soup.find_all(['table', 'div'], class_=lambda x: x and 'fight' in x.lower())
            
            for section in fight_sections:
                rows = section.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 4:
                        fight = {
                            'headline': cells[0].get_text().strip() if len(cells) > 0 else '',
                            'date': cells[1].get_text().strip() if len(cells) > 1 else '',
                            'round': cells[2].get_text().strip() if len(cells) > 2 else '',
                            'time': cells[3].get_text().strip() if len(cells) > 3 else '',
                            'method': cells[4].get_text().strip() if len(cells) > 4 else ''
                        }
                        fights.append(fight)
            
            return fights[:3]  # Return only 3 most recent fights
            
        except Exception as e:
            logging.error(f"Error extracting fight history: {e}")
            return []
    
    def run_full_processing(self):
        """Run complete data processing pipeline"""
        try:
            self.logger.info("Starting full ESPN data processing pipeline")
            
            # Get fighter names to scrape
            fighter_names = self.fighters_name['fighters'].tolist()
            self.logger.info(f"Scraping data for {len(fighter_names)} fighters")
            
            # Scrape fighter HTML files first
            self.logger.info("Scraping fighter HTML files...")
            new_html_files = self.scrape_fighter_htmls(fighter_names)
            self.upsert_html_files(new_html_files)
            
            # Scrape fighter data from ESPN
            scraped_data = self.scrape_fighter_data(fighter_names)
            
            # Apply UPSERT logic for each data type
            if 'clinch' in scraped_data:
                self._upsert_data('clinch', scraped_data['clinch'])
            
            if 'ground' in scraped_data:
                self._upsert_data('ground', scraped_data['ground'])
            
            if 'striking' in scraped_data:
                self._upsert_data('striking', scraped_data['striking'])
            
            if 'profiles' in scraped_data:
                self._upsert_data('profiles', scraped_data['profiles'])
            
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
    
    def _upsert_data(self, data_type: str, new_df: pd.DataFrame):
        """Apply UPSERT logic to merge new data with existing data"""
        try:
            # Get existing data
            existing_df = getattr(self, f"{data_type}_data", pd.DataFrame())
            
            if new_df.empty:
                self.logger.warning(f"No new {data_type} data to process")
                return existing_df  # Return existing data if no new data
            
            if existing_df.empty:
                # No existing data, use new data as is
                setattr(self, f"{data_type}_data", new_df)
                self.logger.info(f"UPSERT: No existing {data_type} data, using {len(new_df)} new records")
                return new_df
            else:
                # Merge with existing data using composite key
                if data_type in ['clinch', 'ground', 'striking']:
                    # Use Player + Date + Opponent as composite key
                    composite_key = ['Player', 'Date', 'Opponent']
                else:
                    # Use Name as key for profiles
                    composite_key = ['Name']
                
                # Combine existing and new data
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                
                # Remove duplicates based on composite key
                combined_df = combined_df.drop_duplicates(subset=composite_key, keep='first')
                
                setattr(self, f"{data_type}_data", combined_df)
                self.logger.info(f"UPSERT: Merged {len(new_df)} new {data_type} records with {len(existing_df)} existing records")
                return combined_df
            
        except Exception as e:
            self.logger.error(f"Error in UPSERT for {data_type}: {e}")
            return existing_df  # Return existing data on error
    
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


def main():
    """Main function to run the ESPN data processor"""
    processor = ESPNDataProcessor()
    processor.run_full_processing()


if __name__ == "__main__":
    main() 