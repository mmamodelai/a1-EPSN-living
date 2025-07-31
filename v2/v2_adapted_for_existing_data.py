#!/usr/bin/env python3

import requests
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import os
from bs4 import BeautifulSoup
import time
import logging
from typing import Dict, List, Optional
import json
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import random
from datetime import datetime
import re

class ESPNEnhancedScraper:
    """
    Enhanced ESPN scraper that uses existing HTML files and outputs to your exact format.
    Combines V2's robust parsing with your existing folder structure and UPSERT logic.
    """
    
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    ]

    def __init__(self, data_folder: str = '../data', max_workers: int = 3, 
                 rate_limit: float = 2.0, max_retries: int = 5):
        """
        Initialize enhanced scraper with your existing folder structure.
        """
        self.data_folder = Path(data_folder)
        self.data_folder.mkdir(parents=True, exist_ok=True)
        
        # V2 infrastructure
        self.max_workers = max_workers
        self.rate_limit = rate_limit
        self.max_retries = max_retries
        self.session = self._create_session()
        
        # Statistics tracking
        self.success_count = 0
        self.failure_count = 0
        self.last_request_time = 0
        self.requests_this_minute = 0
        self.minute_start = datetime.now()
        
        # Your existing file paths
        self.fighter_html_folder = self.data_folder / "FighterHTMLs"
        self.profiles_file = self.data_folder / "fighter_profiles.csv"
        self.clinch_file = self.data_folder / "clinch_data_living.csv"
        self.striking_file = self.data_folder / "striking_data_living.csv"
        self.ground_file = self.data_folder / "ground_data_living.csv"
        
        # Load existing data for UPSERT
        self._load_existing_data()
        
        logging.info(f"ESPN Enhanced Scraper initialized for folder: {self.data_folder}")
        logging.info(f"Fighter HTML folder: {self.fighter_html_folder}")

    def _load_existing_data(self):
        """Load existing data files for UPSERT operations."""
        try:
            if self.clinch_file.exists():
                self.clinch_data = pd.read_csv(self.clinch_file)
                logging.info(f"Loaded clinch data: {len(self.clinch_data)} records")
            else:
                self.clinch_data = pd.DataFrame()
                
            if self.ground_file.exists():
                self.ground_data = pd.read_csv(self.ground_file)
                logging.info(f"Loaded ground data: {len(self.ground_data)} records")
            else:
                self.ground_data = pd.DataFrame()
                
            if self.striking_file.exists():
                self.striking_data = pd.read_csv(self.striking_file)
                logging.info(f"Loaded striking data: {len(self.striking_data)} records")
            else:
                self.striking_data = pd.DataFrame()
                
            if self.profiles_file.exists():
                self.profiles_data = pd.read_csv(self.profiles_file)
                logging.info(f"Loaded profiles data: {len(self.profiles_data)} records")
            else:
                self.profiles_data = pd.DataFrame()
                
        except Exception as e:
            logging.error(f"Error loading existing data: {e}")
            self.clinch_data = pd.DataFrame()
            self.ground_data = pd.DataFrame()
            self.striking_data = pd.DataFrame()
            self.profiles_data = pd.DataFrame()

    def _create_session(self) -> requests.Session:
        """Create robust session with retry logic (from V2)."""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        session.headers.update({
            'User-Agent': random.choice(self.USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        return session

    def _upsert_data(self, new_df: pd.DataFrame, existing_file: Path, composite_key: List[str]) -> pd.DataFrame:
        """
        UPSERT logic from your current system - preserves existing data.
        """
        if new_df.empty:
            logging.warning("No new data to process")
            return pd.DataFrame()
        
        if existing_file.exists():
            try:
                existing_df = pd.read_csv(existing_file)
                logging.info(f"Loaded existing data: {len(existing_df)} records from {existing_file}")
                
                # Create composite key for matching
                if composite_key == ['Name']:
                    # For fighter profiles
                    merged_df = pd.concat([existing_df, new_df]).drop_duplicates(subset=composite_key, keep='last')
                else:
                    # For fight data (multiple columns)
                    merged_df = pd.concat([existing_df, new_df]).drop_duplicates(subset=composite_key, keep='last')
                
                logging.info(f"Merged data: {len(merged_df)} records (was {len(existing_df)}, added {len(new_df)})")
                return merged_df
                
            except Exception as e:
                logging.error(f"Error loading existing data from {existing_file}: {e}")
                return new_df
        else:
            logging.info(f"No existing file found, creating new: {existing_file}")
            return new_df

    def extract_fighter_profiles(self, html_files: List[Path]) -> pd.DataFrame:
        """
        Extract fighter profiles with correct format using enhanced ESPN JSON parsing.
        """
        profiles = []
        
        for html_file in tqdm(html_files, desc="Processing fighter profiles"):
            try:
                fighter_name = html_file.stem.replace('_', ' ')
                
                with open(html_file, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                
                # Enhanced ESPN JSON extraction (from V2's approach)
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
                
                # Navigate to athlete data
                if 'plyrHdr' not in data:
                    logging.debug(f"No plyrHdr found in {fighter_name}")
                    continue
                
                athlete_data = data['plyrHdr']
                
                # Create profile with your exact format
                profile = {
                    'Name': fighter_name,
                    'Division Tr': '',
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
                    'Status': '',
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
                
                # Extract data from ESPN structure
                self._extract_profile_data(athlete_data, profile)
                profiles.append(profile)
                
            except Exception as e:
                logging.error(f"Error extracting profile from {html_file}: {e}")
                continue
        
        result_df = pd.DataFrame(profiles)
        logging.info(f"Extracted {len(profiles)} profiles from HTML files")
        return result_df

    def _extract_profile_data(self, athlete_data: Dict, profile: Dict):
        """
        Extract profile data from ESPN's athlete data structure.
        """
        # Extract personal info from ath section
        if 'ath' in athlete_data:
            ath_data = athlete_data['ath']
            
            if 'htwt' in ath_data:
                profile['Height'] = ath_data['htwt']
            if 'dob' in ath_data:
                profile['Age'] = ath_data['dob']
            if 'rch' in ath_data:
                profile['Reach'] = ath_data['rch']
            if 'stnc' in ath_data:
                profile['Fighting_s'] = ath_data['stnc']
            if 'tm' in ath_data:
                profile['Trains_at'] = ath_data['tm']
            if 'cntry' in ath_data:
                profile['Place_ct'] = ath_data['cntry']
            if 'wghtclss' in ath_data:
                profile['Division Tr'] = ath_data['wghtclss']
        
        # Extract stats from statsBlck section
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
                        
                        # Calculate win types (approximate)
                        profile['Wins by Kn'] = wins // 3  # Rough estimate
                        profile['Wins by Su'] = wins // 4  # Rough estimate
                        profile['Wins by De'] = wins - (profile['Wins by Kn'] + profile['Wins by Su'])
                        
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

    def extract_fight_statistics(self, html_files: List[Path]) -> Dict[str, pd.DataFrame]:
        """
        Extract fight-by-fight statistics using V2's BeautifulSoup approach.
        """
        section_data = {
            'striking': [],
            'Clinch': [],
            'Ground': []
        }

        columns = {
            'striking': ['Date', 'Opponent', 'Event', 'Result', 'SDBL/A', 'SDHL/A', 'SDLL/A',
                        'TSL', 'TSA', 'SSL', 'SSA', 'TSL-TSA', 'KD', '%BODY', '%HEAD', '%LEG'],
            'Clinch': ['Date', 'Opponent', 'Event', 'Result', 'SCBL', 'SCBA', 'SCHL', 'SCHA',
                      'SCLL', 'SCLA', 'RV', 'SR', 'TDL', 'TDA', 'TDS', 'TK ACC'],
            'Ground': ['Date', 'Opponent', 'Event', 'Result', 'SGBL', 'SGBA', 'SGHL', 'SGHA',
                      'SGLL', 'SGLA', 'AD', 'ADHG', 'ADTB', 'ADTM', 'ADTS', 'SM']
        }

        for file_path in tqdm(html_files, desc="Processing fight statistics"):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    soup = BeautifulSoup(f.read(), 'lxml')

                player_name_elem = soup.find('h1', class_='PlayerHeader__Name')
                player_name = player_name_elem.get_text(strip=True) if player_name_elem else 'Unknown Player'

                for section in ['striking', 'Clinch', 'Ground']:
                    title_div = soup.find('div', string=section)
                    if title_div:
                        table = title_div.find_next('table')
                        if table:
                            rows = table.find_all('tr')[1:]  # Skip header
                            for row in rows:
                                cols = [td.get_text(strip=True) for td in row.find_all('td')]
                                if len(cols) >= len(columns[section]):
                                    data = {'Player': player_name}
                                    data.update(dict(zip(columns[section], cols)))
                                    section_data[section].append(data)

            except Exception as e:
                logging.error(f"Error processing file {file_path}: {str(e)}")

        # Convert to DataFrames
        result_dfs = {}
        for section, data in section_data.items():
            if data:
                result_dfs[section] = pd.DataFrame(data)
                logging.info(f"Extracted {len(data)} {section} records")
            else:
                logging.warning(f"No data found for section: {section}")
                result_dfs[section] = pd.DataFrame(columns=['Player'] + columns[section])
                
        return result_dfs

    def process_fighter_profiles(self):
        """
        Process fighter profiles with UPSERT logic.
        """
        logging.info("Processing fighter profiles...")
        
        # Get all HTML files from your existing folder
        html_files = list(self.fighter_html_folder.glob("*.html"))
        logging.info(f"Found {len(html_files)} HTML files to process")
        
        # Extract profiles
        new_profiles_df = self.extract_fighter_profiles(html_files)
        
        # Apply UPSERT logic
        updated_profiles_df = self._upsert_data(new_profiles_df, self.profiles_file, ['Name'])
        
        if not updated_profiles_df.empty:
            # Save with backup
            updated_profiles_df.to_csv(self.profiles_file, index=False)
            logging.info(f"Saved fighter profiles: {len(updated_profiles_df)} records to {self.profiles_file}")
            
            # Create backup
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = self.data_folder / f"fighter_profiles_backup_{timestamp}.csv"
            updated_profiles_df.to_csv(backup_file, index=False)
            logging.info(f"Backup saved to: {backup_file}")
        else:
            logging.warning("No new profiles data to process")

    def process_living_data(self):
        """
        Process living data files with UPSERT logic.
        """
        logging.info("Processing living data...")
        
        # Get all HTML files from your existing folder
        html_files = list(self.fighter_html_folder.glob("*.html"))
        
        # Extract fight-by-fight statistics
        section_dfs = self.extract_fight_statistics(html_files)
        
        # Apply UPSERT logic to each section
        for section, new_df in section_dfs.items():
            if section == 'striking':
                file_path = self.striking_file
                key_columns = ['Player', 'Date', 'Opponent', 'Event']
            elif section == 'Clinch':
                file_path = self.clinch_file
                key_columns = ['Player', 'Date', 'Opponent', 'Event']
            elif section == 'Ground':
                file_path = self.ground_file
                key_columns = ['Player', 'Date', 'Opponent', 'Event']
            else:
                continue
            
            updated_df = self._upsert_data(new_df, file_path, key_columns)
            
            if not updated_df.empty:
                updated_df.to_csv(file_path, index=False)
                logging.info(f"Saved {section} data: {len(updated_df)} records to {file_path}")

    def run_full_processing(self):
        """
        Run full processing pipeline using existing HTML files.
        """
        logging.info("Starting enhanced ESPN processing with existing HTML files")
        
        # Step 1: Process fighter profiles
        self.process_fighter_profiles()
        
        # Step 2: Process living data
        self.process_living_data()
        
        # Step 3: Summary
        print("\nEnhanced ESPN Processing Summary:")
        print(f"Profiles file: {self.profiles_file}")
        print(f"Clinch file: {self.clinch_file}")
        print(f"Striking file: {self.striking_file}")
        print(f"Ground file: {self.ground_file}")
        
        return {
            'profiles_file': self.profiles_file,
            'clinch_file': self.clinch_file,
            'striking_file': self.striking_file,
            'ground_file': self.ground_file
        }


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('enhanced_scraper.log'),
            logging.StreamHandler()
        ]
    )
    
    # Initialize enhanced scraper
    scraper = ESPNEnhancedScraper(data_folder='../data')
    
    # Run processing
    results = scraper.run_full_processing()
    
    print("\nEnhanced ESPN Processing Complete!")
    for key, value in results.items():
        print(f"{key}: {value}")

if __name__ == "__main__":
    main() 