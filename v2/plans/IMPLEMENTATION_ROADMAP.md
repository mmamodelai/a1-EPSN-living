# V2 Integration - Implementation Roadmap

## 🎯 **Project Overview**

**Goal**: Integrate V2's robust ESPN scraping with current system's UPSERT logic and output formats.

**Timeline**: 2-3 days
**Priority**: High
**Risk Level**: Low (V2 is proven, we're just adapting it)

## 📋 **Phase 1: Core Infrastructure Migration (Day 1)**

### **Step 1.1: Create Enhanced Scraper**
**File**: `src/espn_enhanced_scraper.py`

```python
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
    Enhanced ESPN scraper that combines V2's robust scraping 
    with current system's UPSERT logic and output formats.
    """
    
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    ]

    def __init__(self, data_folder: str = 'data', max_workers: int = 3, 
                 rate_limit: float = 2.0, max_retries: int = 5):
        """
        Initialize enhanced scraper with UPSERT logic and proper folder structure.
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
        
        # File paths (current system structure)
        self.fighter_html_folder = self.data_folder / "FighterHTMLs"
        self.fighter_html_folder.mkdir(exist_ok=True)
        
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

    # ... [V2's session creation, rate limiting, request methods] ...
```

### **Step 1.2: Integrate V2's Core Methods**
**Continue in same file**:

```python
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

    def _rotate_user_agent(self):
        """Rotate user agent (from V2)."""
        self.session.headers['User-Agent'] = random.choice(self.USER_AGENTS)

    def _rate_limit_wait(self):
        """Sophisticated rate limiting (from V2)."""
        current_time = datetime.now()
        
        if (current_time - self.minute_start).total_seconds() >= 60:
            self.requests_this_minute = 0
            self.minute_start = current_time
        
        if self.requests_this_minute >= 25:
            sleep_time = 60 - (current_time - self.minute_start).total_seconds()
            if sleep_time > 0:
                time.sleep(sleep_time)
                self.minute_start = datetime.now()
                self.requests_this_minute = 0
        
        time_since_last = time.time() - self.last_request_time
        if time_since_last < self.rate_limit:
            time.sleep(self.rate_limit - time_since_last)
        
        self.last_request_time = time.time()
        self.requests_this_minute += 1

    def _make_request(self, url: str, retries: int = 0) -> requests.Response:
        """Make robust HTTP request with retry logic (from V2)."""
        try:
            self._rate_limit_wait()
            self._rotate_user_agent()
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403 and retries < self.max_retries:
                wait_time = (2 ** retries) + random.uniform(0, 1)
                logging.warning(f"403 error, waiting {wait_time:.2f} seconds before retry {retries + 1}")
                time.sleep(wait_time)
                return self._make_request(url, retries + 1)
            raise
```

## 📋 **Phase 2: Data Extraction Enhancement (Day 2)**

### **Step 2.1: Enhanced Fighter Data Fetching**
**Continue in same file**:

```python
    def fetch_fighter_data(self, fighter_name: str) -> Optional[Dict]:
        """
        Enhanced fighter data fetching using V2's API approach.
        """
        try:
            # Step 1: Search for fighter using ESPN API (from V2)
            encoded_name = requests.utils.quote(fighter_name)
            search_url = f"https://site.web.api.espn.com/apis/search/v2?region=us&lang=en&limit=10&page=1&query={encoded_name}"
            
            search_response = self._make_request(search_url)
            data_json = search_response.json()
            
            # Find player data (from V2)
            player_json_data = None
            if "results" in data_json:
                for result in data_json["results"]:
                    if result.get("type") == "player":
                        contents = result.get("contents", [])
                        if isinstance(contents, list):
                            for content in contents:
                                if content.get("sport") == "mma":
                                    player_json_data = content
                                    break
                    if player_json_data:
                        break
            
            if not player_json_data:
                logging.warning(f"No MMA fighter found for {fighter_name}")
                self.failure_count += 1
                return None
            
            # Step 2: Get fighter stats page (from V2)
            profile_url = player_json_data["link"]["web"]
            stats_url = profile_url.replace("/_/id/", "/stats/_/id/")
            
            profile_response = self._make_request(stats_url)
            
            # Save HTML content to current system structure
            file_path = self.fighter_html_folder / f"{fighter_name.replace(' ', '_')}.html"
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(profile_response.text)
            
            self.success_count += 1
            logging.info(f"Successfully processed {fighter_name}")
            
            return {
                'name': fighter_name,
                'profile_url': profile_url,
                'stats_url': stats_url,
                'file_path': str(file_path)
            }
            
        except Exception as e:
            self.failure_count += 1
            logging.error(f"Error processing {fighter_name}: {str(e)}")
            return None
```

### **Step 2.2: Enhanced Profile Data Extraction**
**Continue in same file**:

```python
    def extract_fighter_profiles(self, html_files: List[Path]) -> pd.DataFrame:
        """
        Extract fighter profiles with correct format using enhanced ESPN JSON parsing.
        """
        profiles = []
        
        for html_file in html_files:
            try:
                fighter_name = html_file.stem.replace('_', ' ')
                
                with open(html_file, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                
                # Enhanced ESPN JSON extraction
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
                
                # Create profile with correct format
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
```

## 📋 **Phase 3: UPSERT Integration (Day 3)**

### **Step 3.1: UPSERT Logic Integration**
**Continue in same file**:

```python
    def _upsert_data(self, new_df: pd.DataFrame, existing_file: Path, composite_key: List[str]) -> pd.DataFrame:
        """
        UPSERT logic from current system - preserves existing data.
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

    def process_fighter_profiles(self):
        """
        Process fighter profiles with UPSERT logic.
        """
        logging.info("Processing fighter profiles...")
        
        # Get all HTML files
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
        
        # Get all HTML files
        html_files = list(self.fighter_html_folder.glob("*.html"))
        
        # Extract fight-by-fight statistics (from V2)
        section_dfs = self._extract_fight_statistics(html_files)
        
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

    def _extract_fight_statistics(self, html_files: List[Path]) -> Dict[str, pd.DataFrame]:
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

        for file_path in html_files:
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
            else:
                logging.warning(f"No data found for section: {section}")
                result_dfs[section] = pd.DataFrame(columns=['Player'] + columns[section])
                
        return result_dfs
```

### **Step 3.2: Main Processing Method**
**Continue in same file**:

```python
    def run_full_processing(self, fighters: List[str]):
        """
        Run full processing pipeline with V2's robust scraping and current system's UPSERT logic.
        """
        logging.info(f"Starting enhanced ESPN processing for {len(fighters)} fighters")
        
        # Step 1: Scrape fighter data using V2's robust approach
        chunk_size = 50
        all_results = []
        
        for i in range(0, len(fighters), chunk_size):
            chunk = fighters[i:i + chunk_size]
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                chunk_results = list(tqdm(
                    executor.map(self.fetch_fighter_data, chunk),
                    total=len(chunk),
                    desc=f"Processing fighters {i+1}-{min(i+chunk_size, len(fighters))}"
                ))
                all_results.extend(chunk_results)
            
            if i + chunk_size < len(fighters):
                pause_time = random.uniform(5, 10)
                logging.info(f"Pausing for {pause_time:.2f} seconds between chunks")
                time.sleep(pause_time)
        
        successful_fighters = [r for r in all_results if r is not None]
        logging.info(f"Successfully processed {len(successful_fighters)} out of {len(fighters)} fighters")
        
        # Step 2: Process data with UPSERT logic
        self.process_fighter_profiles()
        self.process_living_data()
        
        # Step 3: Summary
        print("\nEnhanced ESPN Processing Summary:")
        print(f"Total Success Cases: {self.success_count}")
        print(f"Total Failure Cases: {self.failure_count}")
        
        return {
            'success_count': self.success_count,
            'failure_count': self.failure_count,
            'profiles_file': self.profiles_file,
            'clinch_file': self.clinch_file,
            'striking_file': self.striking_file,
            'ground_file': self.ground_file
        }
```

## 📋 **Phase 4: Integration Testing**

### **Step 4.1: Create Test Script**
**File**: `test_enhanced_scraper.py`

```python
#!/usr/bin/env python3

import logging
from pathlib import Path
import pandas as pd
from src.espn_enhanced_scraper import ESPNEnhancedScraper

def test_enhanced_scraper():
    """Test the enhanced scraper with sample data."""
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Test with small sample
    test_fighters = [
        "Robert Whittaker",
        "Israel Adesanya", 
        "Sean O'Malley",
        "Khamzat Chimaev"
    ]
    
    # Initialize enhanced scraper
    scraper = ESPNEnhancedScraper(data_folder='data')
    
    # Run processing
    results = scraper.run_full_processing(test_fighters)
    
    # Verify results
    print("\nTest Results:")
    for key, value in results.items():
        print(f"{key}: {value}")
    
    # Check output files
    for file_path in [scraper.profiles_file, scraper.clinch_file, scraper.striking_file, scraper.ground_file]:
        if file_path.exists():
            df = pd.read_csv(file_path)
            print(f"{file_path.name}: {len(df)} records")
        else:
            print(f"{file_path.name}: File not created")

if __name__ == "__main__":
    test_enhanced_scraper()
```

## 🚀 **Deployment Steps**

### **Step 1: Backup Current System**
```bash
# Backup current data
cp -r data data_backup_$(date +%Y%m%d_%H%M%S)
```

### **Step 2: Deploy Enhanced Scraper**
```bash
# Copy enhanced scraper
cp v2/espn_fighter_scraper.py src/espn_enhanced_scraper.py
```

### **Step 3: Update Main Runner**
**File**: `run_enhanced_espn_processor.py`

```python
#!/usr/bin/env python3

import logging
from pathlib import Path
import pandas as pd
from src.espn_enhanced_scraper import ESPNEnhancedScraper

def main():
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('enhanced_scraper.log'),
            logging.StreamHandler()
        ]
    )
    
    # Load fighters
    fighters_df = pd.read_csv('data/fighters_name.csv')
    fighters = fighters_df['fighters'].tolist()
    
    logging.info(f"Found {len(fighters)} fighters to process")
    
    # Initialize and run enhanced scraper
    scraper = ESPNEnhancedScraper(data_folder='data')
    results = scraper.run_full_processing(fighters)
    
    print("\nEnhanced ESPN Processing Complete!")
    print(f"Success: {results['success_count']}")
    print(f"Failures: {results['failure_count']}")

if __name__ == "__main__":
    main()
```

## ✅ **Success Criteria Checklist**

- [ ] **V2 infrastructure integrated** - Rate limiting, retry logic, concurrent processing
- [ ] **UPSERT logic preserved** - No data loss, existing records maintained
- [ ] **Output format correct** - Fighter profiles match required structure
- [ ] **Living data files updated** - All 4 files properly maintained
- [ ] **Performance improved** - 5x faster than current system
- [ ] **Reliability enhanced** - 98%+ success rate
- [ ] **Data quality improved** - Complete ESPN statistics captured

---

**Status**: Ready for Implementation
**Next Action**: Begin Phase 1 implementation 