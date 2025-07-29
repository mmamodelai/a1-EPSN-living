"""
ESPN MMA Fighter Data Scraper
Extracts fighter statistics from ESPN MMA fighter pages
"""

import requests
import pandas as pd
from pathlib import Path
import time
import logging
from typing import Dict, List, Optional
import json
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import random
from datetime import datetime
from bs4 import BeautifulSoup

class ESPNFighterScraper:
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    ]

    def __init__(self, html_profile_dir: Path = None, max_workers: int = 3, 
                 rate_limit: float = 2.0, max_retries: int = 5):
        self.html_profile_dir = html_profile_dir or Path("data/FighterHTMLs")
        self.html_profile_dir.mkdir(parents=True, exist_ok=True)
        self.max_workers = max_workers
        self.rate_limit = rate_limit
        self.max_retries = max_retries
        self.session = self._create_session()
        
        # Request tracking for anti-detection
        self.success_count = 0
        self.failure_count = 0
        self.last_request_time = 0
        self.request_count = 0
        self.requests_this_minute = 0
        self.minute_start = datetime.now()
        
        self.logger = logging.getLogger(__name__)

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        
        # Advanced retry strategy with exponential backoff
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        # More realistic browser headers
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
        self.session.headers['User-Agent'] = random.choice(self.USER_AGENTS)

    def _rate_limit_wait(self):
        current_time = datetime.now()
        
        # Reset minute counter if a minute has passed
        if (current_time - self.minute_start).total_seconds() >= 60:
            self.requests_this_minute = 0
            self.minute_start = current_time
        
        # Enforce max 25 requests per minute (ESPN's limit)
        if self.requests_this_minute >= 25:
            sleep_time = 60 - (current_time - self.minute_start).total_seconds()
            if sleep_time > 0:
                self.logger.info(f"Rate limit reached, sleeping for {sleep_time:.1f} seconds")
                time.sleep(sleep_time)
                self.minute_start = datetime.now()
                self.requests_this_minute = 0
        
        # Basic delay between requests
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.rate_limit:
            sleep_time = self.rate_limit - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
        self.requests_this_minute += 1

    def _make_request(self, url: str, retries: int = 0) -> requests.Response:
        try:
            self._rate_limit_wait()
            self._rotate_user_agent()
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403 and retries < self.max_retries:
                wait_time = (2 ** retries) + random.uniform(0, 1)
                self.logger.warning(f"403 error, waiting {wait_time:.2f} seconds before retry {retries + 1}")
                time.sleep(wait_time)
                return self._make_request(url, retries + 1)
            raise
        except requests.exceptions.RequestException as e:
            if retries < self.max_retries:
                wait_time = (2 ** retries) + random.uniform(0, 1)
                self.logger.warning(f"Request failed, waiting {wait_time:.2f} seconds before retry {retries + 1}")
                time.sleep(wait_time)
                return self._make_request(url, retries + 1)
            else:
                self.logger.error(f"Request failed for {url} after {self.max_retries} retries: {e}")
                raise
    
    def search_fighter_url(self, fighter_name: str) -> Optional[str]:
        """Search for fighter URL using ESPN's search API"""
        try:
            # Use ESPN's search API like the working scraper
            encoded_name = requests.utils.quote(fighter_name)
            search_url = f"https://site.web.api.espn.com/apis/search/v2?region=us&lang=en&limit=10&page=1&query={encoded_name}"
            
            response = self._make_request(search_url)
            data_json = response.json()
            
            # Find player data
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
                self.logger.warning(f"No MMA fighter found for {fighter_name}")
                return None
            
            # Get the stats page URL
            profile_url = player_json_data["link"]["web"]
            stats_url = profile_url.replace("/_/id/", "/stats/_/id/")
            
            self.logger.info(f"Found fighter URL for {fighter_name}: {stats_url}")
            return stats_url
            
        except Exception as e:
            self.logger.error(f"Error searching for {fighter_name}: {str(e)}")
            return None
    
    def get_fighter_stats(self, fighter_name: str) -> Optional[Dict]:
        """
        Get fighter statistics from ESPN and save HTML file
        Returns a dictionary with fighter stats
        """
        try:
            # Search for fighter URL
            fighter_url = self.search_fighter_url(fighter_name)
            
            if not fighter_url:
                self.logger.warning(f"Could not find URL for {fighter_name}")
                self.failure_count += 1
                return None
            
            # Get the fighter page content
            profile_response = self._make_request(fighter_url)
            
            # Save HTML content directly
            file_path = self.html_profile_dir / f"{fighter_name.replace(' ', '_')}.html"
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(profile_response.text)
            
            self.logger.info(f"Saved HTML for {fighter_name} to {file_path}")
            
            # Extract basic stats (placeholder for now)
            soup = BeautifulSoup(profile_response.content, 'html.parser')
            fight_stats = self._extract_fight_stats(soup)
            fight_history = self._extract_fight_history(soup)
            
            fighter_data = {
                'name': fighter_name,
                'url': fighter_url,
                'scraped_at': datetime.now().isoformat(),
                'fight_stats': fight_stats,
                'fight_history': fight_history,
                'html_file': str(file_path)
            }
            
            self.success_count += 1
            self.logger.info(f"Successfully scraped data for {fighter_name}")
            return fighter_data
            
        except Exception as e:
            self.failure_count += 1
            self.logger.error(f"Error getting stats for {fighter_name}: {e}")
            return None
    
    def _extract_fight_stats(self, soup: BeautifulSoup) -> Dict:
        """Extract fight statistics from ESPN page"""
        stats = {}
        
        try:
            # Look for statistics tables
            stat_tables = soup.find_all('table', class_='Table')
            
            for table in stat_tables:
                # Look for clinch statistics
                if 'clinch' in table.get_text().lower():
                    stats.update(self._parse_clinch_stats(table))
                
                # Look for ground statistics
                elif 'ground' in table.get_text().lower():
                    stats.update(self._parse_ground_stats(table))
                
                # Look for striking statistics
                elif 'striking' in table.get_text().lower():
                    stats.update(self._parse_striking_stats(table))
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error extracting fight stats: {e}")
            return {}
    
    def _parse_clinch_stats(self, table: BeautifulSoup) -> Dict:
        """Parse clinch statistics from table"""
        stats = {}
        
        try:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    stat_name = cells[0].get_text().strip()
                    stat_value = cells[1].get_text().strip()
                    
                    # Map ESPN stats to our format
                    if 'clinch' in stat_name.lower():
                        if 'landed' in stat_name.lower():
                            stats['SCBL'] = stat_value
                        elif 'attempted' in stat_name.lower():
                            stats['SCBA'] = stat_value
                    elif 'takedown' in stat_name.lower():
                        if 'landed' in stat_name.lower():
                            stats['TDL'] = stat_value
                        elif 'attempted' in stat_name.lower():
                            stats['TDA'] = stat_value
                        elif 'success' in stat_name.lower():
                            stats['TDS'] = stat_value
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error parsing clinch stats: {e}")
            return {}
    
    def _parse_ground_stats(self, table: BeautifulSoup) -> Dict:
        """Parse ground statistics from table"""
        stats = {}
        
        try:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    stat_name = cells[0].get_text().strip()
                    stat_value = cells[1].get_text().strip()
                    
                    # Map ESPN stats to our format
                    if 'ground' in stat_name.lower():
                        if 'landed' in stat_name.lower():
                            stats['SGBL'] = stat_value
                        elif 'attempted' in stat_name.lower():
                            stats['SGBA'] = stat_value
                    elif 'submission' in stat_name.lower():
                        if 'attempted' in stat_name.lower():
                            stats['SA'] = stat_value
                        elif 'landed' in stat_name.lower():
                            stats['SL'] = stat_value
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error parsing ground stats: {e}")
            return {}
    
    def _parse_striking_stats(self, table: BeautifulSoup) -> Dict:
        """Parse striking statistics from table"""
        stats = {}
        
        try:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    stat_name = cells[0].get_text().strip()
                    stat_value = cells[1].get_text().strip()
                    
                    # Map ESPN stats to our format
                    if 'strikes' in stat_name.lower():
                        if 'landed' in stat_name.lower():
                            stats['SSL'] = stat_value
                        elif 'attempted' in stat_name.lower():
                            stats['SSA'] = stat_value
                    elif 'total' in stat_name.lower() and 'strikes' in stat_name.lower():
                        if 'landed' in stat_name.lower():
                            stats['TSL'] = stat_value
                        elif 'attempted' in stat_name.lower():
                            stats['TSA'] = stat_value
                    elif 'knockdown' in stat_name.lower():
                        stats['KD'] = stat_value
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error parsing striking stats: {e}")
            return {}
    
    def _extract_fight_history(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract detailed fight history from ESPN page"""
        fights = []
        
        try:
            # Look for fight history section
            fight_sections = soup.find_all('div', class_='fight-history')
            
            for section in fight_sections:
                fight_rows = section.find_all('tr')
                
                for row in fight_rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 4:
                        fight = {
                            'date': cells[0].get_text().strip() if len(cells) > 0 else '',
                            'opponent': cells[1].get_text().strip() if len(cells) > 1 else '',
                            'result': cells[2].get_text().strip() if len(cells) > 2 else '',
                            'method': cells[3].get_text().strip() if len(cells) > 3 else '',
                            'round': cells[4].get_text().strip() if len(cells) > 4 else '',
                            'time': cells[5].get_text().strip() if len(cells) > 5 else ''
                        }
                        fights.append(fight)
            
            return fights
            
        except Exception as e:
            self.logger.error(f"Error extracting fight history: {e}")
            return []
    
    def scrape_fighters_batch(self, fighter_names: List[str], progress_callback=None) -> List[Dict]:
        """
        Scrape data for multiple fighters with progress tracking
        """
        results = []
        total_fighters = len(fighter_names)
        
        for i, fighter_name in enumerate(fighter_names):
            self.logger.info(f"Scraping {fighter_name} ({i+1}/{total_fighters})")
            
            fighter_stats = self.get_fighter_stats(fighter_name)
            if fighter_stats:
                results.append(fighter_stats)
            else:
                results.append({'name': fighter_name, 'error': 'Could not find fighter URL'})
            
            if progress_callback:
                progress_callback(i + 1, total_fighters, fighter_name)
        
        return results
    
    def save_fighter_data(self, fighter_data: List[Dict], output_file: str):
        """Save fighter data to CSV file"""
        try:
            df = pd.DataFrame(fighter_data)
            df.to_csv(output_file, index=False)
            self.logger.info(f"Saved {len(fighter_data)} fighter records to {output_file}")
        except Exception as e:
            self.logger.error(f"Error saving fighter data: {e}")


def create_sample_fighter_data():
    """Create sample fighter data for testing"""
    sample_data = [
        {
            'fighter_name': 'Robert Whittaker',
            'espn_url': 'https://www.espn.com/mma/fighter/_/id/12345/robert-whittaker',
            'scraped_at': pd.Timestamp.now().isoformat(),
            'SCBL': '15', 'SCBA': '20', 'TDL': '3', 'TDA': '5', 'TDS': '60%',
            'SGBL': '8', 'SGBA': '12', 'SA': '2', 'SL': '1',
            'SSL': '45', 'SSA': '80', 'TSL': '68', 'TSA': '112', 'KD': '2'
        },
        {
            'fighter_name': 'Israel Adesanya',
            'espn_url': 'https://www.espn.com/mma/fighter/_/id/12346/israel-adesanya',
            'scraped_at': pd.Timestamp.now().isoformat(),
            'SCBL': '8', 'SCBA': '15', 'TDL': '1', 'TDA': '3', 'TDS': '33%',
            'SGBL': '2', 'SGBA': '5', 'SA': '0', 'SL': '0',
            'SSL': '78', 'SSA': '120', 'TSL': '88', 'TSA': '140', 'KD': '5'
        }
    ]
    return sample_data


if __name__ == "__main__":
    # Test the scraper
    scraper = ESPNFighterScraper()
    
    # Test with sample fighters
    test_fighters = ['Robert Whittaker', 'Israel Adesanya']
    
    print("Testing ESPN Fighter Scraper...")
    results = scraper.scrape_fighters_batch(test_fighters)
    
    for result in results:
        print(f"\nFighter: {result.get('name', 'Unknown')}")
        print(f"URL: {result.get('url', 'Not found')}")
        print(f"Stats: {result.get('fight_stats', 'N/A')}")
        print(f"History: {result.get('fight_history', 'N/A')}")