"""
ESPN MMA Fighter Data Scraper
Extracts fighter statistics from ESPN MMA fighter pages
"""

import requests
import time
import logging
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import quote
import random
from typing import Dict, List, Optional, Tuple

class ESPNFighterScraper:
    """Scrapes fighter data from ESPN MMA pages"""
    
    def __init__(self, delay_range=(2, 4)):
        self.delay_range = delay_range
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # ESPN MMA base URL
        self.base_url = "https://www.espn.com/mma"
        
        # User agent rotation
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
        ]
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def _random_delay(self):
        """Add random delay between requests"""
        delay = random.uniform(*self.delay_range)
        time.sleep(delay)
    
    def _rotate_user_agent(self):
        """Rotate user agent to avoid detection"""
        self.session.headers['User-Agent'] = random.choice(self.user_agents)
    
    def search_fighter_url(self, fighter_name: str) -> Optional[str]:
        """
        Search for fighter's ESPN page URL
        Returns the fighter's ESPN URL if found
        """
        try:
            # Clean fighter name for search
            search_name = fighter_name.replace(' ', '-').lower()
            
            # Try direct fighter URL first
            direct_url = f"{self.base_url}/fighter/_/name/{search_name}"
            
            self._rotate_user_agent()
            response = self.session.get(direct_url, timeout=30)
            
            if response.status_code == 200:
                return direct_url
            
            # If direct URL fails, try search
            search_url = f"{self.base_url}/search?q={quote(fighter_name)}"
            
            self._rotate_user_agent()
            response = self.session.get(search_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for fighter links in search results
            fighter_links = soup.find_all('a', href=True)
            
            for link in fighter_links:
                href = link.get('href', '')
                if '/mma/fighter/' in href and fighter_name.lower() in link.get_text().lower():
                    return f"https://www.espn.com{href}"
            
            # If no fighter found, return None
            self.logger.warning(f"No ESPN page found for {fighter_name}")
            return None
            
        except Exception as e:
            self.logger.error(f"Error searching for {fighter_name}: {e}")
            return None
    
    def get_fighter_stats(self, fighter_name: str) -> Dict:
        """
        Get comprehensive fighter statistics from ESPN
        Returns a dictionary with all fighter stats
        """
        try:
            # Search for fighter URL
            fighter_url = self.search_fighter_url(fighter_name)
            if not fighter_url:
                self.logger.warning(f"Could not find ESPN page for {fighter_name}")
                return {}
            
            self._rotate_user_agent()
            response = self.session.get(fighter_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract fighter statistics
            stats = {
                'fighter_name': fighter_name,
                'espn_url': fighter_url,
                'scraped_at': pd.Timestamp.now().isoformat()
            }
            
            # Extract fight statistics
            stats.update(self._extract_fight_stats(soup))
            
            # Extract detailed fight history
            fight_history = self._extract_fight_history(soup)
            stats['fight_history'] = fight_history
            
            self._random_delay()
            return stats
            
        except Exception as e:
            self.logger.error(f"Error scraping stats for {fighter_name}: {e}")
            return {'fighter_name': fighter_name, 'error': str(e)}
    
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
            results.append(fighter_stats)
            
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
        print(f"\nFighter: {result.get('fighter_name', 'Unknown')}")
        print(f"URL: {result.get('espn_url', 'Not found')}")
        print(f"Stats: {result}")