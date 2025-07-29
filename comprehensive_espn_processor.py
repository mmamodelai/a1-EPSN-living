#!/usr/bin/env python3
"""
Comprehensive ESPN Data Processor - Extracts ALL detailed fight statistics from ESPN HTML
Uses MOST RECENT fights for First_last_last3, First_last_last4, etc. columns
Matches A1 system data structure with detailed fight-by-fight statistics
"""

import json
import re
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any
from bs4 import BeautifulSoup

class ComprehensiveESPNProcessor:
    """Extract comprehensive fight statistics from ESPN HTML files using most recent fights"""
    
    def __init__(self, data_folder: str = "data"):
        self.data_folder = Path(data_folder)
        self.fighter_html_folder = self.data_folder / "FighterHTMLs"
        
        # A1-compatible column structure
        self.a1_columns = [
            'Fighter Name', 'Division', 'Record', 'Height', 'Weight', 'Reach', 'Stance', 'DOB', 'Team',
            'First_last_last3', 'First_last_last4', 'First_last_last5', 'First_last_last6', 'First_last_last7', 
            'First_last_last8', 'First_last_last9', 'First_last_last10',
            'Striking Accuracy', 'Sig. Strikes Landed', 'Sig. Strikes Attempted', 'Sig. Strikes Landed Per Min', 
            'Sig. Strikes Absorbed Per Min', 'Takedown Accuracy', 'Takedowns Landed', 'Takedowns Attempted', 
            'Takedown avg Per 15 Min', 'Submission avg Per 15 Min', 'Sig. Str. Defense', 'Takedown Defense', 
            'Knockdown Avg', 'Average fight time', 'Wins by KO/TKO', 'Wins by Submission', 'Wins by Decision', 
            'Losses by KO/TKO', 'Losses by Submission', 'Losses by Decision', 'Win Streak', 'Last Fight Date', 
            'Last Fight Opponent', 'Last Fight Result', 'Last Fight Method', 'Last Fight Round', 'Last Fight Time'
        ]
        
    def load_fighters_list(self) -> List[str]:
        """Load list of fighters from fighters_name.csv"""
        fighters_file = self.data_folder / "fighters_name.csv"
        if not fighters_file.exists():
            print(f"Fighters file not found: {fighters_file}")
            return []
            
        try:
            df = pd.read_csv(fighters_file)
            # Check for both possible column names
            if 'Fighter Name' in df.columns:
                return df['Fighter Name'].dropna().tolist()
            elif 'fighters' in df.columns:
                return df['fighters'].dropna().tolist()
            else:
                print(f"Expected 'Fighter Name' or 'fighters' column in {fighters_file}")
                return []
        except Exception as e:
            print(f"Error loading fighters list: {e}")
            return []
    
    def extract_json_from_html(self, html_content: str) -> Optional[Dict]:
        """Extract JSON data from ESPN HTML"""
        try:
            # Look for the JSON data in the HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find script tags containing the data
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string and 'prtlCmnApiRsp' in script.string:
                    # Extract the JSON part - look for the ESPN API response structure
                    script_text = script.string
                    
                    # Find the JSON object containing the fighter data
                    # Look for the pattern that contains the fighter statistics
                    json_match = re.search(r'window\.__INITIAL_STATE__\s*=\s*({.*?});', script_text, re.DOTALL)
                    if json_match:
                        json_str = json_match.group(1)
                        return json.loads(json_str)
                    
            print("No JSON data found in HTML")
            return None
            
        except Exception as e:
            print(f"Error extracting JSON: {e}")
            return None
    
    def extract_fight_data_from_json(self, json_data: Dict, fighter_name: str) -> Dict:
        """Extract comprehensive fight data from ESPN JSON"""
        try:
            # Navigate to the fighter data in the JSON structure
            if 'prtlCmnApiRsp' not in json_data:
                return {}
                
            fighter_data = json_data['prtlCmnApiRsp']
            
            # Extract basic profile information
            profile = self._extract_profile_info(fighter_data, fighter_name)
            
            # Extract fight history with detailed statistics
            fights = self._extract_fight_history(fighter_data)
            
            # Calculate derived statistics
            stats = self._calculate_fighter_stats(fights)
            
            # Combine all data
            result = {**profile, **stats}
            
            # Add fight history columns (most recent fights first)
            result.update(self._create_fight_history_columns(fights))
            
            return result
            
        except Exception as e:
            print(f"Error extracting fight data for {fighter_name}: {e}")
            return {}
    
    def _extract_profile_info(self, fighter_data: Dict, fighter_name: str) -> Dict:
        """Extract basic profile information"""
        try:
            # Look for profile information in the JSON structure
            profile = {
                'Fighter Name': fighter_name,
                'Division': '',
                'Record': '',
                'Height': '',
                'Weight': '',
                'Reach': '',
                'Stance': '',
                'DOB': '',
                'Team': ''
            }
            
            # Extract from the fighter header data
            if 'plyrHdr' in fighter_data and 'ath' in fighter_data['plyrHdr']:
                ath = fighter_data['plyrHdr']['ath']
                profile.update({
                    'Division': ath.get('wghtclss', ''),
                    'Record': ath.get('statsBlck', {}).get('vals', [{}])[0].get('val', '') if ath.get('statsBlck', {}).get('vals') else '',
                    'Height': ath.get('htwt', '').split(',')[0] if ath.get('htwt') else '',
                    'Weight': ath.get('htwt', '').split(',')[1] if ath.get('htwt') and ',' in ath.get('htwt') else '',
                    'Reach': ath.get('rch', ''),
                    'Stance': ath.get('stnc', ''),
                    'DOB': ath.get('dob', ''),
                    'Team': ath.get('tm', '')
                })
            
            return profile
            
        except Exception as e:
            print(f"Error extracting profile info: {e}")
            return {'Fighter Name': fighter_name}
    
    def _extract_fight_history(self, fighter_data: Dict) -> List[Dict]:
        """Extract detailed fight history with statistics"""
        fights = []
        
        try:
            # Look for fight data in the eventsMap structure
            if 'eventsMap' in fighter_data:
                events_map = fighter_data['eventsMap']
                
                for event_id, event_data in events_map.items():
                    if isinstance(event_data, dict) and 'gameDate' in event_data:
                        fight = {
                            'date': event_data.get('gameDate', ''),
                            'result': event_data.get('gameResult', ''),
                            'opponent': event_data.get('opponent', {}).get('displayName', ''),
                            'event': event_data.get('name', ''),
                            'method': event_data.get('status', {}).get('result', {}).get('displayName', ''),
                            'round': event_data.get('status', {}).get('period', ''),
                            'time': event_data.get('status', {}).get('displayClock', ''),
                            'title_fight': event_data.get('titleFight', False)
                        }
                        
                        # Extract detailed fight statistics if available
                        if 'stat' in event_data and 'tbl' in event_data['stat']:
                            fight_stats = self._extract_fight_statistics(event_data['stat']['tbl'])
                            fight.update(fight_stats)
                        
                        fights.append(fight)
            
            # Sort fights by date (most recent first)
            fights.sort(key=lambda x: x.get('date', ''), reverse=True)
            
            return fights
            
        except Exception as e:
            print(f"Error extracting fight history: {e}")
            return []
    
    def _extract_fight_statistics(self, stat_tables: List[Dict]) -> Dict:
        """Extract detailed fight statistics from ESPN stat tables"""
        stats = {}
        
        try:
            for table in stat_tables:
                if table.get('ttl') == 'striking' and 'row' in table:
                    # Extract striking statistics from the first row (most recent fight)
                    if table['row']:
                        row = table['row'][0]
                        if len(row) >= 12:  # Ensure we have enough columns
                            stats.update({
                                'striking_accuracy': row[8] if len(row) > 8 else '',  # TSL-TSA column
                                'sig_strikes_landed': row[5] if len(row) > 5 else '',  # SSL column
                                'sig_strikes_attempted': row[6] if len(row) > 6 else '',  # SSA column
                                'total_strikes_landed': row[3] if len(row) > 3 else '',  # TSL column
                                'total_strikes_attempted': row[4] if len(row) > 4 else '',  # TSA column
                                'knockdowns': row[8] if len(row) > 8 else ''  # KD column
                            })
                
                elif table.get('ttl') == 'Clinch' and 'row' in table:
                    # Extract clinch statistics
                    if table['row']:
                        row = table['row'][0]
                        if len(row) >= 12:
                            stats.update({
                                'takedowns_landed': row[8] if len(row) > 8 else '',  # TDL column
                                'takedowns_attempted': row[9] if len(row) > 9 else '',  # TDA column
                                'takedown_accuracy': row[11] if len(row) > 11 else ''  # TKACC column
                            })
                
                elif table.get('ttl') == 'Ground' and 'row' in table:
                    # Extract ground statistics
                    if table['row']:
                        row = table['row'][0]
                        if len(row) >= 12:
                            stats.update({
                                'ground_strikes_landed': row[0] if len(row) > 0 else '',  # SGBL column
                                'ground_strikes_attempted': row[1] if len(row) > 1 else '',  # SGBA column
                                'advances': row[6] if len(row) > 6 else '',  # AD column
                                'submissions': row[11] if len(row) > 11 else ''  # SM column
                            })
            
            return stats
            
        except Exception as e:
            print(f"Error extracting fight statistics: {e}")
            return {}
    
    def _calculate_fighter_stats(self, fights: List[Dict]) -> Dict:
        """Calculate fighter statistics from fight history"""
        if not fights:
            return {}
        
        try:
            # Calculate basic statistics
            total_fights = len(fights)
            wins = sum(1 for fight in fights if fight.get('result') == 'W')
            losses = sum(1 for fight in fights if fight.get('result') == 'L')
            
            # Calculate win methods
            wins_by_ko = sum(1 for fight in fights if fight.get('result') == 'W' and 'KO' in fight.get('method', ''))
            wins_by_sub = sum(1 for fight in fights if fight.get('result') == 'W' and 'Submission' in fight.get('method', ''))
            wins_by_dec = sum(1 for fight in fights if fight.get('result') == 'W' and 'Decision' in fight.get('method', ''))
            
            losses_by_ko = sum(1 for fight in fights if fight.get('result') == 'L' and 'KO' in fight.get('method', ''))
            losses_by_sub = sum(1 for fight in fights if fight.get('result') == 'L' and 'Submission' in fight.get('method', ''))
            losses_by_dec = sum(1 for fight in fights if fight.get('result') == 'L' and 'Decision' in fight.get('method', ''))
            
            # Calculate win streak
            win_streak = 0
            for fight in fights:
                if fight.get('result') == 'W':
                    win_streak += 1
                else:
                    break
            
            # Calculate average fight time
            fight_times = []
            for fight in fights:
                time_str = fight.get('time', '')
                if time_str and ':' in time_str:
                    try:
                        minutes, seconds = map(int, time_str.split(':'))
                        total_seconds = minutes * 60 + seconds
                        fight_times.append(total_seconds)
                    except:
                        pass
            
            avg_fight_time = sum(fight_times) / len(fight_times) if fight_times else 0
            avg_fight_time_str = f"{int(avg_fight_time // 60):02d}:{int(avg_fight_time % 60):02d}" if avg_fight_time > 0 else ""
            
            # Get last fight information
            last_fight = fights[0] if fights else {}
            
            return {
                'Record': f"{wins}-{losses}-0",
                'Wins by KO/TKO': wins_by_ko,
                'Wins by Submission': wins_by_sub,
                'Wins by Decision': wins_by_dec,
                'Losses by KO/TKO': losses_by_ko,
                'Losses by Submission': losses_by_sub,
                'Losses by Decision': losses_by_dec,
                'Win Streak': win_streak,
                'Average fight time': avg_fight_time_str,
                'Last Fight Date': last_fight.get('date', ''),
                'Last Fight Opponent': last_fight.get('opponent', ''),
                'Last Fight Result': last_fight.get('result', ''),
                'Last Fight Method': last_fight.get('method', ''),
                'Last Fight Round': last_fight.get('round', ''),
                'Last Fight Time': last_fight.get('time', '')
            }
            
        except Exception as e:
            print(f"Error calculating fighter stats: {e}")
            return {}
    
    def _create_fight_history_columns(self, fights: List[Dict]) -> Dict:
        """Create fight history columns using most recent fights"""
        result = {}
        
        try:
            # Create columns for last 3, 4, 5, 6, 7, 8, 9, 10 fights
            for i in range(3, 11):
                if len(fights) >= i:
                    # Take the most recent i fights
                    recent_fights = fights[:i]
                    fight_strings = []
                    
                    for fight in recent_fights:
                        opponent = fight.get('opponent', '')
                        result = fight.get('result', '')
                        method = fight.get('method', '')
                        
                        if opponent and result:
                            fight_str = f"{opponent} {result}"
                            if method:
                                fight_str += f" {method}"
                            fight_strings.append(fight_str)
                    
                    result[f'First_last_last{i}'] = ' | '.join(fight_strings)
                else:
                    result[f'First_last_last{i}'] = ''
            
            return result
            
        except Exception as e:
            print(f"Error creating fight history columns: {e}")
            return {}
    
    def process_fighter(self, fighter_name: str) -> Dict:
        """Process a single fighter's data"""
        try:
            # Create filename from fighter name
            filename = fighter_name.replace(' ', '_') + '.html'
            filepath = self.fighter_html_folder / filename
            
            if not filepath.exists():
                print(f"HTML file not found for {fighter_name}: {filepath}")
                return {'Fighter Name': fighter_name}
            
            # Read HTML content
            with open(filepath, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            # Extract JSON data
            json_data = self.extract_json_from_html(html_content)
            if not json_data:
                print(f"No JSON data found for {fighter_name}")
                return {'Fighter Name': fighter_name}
            
            # Extract comprehensive fight data
            fighter_data = self.extract_fight_data_from_json(json_data, fighter_name)
            
            return fighter_data
            
        except Exception as e:
            print(f"Error processing {fighter_name}: {e}")
            return {'Fighter Name': fighter_name}
    
    def process_all_fighters(self) -> pd.DataFrame:
        """Process all fighters and return comprehensive DataFrame"""
        fighters = self.load_fighters_list()
        
        if not fighters:
            print("No fighters found to process")
            return pd.DataFrame(columns=self.a1_columns)
        
        print(f"Processing {len(fighters)} fighters...")
        
        all_data = []
        processed_count = 0
        
        for fighter_name in fighters:
            print(f"Processing {fighter_name}...")
            fighter_data = self.process_fighter(fighter_name)
            
            # Ensure all required columns are present
            for col in self.a1_columns:
                if col not in fighter_data:
                    fighter_data[col] = ''
            
            all_data.append(fighter_data)
            processed_count += 1
            
            if processed_count % 50 == 0:
                print(f"Processed {processed_count}/{len(fighters)} fighters...")
        
        # Create DataFrame
        df = pd.DataFrame(all_data, columns=self.a1_columns)
        
        print(f"Successfully processed {len(df)} fighters")
        return df
    
    def save_comprehensive_csv(self, df: pd.DataFrame, output_file: str = "comprehensive_fighter_profiles.csv"):
        """Save comprehensive fighter profiles to CSV"""
        output_path = self.data_folder / output_file
        df.to_csv(output_path, index=False)
        print(f"Comprehensive fighter profiles saved to: {output_path}")
        return output_path

def main():
    """Main function to run the comprehensive ESPN processor"""
    processor = ComprehensiveESPNProcessor()
    
    print("Starting comprehensive ESPN data processing...")
    print("Extracting ALL detailed fight statistics using MOST RECENT fights...")
    
    # Process all fighters
    df = processor.process_all_fighters()
    
    if not df.empty:
        # Save comprehensive CSV
        output_file = processor.save_comprehensive_csv(df)
        print(f"Comprehensive processing complete!")
        print(f"Output file: {output_file}")
        print(f"Total fighters processed: {len(df)}")
        print(f"Columns: {len(df.columns)}")
    else:
        print("No data processed!")

if __name__ == "__main__":
    main() 