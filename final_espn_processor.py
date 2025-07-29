#!/usr/bin/env python3
"""
Final ESPN Data Processor - Extracts ALL detailed fight statistics from ESPN HTML
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

class FinalESPNProcessor:
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
    
    def extract_fight_data_from_html(self, html_content: str, fighter_name: str) -> Dict:
        """Extract comprehensive fight data directly from HTML patterns"""
        try:
            # Extract basic profile information using regex patterns
            profile = self._extract_profile_info_from_html(html_content, fighter_name)
            
            # Extract fight history with detailed statistics
            fights = self._extract_fight_history_from_html(html_content)
            
            # Calculate derived statistics
            stats = self._calculate_fighter_stats(fights)
            
            # Combine all data
            result = {**profile, **stats}
            
            # Add fight history columns (most recent fights first)
            result.update(self._create_fight_history_columns(fights))
            
            return result
            
        except Exception as e:
            print(f"Error extracting fight data for {fighter_name}: {e}")
            return {'Fighter Name': fighter_name}
    
    def _extract_profile_info_from_html(self, html_content: str, fighter_name: str) -> Dict:
        """Extract basic profile information from HTML patterns"""
        try:
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
            
            # Extract division
            division_match = re.search(r'"wghtclss":"([^"]+)"', html_content)
            if division_match:
                profile['Division'] = division_match.group(1)
            
            # Extract record - look for the specific pattern in the stats block
            # Pattern: "name":"Wins-Losses-Draws","lbl":"W-L-D","val":"27-9-0"
            record_match = re.search(r'"name":"Wins-Losses-Draws","lbl":"W-L-D","val":"(\d+-\d+-\d+)"', html_content)
            if record_match:
                profile['Record'] = record_match.group(1)
            
            # Extract height/weight
            htwt_match = re.search(r'"htwt":"([^"]+)"', html_content)
            if htwt_match:
                htwt = htwt_match.group(1)
                if ',' in htwt:
                    parts = htwt.split(',')
                    profile['Height'] = parts[0].strip()
                    profile['Weight'] = parts[1].strip()
            
            # Extract reach
            reach_match = re.search(r'"rch":"([^"]+)"', html_content)
            if reach_match:
                profile['Reach'] = reach_match.group(1)
            
            # Extract stance
            stance_match = re.search(r'"stnc":"([^"]+)"', html_content)
            if stance_match:
                profile['Stance'] = stance_match.group(1)
            
            # Extract DOB
            dob_match = re.search(r'"dob":"([^"]+)"', html_content)
            if dob_match:
                profile['DOB'] = dob_match.group(1)
            
            # Extract team
            team_match = re.search(r'"tm":"([^"]+)"', html_content)
            if team_match:
                profile['Team'] = team_match.group(1)
            
            return profile
            
        except Exception as e:
            print(f"Error extracting profile info: {e}")
            return {'Fighter Name': fighter_name}
    
    def _extract_fight_history_from_html(self, html_content: str) -> List[Dict]:
        """Extract detailed fight history with statistics from HTML patterns"""
        fights = []
        
        try:
            # Find all fight entries in the HTML using a more specific pattern
            # Look for the pattern: "gameDate":"2025-07-26T16:00:00.000+00:00","gameResult":"L","name":"UFC Fight Night","opponent":{"displayName":"Reinier de Ridder"
            
            # More specific pattern to match the ESPN structure
            fight_pattern = r'"gameDate":"([^"]+)","gameResult":"([^"]+)","name":"([^"]+)","opponent":\{"displayName":"([^"]+)"'
            
            matches = re.findall(fight_pattern, html_content)
            
            for match in matches:
                date, result, event_name, opponent = match
                
                # Extract method from the same section - look for the result displayName
                # Pattern: "displayName":"Decision - Unanimous"
                method_pattern = r'"displayName":"([^"]+)"'
                method_matches = re.findall(method_pattern, html_content)
                
                # Extract round and time
                round_pattern = r'"period":(\d+)'
                round_match = re.search(round_pattern, html_content)
                round_num = round_match.group(1) if round_match else ''
                
                time_pattern = r'"displayClock":"([^"]+)"'
                time_match = re.search(time_pattern, html_content)
                time_str = time_match.group(1) if time_match else ''
                
                fight = {
                    'date': date,
                    'result': result,
                    'opponent': opponent,
                    'event': event_name,
                    'method': method_matches[0] if method_matches else '',
                    'round': round_num,
                    'time': time_str,
                    'title_fight': 'titleFight":true' in html_content
                }
                
                fights.append(fight)
            
            # Sort fights by date (most recent first)
            fights.sort(key=lambda x: x.get('date', ''), reverse=True)
            
            return fights
            
        except Exception as e:
            print(f"Error extracting fight history: {e}")
            return []
    
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
                        fight_result = fight.get('result', '')
                        method = fight.get('method', '')
                        
                        if opponent and fight_result:
                            fight_str = f"{opponent} {fight_result}"
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
            
            # Extract comprehensive fight data directly from HTML
            fighter_data = self.extract_fight_data_from_html(html_content, fighter_name)
            
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
    
    def save_final_csv(self, df: pd.DataFrame, output_file: str = "fighter_profiles.csv"):
        """Save final fighter profiles to CSV"""
        output_path = self.data_folder / output_file
        df.to_csv(output_path, index=False)
        print(f"Final fighter profiles saved to: {output_path}")
        return output_path

def main():
    """Main function to run the final ESPN processor"""
    processor = FinalESPNProcessor()
    
    print("Starting final ESPN data processing...")
    print("Extracting ALL detailed fight statistics using MOST RECENT fights...")
    
    # Process all fighters
    df = processor.process_all_fighters()
    
    if not df.empty:
        # Save final CSV
        output_file = processor.save_final_csv(df)
        print(f"Final processing complete!")
        print(f"Output file: {output_file}")
        print(f"Total fighters processed: {len(df)}")
        print(f"Columns: {len(df.columns)}")
    else:
        print("No data processed!")

if __name__ == "__main__":
    main() 