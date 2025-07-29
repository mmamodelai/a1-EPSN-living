#!/usr/bin/env python3
"""
Direct ESPN Data Processor - Extracts fight data directly from HTML patterns
Uses MOST RECENT fights for First_last_last3, First_last_last4, etc. columns
"""

import re
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
from bs4 import BeautifulSoup

class DirectESPNProcessor:
    """Extract fight data directly from ESPN HTML patterns"""
    
    def __init__(self, data_folder: str = "data"):
        self.data_folder = Path(data_folder)
        self.fighter_html_folder = self.data_folder / "FighterHTMLs"
        
        # A1-compatible column structure
        self.a1_columns = [
            'Fighter Name', 'Division', 'Record', 'Height', 'Weight', 'Reach', 'Stance', 'DOB', 'Team',
            'First_last_last3', 'First_last_last4', 'First_last_last5', 'First_last_last6', 'First_last_last7', 'First_last_last8', 'First_last_last9', 'First_last_last10',
            'Striking Accuracy', 'Sig. Strikes Landed', 'Sig. Strikes Attempted', 'Sig. Strikes Landed Per Min', 'Sig. Strikes Absorbed Per Min',
            'Takedown Accuracy', 'Takedowns Landed', 'Takedowns Attempted', 'Takedown avg Per 15 Min',
            'Submission avg Per 15 Min', 'Sig. Str. Defense', 'Takedown Defense', 'Knockdown Avg', 'Average fight time',
            'Wins by KO/TKO', 'Wins by Submission', 'Wins by Decision', 'Losses by KO/TKO', 'Losses by Submission', 'Losses by Decision',
            'Win Streak', 'Last Fight Date', 'Last Fight Opponent', 'Last Fight Result', 'Last Fight Method', 'Last Fight Round', 'Last Fight Time'
        ]
    
    def load_fighters_list(self) -> List[str]:
        """Load list of fighters from fighters_name.csv"""
        fighters_file = self.data_folder / "fighters_name.csv"
        if not fighters_file.exists():
            print(f"Fighters file not found: {fighters_file}")
            return []
        
        df = pd.read_csv(fighters_file)
        if 'Fighter Name' in df.columns:
            return df['Fighter Name'].tolist()
        elif 'fighters' in df.columns:
            return df['fighters'].tolist()
        else:
            print("No valid fighter name column found")
            return []
    
    def extract_fight_data_from_html(self, html_content: str, fighter_name: str) -> Dict:
        """Extract fight data directly from HTML patterns"""
        try:
            # Extract basic profile info
            profile_data = self._extract_profile_info(html_content, fighter_name)
            
            # Extract fight-by-fight data
            fight_data = self._extract_fight_by_fight_data(html_content)
            
            # Combine profile and fight data
            result = {**profile_data, **fight_data}
            
            return result
            
        except Exception as e:
            print(f"Error extracting fight data for {fighter_name}: {e}")
            return {}
    
    def _extract_profile_info(self, html_content: str, fighter_name: str) -> Dict:
        """Extract basic profile information from HTML"""
        try:
            # Look for basic profile data in the HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract record from HTML patterns
            record = "0-0-0"
            record_pattern = r'"Wins-Losses-Draws","lbl":"W-L-D","val":"([^"]+)"'
            record_match = re.search(record_pattern, html_content)
            if record_match:
                record = record_match.group(1)
            
            # Extract division/weight class
            division = ""
            division_pattern = r'"wghtclss":"([^"]+)"'
            division_match = re.search(division_pattern, html_content)
            if division_match:
                division = division_match.group(1)
            
            # Extract height/weight
            height = ""
            weight = ""
            htwt_pattern = r'"htwt":"([^"]+)"'
            htwt_match = re.search(htwt_pattern, html_content)
            if htwt_match:
                htwt = htwt_match.group(1)
                if ',' in htwt:
                    parts = htwt.split(',')
                    height = parts[0].strip()
                    weight = parts[1].strip() if len(parts) > 1 else ""
                else:
                    height = htwt
            
            # Extract reach
            reach = ""
            reach_pattern = r'"rch":"([^"]+)"'
            reach_match = re.search(reach_pattern, html_content)
            if reach_match:
                reach = reach_match.group(1)
            
            # Extract stance
            stance = ""
            stance_pattern = r'"stnc":"([^"]+)"'
            stance_match = re.search(stance_pattern, html_content)
            if stance_match:
                stance = stance_match.group(1)
            
            # Extract DOB
            dob = ""
            dob_pattern = r'"dob":"([^"]+)"'
            dob_match = re.search(dob_pattern, html_content)
            if dob_match:
                dob = dob_match.group(1)
            
            # Extract team
            team = ""
            team_pattern = r'"tm":"([^"]+)"'
            team_match = re.search(team_pattern, html_content)
            if team_match:
                team = team_match.group(1)
            
            return {
                'Fighter Name': fighter_name,
                'Division': division,
                'Record': record,
                'Height': height,
                'Weight': weight,
                'Reach': reach,
                'Stance': stance,
                'DOB': dob,
                'Team': team
            }
            
        except Exception as e:
            print(f"Error extracting profile info: {e}")
            return {}
    
    def _extract_fight_by_fight_data(self, html_content: str) -> Dict:
        """Extract fight-by-fight data from HTML patterns"""
        try:
            # Look for fight data patterns in the HTML
            fight_records = []
            
            # Pattern to find fight data: date, opponent, result
            # Look for patterns like: "dte":"2025-07-26T16:00:00.000+00:00","txt":"Reinier de Ridder","rslt":"L"
            
            # Extract all fight data using regex
            fight_pattern = r'"dte":"([^"]+)","txt":"([^"]+)","rslt":"([^"]+)"'
            fight_matches = re.findall(fight_pattern, html_content)
            
            for date_str, opponent, result in fight_matches:
                try:
                    # Parse date
                    date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    date_formatted = date.strftime('%Y-%m-%d')
                except:
                    date_formatted = date_str
                
                fight_records.append({
                    'date': date_formatted,
                    'opponent': opponent,
                    'result': result
                })
            
            # Sort fights by date (most recent first)
            fight_records.sort(key=lambda x: x['date'], reverse=True)
            
            # Create the result data
            result_data = {}
            
            # Add most recent fights for First_last_last3, First_last_last4, etc.
            for i in range(3, 11):  # 3 to 10 fights
                if len(fight_records) >= i:
                    recent_fights = fight_records[:i]  # Most recent i fights
                    fight_strings = []
                    for fight in recent_fights:
                        fight_str = f"{fight['result']} {fight['opponent']}"
                        fight_strings.append(fight_str)
                    
                    result_data[f'First_last_last{i}'] = ' | '.join(fight_strings)
                else:
                    result_data[f'First_last_last{i}'] = ''
            
            # Add last fight details
            if fight_records:
                last_fight = fight_records[0]  # Most recent fight
                result_data['Last Fight Date'] = last_fight['date']
                result_data['Last Fight Opponent'] = last_fight['opponent']
                result_data['Last Fight Result'] = last_fight['result']
                result_data['Last Fight Method'] = last_fight['result']  # Simplified for now
                result_data['Last Fight Round'] = ''  # Would need to extract from fight details
                result_data['Last Fight Time'] = ''   # Would need to extract from fight details
            else:
                result_data['Last Fight Date'] = ''
                result_data['Last Fight Opponent'] = ''
                result_data['Last Fight Result'] = ''
                result_data['Last Fight Method'] = ''
                result_data['Last Fight Round'] = ''
                result_data['Last Fight Time'] = ''
            
            # Calculate aggregate statistics
            result_data.update(self._calculate_aggregate_stats(fight_records))
            
            return result_data
            
        except Exception as e:
            print(f"Error extracting fight-by-fight data: {e}")
            return {}
    
    def _calculate_aggregate_stats(self, fight_records: List) -> Dict:
        """Calculate aggregate statistics from fight records"""
        try:
            if not fight_records:
                return {}
            
            # Count wins by method (simplified - would need more detailed extraction)
            wins = sum(1 for fight in fight_records if fight['result'] == 'W')
            losses = sum(1 for fight in fight_records if fight['result'] == 'L')
            
            # Calculate win streak
            win_streak = 0
            for fight in fight_records:
                if fight['result'] == 'W':
                    win_streak += 1
                else:
                    break
            
            return {
                'Wins by KO/TKO': 0,  # Would need detailed extraction
                'Wins by Submission': 0,  # Would need detailed extraction
                'Wins by Decision': wins,  # Simplified
                'Losses by KO/TKO': 0,  # Would need detailed extraction
                'Losses by Submission': 0,  # Would need detailed extraction
                'Losses by Decision': losses,  # Simplified
                'Win Streak': win_streak,
                # Placeholder values for now - would need to extract from detailed stats
                'Striking Accuracy': '',
                'Sig. Strikes Landed': '',
                'Sig. Strikes Attempted': '',
                'Sig. Strikes Landed Per Min': '',
                'Sig. Strikes Absorbed Per Min': '',
                'Takedown Accuracy': '',
                'Takedowns Landed': '',
                'Takedowns Attempted': '',
                'Takedown avg Per 15 Min': '',
                'Submission avg Per 15 Min': '',
                'Sig. Str. Defense': '',
                'Takedown Defense': '',
                'Knockdown Avg': '',
                'Average fight time': ''
            }
            
        except Exception as e:
            print(f"Error calculating aggregate stats: {e}")
            return {}
    
    def process_fighter(self, fighter_name: str) -> Dict:
        """Process a single fighter's data"""
        try:
            # Create filename
            filename = fighter_name.replace(' ', '_') + '.html'
            filepath = self.fighter_html_folder / filename
            
            if not filepath.exists():
                print(f"HTML file not found for {fighter_name}: {filepath}")
                return {}
            
            # Read HTML content
            with open(filepath, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            # Extract fight data
            fight_data = self.extract_fight_data_from_html(html_content, fighter_name)
            
            return fight_data
            
        except Exception as e:
            print(f"Error processing {fighter_name}: {e}")
            return {}
    
    def process_all_fighters(self) -> pd.DataFrame:
        """Process all fighters and return DataFrame"""
        fighters = self.load_fighters_list()
        if not fighters:
            print("No fighters found to process")
            return pd.DataFrame(columns=self.a1_columns)
        
        print(f"Processing {len(fighters)} fighters...")
        
        all_data = []
        for i, fighter in enumerate(fighters, 1):
            print(f"Processing {i}/{len(fighters)}: {fighter}")
            fighter_data = self.process_fighter(fighter)
            if fighter_data:
                all_data.append(fighter_data)
        
        # Create DataFrame
        df = pd.DataFrame(all_data)
        
        # Ensure all columns exist
        for col in self.a1_columns:
            if col not in df.columns:
                df[col] = ''
        
        # Reorder columns to match A1 structure
        df = df[self.a1_columns]
        
        print(f"Successfully processed {len(df)} fighters")
        return df
    
    def save_csv(self, df: pd.DataFrame, filename: str = "direct_fighter_profiles.csv"):
        """Save DataFrame to CSV"""
        output_file = self.data_folder / filename
        df.to_csv(output_file, index=False)
        print(f"Saved direct fighter profiles to: {output_file}")
        return output_file

def main():
    """Main execution function"""
    processor = DirectESPNProcessor()
    
    # Process all fighters
    df = processor.process_all_fighters()
    
    # Save results
    processor.save_csv(df)
    
    print(f"Direct processing complete! Generated {len(df)} fighter profiles")

if __name__ == "__main__":
    main() 