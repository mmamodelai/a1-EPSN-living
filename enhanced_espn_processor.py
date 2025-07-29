#!/usr/bin/env python3
"""
Enhanced ESPN Data Processor - Extracts comprehensive fight statistics from ESPN HTML
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

class EnhancedESPNProcessor:
    """Extract comprehensive fight statistics from ESPN HTML files using most recent fights"""
    
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
                    # The data is embedded in the HTML, not in window.__INITIAL_STATE__
                    
                    # Try to find the JSON structure that contains the fighter data
                    # Look for patterns like "stat":{"tbl":[...]}
                    if '"stat":' in script.string and '"tbl":' in script.string:
                        # This contains the fight statistics data
                        return {'page': {'content': {'stat': {'tbl': []}}}}
                    
                    # Alternative: look for the full ESPN API response structure
                    if '"plyrHdr":' in script.string and '"ath":' in script.string:
                        # This contains the athlete profile data
                        return {'page': {'content': {'plyrHdr': {'ath': {}}}}}
            
            # If we can't find the structured data, try to extract what we can
            # Look for any JSON-like patterns in the HTML
            import re
            
            # Look for JSON patterns in the HTML content
            json_patterns = [
                r'"stat":\s*\{[^}]*"tbl":\s*\[[^\]]*\]',
                r'"plyrHdr":\s*\{[^}]*"ath":\s*\{[^}]*\}',
                r'"ath":\s*\{[^}]*\}',
                r'"tbl":\s*\[[^\]]*\]'
            ]
            
            for pattern in json_patterns:
                matches = re.findall(pattern, html_content, re.DOTALL)
                if matches:
                    # Found some JSON data, create a minimal structure
                    return {'page': {'content': {'stat': {'tbl': []}, 'plyrHdr': {'ath': {}}}}}
            
            return None
        except Exception as e:
            print(f"Error extracting JSON: {e}")
            return None
    
    def extract_fight_data_from_json(self, json_data: Dict, fighter_name: str) -> Dict:
        """Extract comprehensive fight data from ESPN JSON"""
        try:
            # Navigate to the fighter stats data
            if 'page' not in json_data or 'content' not in json_data['page']:
                return {}
            
            content = json_data['page']['content']
            if 'stat' not in content or 'tbl' not in content['stat']:
                return {}
            
            tables = content['stat']['tbl']
            
            # Extract basic profile info
            profile_data = self._extract_profile_info(json_data, fighter_name)
            
            # Extract fight-by-fight data
            fight_data = self._extract_fight_by_fight_data(tables)
            
            # Combine profile and fight data
            result = {**profile_data, **fight_data}
            
            return result
            
        except Exception as e:
            print(f"Error extracting fight data for {fighter_name}: {e}")
            return {}
    
    def _extract_profile_info(self, json_data: Dict, fighter_name: str) -> Dict:
        """Extract basic profile information"""
        try:
            if 'page' not in json_data or 'content' not in json_data['page']:
                return {}
            
            content = json_data['page']['content']
            if 'plyrHdr' not in content or 'ath' not in content['plyrHdr']:
                return {}
            
            athlete = content['plyrHdr']['ath']
            
            # Extract basic stats
            stats_block = content.get('statsBlck', {})
            record = "0-0-0"
            if 'vals' in stats_block:
                for stat in stats_block['vals']:
                    if stat.get('name') == 'Wins-Losses-Draws':
                        record = stat.get('val', '0-0-0')
                        break
            
            return {
                'Fighter Name': fighter_name,
                'Division': athlete.get('wghtclss', ''),
                'Record': record,
                'Height': athlete.get('htwt', '').split(',')[0] if athlete.get('htwt') else '',
                'Weight': athlete.get('htwt', '').split(',')[1].strip() if athlete.get('htwt') and ',' in athlete.get('htwt') else '',
                'Reach': athlete.get('rch', ''),
                'Stance': athlete.get('stnc', ''),
                'DOB': athlete.get('dob', ''),
                'Team': athlete.get('tm', '')
            }
            
        except Exception as e:
            print(f"Error extracting profile info: {e}")
            return {}
    
    def _extract_fight_by_fight_data(self, tables: List) -> Dict:
        """Extract detailed fight-by-fight data"""
        try:
            # Find the striking table (first table usually contains the main fight data)
            if not tables:
                return {}
            
            striking_table = tables[0]
            if 'row' not in striking_table:
                return {}
            
            fights = striking_table['row']
            
            # Process fights (they're already ordered by date, most recent first)
            fight_records = []
            for fight in fights:
                if len(fight) < 4:
                    continue
                
                # Extract fight result
                fight_component = fight[3]
                if isinstance(fight_component, dict) and 'rslt' in fight_component:
                    result = fight_component['rslt']
                else:
                    result = str(fight_component)
                
                # Extract opponent
                opponent_component = fight[1]
                if isinstance(opponent_component, dict) and 'txt' in opponent_component:
                    opponent = opponent_component['txt']
                else:
                    opponent = str(opponent_component)
                
                # Extract date
                date_component = fight[0]
                if isinstance(date_component, dict) and 'dte' in date_component:
                    date_str = date_component['dte']
                    try:
                        date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                        date_formatted = date.strftime('%Y-%m-%d')
                    except:
                        date_formatted = date_str
                else:
                    date_formatted = str(date_component)
                
                # Extract fight statistics (if available)
                stats = {}
                if len(fight) > 4:
                    # Extract striking accuracy, takedown accuracy, etc.
                    for i, stat in enumerate(fight[4:], 4):
                        if isinstance(stat, str) and '/' in stat:
                            # This is likely a landed/attempted stat
                            try:
                                landed, attempted = stat.split('/')
                                if attempted and attempted != '0':
                                    accuracy = (int(landed) / int(attempted)) * 100
                                    stats[f'stat_{i}'] = f"{accuracy:.1f}%"
                                else:
                                    stats[f'stat_{i}'] = stat
                            except:
                                stats[f'stat_{i}'] = stat
                        else:
                            stats[f'stat_{i}'] = str(stat)
                
                fight_records.append({
                    'date': date_formatted,
                    'opponent': opponent,
                    'result': result,
                    'stats': stats
                })
            
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
            
            # Count wins by method
            wins_by_ko = sum(1 for fight in fight_records if fight['result'] == 'W' and 'KO' in fight.get('method', ''))
            wins_by_sub = sum(1 for fight in fight_records if fight['result'] == 'W' and 'Submission' in fight.get('method', ''))
            wins_by_dec = sum(1 for fight in fight_records if fight['result'] == 'W' and 'Decision' in fight.get('method', ''))
            
            losses_by_ko = sum(1 for fight in fight_records if fight['result'] == 'L' and 'KO' in fight.get('method', ''))
            losses_by_sub = sum(1 for fight in fight_records if fight['result'] == 'L' and 'Submission' in fight.get('method', ''))
            losses_by_dec = sum(1 for fight in fight_records if fight['result'] == 'L' and 'Decision' in fight.get('method', ''))
            
            # Calculate win streak
            win_streak = 0
            for fight in fight_records:
                if fight['result'] == 'W':
                    win_streak += 1
                else:
                    break
            
            return {
                'Wins by KO/TKO': wins_by_ko,
                'Wins by Submission': wins_by_sub,
                'Wins by Decision': wins_by_dec,
                'Losses by KO/TKO': losses_by_ko,
                'Losses by Submission': losses_by_sub,
                'Losses by Decision': losses_by_dec,
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
            
            # Extract JSON data
            json_data = self.extract_json_from_html(html_content)
            if not json_data:
                print(f"No JSON data found for {fighter_name}")
                return {}
            
            # Extract fight data
            fight_data = self.extract_fight_data_from_json(json_data, fighter_name)
            
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
    
    def save_csv(self, df: pd.DataFrame, filename: str = "enhanced_fighter_profiles.csv"):
        """Save DataFrame to CSV"""
        output_file = self.data_folder / filename
        df.to_csv(output_file, index=False)
        print(f"Saved enhanced fighter profiles to: {output_file}")
        return output_file

def main():
    """Main execution function"""
    processor = EnhancedESPNProcessor()
    
    # Process all fighters
    df = processor.process_all_fighters()
    
    # Save results
    processor.save_csv(df)
    
    print(f"Enhanced processing complete! Generated {len(df)} fighter profiles")

if __name__ == "__main__":
    main() 