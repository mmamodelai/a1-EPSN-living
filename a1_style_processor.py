#!/usr/bin/env python3
"""
A1-Style ESPN Data Processor - Processes existing ESPN HTML files in A1 format
"""

import json
import re
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
from bs4 import BeautifulSoup

class A1StyleESPNProcessor:
    """Process ESPN HTML files in A1-compatible format"""
    
    def __init__(self, data_folder: str = "data"):
        self.data_folder = Path(data_folder)
        self.fighter_html_folder = self.data_folder / "FighterHTMLs"
        self.fighters_file = self.data_folder / "fighters_name.csv"
        
        # A1-compatible column structure
        self.a1_columns = [
            'Name', 'Division Title', 'Division Record', 'Wins by Knockout', 'Wins by Submission', 
            'First Round Finishes', 'Wins by Decision', 'Striking accuracy', 'Sig. Strikes Landed', 
            'Sig. Strikes Attempted', 'Takedown Accuracy', 'Takedowns Landed', 'Takedowns Attempted', 
            'Sig. Str. Landed Per Min', 'Sig. Str. Absorbed Per Min', 'Takedown avg Per 15 Min', 
            'Submission avg Per 15 Min', 'Sig. Str. Defense', 'Takedown Defense', 'Knockdown Avg', 
            'Average fight time', 'Sig. Str. By Position - Standing', 'Sig. Str. By Position - Clinch', 
            'Sig. Str. By Position - Ground', 'Win by Method - KO/TKO', 'Win by Method - DEC', 
            'Win by Method - SUB', 'Sig. Str. by target - Head Strike Percentage', 
            'Sig. Str. by target - Head Strike Count', 'Sig. Str. by target - Body Strike Percentage', 
            'Sig. Str. by target - Body Strike Count', 'Sig. Str. by target - Leg Strike Percentage', 
            'Sig. Str. by target - Leg Strike Count', 'Event_1_Headline', 'Event_1_Date', 'Event_1_Round', 
            'Event_1_Time', 'Event_1_Method', 'Event_2_Headline', 'Event_2_Date', 'Event_2_Round', 
            'Event_2_Time', 'Event_2_Method', 'Event_3_Headline', 'Event_3_Date', 'Event_3_Round', 
            'Event_3_Time', 'Event_3_Method', 'Status', 'Place of Birth', 'Fighting style', 'Age', 
            'Height', 'Weight', 'Octagon Debut', 'Reach', 'Leg reach', 'Trains at', 'Fight Win Streak', 
            'Title Defenses', 'Former Champion'
        ]
        
    def load_fighters_list(self) -> List[str]:
        """Load fighters list from CSV"""
        if not self.fighters_file.exists():
            print(f"Fighters file not found: {self.fighters_file}")
            return []
        
        df = pd.read_csv(self.fighters_file)
        return df['fighters'].tolist()
    
    def extract_a1_style_data(self, fighter_name: str) -> Optional[Dict]:
        """Extract data from ESPN HTML in A1-compatible format"""
        
        # Read HTML file
        safe_name = fighter_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
        html_file = self.fighter_html_folder / f"{safe_name}.html"
        
        if not html_file.exists():
            print(f"HTML file not found for {fighter_name}: {html_file}")
            return None
        
        with open(html_file, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Extract JSON data from ESPN HTML
        json_data = self._extract_json_from_html(html_content)
        if not json_data:
            print(f"No JSON data found for {fighter_name}")
            return None
        
        # Extract A1-compatible data
        fighter_data = self._extract_a1_style_profile(json_data, fighter_name)
        return fighter_data
    
    def _extract_json_from_html(self, html_content: str) -> Optional[Dict]:
        """Extract JSON data from ESPN HTML content"""
        try:
            json_start_pattern = r'"prtlCmnApiRsp":\s*{'
            start_match = re.search(json_start_pattern, html_content)
            
            if not start_match:
                return None
            
            start_pos = start_match.end() - 1
            brace_count = 0
            in_string = False
            escape_next = False
            end_pos = start_pos
            
            for i, char in enumerate(html_content[start_pos:], start_pos):
                if escape_next:
                    escape_next = False
                    continue
                
                if char == '\\':
                    escape_next = True
                    continue
                
                if char == '"' and not escape_next:
                    in_string = not in_string
                    continue
                
                if not in_string:
                    if char == '{':
                        brace_count += 1
                    elif char == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            end_pos = i + 1
                            break
            
            if brace_count != 0:
                return None
            
            json_str = html_content[start_pos:end_pos]
            json_str = re.sub(r'//.*?\n', '\n', json_str)
            json_str = re.sub(r',\s*}', '}', json_str)
            
            return json.loads(json_str)
            
        except Exception as e:
            print(f"Error extracting JSON: {e}")
            return None
    
    def _extract_a1_style_profile(self, json_data: Dict, fighter_name: str) -> Dict:
        """Extract profile data in A1-compatible format from ESPN JSON"""
        athlete = json_data.get('athlete', {})
        
        # Extract basic info
        display_name = athlete.get('displayName', fighter_name)
        weight_class = athlete.get('weightClass', {})
        weight_class_text = weight_class.get('text', '') if isinstance(weight_class, dict) else str(weight_class)
        
        # Extract record and stats from statsSummary
        record = '0-0-0'
        ko_wins = 0
        sub_wins = 0
        dec_wins = 0
        
        stats_summary = athlete.get('statsSummary', {})
        if 'statistics' in stats_summary:
            for stat in stats_summary['statistics']:
                stat_name = stat.get('name', '')
                stat_value = stat.get('displayValue', '')
                
                if stat_name == 'wins-losses-draws':
                    record = stat_value
                elif stat_name == 'tkos-tkoLosses':
                    if '-' in stat_value:
                        ko_wins = int(stat_value.split('-')[0])
                elif stat_name == 'submissions-submissionLosses':
                    if '-' in stat_value:
                        sub_wins = int(stat_value.split('-')[0])
        
        # Calculate decision wins
        if '-' in record:
            total_wins = int(record.split('-')[0])
            dec_wins = total_wins - ko_wins - sub_wins
        
        # Extract stance
        stance_data = athlete.get('stance', {})
        stance = stance_data.get('text', '') if isinstance(stance_data, dict) else str(stance_data)
        
        # Extract fight history
        events_map = json_data.get('eventsMap', {})
        fights = []
        
        for event_id, event_data in events_map.items():
            if not isinstance(event_data, dict):
                continue
            
            fight = {
                'event_id': event_id,
                'date': event_data.get('gameDate', ''),
                'result': event_data.get('gameResult', ''),
                'event_name': event_data.get('name', ''),
                'title_fight': event_data.get('titleFight', False),
                'opponent': event_data.get('opponent', {}).get('displayName', ''),
                'finish_method': event_data.get('status', {}).get('result', {}).get('displayName', ''),
                'round': event_data.get('status', {}).get('period', ''),
                'time': event_data.get('status', {}).get('displayClock', '')
            }
            
            fights.append(fight)
        
        # Sort by date (newest first)
        fights.sort(key=lambda x: x['date'], reverse=True)
        
        # Calculate additional stats from fight history
        win_streak = 0
        title_defenses = 0
        former_champion = False
        
        for fight in fights:
            if fight['result'] == 'W':
                win_streak += 1
            else:
                break
            
            if fight['title_fight'] and fight['result'] == 'W':
                title_defenses += 1
                former_champion = True
        
        # Create A1-compatible data structure
        fighter_data = {
            'Name': display_name,
            'Division Title': weight_class_text,
            'Division Record': record,
            'Wins by Knockout': ko_wins,
            'Wins by Submission': sub_wins,
            'First Round Finishes': 0,  # ESPN doesn't provide this
            'Wins by Decision': dec_wins,
            'Striking accuracy': '',  # ESPN doesn't provide this
            'Sig. Strikes Landed': '',  # ESPN doesn't provide this
            'Sig. Strikes Attempted': '',  # ESPN doesn't provide this
            'Takedown Accuracy': '',  # ESPN doesn't provide this
            'Takedowns Landed': '',  # ESPN doesn't provide this
            'Takedowns Attempted': '',  # ESPN doesn't provide this
            'Sig. Str. Landed Per Min': '',  # ESPN doesn't provide this
            'Sig. Str. Absorbed Per Min': '',  # ESPN doesn't provide this
            'Takedown avg Per 15 Min': '',  # ESPN doesn't provide this
            'Submission avg Per 15 Min': '',  # ESPN doesn't provide this
            'Sig. Str. Defense': '',  # ESPN doesn't provide this
            'Takedown Defense': '',  # ESPN doesn't provide this
            'Knockdown Avg': '',  # ESPN doesn't provide this
            'Average fight time': '',  # ESPN doesn't provide this
            'Sig. Str. By Position - Standing': '',  # ESPN doesn't provide this
            'Sig. Str. By Position - Clinch': '',  # ESPN doesn't provide this
            'Sig. Str. By Position - Ground': '',  # ESPN doesn't provide this
            'Win by Method - KO/TKO': ko_wins,
            'Win by Method - DEC': dec_wins,
            'Win by Method - SUB': sub_wins,
            'Sig. Str. by target - Head Strike Percentage': '',  # ESPN doesn't provide this
            'Sig. Str. by target - Head Strike Count': '',  # ESPN doesn't provide this
            'Sig. Str. by target - Body Strike Percentage': '',  # ESPN doesn't provide this
            'Sig. Str. by target - Body Strike Count': '',  # ESPN doesn't provide this
            'Sig. Str. by target - Leg Strike Percentage': '',  # ESPN doesn't provide this
            'Sig. Str. by target - Leg Strike Count': '',  # ESPN doesn't provide this
            'Status': 'Active' if athlete.get('active', False) else 'Inactive',
            'Place of Birth': '',  # ESPN doesn't provide this
            'Fighting style': stance,
            'Age': athlete.get('age', ''),
            'Height': athlete.get('displayHeight', ''),
            'Weight': athlete.get('displayWeight', ''),
            'Octagon Debut': '',  # ESPN doesn't provide this
            'Reach': athlete.get('displayReach', ''),
            'Leg reach': '',  # ESPN doesn't provide this
            'Trains at': athlete.get('team', {}).get('name', ''),
            'Fight Win Streak': win_streak,
            'Title Defenses': title_defenses,
            'Former Champion': former_champion
        }
        
        # Add fight history (up to 3 most recent fights)
        for i, fight in enumerate(fights[:3]):
            fight_num = i + 1
            fighter_data[f'Event_{fight_num}_Headline'] = fight['event_name']
            fighter_data[f'Event_{fight_num}_Date'] = fight['date']
            fighter_data[f'Event_{fight_num}_Round'] = fight['round']
            fighter_data[f'Event_{fight_num}_Time'] = fight['time']
            fighter_data[f'Event_{fight_num}_Method'] = fight['finish_method']
        
        # Fill remaining fight slots with empty values
        for i in range(len(fights), 3):
            fight_num = i + 1
            fighter_data[f'Event_{fight_num}_Headline'] = ''
            fighter_data[f'Event_{fight_num}_Date'] = ''
            fighter_data[f'Event_{fight_num}_Round'] = ''
            fighter_data[f'Event_{fight_num}_Time'] = ''
            fighter_data[f'Event_{fight_num}_Method'] = ''
        
        return fighter_data
    
    def process_all_fighters(self) -> pd.DataFrame:
        """Process all fighters and create A1-compatible dataset"""
        fighters_list = self.load_fighters_list()
        
        if not fighters_list:
            print("No fighters list found!")
            return pd.DataFrame()
        
        print(f"Processing {len(fighters_list)} fighters...")
        
        all_data = []
        processed_count = 0
        failed_count = 0
        
        for i, fighter_name in enumerate(fighters_list, 1):
            print(f"Processing {i}/{len(fighters_list)}: {fighter_name}")
            
            fighter_data = self.extract_a1_style_data(fighter_name)
            if fighter_data:
                all_data.append(fighter_data)
                processed_count += 1
                print(f"  ✓ Extracted data for {fighter_name}")
            else:
                failed_count += 1
                print(f"  ✗ No data found for {fighter_name}")
        
        if all_data:
            # Create DataFrame with A1 column structure
            df = pd.DataFrame(all_data)
            
            # Ensure all A1 columns are present
            for col in self.a1_columns:
                if col not in df.columns:
                    df[col] = ''
            
            # Reorder columns to match A1 structure
            df = df[self.a1_columns]
            
            print(f"\n=== PROCESSING COMPLETE ===")
            print(f"Successfully processed: {processed_count} fighters")
            print(f"Failed to process: {failed_count} fighters")
            print(f"Total records: {len(df)}")
            
            return df
        else:
            print("No data extracted!")
            return pd.DataFrame()
    
    def save_a1_compatible_csv(self, df: pd.DataFrame, filename: str = "fighter_profiles_a1_style.csv"):
        """Save DataFrame in A1-compatible format"""
        output_file = self.data_folder / filename
        df.to_csv(output_file, index=False)
        print(f"Saved A1-compatible data to: {output_file}")
        return output_file

def main():
    """Run the A1-style processor"""
    processor = A1StyleESPNProcessor()
    
    print("=== A1-STYLE ESPN PROCESSOR ===")
    print("Processing existing ESPN HTML files in A1-compatible format...")
    
    # Process all fighters
    df = processor.process_all_fighters()
    
    if not df.empty:
        # Save to file
        output_file = processor.save_a1_compatible_csv(df)
        
        print(f"\n=== SAMPLE DATA ===")
        print(f"Columns: {len(df.columns)}")
        print(f"Records: {len(df)}")
        print("\nFirst 3 fighters:")
        print(df[['Name', 'Division Title', 'Division Record', 'Wins by Knockout', 'Wins by Submission', 'Wins by Decision']].head(3))
        
        # Check for Robert Whittaker specifically
        whittaker = df[df['Name'] == 'Robert Whittaker']
        if not whittaker.empty:
            print(f"\n=== ROBERT WHITTAKER DATA ===")
            print(f"Record: {whittaker.iloc[0]['Division Record']}")
            print(f"KO Wins: {whittaker.iloc[0]['Wins by Knockout']}")
            print(f"Sub Wins: {whittaker.iloc[0]['Wins by Submission']}")
            print(f"Dec Wins: {whittaker.iloc[0]['Wins by Decision']}")
            print(f"Event 1: {whittaker.iloc[0]['Event_1_Headline']}")
            print(f"Event 2: {whittaker.iloc[0]['Event_2_Headline']}")
            print(f"Event 3: {whittaker.iloc[0]['Event_3_Headline']}")
    else:
        print("No data was processed!")

if __name__ == "__main__":
    main() 