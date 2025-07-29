#!/usr/bin/env python3
"""
A1-Compatible ESPN Data Extractor - Matches A1 system data structure exactly
"""

import json
import re
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

class A1CompatibleESPNExtractor:
    """Extract ESPN data in A1-compatible format"""
    
    def __init__(self, data_folder: str = "data"):
        self.data_folder = Path(data_folder)
        self.fighter_html_folder = self.data_folder / "FighterHTMLs"
        
    def extract_a1_compatible_data(self, fighter_name: str) -> Optional[Dict]:
        """Extract data in A1-compatible format"""
        
        # Read HTML file
        safe_name = fighter_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
        html_file = self.fighter_html_folder / f"{safe_name}.html"
        
        if not html_file.exists():
            print(f"HTML file not found for {fighter_name}: {html_file}")
            return None
        
        with open(html_file, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Extract JSON data
        json_data = self._extract_json_from_html(html_content)
        if not json_data:
            return None
        
        # Extract A1-compatible data
        fighter_data = {
            'fighter_name': fighter_name,
            'profile': self._extract_a1_profile(json_data),
            'fight_history': self._extract_fight_history(json_data),
            'extracted_at': datetime.now().isoformat()
        }
        
        return fighter_data
    
    def _extract_json_from_html(self, html_content: str) -> Optional[Dict]:
        """Extract JSON data from HTML content"""
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
    
    def _extract_a1_profile(self, json_data: Dict) -> Dict:
        """Extract profile data in A1-compatible format"""
        athlete = json_data.get('athlete', {})
        
        # Extract basic info
        display_name = athlete.get('displayName', '')
        weight_class = athlete.get('weightClass', {})
        weight_class_text = weight_class.get('text', '') if isinstance(weight_class, dict) else str(weight_class)
        
        # Extract record from statsSummary
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
                    # Parse "11-3" format to get KO wins
                    if '-' in stat_value:
                        ko_wins = int(stat_value.split('-')[0])
                elif stat_name == 'submissions-submissionLosses':
                    # Parse "5-2" format to get submission wins
                    if '-' in stat_value:
                        sub_wins = int(stat_value.split('-')[0])
        
        # Calculate decision wins (total wins - KO wins - sub wins)
        if '-' in record:
            total_wins = int(record.split('-')[0])
            dec_wins = total_wins - ko_wins - sub_wins
        
        # Extract stance
        stance_data = athlete.get('stance', {})
        stance = stance_data.get('text', '') if isinstance(stance_data, dict) else str(stance_data)
        
        # Extract country
        country_data = athlete.get('citizenshipCountry', {})
        country = country_data.get('abbreviation', '') if isinstance(country_data, dict) else str(country_data)
        
        profile = {
            'Name': display_name,
            'Division_Title': weight_class_text,
            'Division_Record': record,
            'Wins_by_Knockout': ko_wins,
            'Wins_by_Submission': sub_wins,
            'First_Round_Finishes': 0,  # ESPN doesn't provide this directly
            'Wins_by_Decision': dec_wins,
            'Striking_accuracy': '',  # ESPN doesn't provide this
            'Sig_Strikes_Landed': '',  # ESPN doesn't provide this
            'Sig_Strikes_Attempted': '',  # ESPN doesn't provide this
            'Takedown_Accuracy': '',  # ESPN doesn't provide this
            'Takedowns_Landed': '',  # ESPN doesn't provide this
            'Takedowns_Attempted': '',  # ESPN doesn't provide this
            'Sig_Str_Landed_Per_Min': '',  # ESPN doesn't provide this
            'Sig_Str_Absorbed_Per_Min': '',  # ESPN doesn't provide this
            'Takedown_avg_Per_15_Min': '',  # ESPN doesn't provide this
            'Submission_avg_Per_15_Min': '',  # ESPN doesn't provide this
            'Sig_Str_Defense': '',  # ESPN doesn't provide this
            'Takedown_Defense': '',  # ESPN doesn't provide this
            'Knockdown_Avg': '',  # ESPN doesn't provide this
            'Average_fight_time': '',  # ESPN doesn't provide this
            'Sig_Str_By_Position_Standing': '',  # ESPN doesn't provide this
            'Sig_Str_By_Position_Clinch': '',  # ESPN doesn't provide this
            'Sig_Str_By_Position_Ground': '',  # ESPN doesn't provide this
            'Win_by_Method_KO_TKO': ko_wins,
            'Win_by_Method_DEC': dec_wins,
            'Win_by_Method_SUB': sub_wins,
            'Sig_Str_by_target_Head_Strike_Percentage': '',  # ESPN doesn't provide this
            'Sig_Str_by_target_Head_Strike_Count': '',  # ESPN doesn't provide this
            'Sig_Str_by_target_Body_Strike_Percentage': '',  # ESPN doesn't provide this
            'Sig_Str_by_target_Body_Strike_Count': '',  # ESPN doesn't provide this
            'Sig_Str_by_target_Leg_Strike_Percentage': '',  # ESPN doesn't provide this
            'Sig_Str_by_target_Leg_Strike_Count': '',  # ESPN doesn't provide this
            'Status': 'Active' if athlete.get('active', False) else 'Inactive',
            'Place_of_Birth': '',  # ESPN doesn't provide this
            'Fighting_style': stance,
            'Age': athlete.get('age', ''),
            'Height': athlete.get('displayHeight', ''),
            'Weight': athlete.get('displayWeight', ''),
            'Octagon_Debut': '',  # ESPN doesn't provide this
            'Reach': athlete.get('displayReach', ''),
            'Leg_reach': '',  # ESPN doesn't provide this
            'Trains_at': athlete.get('team', {}).get('name', ''),
            'Fight_Win_Streak': 0,  # Will calculate from fight history
            'Title_Defenses': 0,  # Will calculate from fight history
            'Former_Champion': False  # Will determine from fight history
        }
        
        return profile
    
    def _extract_fight_history(self, json_data: Dict) -> List[Dict]:
        """Extract fight history for A1-compatible format"""
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
        
        return fights
    
    def create_a1_compatible_csv(self, fighter_data: Dict) -> pd.DataFrame:
        """Create A1-compatible CSV with fight-by-fight data"""
        profile = fighter_data['profile']
        fights = fighter_data['fight_history']
        
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
        
        # Update profile with calculated stats
        profile['Fight_Win_Streak'] = win_streak
        profile['Title_Defenses'] = title_defenses
        profile['Former_Champion'] = former_champion
        
        # Create A1-compatible columns
        row = {
            'Name': profile['Name'],
            'Division Title': profile['Division_Title'],
            'Division Record': profile['Division_Record'],
            'Wins by Knockout': profile['Wins_by_Knockout'],
            'Wins by Submission': profile['Wins_by_Submission'],
            'First Round Finishes': profile['First_Round_Finishes'],
            'Wins by Decision': profile['Wins_by_Decision'],
            'Striking accuracy': profile['Striking_accuracy'],
            'Sig. Strikes Landed': profile['Sig_Strikes_Landed'],
            'Sig. Strikes Attempted': profile['Sig_Strikes_Attempted'],
            'Takedown Accuracy': profile['Takedown_Accuracy'],
            'Takedowns Landed': profile['Takedowns_Landed'],
            'Takedowns Attempted': profile['Takedowns_Attempted'],
            'Sig. Str. Landed Per Min': profile['Sig_Str_Landed_Per_Min'],
            'Sig. Str. Absorbed Per Min': profile['Sig_Str_Absorbed_Per_Min'],
            'Takedown avg Per 15 Min': profile['Takedown_avg_Per_15_Min'],
            'Submission avg Per 15 Min': profile['Submission_avg_Per_15_Min'],
            'Sig. Str. Defense': profile['Sig_Str_Defense'],
            'Takedown Defense': profile['Takedown_Defense'],
            'Knockdown Avg': profile['Knockdown_Avg'],
            'Average fight time': profile['Average_fight_time'],
            'Sig. Str. By Position - Standing': profile['Sig_Str_By_Position_Standing'],
            'Sig. Str. By Position - Clinch': profile['Sig_Str_By_Position_Clinch'],
            'Sig. Str. By Position - Ground': profile['Sig_Str_By_Position_Ground'],
            'Win by Method - KO/TKO': profile['Win_by_Method_KO_TKO'],
            'Win by Method - DEC': profile['Win_by_Method_DEC'],
            'Win by Method - SUB': profile['Win_by_Method_SUB'],
            'Sig. Str. by target - Head Strike Percentage': profile['Sig_Str_by_target_Head_Strike_Percentage'],
            'Sig. Str. by target - Head Strike Count': profile['Sig_Str_by_target_Head_Strike_Count'],
            'Sig. Str. by target - Body Strike Percentage': profile['Sig_Str_by_target_Body_Strike_Percentage'],
            'Sig. Str. by target - Body Strike Count': profile['Sig_Str_by_target_Body_Strike_Count'],
            'Sig. Str. by target - Leg Strike Percentage': profile['Sig_Str_by_target_Leg_Strike_Percentage'],
            'Sig. Str. by target - Leg Strike Count': profile['Sig_Str_by_target_Leg_Strike_Count'],
            'Status': profile['Status'],
            'Place of Birth': profile['Place_of_Birth'],
            'Fighting style': profile['Fighting_style'],
            'Age': profile['Age'],
            'Height': profile['Height'],
            'Weight': profile['Weight'],
            'Octagon Debut': profile['Octagon_Debut'],
            'Reach': profile['Reach'],
            'Leg reach': profile['Leg_reach'],
            'Trains at': profile['Trains_at'],
            'Fight Win Streak': profile['Fight_Win_Streak'],
            'Title Defenses': profile['Title_Defenses'],
            'Former Champion': profile['Former_Champion']
        }
        
        # Add fight history columns (up to 3 most recent fights)
        for i, fight in enumerate(fights[:3]):
            fight_num = i + 1
            row[f'Event_{fight_num}_Headline'] = fight['event_name']
            row[f'Event_{fight_num}_Date'] = fight['date']
            row[f'Event_{fight_num}_Round'] = fight['round']
            row[f'Event_{fight_num}_Time'] = fight['time']
            row[f'Event_{fight_num}_Method'] = fight['finish_method']
        
        # Fill remaining fight slots with empty values
        for i in range(len(fights), 3):
            fight_num = i + 1
            row[f'Event_{fight_num}_Headline'] = ''
            row[f'Event_{fight_num}_Date'] = ''
            row[f'Event_{fight_num}_Round'] = ''
            row[f'Event_{fight_num}_Time'] = ''
            row[f'Event_{fight_num}_Method'] = ''
        
        return pd.DataFrame([row])
    
    def process_all_fighters(self, fighters_list: List[str]) -> pd.DataFrame:
        """Process all fighters and create A1-compatible dataset"""
        all_data = []
        
        for fighter_name in fighters_list:
            print(f"Processing {fighter_name}...")
            
            fighter_data = self.extract_a1_compatible_data(fighter_name)
            if fighter_data:
                df = self.create_a1_compatible_csv(fighter_data)
                all_data.append(df)
                print(f"  Extracted A1-compatible data for {fighter_name}")
            else:
                print(f"  No data found for {fighter_name}")
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            return combined_df
        else:
            return pd.DataFrame()

def main():
    """Test the A1-compatible extractor with Robert Whittaker"""
    extractor = A1CompatibleESPNExtractor()
    
    # Test with Robert Whittaker
    fighter_data = extractor.extract_a1_compatible_data("Robert Whittaker")
    
    if fighter_data:
        print("=== A1-COMPATIBLE PROFILE ===")
        profile = fighter_data['profile']
        print(f"Name: {profile['Name']}")
        print(f"Division: {profile['Division_Title']}")
        print(f"Record: {profile['Division_Record']}")
        print(f"KO Wins: {profile['Wins_by_Knockout']}")
        print(f"Sub Wins: {profile['Wins_by_Submission']}")
        print(f"Dec Wins: {profile['Wins_by_Decision']}")
        print(f"Fighting Style: {profile['Fighting_style']}")
        print(f"Age: {profile['Age']}")
        print(f"Height: {profile['Height']}")
        print(f"Weight: {profile['Weight']}")
        print(f"Reach: {profile['Reach']}")
        print(f"Team: {profile['Trains_at']}")
        
        print(f"\n=== FIGHT HISTORY ({len(fighter_data['fight_history'])} fights) ===")
        for i, fight in enumerate(fighter_data['fight_history'][:3]):
            print(f"Fight {i+1}: {fight['event_name']} - {fight['result']} vs {fight['opponent']} ({fight['finish_method']})")
        
        # Create A1-compatible CSV
        df = extractor.create_a1_compatible_csv(fighter_data)
        print(f"\n=== A1-COMPATIBLE CSV ({len(df)} rows) ===")
        print("Columns:", list(df.columns))
        print(df.head())
        
        # Save to file
        output_file = extractor.data_folder / "a1_compatible_fighter_profiles.csv"
        df.to_csv(output_file, index=False)
        print(f"\nSaved A1-compatible data to: {output_file}")
        
    else:
        print("No data extracted for Robert Whittaker")

if __name__ == "__main__":
    main() 