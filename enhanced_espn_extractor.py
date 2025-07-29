#!/usr/bin/env python3
"""
Enhanced ESPN Data Extractor - Extracts comprehensive fight statistics from ESPN HTML
Matches A1 system data structure with detailed fight-by-fight statistics
"""

import json
import re
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any
from bs4 import BeautifulSoup

class EnhancedESPNExtractor:
    """Extract comprehensive fight statistics from ESPN HTML files"""
    
    def __init__(self, data_folder: str = "data"):
        self.data_folder = Path(data_folder)
        self.fighter_html_folder = self.data_folder / "FighterHTMLs"
        
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
    
    def extract_json_data(self, html_content: str) -> Optional[Dict[str, Any]]:
        """Extract JSON data from ESPN HTML"""
        try:
            # Find the prtlCmnApiRsp JSON object
            pattern = r'window\.__INITIAL_STATE__\s*=\s*({.*?});'
            match = re.search(pattern, html_content, re.DOTALL)
            if match:
                json_str = match.group(1)
                data = json.loads(json_str)
                return data
            return None
        except Exception as e:
            print(f"Error extracting JSON data: {e}")
            return None
    
    def extract_fighter_profile(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract basic fighter profile information"""
        profile = {}
        
        try:
            # Extract athlete information
            if 'ath' in json_data.get('plyrHdr', {}):
                ath = json_data['plyrHdr']['ath']
                profile['Name'] = ath.get('dspNm', '')
                profile['Division Title'] = ath.get('wghtclss', '')
                profile['Status'] = 'Active'  # Default to active
                profile['Place of Birth'] = ath.get('cntry', '')
                profile['Fighting style'] = ath.get('stnc', '')
                profile['Age'] = self._extract_age(ath.get('dob', ''))
                profile['Height'] = self._extract_height(ath.get('htwt', ''))
                profile['Weight'] = self._extract_weight(ath.get('htwt', ''))
                profile['Reach'] = self._extract_reach(ath.get('rch', ''))
                profile['Trains at'] = ath.get('tm', '')
            
            # Extract record from stats block
            if 'statsBlck' in json_data.get('plyrHdr', {}):
                stats = json_data['plyrHdr']['statsBlck']
                for val in stats.get('vals', []):
                    if val.get('lbl') == 'W-L-D':
                        profile['Division Record'] = val.get('val', '')
                    elif val.get('lbl') == '(T)KO':
                        ko_stats = val.get('val', '').split('-')
                        if len(ko_stats) >= 2:
                            profile['Wins by Knockout'] = ko_stats[0]
                    elif val.get('lbl') == 'SUB':
                        sub_stats = val.get('val', '').split('-')
                        if len(sub_stats) >= 2:
                            profile['Wins by Submission'] = sub_stats[0]
            
        except Exception as e:
            print(f"Error extracting fighter profile: {e}")
        
        return profile
    
    def extract_detailed_statistics(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract detailed fight-by-fight statistics"""
        stats = {}
        
        try:
            # Extract statistics from the stat table
            if 'stat' in json_data and 'tbl' in json_data['stat']:
                tables = json_data['stat']['tbl']
                
                # Process each table (Striking, Clinch, Ground)
                for table in tables:
                    table_title = table.get('ttl', '').lower()
                    rows = table.get('row', [])
                    
                    if rows:
                        # Get the most recent fight data (first row)
                        recent_fight = rows[0]
                        cols = table.get('col', [])
                        
                        for i, col in enumerate(cols):
                            if i < len(recent_fight):
                                col_title = col.get('ttl', '')
                                value = recent_fight[i]
                                
                                # Map ESPN columns to A1 columns
                                if 'Significant Strikes Landed' in col_title:
                                    stats['Sig. Strikes Landed'] = value
                                elif 'Significant Strikes Attempts' in col_title:
                                    stats['Sig. Strikes Attempted'] = value
                                elif 'Total Strikes Landed' in col_title:
                                    stats['Total Strikes Landed'] = value
                                elif 'Total Strikes Attempts' in col_title:
                                    stats['Total Strikes Attempted'] = value
                                elif 'Takedowns Landed' in col_title:
                                    stats['Takedowns Landed'] = value
                                elif 'Takedowns Attempted' in col_title:
                                    stats['Takedowns Attempted'] = value
                                elif 'Knockdowns' in col_title:
                                    stats['Knockdowns'] = value
                                elif 'Target Breakdown Head' in col_title:
                                    stats['Sig. Str. by target - Head Strike Percentage'] = value
                                elif 'Target Breakdown Body' in col_title:
                                    stats['Sig. Str. by target - Body Strike Percentage'] = value
                                elif 'Target Breakdown Leg' in col_title:
                                    stats['Sig. Str. by target - Leg Strike Percentage'] = value
                        
                        # Calculate derived statistics
                        self._calculate_derived_stats(stats)
        
        except Exception as e:
            print(f"Error extracting detailed statistics: {e}")
        
        return stats
    
    def extract_fight_history(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract recent fight history"""
        fights = {}
        
        try:
            # Extract events from the eventsMap
            events_map = json_data.get('eventsMap', {})
            recent_events = []
            
            for event_id, event_data in events_map.items():
                if isinstance(event_data, dict) and 'gameDate' in event_data:
                    recent_events.append(event_data)
            
            # Sort by date (most recent first)
            recent_events.sort(key=lambda x: x.get('gameDate', ''), reverse=True)
            
            # Extract the 3 most recent fights
            for i, event in enumerate(recent_events[:3]):
                prefix = f'Event_{i+1}_'
                
                fights[f'{prefix}Headline'] = event.get('name', '')
                fights[f'{prefix}Date'] = event.get('gameDate', '')
                
                # Extract fight details
                status = event.get('status', {})
                fights[f'{prefix}Round'] = str(status.get('period', ''))
                fights[f'{prefix}Time'] = status.get('displayClock', '')
                
                result = status.get('result', {})
                fights[f'{prefix}Method'] = result.get('displayName', '')
        
        except Exception as e:
            print(f"Error extracting fight history: {e}")
        
        return fights
    
    def _calculate_derived_stats(self, stats: Dict[str, Any]):
        """Calculate derived statistics"""
        try:
            # Calculate striking accuracy
            sig_landed = self._safe_int(stats.get('Sig. Strikes Landed', 0))
            sig_attempted = self._safe_int(stats.get('Sig. Strikes Attempted', 0))
            if sig_attempted > 0:
                stats['Striking accuracy'] = f"{round((sig_landed / sig_attempted) * 100)}%"
            
            # Calculate takedown accuracy
            td_landed = self._safe_int(stats.get('Takedowns Landed', 0))
            td_attempted = self._safe_int(stats.get('Takedowns Attempted', 0))
            if td_attempted > 0:
                stats['Takedown Accuracy'] = f"{round((td_landed / td_attempted) * 100)}%"
            
            # Calculate wins by method
            record = stats.get('Division Record', '0-0-0')
            wins = self._extract_wins_from_record(record)
            
            # Estimate win methods (this would need more detailed data)
            stats['Wins by Knockout'] = stats.get('Wins by Knockout', '0')
            stats['Wins by Submission'] = stats.get('Wins by Submission', '0')
            stats['Wins by Decision'] = str(max(0, wins - self._safe_int(stats.get('Wins by Knockout', 0)) - self._safe_int(stats.get('Wins by Submission', 0))))
            
        except Exception as e:
            print(f"Error calculating derived stats: {e}")
    
    def _safe_int(self, value) -> int:
        """Safely convert value to integer"""
        try:
            if isinstance(value, str):
                # Remove non-numeric characters
                value = re.sub(r'[^\d.-]', '', value)
            return int(float(value)) if value else 0
        except:
            return 0
    
    def _extract_age(self, dob: str) -> str:
        """Extract age from date of birth"""
        try:
            if '(' in dob:
                age = re.search(r'\((\d+)\)', dob)
                return age.group(1) if age else ''
            return ''
        except:
            return ''
    
    def _extract_height(self, htwt: str) -> str:
        """Extract height from height/weight string"""
        try:
            if '"' in htwt:
                height = re.search(r'(\d+\'\s*\d+")', htwt)
                return height.group(1) if height else ''
            return ''
        except:
            return ''
    
    def _extract_weight(self, htwt: str) -> str:
        """Extract weight from height/weight string"""
        try:
            if 'lbs' in htwt:
                weight = re.search(r'(\d+)\s*lbs', htwt)
                return f"{weight.group(1)} lbs" if weight else ''
            return ''
        except:
            return ''
    
    def _extract_reach(self, reach: str) -> str:
        """Extract reach from reach string"""
        try:
            if '"' in reach:
                reach_match = re.search(r'(\d+\.?\d*)"', reach)
                return f"{reach_match.group(1)}\"" if reach_match else ''
            return ''
        except:
            return ''
    
    def _extract_wins_from_record(self, record: str) -> int:
        """Extract number of wins from record string"""
        try:
            if '-' in record:
                parts = record.split('-')
                return int(parts[0]) if parts[0].isdigit() else 0
            return 0
        except:
            return 0
    
    def process_fighter_html(self, fighter_name: str) -> Optional[Dict[str, Any]]:
        """Process a single fighter's HTML file"""
        html_file = self.fighter_html_folder / f"{fighter_name.replace(' ', '_')}.html"
        
        if not html_file.exists():
            print(f"HTML file not found for {fighter_name}: {html_file}")
            return None
        
        try:
            with open(html_file, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            # Extract JSON data
            json_data = self.extract_json_data(html_content)
            if not json_data:
                print(f"No JSON data found for {fighter_name}")
                return None
            
            # Extract all data
            profile = self.extract_fighter_profile(json_data)
            stats = self.extract_detailed_statistics(json_data)
            fights = self.extract_fight_history(json_data)
            
            # Combine all data
            fighter_data = {**profile, **stats, **fights}
            
            # Ensure all A1 columns are present
            for col in self.a1_columns:
                if col not in fighter_data:
                    fighter_data[col] = ''
            
            return fighter_data
            
        except Exception as e:
            print(f"Error processing {fighter_name}: {e}")
            return None
    
    def process_all_fighters(self, fighters_list: List[str]) -> pd.DataFrame:
        """Process all fighters and create comprehensive DataFrame"""
        all_data = []
        
        for i, fighter_name in enumerate(fighters_list, 1):
            print(f"Processing {i}/{len(fighters_list)}: {fighter_name}")
            
            fighter_data = self.process_fighter_html(fighter_name)
            if fighter_data:
                all_data.append(fighter_data)
                print(f"  ✓ Extracted data for {fighter_name}")
            else:
                print(f"  ✗ No data found for {fighter_name}")
        
        # Create DataFrame with A1 column structure
        df = pd.DataFrame(all_data, columns=self.a1_columns)
        return df

def main():
    """Main function to run enhanced ESPN extraction"""
    extractor = EnhancedESPNExtractor()
    
    # Load fighters list
    fighters_file = extractor.data_folder / "fighters_name.csv"
    if fighters_file.exists():
        df = pd.read_csv(fighters_file)
        if 'fighters' in df.columns:
            fighters_list = df['fighters'].tolist()
        elif 'Fighter Name' in df.columns:
            fighters_list = df['Fighter Name'].tolist()
        else:
            print("No 'fighters' or 'Fighter Name' column found")
            return
    else:
        print(f"Fighters file not found: {fighters_file}")
        return
    
    print(f"Processing {len(fighters_list)} fighters...")
    
    # Process all fighters
    df = extractor.process_all_fighters(fighters_list)
    
    # Save results
    output_file = extractor.data_folder / "enhanced_fighter_profiles.csv"
    df.to_csv(output_file, index=False)
    
    print(f"\nEnhanced extraction completed!")
    print(f"Processed: {len(df)} fighters")
    print(f"Saved to: {output_file}")
    
    # Show sample data
    if not df.empty:
        print(f"\nSample data for {df.iloc[0]['Name']}:")
        sample = df.iloc[0]
        for col in ['Name', 'Division Record', 'Sig. Strikes Landed', 'Sig. Strikes Attempted', 'Striking accuracy']:
            if col in sample:
                print(f"  {col}: {sample[col]}")

if __name__ == "__main__":
    main() 