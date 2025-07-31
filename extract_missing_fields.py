import pandas as pd
import numpy as np
from pathlib import Path
import re
from datetime import datetime
from bs4 import BeautifulSoup
import json

def extract_personal_info_from_html(html_file_path: str) -> dict:
    """Extract personal information from HTML files."""
    try:
        with open(html_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        soup = BeautifulSoup(content, 'html.parser')
        personal_info = {}
        
        # Extract nickname
        nickname_elem = soup.find('div', string=re.compile(r'Nickname', re.I))
        if nickname_elem and nickname_elem.find_next_sibling():
            personal_info['Nickname'] = nickname_elem.find_next_sibling().get_text(strip=True)
        
        # Extract stance
        stance_elem = soup.find('div', string=re.compile(r'Stance', re.I))
        if stance_elem and stance_elem.find_next_sibling():
            personal_info['Stance'] = stance_elem.find_next_sibling().get_text(strip=True)
        
        # Extract country
        country_elem = soup.find('img', alt=True)
        if country_elem and 'alt' in country_elem.attrs:
            personal_info['Country'] = country_elem['alt']
        
        # Extract birth date
        birth_elem = soup.find('div', string=re.compile(r'Birthdate', re.I))
        if birth_elem and birth_elem.find_next_sibling():
            birth_text = birth_elem.find_next_sibling().get_text(strip=True)
            personal_info['Birth_Date'] = birth_text
        
        # Extract team
        team_elem = soup.find('div', string=re.compile(r'Team', re.I))
        if team_elem and team_elem.find_next_sibling():
            personal_info['Team'] = team_elem.find_next_sibling().get_text(strip=True)
        
        return personal_info
    except Exception as e:
        print(f"Error extracting personal info from {html_file_path}: {e}")
        return {}

def extract_advanced_stats_from_living_docs():
    """Extract advanced statistics from living documents."""
    try:
        # Load living documents
        striking_df = pd.read_csv('data/striking_data_living.csv', low_memory=False)
        clinch_df = pd.read_csv('data/clinch_data_living.csv', low_memory=False)
        ground_df = pd.read_csv('data/ground_data_living.csv', low_memory=False)
        
        advanced_stats = {}
        
        # Process striking data for target breakdowns
        for _, row in striking_df.iterrows():
            fighter = row.get('Fighter', '')
            if not fighter:
                continue
                
            if fighter not in advanced_stats:
                advanced_stats[fighter] = {
                    'total_fights': 0,
                    'wins': 0,
                    'losses': 0,
                    'first_round_wins': 0,
                    'ko_tko_wins': 0,
                    'submission_wins': 0,
                    'decision_wins': 0,
                    'total_control_time': 0,
                    'total_advancements': 0,
                    'total_reversals': 0,
                    'total_slams': 0,
                    'distance_strikes': 0,
                    'clinch_strikes': 0,
                    'ground_strikes': 0,
                    'head_strikes': 0,
                    'body_strikes': 0,
                    'leg_strikes': 0
                }
            
            advanced_stats[fighter]['total_fights'] += 1
            
            # Count wins/losses
            result = str(row.get('Result', '')).upper()
            if 'W' in result:
                advanced_stats[fighter]['wins'] += 1
                
                # Count win methods
                if 'KO' in result or 'TKO' in result:
                    advanced_stats[fighter]['ko_tko_wins'] += 1
                elif 'SUB' in result:
                    advanced_stats[fighter]['submission_wins'] += 1
                elif 'DEC' in result:
                    advanced_stats[fighter]['decision_wins'] += 1
                
                # Count first round wins
                round_info = str(row.get('Round', ''))
                if '1' in round_info or 'First' in round_info:
                    advanced_stats[fighter]['first_round_wins'] += 1
            elif 'L' in result:
                advanced_stats[fighter]['losses'] += 1
            
            # Extract target breakdowns
            head_pct = row.get('%HEAD', 0) or 0
            body_pct = row.get('%BODY', 0) or 0
            leg_pct = row.get('%LEG', 0) or 0
            
            total_strikes = row.get('SSL', 0) or 0
            
            if total_strikes > 0:
                advanced_stats[fighter]['head_strikes'] += int((head_pct / 100) * total_strikes)
                advanced_stats[fighter]['body_strikes'] += int((body_pct / 100) * total_strikes)
                advanced_stats[fighter]['leg_strikes'] += int((leg_pct / 100) * total_strikes)
        
        # Process clinch data
        for _, row in clinch_df.iterrows():
            fighter = row.get('Fighter', '')
            if fighter in advanced_stats:
                # Count clinch strikes
                clinch_strikes = row.get('SCBL', 0) or 0
                clinch_strikes += row.get('SCHL', 0) or 0
                clinch_strikes += row.get('SCLL', 0) or 0
                advanced_stats[fighter]['clinch_strikes'] += clinch_strikes
        
        # Process ground data
        for _, row in ground_df.iterrows():
            fighter = row.get('Fighter', '')
            if fighter in advanced_stats:
                # Count ground strikes
                ground_strikes = row.get('SGBL', 0) or 0
                ground_strikes += row.get('SGHL', 0) or 0
                ground_strikes += row.get('SGLL', 0) or 0
                advanced_stats[fighter]['ground_strikes'] += ground_strikes
                
                # Count advancements
                advancements = row.get('AD', 0) or 0
                advanced_stats[fighter]['total_advancements'] += advancements
                
                # Count reversals
                reversals = row.get('RV', 0) or 0
                advanced_stats[fighter]['total_reversals'] += reversals
                
                # Count slams
                slams = row.get('TDS', 0) or 0
                advanced_stats[fighter]['total_slams'] += slams
        
        return advanced_stats
    except Exception as e:
        print(f"Error extracting advanced stats: {e}")
        return {}

def extract_fight_history_details():
    """Extract detailed fight history information."""
    try:
        striking_df = pd.read_csv('data/striking_data_living.csv', low_memory=False)
        
        fight_history = {}
        
        for _, row in striking_df.iterrows():
            fighter = row.get('Fighter', '')
            if not fighter:
                continue
                
            if fighter not in fight_history:
                fight_history[fighter] = {
                    'fight_dates': [],
                    'opponents': [],
                    'events': [],
                    'results': [],
                    'rounds': [],
                    'times': [],
                    'methods': []
                }
            
            # Extract fight details
            date = row.get('Date', '')
            opponent = row.get('Opponent', '')
            event = row.get('Event', '')
            result = row.get('Result', '')
            round_info = row.get('Round', '')
            time = row.get('Time', '')
            method = row.get('Method', '')
            
            if date:
                fight_history[fighter]['fight_dates'].append(date)
            if opponent:
                fight_history[fighter]['opponents'].append(opponent)
            if event:
                fight_history[fighter]['events'].append(event)
            if result:
                fight_history[fighter]['results'].append(result)
            if round_info:
                fight_history[fighter]['rounds'].append(round_info)
            if time:
                fight_history[fighter]['times'].append(time)
            if method:
                fight_history[fighter]['methods'].append(method)
        
        return fight_history
    except Exception as e:
        print(f"Error extracting fight history: {e}")
        return {}

def enhance_fighter_profiles_with_missing_fields():
    """Enhance fighter profiles with all missing fields."""
    
    # Load existing profiles
    profiles_df = pd.read_csv('data/fighter_profiles.csv')
    print(f"Loaded {len(profiles_df)} fighter profiles")
    
    # Extract additional data
    print("Extracting personal information from HTML files...")
    personal_info = {}
    html_dir = Path('data/FighterHTMLs')
    html_files = list(html_dir.glob('*.html'))
    
    for html_file in html_files[:100]:  # Process first 100 for testing
        fighter_name = html_file.stem.replace('_', '')
        personal_info[fighter_name] = extract_personal_info_from_html(str(html_file))
    
    print("Extracting advanced statistics from living documents...")
    advanced_stats = extract_advanced_stats_from_living_docs()
    
    print("Extracting fight history details...")
    fight_history = extract_fight_history_details()
    
    # Add new columns
    new_columns = [
        'Nickname', 'Stance', 'Country', 'Birth_Date', 'Team',
        'First_Round_Wins', 'KO_TKO_Wins', 'Submission_Wins', 'Decision_Wins',
        'Total_Control_Time', 'Total_Advancements', 'Total_Reversals', 'Total_Slams',
        'Distance_Strikes', 'Clinch_Strikes', 'Ground_Strikes',
        'Head_Strikes', 'Body_Strikes', 'Leg_Strikes',
        'UFC_Debut_Date', 'Title_Fight_Record', 'Main_Event_Record',
        'Fight_of_Night_Awards', 'Performance_of_Night_Awards'
    ]
    
    for col in new_columns:
        if col not in profiles_df.columns:
            profiles_df[col] = ''
    
    # Update profiles with extracted data
    updated_count = 0
    for idx, row in profiles_df.iterrows():
        fighter_name = row['Name']
        
        # Add personal information
        if fighter_name in personal_info:
            info = personal_info[fighter_name]
            for key, value in info.items():
                if key in profiles_df.columns:
                    profiles_df.at[idx, key] = value
        
        # Add advanced statistics
        if fighter_name in advanced_stats:
            stats = advanced_stats[fighter_name]
            profiles_df.at[idx, 'First_Round_Wins'] = stats.get('first_round_wins', 0)
            profiles_df.at[idx, 'KO_TKO_Wins'] = stats.get('ko_tko_wins', 0)
            profiles_df.at[idx, 'Submission_Wins'] = stats.get('submission_wins', 0)
            profiles_df.at[idx, 'Decision_Wins'] = stats.get('decision_wins', 0)
            profiles_df.at[idx, 'Total_Control_Time'] = stats.get('total_control_time', 0)
            profiles_df.at[idx, 'Total_Advancements'] = stats.get('total_advancements', 0)
            profiles_df.at[idx, 'Total_Reversals'] = stats.get('total_reversals', 0)
            profiles_df.at[idx, 'Total_Slams'] = stats.get('total_slams', 0)
            profiles_df.at[idx, 'Distance_Strikes'] = stats.get('distance_strikes', 0)
            profiles_df.at[idx, 'Clinch_Strikes'] = stats.get('clinch_strikes', 0)
            profiles_df.at[idx, 'Ground_Strikes'] = stats.get('ground_strikes', 0)
            profiles_df.at[idx, 'Head_Strikes'] = stats.get('head_strikes', 0)
            profiles_df.at[idx, 'Body_Strikes'] = stats.get('body_strikes', 0)
            profiles_df.at[idx, 'Leg_Strikes'] = stats.get('leg_strikes', 0)
        
        # Add fight history details
        if fighter_name in fight_history:
            history = fight_history[fighter_name]
            if history['fight_dates']:
                # Find UFC debut (first fight)
                profiles_df.at[idx, 'UFC_Debut_Date'] = history['fight_dates'][-1] if history['fight_dates'] else ''
        
        updated_count += 1
        if updated_count % 100 == 0:
            print(f"Updated {updated_count} fighters...")
    
    # Save enhanced profiles
    profiles_df.to_csv('data/enhanced_fighter_profiles_final.csv', index=False)
    print(f"Enhanced {len(profiles_df)} fighter profiles with missing fields")
    
    # Show sample of enhanced data
    print("\nSample enhanced fighter profile:")
    sample_fighter = profiles_df.iloc[0]
    print(f"Name: {sample_fighter['Name']}")
    print(f"Nickname: {sample_fighter['Nickname']}")
    print(f"Stance: {sample_fighter['Stance']}")
    print(f"Country: {sample_fighter['Country']}")
    print(f"First Round Wins: {sample_fighter['First_Round_Wins']}")
    print(f"KO/TKO Wins: {sample_fighter['KO_TKO_Wins']}")
    print(f"Total Advancements: {sample_fighter['Total_Advancements']}")
    print(f"Head Strikes: {sample_fighter['Head_Strikes']}")
    print(f"Clinch Strikes: {sample_fighter['Clinch_Strikes']}")
    
    return profiles_df

if __name__ == "__main__":
    enhance_fighter_profiles_with_missing_fields() 