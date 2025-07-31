import pandas as pd
import numpy as np
from pathlib import Path
import re
from datetime import datetime
from bs4 import BeautifulSoup

def extract_robert_whittaker_data():
    """Extract all available data for Robert Whittaker."""
    
    # Load enhanced fighter profiles
    profiles_df = pd.read_csv('data/enhanced_fighter_profiles_final.csv')
    whittaker_profile = profiles_df[profiles_df['Name'].str.contains('RobertWhittaker', case=False, na=False)]
    
    # Load living documents for detailed fight data
    striking_df = pd.read_csv('data/striking_data_living.csv', low_memory=False)
    clinch_df = pd.read_csv('data/clinch_data_living.csv', low_memory=False)
    ground_df = pd.read_csv('data/ground_data_living.csv', low_memory=False)
    
    # Filter for Robert Whittaker fights
    whittaker_striking = striking_df[striking_df['Fighter'].str.contains('RobertWhittaker', case=False, na=False)]
    whittaker_clinch = clinch_df[clinch_df['Fighter'].str.contains('RobertWhittaker', case=False, na=False)]
    whittaker_ground = ground_df[ground_df['Fighter'].str.contains('RobertWhittaker', case=False, na=False)]
    
    return whittaker_profile, whittaker_striking, whittaker_clinch, whittaker_ground

def create_fighter_profile_sheet(whittaker_profile):
    """Create the fighter profile sheet following the dossier structure."""
    
    if whittaker_profile.empty:
        print("No Robert Whittaker profile found!")
        return pd.DataFrame()
    
    profile = whittaker_profile.iloc[0]
    
    # Create fighter profile data following the exact dossier structure
    fighter_profile_data = {
        'Name': [profile.get('Name', 'Robert Whittaker')],
        'Division Title': [profile.get('Division_Title', 'Middleweight Division')],
        'Division Record': [profile.get('Division_Record', '')],
        'Striking accuracy': [profile.get('Striking_Accuracy', 0)],
        'Sig. Strikes Landed': [profile.get('Sig_Strikes_Landed', 0)],
        'Sig. Strikes Attempted': [profile.get('Sig_Strikes_Attempted', 0)],
        'Takedown Accuracy': [profile.get('Takedown_Accuracy', 0)],
        'Takedowns Landed': [profile.get('Takedowns_Landed', 0)],
        'Takedowns Attempted': [profile.get('Takedowns_Attempted', 0)],
        'Sig. Str. Landed Per Min': [profile.get('Sig_Strikes_Landed_Per_Min', 0)],
        'Sig. Str. Absorbed Per Min': [profile.get('Sig_Strikes_Absorbed_Per_Min', 0)],
        'Takedown avg Per 15 Min': [profile.get('Takedown_Avg_Per_15_Min', 0)],
        'Submission avg Per 15 Min': [profile.get('Submission_Avg_Per_15_Min', 0)],
        'Sig. Str. Defense': [profile.get('Striking_Defense', 0)],
        'Takedown Defense': [profile.get('Takedown_Defense', 0)],
        'Knockdown Avg': [profile.get('Knockdown_Avg', 0)],
        'Average fight time': [profile.get('Average_Fight_Time', 0)],
        'Sig. Str. By Position - Standing': [profile.get('Distance_Strikes', 0)],
        'Sig. Str. By Position - Clinch': [profile.get('Clinch_Strikes', 0)],
        'Sig. Str. By Position - Ground': [profile.get('Ground_Strikes', 0)],
        'Win by Method - KO/TKO': [profile.get('KO_TKO_Wins', 0)],
        'Win by Method - DEC': [profile.get('Decision_Wins', 0)],
        'Win by Method - SUB': [profile.get('Submission_Wins', 0)],
        'Sig. Str. by target - Head Strike Percentage': [profile.get('Sig_Strikes_Head_Pct', 0)],
        'Sig. Str. by target - Head Strike Count': [profile.get('Head_Strikes', 0)],
        'Sig. Str. by target - Body Strike Percentage': [profile.get('Sig_Strikes_Body_Pct', 0)],
        'Sig. Str. by target - Body Strike Count': [profile.get('Body_Strikes', 0)],
        'Sig. Str. by target - Leg Strike Percentage': [profile.get('Sig_Strikes_Leg_Pct', 0)],
        'Sig. Str. by target - Leg Strike Count': [profile.get('Leg_Strikes', 0)],
        'Status': [profile.get('Status', 'Active')],
        'Place_of_Birth': [profile.get('Place_of_Birth', '')],
        'Fighting_style': [profile.get('Fighting_Style', 'Mixed Martial Arts')],
        'Age': [profile.get('Age', 34)],
        'Height': [profile.get('Height', "6' 0\"")],
        'Weight': [profile.get('Weight', '185 lbs')],
        'Octagon_Debut': [profile.get('Octagon_Debut', '')],
        'Reach': [profile.get('Reach', '78"')],
        'Leg_reach': [profile.get('Leg_Reach', '')],
        'Trains_at': [profile.get('Team', 'Gracie Jiu-Jitsu Smeaton Grange')],
        'Fight Win Streak': [profile.get('Fight_Win_Streak', 0)],
        'Title Defenses': [profile.get('Title_Defenses', 0)],
        'Former Champion': [profile.get('Former_Champion', 'No')]
    }
    
    return pd.DataFrame(fighter_profile_data)

def create_processed_ufc_fight_results_sheet(whittaker_striking):
    """Create the processed UFC fight results sheet."""
    
    if whittaker_striking.empty:
        return pd.DataFrame()
    
    fight_results = []
    
    for _, fight in whittaker_striking.iterrows():
        fight_data = {
            'EVENT': fight.get('Event', ''),
            'BOUT': f"Whittaker vs {fight.get('Opponent', '')}",
            'Weight Division': 'Middleweight',
            'Fighter 1': 'Robert Whittaker',
            'Fighter 2': fight.get('Opponent', ''),
            'Winning Fighter': 'Robert Whittaker' if 'W' in str(fight.get('Result', '')).upper() else fight.get('Opponent', ''),
            'Losing Fighter': fight.get('Opponent', '') if 'W' in str(fight.get('Result', '')).upper() else 'Robert Whittaker',
            'METHOD': fight.get('Method', ''),
            'ROUND': fight.get('Round', ''),
            'TIME': fight.get('Time', ''),
            'TIME FORMAT': '3-round' if '3' in str(fight.get('Round', '')) else '5-round',
            'REFEREE': '',
            'DETAILS': '',
            'Date': fight.get('Date', ''),
            'Fight Time (min)': 15  # Default estimate
        }
        fight_results.append(fight_data)
    
    return pd.DataFrame(fight_results)

def create_fight_data_offensive_sheet(whittaker_striking, whittaker_clinch, whittaker_ground):
    """Create the offensive fight data sheet."""
    
    if whittaker_striking.empty:
        return pd.DataFrame()
    
    offensive_data = []
    
    for _, fight in whittaker_striking.iterrows():
        # Find corresponding clinch and ground data
        fight_date = fight.get('Date', '')
        fight_opponent = fight.get('Opponent', '')
        
        clinch_fight = whittaker_clinch[
            (whittaker_clinch['Date'] == fight_date) & 
            (whittaker_clinch['Opponent'] == fight_opponent)
        ]
        
        ground_fight = whittaker_ground[
            (whittaker_ground['Date'] == fight_date) & 
            (whittaker_ground['Opponent'] == fight_opponent)
        ]
        
        offensive_fight = {
            'Player': 'Robert Whittaker',
            'Date': fight_date,
            'Opponent': fight_opponent,
            'Event': fight.get('Event', ''),
            'Result': fight.get('Result', ''),
            'Stats Type': 'Striking',
            'Fight Time (min)': 15,  # Default estimate
            'Striking-TSL': fight.get('TSL', 0),
            'Striking-TSA': fight.get('TSA', 0),
            'Striking-SSL': fight.get('SSL', 0),
            'Striking-SSA': fight.get('SSA', 0),
            'Striking-KD': fight.get('KD', 0),
            'Striking-%HEAD': fight.get('%HEAD', 0),
            'Striking-%BODY': fight.get('%BODY', 0),
            'Striking-%LEG': fight.get('%LEG', 0),
            'Clinch-TDL': clinch_fight['TDL'].iloc[0] if not clinch_fight.empty else 0,
            'Clinch-TDA': clinch_fight['TDA'].iloc[0] if not clinch_fight.empty else 0,
            'Ground-SGBL': ground_fight['SGBL'].iloc[0] if not ground_fight.empty else 0,
            'Ground-SGBA': ground_fight['SGBA'].iloc[0] if not ground_fight.empty else 0,
            'Ground-ADHG': ground_fight['ADHG'].iloc[0] if not ground_fight.empty else 0
        }
        
        offensive_data.append(offensive_fight)
    
    return pd.DataFrame(offensive_data)

def create_fight_data_defensive_sheet(whittaker_striking, whittaker_clinch, whittaker_ground):
    """Create the defensive fight data sheet."""
    
    if whittaker_striking.empty:
        return pd.DataFrame()
    
    defensive_data = []
    
    for _, fight in whittaker_striking.iterrows():
        # Find corresponding clinch and ground data
        fight_date = fight.get('Date', '')
        fight_opponent = fight.get('Opponent', '')
        
        clinch_fight = whittaker_clinch[
            (whittaker_clinch['Date'] == fight_date) & 
            (whittaker_clinch['Opponent'] == fight_opponent)
        ]
        
        ground_fight = whittaker_ground[
            (whittaker_ground['Date'] == fight_date) & 
            (whittaker_ground['Opponent'] == fight_opponent)
        ]
        
        defensive_fight = {
            'Player': 'Robert Whittaker',
            'Date': fight_date,
            'Opponent': fight_opponent,
            'Event': fight.get('Event', ''),
            'Result': fight.get('Result', ''),
            'Stats Type': 'Defensive',
            'Striking-SSL': 0,  # Would need opponent data
            'Striking-SSA': 0,  # Would need opponent data
            'Striking-KD': 0,   # Would need opponent data
            'Striking-%HEAD': 0, # Would need opponent data
            'Striking-%BODY': 0, # Would need opponent data
            'Striking-%LEG': 0,  # Would need opponent data
            'Clinch-TDL': 0,     # Would need opponent data
            'Clinch-TDA': 0,     # Would need opponent data
            'Ground-SGBL': 0,    # Would need opponent data
            'Ground-SGBA': 0,    # Would need opponent data
            'Ground-ADHG': 0     # Would need opponent data
        }
        
        defensive_data.append(defensive_fight)
    
    return pd.DataFrame(defensive_data)

def create_robert_whittaker_dossier():
    """Create a comprehensive Robert Whittaker dossier."""
    
    print("Extracting Robert Whittaker data...")
    whittaker_profile, whittaker_striking, whittaker_clinch, whittaker_ground = extract_robert_whittaker_data()
    
    print("Creating fighter profile sheet...")
    fighter_profile_sheet = create_fighter_profile_sheet(whittaker_profile)
    
    print("Creating processed UFC fight results sheet...")
    fight_results_sheet = create_processed_ufc_fight_results_sheet(whittaker_striking)
    
    print("Creating offensive fight data sheet...")
    offensive_sheet = create_fight_data_offensive_sheet(whittaker_striking, whittaker_clinch, whittaker_ground)
    
    print("Creating defensive fight data sheet...")
    defensive_sheet = create_fight_data_defensive_sheet(whittaker_striking, whittaker_clinch, whittaker_ground)
    
    # Create Excel file with multiple sheets
    with pd.ExcelWriter('Robert_Whittaker_Dossier.xlsx', engine='openpyxl') as writer:
        fighter_profile_sheet.to_excel(writer, sheet_name='fighter_profiles', index=False)
        fight_results_sheet.to_excel(writer, sheet_name='processed_ufc_fight_results', index=False)
        offensive_sheet.to_excel(writer, sheet_name='fight_data_offensive_combined', index=False)
        defensive_sheet.to_excel(writer, sheet_name='fight_data_defensive_combined', index=False)
    
    print("Robert Whittaker Dossier created successfully!")
    
    # Display summary
    print(f"\nDossier Summary:")
    print(f"Fighter Profile: {len(fighter_profile_sheet)} records")
    print(f"Fight Results: {len(fight_results_sheet)} fights")
    print(f"Offensive Data: {len(offensive_sheet)} fights")
    print(f"Defensive Data: {len(defensive_sheet)} fights")
    
    # Show sample fighter profile
    if not fighter_profile_sheet.empty:
        print(f"\nRobert Whittaker Profile:")
        profile = fighter_profile_sheet.iloc[0]
        print(f"Name: {profile['Name']}")
        print(f"Division: {profile['Division Title']}")
        print(f"Record: {profile['Sig. Strikes Landed']} strikes landed, {profile['Striking accuracy']}% accuracy")
        print(f"Takedowns: {profile['Takedowns Landed']} landed, {profile['Takedown Accuracy']}% accuracy")
        print(f"Win Methods: {profile['Win by Method - KO/TKO']} KO/TKO, {profile['Win by Method - SUB']} SUB, {profile['Win by Method - DEC']} DEC")
        print(f"Team: {profile['Trains_at']}")
        print(f"Stance: {profile.get('Stance', 'N/A')}")
        print(f"Country: {profile.get('Country', 'N/A')}")
    
    return {
        'fighter_profile': fighter_profile_sheet,
        'fight_results': fight_results_sheet,
        'offensive_data': offensive_sheet,
        'defensive_data': defensive_sheet
    }

if __name__ == "__main__":
    create_robert_whittaker_dossier() 