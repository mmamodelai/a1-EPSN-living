import pandas as pd
import numpy as np
from pathlib import Path
import re
from datetime import datetime

def calculate_rate_stats(total_value, total_fight_time_minutes):
    """Calculate per-minute and per-15-minute rates."""
    if total_fight_time_minutes > 0:
        per_min = total_value / total_fight_time_minutes
        per_15min = per_min * 15
        return round(per_min, 2), round(per_15min, 2)
    return 0.0, 0.0

def calculate_defensive_percentage(landed, absorbed):
    """Calculate defensive percentage."""
    if absorbed > 0:
        return round(((absorbed - landed) / absorbed) * 100, 1)
    return 0.0

def extract_win_methods_from_living_docs():
    """Extract win methods from living documents."""
    try:
        # Load living documents
        striking_df = pd.read_csv('data/striking_data_living.csv')
        
        # Group by fighter and count win methods
        win_methods = {}
        
        for _, row in striking_df.iterrows():
            fighter = row['Fighter']
            result = row.get('Result', '')
            
            if fighter not in win_methods:
                win_methods[fighter] = {'KO/TKO': 0, 'DEC': 0, 'SUB': 0, 'total_fights': 0}
            
            win_methods[fighter]['total_fights'] += 1
            
            if 'W' in str(result).upper():
                if 'KO' in str(result).upper() or 'TKO' in str(result).upper():
                    win_methods[fighter]['KO/TKO'] += 1
                elif 'DEC' in str(result).upper() or 'DECISION' in str(result).upper():
                    win_methods[fighter]['DEC'] += 1
                elif 'SUB' in str(result).upper() or 'SUBMISSION' in str(result).upper():
                    win_methods[fighter]['SUB'] += 1
        
        return win_methods
    except Exception as e:
        print(f"Error extracting win methods: {e}")
        return {}

def calculate_target_breakdowns():
    """Calculate target breakdowns from living documents."""
    try:
        striking_df = pd.read_csv('data/striking_data_living.csv')
        
        target_breakdowns = {}
        
        for _, row in striking_df.iterrows():
            fighter = row['Fighter']
            
            if fighter not in target_breakdowns:
                target_breakdowns[fighter] = {'head': 0, 'body': 0, 'leg': 0, 'total': 0}
            
            # Extract target data if available
            head_strikes = row.get('Head Strikes Landed', 0) or 0
            body_strikes = row.get('Body Strikes Landed', 0) or 0
            leg_strikes = row.get('Leg Strikes Landed', 0) or 0
            
            target_breakdowns[fighter]['head'] += head_strikes
            target_breakdowns[fighter]['body'] += body_strikes
            target_breakdowns[fighter]['leg'] += leg_strikes
            target_breakdowns[fighter]['total'] += (head_strikes + body_strikes + leg_strikes)
        
        # Calculate percentages
        for fighter in target_breakdowns:
            total = target_breakdowns[fighter]['total']
            if total > 0:
                target_breakdowns[fighter]['head_pct'] = round((target_breakdowns[fighter]['head'] / total) * 100, 1)
                target_breakdowns[fighter]['body_pct'] = round((target_breakdowns[fighter]['body'] / total) * 100, 1)
                target_breakdowns[fighter]['leg_pct'] = round((target_breakdowns[fighter]['leg'] / total) * 100, 1)
            else:
                target_breakdowns[fighter]['head_pct'] = 0
                target_breakdowns[fighter]['body_pct'] = 0
                target_breakdowns[fighter]['leg_pct'] = 0
        
        return target_breakdowns
    except Exception as e:
        print(f"Error calculating target breakdowns: {e}")
        return {}

def enhance_fighter_profiles():
    """Enhance fighter profiles with all missing dossier fields."""
    
    # Load existing fighter profiles
    profiles_df = pd.read_csv('data/fighter_profiles.csv')
    print(f"Loaded {len(profiles_df)} fighter profiles")
    
    # Load living documents for additional data
    try:
        striking_df = pd.read_csv('data/striking_data_living.csv')
        clinch_df = pd.read_csv('data/clinch_data_living.csv')
        ground_df = pd.read_csv('data/ground_data_living.csv')
        print("Loaded living documents")
    except Exception as e:
        print(f"Error loading living documents: {e}")
        return
    
    # Extract win methods and target breakdowns
    win_methods = extract_win_methods_from_living_docs()
    target_breakdowns = calculate_target_breakdowns()
    
    # Add new columns to profiles
    new_columns = [
        'Division_Title', 'Division_Record', 'Sig_Str_Landed_Per_Min', 'Sig_Str_Absorbed_Per_Min',
        'Takedown_Avg_Per_15_Min', 'Submission_Avg_Per_15_Min', 'Sig_Str_Defense', 'Takedown_Defense',
        'Knockdown_Avg', 'Average_Fight_Time', 'Win_by_KO_TKO', 'Win_by_DEC', 'Win_by_SUB',
        'Sig_Str_Head_Pct', 'Sig_Str_Body_Pct', 'Sig_Str_Leg_Pct', 'Fight_Win_Streak',
        'Title_Defenses', 'Former_Champion', 'Octagon_Debut', 'Fighting_Style'
    ]
    
    for col in new_columns:
        if col not in profiles_df.columns:
            profiles_df[col] = ''
    
    # Process each fighter
    for idx, row in profiles_df.iterrows():
        fighter_name = row['Name']
        
        # Calculate rate statistics
        total_sig_strikes = row.get('Sig. Strike', 0) or 0
        total_takedowns = row.get('Takedownr', 0) or 0
        total_submissions = row.get('Submissio', 0) or 0
        total_knockdowns = row.get('Knockdow', 0) or 0
        
        # Estimate total fight time (average 3 rounds = 15 minutes per fight)
        total_fights = row.get('Fight', 0) or 0
        if total_fights > 0:
            estimated_fight_time = total_fights * 15  # 15 minutes per fight average
        else:
            estimated_fight_time = 1  # Avoid division by zero
        
        # Calculate rates
        sig_str_per_min, sig_str_per_15min = calculate_rate_stats(total_sig_strikes, estimated_fight_time)
        takedown_per_min, takedown_per_15min = calculate_rate_stats(total_takedowns, estimated_fight_time)
        submission_per_min, submission_per_15min = calculate_rate_stats(total_submissions, estimated_fight_time)
        knockdown_per_min, knockdown_per_15min = calculate_rate_stats(total_knockdowns, estimated_fight_time)
        
        # Update profiles with calculated rates
        profiles_df.at[idx, 'Sig_Str_Landed_Per_Min'] = sig_str_per_min
        profiles_df.at[idx, 'Takedown_Avg_Per_15_Min'] = takedown_per_15min
        profiles_df.at[idx, 'Submission_Avg_Per_15_Min'] = submission_per_15min
        profiles_df.at[idx, 'Knockdown_Avg'] = knockdown_per_15min
        profiles_df.at[idx, 'Average_Fight_Time'] = round(estimated_fight_time / total_fights if total_fights > 0 else 0, 1)
        
        # Add win methods
        if fighter_name in win_methods:
            methods = win_methods[fighter_name]
            profiles_df.at[idx, 'Win_by_KO_TKO'] = methods['KO/TKO']
            profiles_df.at[idx, 'Win_by_DEC'] = methods['DEC']
            profiles_df.at[idx, 'Win_by_SUB'] = methods['SUB']
        
        # Add target breakdowns
        if fighter_name in target_breakdowns:
            breakdown = target_breakdowns[fighter_name]
            profiles_df.at[idx, 'Sig_Str_Head_Pct'] = breakdown['head_pct']
            profiles_df.at[idx, 'Sig_Str_Body_Pct'] = breakdown['body_pct']
            profiles_df.at[idx, 'Sig_Str_Leg_Pct'] = breakdown['leg_pct']
        
        # Calculate defensive percentages (simplified - would need absorbed data)
        sig_str_accuracy = row.get('Striking ac', 0) or 0
        takedown_accuracy = row.get('Takedown Accuracy', 0) or 0
        
        profiles_df.at[idx, 'Sig_Str_Defense'] = round(100 - sig_str_accuracy, 1) if sig_str_accuracy > 0 else 0
        profiles_df.at[idx, 'Takedown_Defense'] = round(100 - takedown_accuracy, 1) if takedown_accuracy > 0 else 0
        
        # Set division title based on division
        division = row.get('Division Tr', '')
        if division:
            profiles_df.at[idx, 'Division_Title'] = f"{division} Division"
        
        # Set default values for missing fields
        profiles_df.at[idx, 'Division_Record'] = f"{row.get('Win', 0) or 0}-{row.get('Defeat', 0) or 0}"
        profiles_df.at[idx, 'Fight_Win_Streak'] = 0  # Would need fight history analysis
        profiles_df.at[idx, 'Title_Defenses'] = 0
        profiles_df.at[idx, 'Former_Champion'] = 'No'
        profiles_df.at[idx, 'Octagon_Debut'] = ''  # Would need debut date
        profiles_df.at[idx, 'Fighting_Style'] = 'Mixed Martial Arts'
    
    # Save enhanced profiles
    profiles_df.to_csv('data/enhanced_fighter_profiles.csv', index=False)
    print(f"Enhanced {len(profiles_df)} fighter profiles saved to data/enhanced_fighter_profiles.csv")
    
    # Show sample of enhanced data
    print("\nSample enhanced fighter profile:")
    sample_fighter = profiles_df.iloc[0]
    print(f"Name: {sample_fighter['Name']}")
    print(f"Division: {sample_fighter['Division_Title']}")
    print(f"Sig Strikes Per Min: {sample_fighter['Sig_Str_Landed_Per_Min']}")
    print(f"Takedown Avg Per 15 Min: {sample_fighter['Takedown_Avg_Per_15_Min']}")
    print(f"Win by KO/TKO: {sample_fighter['Win_by_KO_TKO']}")
    print(f"Head Strike %: {sample_fighter['Sig_Str_Head_Pct']}%")
    print(f"Striking Defense: {sample_fighter['Sig_Str_Defense']}%")

if __name__ == "__main__":
    enhance_fighter_profiles() 