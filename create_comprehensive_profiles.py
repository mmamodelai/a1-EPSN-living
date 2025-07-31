import pandas as pd
import numpy as np
from pathlib import Path
import re
from datetime import datetime

def create_comprehensive_fighter_profiles():
    """Create comprehensive fighter profiles matching professional dossier structure."""
    
    # Load enhanced profiles
    enhanced_df = pd.read_csv('data/enhanced_fighter_profiles.csv')
    print(f"Loaded {len(enhanced_df)} enhanced fighter profiles")
    
    # Create comprehensive profiles with proper column structure
    comprehensive_columns = [
        # Basic Information
        'Name', 'Division_Title', 'Division_Record', 'Status', 'Age', 'Height', 'Weight', 
        'Reach', 'Leg_Reach', 'Octagon_Reach', 'Trains_at', 'Fighting_Style',
        
        # Career Records
        'Total_Fights', 'Wins', 'Losses', 'Draws', 'Win_Percentage', 'Title_Defenses', 
        'Former_Champion', 'Fight_Win_Streak', 'Octagon_Debut',
        
        # Win Methods
        'Wins_by_KO_TKO', 'Wins_by_SUB', 'Wins_by_DEC', 'First_Round_Wins',
        
        # Striking Statistics
        'Sig_Strikes_Landed', 'Sig_Strikes_Attempted', 'Striking_Accuracy', 'Striking_Defense',
        'Sig_Strikes_Landed_Per_Min', 'Sig_Strikes_Absorbed_Per_Min', 'Average_Sig_Strikes',
        
        # Target Breakdown
        'Sig_Strikes_Head_Pct', 'Sig_Strikes_Body_Pct', 'Sig_Strikes_Leg_Pct',
        
        # Grappling Statistics
        'Takedowns_Landed', 'Takedowns_Attempted', 'Takedown_Accuracy', 'Takedown_Defense',
        'Takedown_Avg_Per_15_Min', 'Submissions', 'Submission_Avg_Per_15_Min',
        
        # Advanced Statistics
        'Knockdowns', 'Knockdown_Avg', 'Average_Fight_Time', 'Control_Time',
        
        # Recent Performance (Last 3 Fights)
        'Event_1_Headline', 'Event_1_Date', 'Event_1_Result', 'Event_1_Time', 'Event_1_Method',
        'Event_2_Headline', 'Event_2_Date', 'Event_2_Result', 'Event_2_Time', 'Event_2_Method',
        'Event_3_Headline', 'Event_3_Date', 'Event_3_Result', 'Event_3_Time', 'Event_3_Method',
        
        # Additional Fields
        'Country', 'Place_of_Birth', 'Nickname', 'Stance'
    ]
    
    # Create new comprehensive dataframe
    comprehensive_df = pd.DataFrame(columns=comprehensive_columns)
    
    # Process each fighter
    for idx, row in enhanced_df.iterrows():
        fighter_data = {}
        
        # Basic Information
        fighter_data['Name'] = row.get('Name', '')
        fighter_data['Division_Title'] = row.get('Division_Title', '')
        fighter_data['Division_Record'] = row.get('Division_Record', '')
        fighter_data['Status'] = row.get('Status', 'Active')
        fighter_data['Age'] = row.get('Age', '')
        fighter_data['Height'] = row.get('Height', '')
        fighter_data['Weight'] = row.get('Weight', '')
        fighter_data['Reach'] = row.get('Reach', '')
        fighter_data['Leg_Reach'] = row.get('Leg_reach', '')
        fighter_data['Octagon_Reach'] = row.get('Octagon_Reach', '')
        fighter_data['Trains_at'] = row.get('Trains_at', '')
        fighter_data['Fighting_Style'] = row.get('Fighting_Style', 'Mixed Martial Arts')
        
        # Career Records
        total_fights = row.get('Fight', 0) or 0
        wins = row.get('Win', 0) or 0
        losses = row.get('Defeat', 0) or 0
        
        fighter_data['Total_Fights'] = total_fights
        fighter_data['Wins'] = wins
        fighter_data['Losses'] = losses
        fighter_data['Draws'] = 0  # Would need to calculate from fight history
        fighter_data['Win_Percentage'] = round((wins / total_fights * 100), 1) if total_fights > 0 else 0
        fighter_data['Title_Defenses'] = row.get('Title_Defenses', 0)
        fighter_data['Former_Champion'] = row.get('Former_Champion', 'No')
        fighter_data['Fight_Win_Streak'] = row.get('Fight_Win_Streak', 0)
        fighter_data['Octagon_Debut'] = row.get('Octagon_Debut', '')
        
        # Win Methods
        fighter_data['Wins_by_KO_TKO'] = row.get('Win_by_KO_TKO', 0)
        fighter_data['Wins_by_SUB'] = row.get('Win_by_SUB', 0)
        fighter_data['Wins_by_DEC'] = row.get('Win_by_DEC', 0)
        fighter_data['First_Round_Wins'] = row.get('First Roun', 0) or 0
        
        # Striking Statistics
        fighter_data['Sig_Strikes_Landed'] = row.get('Sig. Strike', 0) or 0
        fighter_data['Sig_Strikes_Attempted'] = row.get('Sig. Str. At', 0) or 0
        fighter_data['Striking_Accuracy'] = row.get('Striking ac', 0) or 0
        fighter_data['Striking_Defense'] = row.get('Sig_Str_Defense', 0)
        fighter_data['Sig_Strikes_Landed_Per_Min'] = row.get('Sig_Str_Landed_Per_Min', 0)
        fighter_data['Sig_Strikes_Absorbed_Per_Min'] = row.get('Sig_Str_Absorbed_Per_Min', 0)
        fighter_data['Average_Sig_Strikes'] = row.get('Average Sig', 0) or 0
        
        # Target Breakdown
        fighter_data['Sig_Strikes_Head_Pct'] = row.get('Sig_Str_Head_Pct', 0)
        fighter_data['Sig_Strikes_Body_Pct'] = row.get('Sig_Str_Body_Pct', 0)
        fighter_data['Sig_Strikes_Leg_Pct'] = row.get('Sig_Str_Leg_Pct', 0)
        
        # Grappling Statistics
        fighter_data['Takedowns_Landed'] = row.get('Takedownr', 0) or 0
        fighter_data['Takedowns_Attempted'] = 0  # Would need to calculate from data
        fighter_data['Takedown_Accuracy'] = row.get('Takedown Accuracy', 0) or 0
        fighter_data['Takedown_Defense'] = row.get('Takedown_Defense', 0)
        fighter_data['Takedown_Avg_Per_15_Min'] = row.get('Takedown_Avg_Per_15_Min', 0)
        fighter_data['Submissions'] = row.get('Submissio', 0) or 0
        fighter_data['Submission_Avg_Per_15_Min'] = row.get('Submission_Avg_Per_15_Min', 0)
        
        # Advanced Statistics
        fighter_data['Knockdowns'] = row.get('Knockdow', 0) or 0
        fighter_data['Knockdown_Avg'] = row.get('Knockdown_Avg', 0)
        fighter_data['Average_Fight_Time'] = row.get('Average_Fight_Time', 0)
        fighter_data['Control_Time'] = 0  # Would need to calculate from fight data
        
        # Recent Performance (Last 3 Fights)
        fighter_data['Event_1_Headline'] = row.get('Event_1_H', '')
        fighter_data['Event_1_Date'] = row.get('Event_1_D', '')
        fighter_data['Event_1_Result'] = row.get('Event_1_R', '')
        fighter_data['Event_1_Time'] = row.get('Event_1_Ti', '')
        fighter_data['Event_1_Method'] = row.get('Event_1_M', '')
        
        fighter_data['Event_2_Headline'] = row.get('Event_2_H', '')
        fighter_data['Event_2_Date'] = row.get('Event_2_D', '')
        fighter_data['Event_2_Result'] = row.get('Event_2_R', '')
        fighter_data['Event_2_Time'] = row.get('Event_2_Ti', '')
        fighter_data['Event_2_Method'] = row.get('Event_2_M', '')
        
        fighter_data['Event_3_Headline'] = row.get('Event_3_H', '')
        fighter_data['Event_3_Date'] = row.get('Event_3_D', '')
        fighter_data['Event_3_Result'] = row.get('Event_3_R', '')
        fighter_data['Event_3_Time'] = row.get('Event_3_Ti', '')
        fighter_data['Event_3_Method'] = row.get('Event_3_M', '')
        
        # Additional Fields
        fighter_data['Country'] = ''  # Would need to extract from data
        fighter_data['Place_of_Birth'] = ''  # Would need to extract from data
        fighter_data['Nickname'] = ''  # Would need to extract from data
        fighter_data['Stance'] = ''  # Would need to extract from data
        
        # Add to comprehensive dataframe
        comprehensive_df = pd.concat([comprehensive_df, pd.DataFrame([fighter_data])], ignore_index=True)
    
    # Save comprehensive profiles
    comprehensive_df.to_csv('data/comprehensive_fighter_profiles.csv', index=False)
    print(f"Created comprehensive fighter profiles with {len(comprehensive_df)} fighters")
    
    # Show sample of comprehensive data
    print("\nSample comprehensive fighter profile:")
    sample_fighter = comprehensive_df.iloc[0]
    print(f"Name: {sample_fighter['Name']}")
    print(f"Division: {sample_fighter['Division_Title']}")
    print(f"Record: {sample_fighter['Wins']}-{sample_fighter['Losses']} ({sample_fighter['Win_Percentage']}%)")
    print(f"Sig Strikes: {sample_fighter['Sig_Strikes_Landed']} landed, {sample_fighter['Striking_Accuracy']}% accuracy")
    print(f"Takedowns: {sample_fighter['Takedowns_Landed']} landed, {sample_fighter['Takedown_Accuracy']}% accuracy")
    print(f"Win Methods: {sample_fighter['Wins_by_KO_TKO']} KO/TKO, {sample_fighter['Wins_by_SUB']} SUB, {sample_fighter['Wins_by_DEC']} DEC")
    print(f"Target Breakdown: Head {sample_fighter['Sig_Strikes_Head_Pct']}%, Body {sample_fighter['Sig_Strikes_Body_Pct']}%, Leg {sample_fighter['Sig_Strikes_Leg_Pct']}%")
    
    # Show statistics
    print(f"\nComprehensive Profile Statistics:")
    print(f"Total Fighters: {len(comprehensive_df)}")
    print(f"Fighters with Real Stats: {len(comprehensive_df[comprehensive_df['Sig_Strikes_Landed'] > 0])}")
    print(f"Fighters with Takedowns: {len(comprehensive_df[comprehensive_df['Takedowns_Landed'] > 0])}")
    print(f"Fighters with Submissions: {len(comprehensive_df[comprehensive_df['Submissions'] > 0])}")
    
    return comprehensive_df

if __name__ == "__main__":
    create_comprehensive_fighter_profiles() 