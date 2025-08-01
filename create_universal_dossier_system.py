import pandas as pd
import numpy as np
from pathlib import Path
import re
from datetime import datetime
from bs4 import BeautifulSoup

class UniversalDossierSystem:
    """Universal system to create professional fighter dossiers from ESPN data."""
    
    def __init__(self):
        """Initialize the dossier system with data sources."""
        self.profiles_df = None
        self.striking_df = None
        self.clinch_df = None
        self.ground_df = None
        self.load_data_sources()
    
    def load_data_sources(self):
        """Load all data sources for dossier creation."""
        print("Loading data sources...")
        
        # Load enhanced fighter profiles
        try:
            self.profiles_df = pd.read_csv('data/enhanced_fighter_profiles_final.csv')
            print(f"✓ Loaded {len(self.profiles_df)} fighter profiles")
        except:
            self.profiles_df = pd.read_csv('data/fighter_profiles.csv')
            print(f"✓ Loaded {len(self.profiles_df)} fighter profiles (fallback)")
        
        # Load living documents
        try:
            self.striking_df = pd.read_csv('data/striking_data_living.csv', low_memory=False)
            self.clinch_df = pd.read_csv('data/clinch_data_living.csv', low_memory=False)
            self.ground_df = pd.read_csv('data/ground_data_living.csv', low_memory=False)
            print(f"✓ Loaded living documents: {len(self.striking_df)} striking, {len(self.clinch_df)} clinch, {len(self.ground_df)} ground records")
        except Exception as e:
            print(f"Error loading living documents: {e}")
    
    def get_fighter_data(self, fighter_name):
        """Extract all data for a specific fighter."""
        # Normalize fighter name for matching
        normalized_name = fighter_name.replace(' ', '').replace('_', '').replace('-', '')
        
        # Get fighter profile
        fighter_profile = self.profiles_df[
            self.profiles_df['Name'].str.contains(normalized_name, case=False, na=False)
        ]
        
        # Get fight data
        fighter_striking = self.striking_df[
            self.striking_df['Player'].str.contains(normalized_name, case=False, na=False)
        ]
        fighter_clinch = self.clinch_df[
            self.clinch_df['Player'].str.contains(normalized_name, case=False, na=False)
        ]
        fighter_ground = self.ground_df[
            self.ground_df['Player'].str.contains(normalized_name, case=False, na=False)
        ]
        
        return fighter_profile, fighter_striking, fighter_clinch, fighter_ground
    
    def create_fighter_profile_sheet(self, fighter_profile, fighter_name):
        """Create the fighter profile sheet following professional dossier structure."""
        
        if fighter_profile.empty:
            print(f"No profile found for {fighter_name}")
            return pd.DataFrame()
        
        profile = fighter_profile.iloc[0]
        
        # Calculate derived statistics
        total_fights = len(self.striking_df[self.striking_df['Player'].str.contains(fighter_name.replace(' ', ''), case=False, na=False)])
        total_fight_time = total_fights * 15  # Estimate 15 minutes per fight
        
        # Calculate win methods from fight data
        fighter_fights = self.striking_df[self.striking_df['Player'].str.contains(fighter_name.replace(' ', ''), case=False, na=False)]
        ko_tko_wins = len(fighter_fights[fighter_fights['Result'].str.contains('W', na=False) & 
                                       (fighter_fights['Method'].str.contains('KO', na=False) | 
                                        fighter_fights['Method'].str.contains('TKO', na=False))])
        decision_wins = len(fighter_fights[fighter_fights['Result'].str.contains('W', na=False) & 
                                         fighter_fights['Method'].str.contains('DEC', na=False)])
        submission_wins = len(fighter_fights[fighter_fights['Result'].str.contains('W', na=False) & 
                                           fighter_fights['Method'].str.contains('SUB', na=False)])
        
        # Create comprehensive fighter profile data
        fighter_profile_data = {
            'Name': [fighter_name],
            'Division Title': [profile.get('Division_Title', 'Unknown Division')],
            'Division Record': [profile.get('Division_Record', '')],
            'Striking accuracy': [profile.get('Striking_Accuracy', 0)],
            'Sig. Strikes Landed': [profile.get('Sig_Strikes_Landed', 0)],
            'Sig. Strikes Attempted': [profile.get('Sig_Strikes_Attempted', 0)],
            'Takedown Accuracy': [profile.get('Takedown_Accuracy', 0)],
            'Takedowns Landed': [profile.get('Takedowns_Landed', 0)],
            'Takedowns Attempted': [profile.get('Takedowns_Attempted', 0)],
            'Sig. Str. Landed Per Min': [round(profile.get('Sig_Strikes_Landed', 0) / total_fight_time, 2) if total_fight_time > 0 else 0],
            'Sig. Str. Absorbed Per Min': [round(profile.get('Sig_Strikes_Absorbed_Per_Min', 0), 2)],
            'Takedown avg Per 15 Min': [round((profile.get('Takedowns_Landed', 0) / total_fights) * 15, 2) if total_fights > 0 else 0],
            'Submission avg Per 15 Min': [round((submission_wins / total_fights) * 15, 2) if total_fights > 0 else 0],
            'Sig. Str. Defense': [profile.get('Striking_Defense', 0)],
            'Takedown Defense': [profile.get('Takedown_Defense', 0)],
            'Knockdown Avg': [round(profile.get('Knockdown_Avg', 0), 1)],
            'Average fight time': [round(total_fight_time / total_fights, 1) if total_fights > 0 else 0],
            'Sig. Str. By Position - Standing': [profile.get('Distance_Strikes', 0)],
            'Sig. Str. By Position - Clinch': [profile.get('Clinch_Strikes', 0)],
            'Sig. Str. By Position - Ground': [profile.get('Ground_Strikes', 0)],
            'Win by Method - KO/TKO': [ko_tko_wins],
            'Win by Method - DEC': [decision_wins],
            'Win by Method - SUB': [submission_wins],
            'Sig. Str. by target - Head Strike Percentage': [profile.get('Sig_Strikes_Head_Pct', 0)],
            'Sig. Str. by target - Head Strike Count': [profile.get('Head_Strikes', 0)],
            'Sig. Str. by target - Body Strike Percentage': [profile.get('Sig_Strikes_Body_Pct', 0)],
            'Sig. Str. by target - Body Strike Count': [profile.get('Body_Strikes', 0)],
            'Sig. Str. by target - Leg Strike Percentage': [profile.get('Sig_Strikes_Leg_Pct', 0)],
            'Sig. Str. by target - Leg Strike Count': [profile.get('Leg_Strikes', 0)],
            'Status': [profile.get('Status', 'Active')],
            'Place_of_Birth': [profile.get('Place_of_Birth', '')],
            'Fighting_style': [profile.get('Fighting_Style', 'Mixed Martial Arts')],
            'Age': [profile.get('Age', 0)],
            'Height': [profile.get('Height', '')],
            'Weight': [profile.get('Weight', '')],
            'Octagon_Debut': [profile.get('Octagon_Debut', '')],
            'Reach': [profile.get('Reach', '')],
            'Leg_reach': [profile.get('Leg_Reach', '')],
            'Trains_at': [profile.get('Team', '')],
            'Fight Win Streak': [profile.get('Fight_Win_Streak', 0)],
            'Title Defenses': [profile.get('Title_Defenses', 0)],
            'Former Champion': [profile.get('Former_Champion', 'No')]
        }
        
        return pd.DataFrame(fighter_profile_data)
    
    def create_fight_results_sheet(self, fighter_striking, fighter_name):
        """Create the processed UFC fight results sheet."""
        
        if fighter_striking.empty:
            return pd.DataFrame()
        
        fight_results = []
        
        for _, fight in fighter_striking.iterrows():
            fight_data = {
                'EVENT': fight.get('Event', ''),
                'BOUT': f"{fighter_name} vs {fight.get('Opponent', '')}",
                'Weight Division': 'Unknown',
                'Fighter 1': fighter_name,
                'Fighter 2': fight.get('Opponent', ''),
                'Winning Fighter': fighter_name if 'W' in str(fight.get('Result', '')).upper() else fight.get('Opponent', ''),
                'Losing Fighter': fight.get('Opponent', '') if 'W' in str(fight.get('Result', '')).upper() else fighter_name,
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
    
    def create_offensive_sheet(self, fighter_striking, fighter_clinch, fighter_ground, fighter_name):
        """Create the offensive fight data sheet."""
        
        if fighter_striking.empty:
            return pd.DataFrame()
        
        offensive_data = []
        
        for _, fight in fighter_striking.iterrows():
            # Find corresponding clinch and ground data
            fight_date = fight.get('Date', '')
            fight_opponent = fight.get('Opponent', '')
            
            clinch_fight = fighter_clinch[
                (fighter_clinch['Date'] == fight_date) & 
                (fighter_clinch['Opponent'] == fight_opponent)
            ]
            
            ground_fight = fighter_ground[
                (fighter_ground['Date'] == fight_date) & 
                (fighter_ground['Opponent'] == fight_opponent)
            ]
            
            offensive_fight = {
                'Player': fighter_name,
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
    
    def create_defensive_sheet(self, fighter_striking, fighter_clinch, fighter_ground, fighter_name):
        """Create the defensive fight data sheet."""
        
        if fighter_striking.empty:
            return pd.DataFrame()
        
        defensive_data = []
        
        for _, fight in fighter_striking.iterrows():
            # Find corresponding clinch and ground data
            fight_date = fight.get('Date', '')
            fight_opponent = fight.get('Opponent', '')
            
            clinch_fight = fighter_clinch[
                (fighter_clinch['Date'] == fight_date) & 
                (fighter_clinch['Opponent'] == fight_opponent)
            ]
            
            ground_fight = fighter_ground[
                (fighter_ground['Date'] == fight_date) & 
                (fighter_ground['Opponent'] == fight_opponent)
            ]
            
            defensive_fight = {
                'Player': fighter_name,
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
    
    def create_fighter_dossier(self, fighter_name, output_dir='dossiers'):
        """Create a comprehensive dossier for any fighter."""
        
        print(f"\n🎯 Creating dossier for: {fighter_name}")
        
        # Create output directory
        Path(output_dir).mkdir(exist_ok=True)
        
        # Get fighter data
        fighter_profile, fighter_striking, fighter_clinch, fighter_ground = self.get_fighter_data(fighter_name)
        
        if fighter_profile.empty:
            print(f"❌ No data found for {fighter_name}")
            return None
        
        # Create all sheets
        print("Creating fighter profile sheet...")
        fighter_profile_sheet = self.create_fighter_profile_sheet(fighter_profile, fighter_name)
        
        print("Creating fight results sheet...")
        fight_results_sheet = self.create_fight_results_sheet(fighter_striking, fighter_name)
        
        print("Creating offensive data sheet...")
        offensive_sheet = self.create_offensive_sheet(fighter_striking, fighter_clinch, fighter_ground, fighter_name)
        
        print("Creating defensive data sheet...")
        defensive_sheet = self.create_defensive_sheet(fighter_striking, fighter_clinch, fighter_ground, fighter_name)
        
        # Create Excel file
        safe_name = fighter_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
        filename = f"{output_dir}/{safe_name}_Dossier.xlsx"
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            fighter_profile_sheet.to_excel(writer, sheet_name='fighter_profiles', index=False)
            fight_results_sheet.to_excel(writer, sheet_name='processed_ufc_fight_results', index=False)
            offensive_sheet.to_excel(writer, sheet_name='fight_data_offensive_combined', index=False)
            defensive_sheet.to_excel(writer, sheet_name='fight_data_defensive_combined', index=False)
        
        print(f"✅ Dossier created: {filename}")
        
        # Display summary
        print(f"\n📊 Dossier Summary for {fighter_name}:")
        print(f"  Fighter Profile: {len(fighter_profile_sheet)} record")
        print(f"  Fight Results: {len(fight_results_sheet)} fights")
        print(f"  Offensive Data: {len(offensive_sheet)} fights")
        print(f"  Defensive Data: {len(defensive_sheet)} fights")
        
        return {
            'filename': filename,
            'fighter_profile': fighter_profile_sheet,
            'fight_results': fight_results_sheet,
            'offensive_data': offensive_sheet,
            'defensive_data': defensive_sheet
        }
    
    def create_multiple_dossiers(self, fighter_names, output_dir='dossiers'):
        """Create dossiers for multiple fighters."""
        
        print(f"🎯 Creating dossiers for {len(fighter_names)} fighters...")
        
        results = []
        for fighter_name in fighter_names:
            try:
                result = self.create_fighter_dossier(fighter_name, output_dir)
                if result:
                    results.append(result)
            except Exception as e:
                print(f"❌ Error creating dossier for {fighter_name}: {e}")
        
        print(f"\n✅ Successfully created {len(results)} dossiers")
        return results
    
    def get_available_fighters(self):
        """Get list of all available fighters."""
        return self.profiles_df['Name'].tolist()

def main():
    """Main function to demonstrate the universal dossier system."""
    
    # Initialize the system
    dossier_system = UniversalDossierSystem()
    
    # Example: Create dossier for a specific fighter
    fighter_name = "Robert Whittaker"
    dossier_system.create_fighter_dossier(fighter_name)
    
    # Example: Create dossiers for multiple fighters
    test_fighters = ["Robert Whittaker", "Israel Adesanya", "Paulo Costa"]
    dossier_system.create_multiple_dossiers(test_fighters)
    
    # Show available fighters
    print(f"\n📋 Available fighters: {len(dossier_system.get_available_fighters())}")

if __name__ == "__main__":
    main() 