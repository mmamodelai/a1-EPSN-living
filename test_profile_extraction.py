#!/usr/bin/env python3
"""
Test profile extraction for Robert Whittaker
"""

from src.espn_data_processor import ESPNDataProcessor

def test_profile_extraction():
    """Test if Robert Whittaker's profile is extracted"""
    
    processor = ESPNDataProcessor()
    profiles = processor._extract_profiles_from_html()
    
    print(f"Extracted {len(profiles)} profiles")
    
    # Check for Robert Whittaker
    whittaker = profiles[profiles['Fighter Name'] == 'Robert Whittaker']
    
    if len(whittaker) > 0:
        print("Robert Whittaker found!")
        print(whittaker.iloc[0])
    else:
        print("Robert Whittaker NOT found!")
        
        # Check what fighters we do have
        print("\nFirst 10 fighters found:")
        print(profiles['Fighter Name'].head(10).tolist())
        
        # Check if there are any fighters with "Robert" in the name
        robert_fighters = profiles[profiles['Fighter Name'].str.contains('Robert', na=False)]
        print(f"\nFighters with 'Robert' in name: {len(robert_fighters)}")
        if len(robert_fighters) > 0:
            print(robert_fighters['Fighter Name'].tolist())

if __name__ == "__main__":
    test_profile_extraction()