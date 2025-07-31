#!/usr/bin/env python3

import json
import re
import pandas as pd
from pathlib import Path
import logging

def extract_fighter_profiles_from_html(html_file_path):
    """Extract fighter profile data from ESPN HTML file"""
    try:
        with open(html_file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Look for the ESPN embedded JSON object - it's in a script tag at the very end
        # The pattern is: "plyrHdr":{"ath":{"dob":"12/20/1990(34)",...},"statsBlck":{...}}
        # But it's embedded in a larger JSON object that ends with </script>
        
        # Find the last script tag that contains the JSON data
        script_tags = re.findall(r'<script[^>]*>([^<]+)</script>', html_content, re.DOTALL)
        
        if not script_tags:
            logging.debug(f"No script tags found in {html_file_path}")
            return None
        
        # Look for the plyrHdr section in the last script tag (which should contain the data)
        last_script = script_tags[-1]
        
        # Look for the plyrHdr section in the script content
        # The exact pattern is: "plyrHdr":{"ath":{"dob":"12/20/1990(34)",...},"statsBlck":{...}}
        plyr_hdr_match = re.search(r'"plyrHdr":\s*({[^}]*"ath":\s*{[^}]*"statsBlck":\s*{[^}]*}[^}]*})', last_script, re.DOTALL)
        
        if not plyr_hdr_match:
            # Try a simpler pattern just for plyrHdr
            plyr_hdr_match = re.search(r'"plyrHdr":\s*({[^}]*})', last_script, re.DOTALL)
            
        if not plyr_hdr_match:
            logging.debug(f"No plyrHdr found in {html_file_path}")
            return None
        
        # Extract the plyrHdr JSON string
        plyr_hdr_str = plyr_hdr_match.group(1)
        
        # Try to parse the JSON
        try:
            plyr_hdr = json.loads(plyr_hdr_str)
        except json.JSONDecodeError as e:
            logging.debug(f"JSON parse error for {html_file_path}: {e}")
            return None
        
        # Extract athlete data
        if 'ath' not in plyr_hdr:
            logging.debug(f"No ath data found in {html_file_path}")
            return None
        
        athlete = plyr_hdr['ath']
        
        # Extract stats data
        stats = {}
        if 'statsBlck' in plyr_hdr and 'vals' in plyr_hdr['statsBlck']:
            for stat in plyr_hdr['statsBlck']['vals']:
                if 'name' in stat and 'val' in stat:
                    stats[stat['name']] = stat['val']
        
        # Create profile dictionary
        profile = {
            'Name': athlete.get('dspNm', ''),
            'Division Tr': athlete.get('wghtclss', ''),
            'Division Rk': athlete.get('wghtclssshrt', ''),
            'Wins by Kn': stats.get('Technical Knockout-Technical Knockout Losses', '').split('-')[0] if 'Technical Knockout-Technical Knockout Losses' in stats else '',
            'Wins by Su': stats.get('Submissions-Submission Losses', '').split('-')[0] if 'Submissions-Submission Losses' in stats else '',
            'First Roun': '',  # Not available in this data structure
            'Wins by De': stats.get('Wins-Losses-Draws', '').split('-')[0] if 'Wins-Losses-Draws' in stats else '',
            'Height': athlete.get('htwt', '').split(',')[0] if athlete.get('htwt') else '',
            'Weight': athlete.get('htwt', '').split(',')[1].strip() if athlete.get('htwt') and ',' in athlete.get('htwt') else '',
            'Reach': athlete.get('rch', ''),
            'Stance': athlete.get('stnc', ''),
            'DOB': athlete.get('dob', ''),
            'Team': athlete.get('tm', ''),
            'Country': athlete.get('cntry', ''),
            'Nickname': athlete.get('ncknm', '')
        }
        
        return profile
        
    except Exception as e:
        logging.error(f"Error processing {html_file_path}: {e}")
        return None

def main():
    logging.basicConfig(level=logging.INFO)
    
    # Test with a few HTML files
    html_dir = Path('data/FighterHTMLs')
    test_files = ['Robert_Whittaker.html', 'Sean_O\'Malley.html', 'Aaron_Jeffery.html']
    
    profiles = []
    
    for test_file in test_files:
        file_path = html_dir / test_file
        if file_path.exists():
            profile = extract_fighter_profiles_from_html(file_path)
            if profile:
                profiles.append(profile)
                print(f"✅ Extracted profile for {profile['Name']}")
            else:
                print(f"❌ Failed to extract profile from {test_file}")
        else:
            print(f"❌ File not found: {test_file}")
    
    if profiles:
        df = pd.DataFrame(profiles)
        print(f"\n📊 Extracted {len(profiles)} profiles:")
        print(df.to_string(index=False))
        
        # Save to CSV
        df.to_csv('test_fighter_profiles.csv', index=False)
        print(f"\n💾 Saved to test_fighter_profiles.csv")
    else:
        print("❌ No profiles extracted")

if __name__ == "__main__":
    main()