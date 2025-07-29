#!/usr/bin/env python3
"""
Debug script to test extracting a single profile
"""

import json
import re
from pathlib import Path
from datetime import datetime

def debug_single_profile():
    """Debug extracting a single profile"""
    
    # Get a sample HTML file
    html_folder = Path("data/FighterHTMLs")
    html_files = list(html_folder.glob("*.html"))
    
    if not html_files:
        print("No HTML files found!")
        return
    
    # Use the first large HTML file
    sample_file = None
    for html_file in html_files:
        if html_file.stat().st_size > 10000:
            sample_file = html_file
            break
    
    if not sample_file:
        print("No large HTML files found!")
        return
    
    print(f"Testing with file: {sample_file.name}")
    print(f"File size: {sample_file.stat().st_size} bytes")
    
    # Extract fighter name from filename
    fighter_name = sample_file.stem.replace('_', ' ')
    print(f"Fighter name: {fighter_name}")
    
    # Read HTML content
    with open(sample_file, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    print(f"HTML content length: {len(html_content)}")
    
    # Look for the complete JSON structure
    json_start_pattern = r'"prtlCmnApiRsp":\s*{'
    start_match = re.search(json_start_pattern, html_content)
    
    if not start_match:
        print("No prtlCmnApiRsp found!")
        return
    
    print("Found prtlCmnApiRsp!")
    
    # Find the start position (after the key)
    start_pos = start_match.end() - 1  # Start at the opening brace
    print(f"Start position: {start_pos}")
    
    # Find the matching closing brace by counting braces
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
    
    print(f"End position: {end_pos}")
    print(f"Brace count: {brace_count}")
    
    if brace_count != 0:
        print("Brace count not balanced!")
        return
    
    # Extract the JSON string (just the object part)
    json_str = html_content[start_pos:end_pos]
    print(f"JSON string length: {len(json_str)}")
    print(f"First 200 chars: {json_str[:200]}")
    
    # Clean up the JSON string
    json_str = re.sub(r'//.*?\n', '\n', json_str)  # Remove comments
    json_str = re.sub(r',\s*}', '}', json_str)  # Remove trailing commas
    
    try:
        data = json.loads(json_str)
        print("JSON parsed successfully!")
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
        return
    
    # Navigate to athlete data
    if 'athlete' not in data:
        print("No athlete key found!")
        print("Available keys:", list(data.keys()))
        return
    
    athlete_data = data['athlete']
    print("Found athlete data!")
    
    # Extract profile information
    profile = {
        'Fighter Name': fighter_name,
        'ESPN URL': f"https://www.espn.com/mma/fighter/_/id/{athlete_data.get('id', '')}/{fighter_name.lower().replace(' ', '-')}",
        'Scraped At': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'Total Fights': 0,
        'Last Updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    print(f"Basic profile: {profile}")
    
    # Extract record from statsSummary
    if 'statsSummary' in athlete_data and 'statistics' in athlete_data['statsSummary']:
        stats = athlete_data['statsSummary']['statistics']
        print(f"Found {len(stats)} statistics")
        for stat in stats:
            print(f"  - {stat.get('name', 'unknown')}: {stat.get('displayValue', 'unknown')}")
            
            if stat.get('name') == 'wins-losses-draws':
                record = stat.get('displayValue', '0-0-0')
                profile['Division Record'] = record
                print(f"Found record: {record}")
                
                # Parse W-L-D
                parts = record.split('-')
                if len(parts) >= 3:
                    wins = int(parts[0]) if parts[0].isdigit() else 0
                    losses = int(parts[1]) if parts[1].isdigit() else 0
                    draws = int(parts[2]) if parts[2].isdigit() else 0
                    profile['Total Fights'] = wins + losses + draws
                    profile['Wins by Knockout'] = 0
                    profile['Wins by Submission'] = 0
                    profile['Wins by Decision'] = 0
                    print(f"Parsed: W={wins}, L={losses}, D={draws}")
    else:
        print("No statsSummary found!")
        print("Available keys in athlete:", list(athlete_data.keys()))
    
    print(f"Final profile: {profile}")

if __name__ == "__main__":
    debug_single_profile()