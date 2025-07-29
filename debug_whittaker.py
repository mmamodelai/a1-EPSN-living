#!/usr/bin/env python3
"""
Debug script to test Robert Whittaker profile extraction
"""

import json
import re
from pathlib import Path

def debug_whittaker_extraction():
    """Debug why Robert Whittaker's profile isn't being extracted"""
    
    # Read Robert Whittaker's HTML file
    html_file = Path("data/FighterHTMLs/Robert_Whittaker.html")
    
    if not html_file.exists():
        print("Robert Whittaker HTML file not found!")
        return
    
    with open(html_file, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    print(f"HTML file size: {len(html_content)} characters")
    
    # Look for the JSON structure
    json_start_pattern = r'"prtlCmnApiRsp":\s*{'
    start_match = re.search(json_start_pattern, html_content)
    
    if not start_match:
        print("No prtlCmnApiRsp found in HTML!")
        return
    
    print("Found prtlCmnApiRsp in HTML")
    
    # Find the start position (after the key)
    start_pos = start_match.end() - 1  # Start at the opening brace
    
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
    
    if brace_count != 0:
        print(f"Brace count mismatch: {brace_count}")
        return
    
    # Extract the JSON string (just the object part)
    json_str = html_content[start_pos:end_pos]
    
    # Clean up the JSON string
    json_str = re.sub(r'//.*?\n', '\n', json_str)  # Remove comments
    json_str = re.sub(r',\s*}', '}', json_str)  # Remove trailing commas
    
    try:
        data = json.loads(json_str)
        print("Successfully parsed JSON!")
        
        # Navigate to athlete data
        if 'athlete' not in data:
            print("No athlete data found in JSON!")
            return
        
        athlete_data = data['athlete']
        print(f"Athlete data found: {athlete_data.get('displayName', 'Unknown')}")
        
        # Check for statsSummary
        if 'statsSummary' in athlete_data and 'statistics' in athlete_data['statsSummary']:
            stats = athlete_data['statsSummary']['statistics']
            print(f"Found {len(stats)} statistics")
            
            for stat in stats:
                if stat.get('name') == 'wins-losses-draws':
                    record = stat.get('displayValue', '0-0-0')
                    print(f"Record found: {record}")
                    break
        else:
            print("No statsSummary found in athlete data")
            
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
        return

if __name__ == "__main__":
    debug_whittaker_extraction() 