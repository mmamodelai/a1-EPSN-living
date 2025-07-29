#!/usr/bin/env python3
"""
Examine comprehensive ESPN data structure
"""

import json
import re
from pathlib import Path

def examine_espn_data():
    """Examine what comprehensive data is available in ESPN HTML files"""
    
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
        
        # Print all available keys in athlete data
        print("\n=== AVAILABLE ATHLETE DATA KEYS ===")
        for key in athlete_data.keys():
            print(f"- {key}")
        
        # Check for fight history/stats
        if 'stats' in athlete_data:
            stats = athlete_data['stats']
            print(f"\n=== FIGHT STATS ({len(stats)} records) ===")
            if stats:
                print("Sample stat keys:", list(stats[0].keys()) if isinstance(stats[0], dict) else "Not a dict")
                print("First 3 fights:")
                for i, stat in enumerate(stats[:3]):
                    print(f"  Fight {i+1}: {stat}")
        
        # Check for statsSummary
        if 'statsSummary' in athlete_data:
            stats_summary = athlete_data['statsSummary']
            print(f"\n=== STATS SUMMARY ===")
            print("Keys:", list(stats_summary.keys()))
            if 'statistics' in stats_summary:
                stats_list = stats_summary['statistics']
                print(f"Statistics ({len(stats_list)} items):")
                for stat in stats_list:
                    print(f"  - {stat.get('name', 'Unknown')}: {stat.get('displayValue', 'No value')}")
        
        # Check for bio information
        if 'bio' in athlete_data:
            bio = athlete_data['bio']
            print(f"\n=== BIO INFORMATION ===")
            print("Keys:", list(bio.keys()))
            for key, value in bio.items():
                print(f"  {key}: {value}")
        
        # Check for career stats
        if 'careerStats' in athlete_data:
            career_stats = athlete_data['careerStats']
            print(f"\n=== CAREER STATS ===")
            print("Keys:", list(career_stats.keys()))
            for key, value in career_stats.items():
                print(f"  {key}: {value}")
        
        # Check for recent fights
        if 'recentFights' in athlete_data:
            recent_fights = athlete_data['recentFights']
            print(f"\n=== RECENT FIGHTS ===")
            print(f"Number of recent fights: {len(recent_fights)}")
            if recent_fights:
                print("Sample fight keys:", list(recent_fights[0].keys()) if isinstance(recent_fights[0], dict) else "Not a dict")
                print("First 3 recent fights:")
                for i, fight in enumerate(recent_fights[:3]):
                    print(f"  Fight {i+1}: {fight}")
        
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
        return

if __name__ == "__main__":
    examine_espn_data() 