#!/usr/bin/env python3
"""
Extract comprehensive fight-by-fight data from ESPN HTML files
"""

import json
import re
from pathlib import Path
from bs4 import BeautifulSoup

def extract_fight_data():
    """Extract comprehensive fight data from ESPN HTML"""
    
    # Read Robert Whittaker's HTML file
    html_file = Path("data/FighterHTMLs/Robert_Whittaker.html")
    
    if not html_file.exists():
        print("Robert Whittaker HTML file not found!")
        return
    
    with open(html_file, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    print(f"HTML file size: {len(html_content)} characters")
    
    # Look for fight data patterns
    fight_patterns = [
        r'"result":\s*({[^}]+})',
        r'"opponent":\s*({[^}]+})',
        r'"event":\s*({[^}]+})',
        r'"date":\s*"([^"]+)"',
        r'"method":\s*"([^"]+)"',
        r'"round":\s*(\d+)',
        r'"time":\s*"([^"]+)"'
    ]
    
    print("=== EXTRACTING FIGHT DATA ===")
    
    for pattern in fight_patterns:
        matches = re.findall(pattern, html_content)
        if matches:
            print(f"\nPattern: {pattern}")
            print(f"Found {len(matches)} matches")
            print("First 5 matches:")
            for i, match in enumerate(matches[:5]):
                print(f"  {i+1}: {match}")
    
    # Look for more specific fight data structures
    print("\n=== LOOKING FOR FIGHT ARRAYS ===")
    
    # Look for arrays that might contain fight data
    array_patterns = [
        r'"fights":\s*(\[[^\]]+\])',
        r'"fightHistory":\s*(\[[^\]]+\])',
        r'"recentFights":\s*(\[[^\]]+\])',
        r'"stats":\s*(\[[^\]]+\])',
        r'"results":\s*(\[[^\]]+\])'
    ]
    
    for pattern in array_patterns:
        matches = re.findall(pattern, html_content)
        if matches:
            print(f"\nPattern: {pattern}")
            print(f"Found {len(matches)} matches")
            for i, match in enumerate(matches[:3]):
                print(f"  Match {i+1}: {match[:300]}...")
    
    # Look for table data
    print("\n=== EXTRACTING TABLE DATA ===")
    
    soup = BeautifulSoup(html_content, 'html.parser')
    tables = soup.find_all('table')
    
    for i, table in enumerate(tables):
        print(f"\nTable {i+1}:")
        rows = table.find_all('tr')
        print(f"  Rows: {len(rows)}")
        
        # Print first few rows
        for j, row in enumerate(rows[:3]):
            cells = row.find_all(['td', 'th'])
            cell_texts = [cell.get_text(strip=True) for cell in cells]
            print(f"    Row {j+1}: {cell_texts}")
    
    # Look for specific ESPN fight data structures
    print("\n=== LOOKING FOR ESPN FIGHT STRUCTURES ===")
    
    # Look for the main JSON structure and extract fight data
    json_start_pattern = r'"prtlCmnApiRsp":\s*{'
    start_match = re.search(json_start_pattern, html_content)
    
    if start_match:
        print("Found prtlCmnApiRsp, extracting complete JSON...")
        
        # Find the complete JSON object
        start_pos = start_match.end() - 1
        
        # Find the matching closing brace
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
        
        if brace_count == 0:
            json_str = html_content[start_pos:end_pos]
            json_str = re.sub(r'//.*?\n', '\n', json_str)
            json_str = re.sub(r',\s*}', '}', json_str)
            
            try:
                data = json.loads(json_str)
                print("Successfully parsed complete JSON!")
                
                # Look for fight data in the complete structure
                def find_fight_data(obj, path=""):
                    """Recursively search for fight-related data"""
                    if isinstance(obj, dict):
                        for key, value in obj.items():
                            current_path = f"{path}.{key}" if path else key
                            
                            # Check if this looks like fight data
                            if any(fight_key in key.lower() for fight_key in ['fight', 'result', 'opponent', 'event', 'date', 'method', 'round', 'time']):
                                print(f"  Found potential fight data at {current_path}: {value}")
                            
                            # Recursively search
                            find_fight_data(value, current_path)
                    elif isinstance(obj, list):
                        for i, item in enumerate(obj):
                            current_path = f"{path}[{i}]"
                            find_fight_data(item, current_path)
                
                print("\nSearching for fight data in JSON structure:")
                find_fight_data(data)
                
            except json.JSONDecodeError as e:
                print(f"JSON decode error: {e}")

if __name__ == "__main__":
    extract_fight_data() 