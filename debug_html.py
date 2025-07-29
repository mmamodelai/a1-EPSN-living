#!/usr/bin/env python3
"""
Debug script to test HTML JSON extraction
"""

import json
import re
from pathlib import Path

def debug_html_extraction():
    """Debug the HTML JSON extraction process"""
    
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
    
    # Read HTML content
    with open(sample_file, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    print(f"HTML content length: {len(html_content)}")
    
    # Look for JSON patterns
    json_patterns = [
        r'window\.__INITIAL_STATE__\s*=\s*({.*?});',
        r'window\.__PRELOADED_STATE__\s*=\s*({.*?});',
        r'"athlete":\s*({.*?"statsSummary".*?})',
        r'"prtlCmnApiRsp":\s*({.*?"athlete".*?})',
    ]
    
    for i, pattern in enumerate(json_patterns):
        print(f"\nTrying pattern {i+1}: {pattern}")
        match = re.search(pattern, html_content, re.DOTALL)
        if match:
            print(f"Pattern {i+1} found a match!")
            json_str = match.group(1)
            print(f"JSON string length: {len(json_str)}")
            print(f"First 200 chars: {json_str[:200]}")
            
            try:
                # Clean up the JSON string
                json_str = re.sub(r'//.*?\n', '\n', json_str)  # Remove comments
                json_str = re.sub(r',\s*}', '}', json_str)  # Remove trailing commas
                data = json.loads(json_str)
                print("JSON parsed successfully!")
                
                # Check for athlete data
                if 'athlete' in data:
                    print("Found 'athlete' key in data")
                    athlete = data['athlete']
                    if 'statsSummary' in athlete:
                        print("Found 'statsSummary' in athlete data")
                        stats = athlete['statsSummary'].get('statistics', [])
                        print(f"Found {len(stats)} statistics")
                        for stat in stats:
                            print(f"  - {stat.get('name', 'unknown')}: {stat.get('displayValue', 'unknown')}")
                elif 'prtlCmnApiRsp' in data and 'athlete' in data['prtlCmnApiRsp']:
                    print("Found 'athlete' key in prtlCmnApiRsp")
                    athlete = data['prtlCmnApiRsp']['athlete']
                    if 'statsSummary' in athlete:
                        print("Found 'statsSummary' in athlete data")
                        stats = athlete['statsSummary'].get('statistics', [])
                        print(f"Found {len(stats)} statistics")
                        for stat in stats:
                            print(f"  - {stat.get('name', 'unknown')}: {stat.get('displayValue', 'unknown')}")
                else:
                    print("No athlete data found in parsed JSON")
                    print("Available keys:", list(data.keys()))
                
                break
                
            except json.JSONDecodeError as e:
                print(f"JSON decode error: {e}")
                continue
        else:
            print(f"Pattern {i+1} no match")
    
    # Also try a simpler approach - look for the specific record pattern
    print(f"\nLooking for record pattern in HTML...")
    record_matches = re.findall(r'"displayValue":"(\d+-\d+-\d+)"', html_content)
    print(f"Found {len(record_matches)} record matches: {record_matches[:5]}")

if __name__ == "__main__":
    debug_html_extraction()