#!/usr/bin/env python3
"""
Deep analysis of ESPN HTML structure to find comprehensive fight data
"""

import json
import re
from pathlib import Path
from bs4 import BeautifulSoup

def deep_espn_analysis():
    """Do a deep analysis of ESPN HTML to find all available data structures"""
    
    # Read Robert Whittaker's HTML file
    html_file = Path("data/FighterHTMLs/Robert_Whittaker.html")
    
    if not html_file.exists():
        print("Robert Whittaker HTML file not found!")
        return
    
    with open(html_file, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    print(f"HTML file size: {len(html_content)} characters")
    
    # Parse with BeautifulSoup to look for different data structures
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Look for all script tags that might contain data
    script_tags = soup.find_all('script')
    print(f"\n=== FOUND {len(script_tags)} SCRIPT TAGS ===")
    
    data_structures = []
    
    for i, script in enumerate(script_tags):
        script_content = script.get_text()
        
        # Look for various data patterns
        patterns = [
            r'"prtlCmnApiRsp":\s*({[^}]+})',
            r'"athlete":\s*({[^}]+})',
            r'"stats":\s*(\[[^\]]+\])',
            r'"fights":\s*(\[[^\]]+\])',
            r'"fightHistory":\s*(\[[^\]]+\])',
            r'"recentFights":\s*(\[[^\]]+\])',
            r'"careerStats":\s*({[^}]+})',
            r'"bio":\s*({[^}]+})',
            r'"data":\s*({[^}]+})',
            r'"content":\s*({[^}]+})'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, script_content)
            if matches:
                data_structures.append({
                    'script_index': i,
                    'pattern': pattern,
                    'matches': len(matches),
                    'sample': matches[0][:200] + '...' if len(matches[0]) > 200 else matches[0]
                })
    
    print(f"\n=== DATA STRUCTURES FOUND ===")
    for structure in data_structures:
        print(f"Script {structure['script_index']}: {structure['pattern']} - {structure['matches']} matches")
        print(f"  Sample: {structure['sample']}")
        print()
    
    # Look for specific ESPN data patterns
    print("=== SEARCHING FOR ESPN-SPECIFIC DATA ===")
    
    # Look for window.__INITIAL_STATE__ or similar
    initial_state_pattern = r'window\.__INITIAL_STATE__\s*=\s*({[^}]+})'
    initial_state_match = re.search(initial_state_pattern, html_content)
    if initial_state_match:
        print("Found window.__INITIAL_STATE__")
    
    # Look for ESPN API responses
    api_patterns = [
        r'"api":\s*({[^}]+})',
        r'"response":\s*({[^}]+})',
        r'"result":\s*({[^}]+})',
        r'"data":\s*({[^}]+})'
    ]
    
    for pattern in api_patterns:
        matches = re.findall(pattern, html_content)
        if matches:
            print(f"Found {len(matches)} matches for {pattern}")
    
    # Look for fight-specific data
    fight_patterns = [
        r'"fight":\s*({[^}]+})',
        r'"event":\s*({[^}]+})',
        r'"opponent":\s*({[^}]+})',
        r'"result":\s*({[^}]+})'
    ]
    
    for pattern in fight_patterns:
        matches = re.findall(pattern, html_content)
        if matches:
            print(f"Found {len(matches)} matches for {pattern}")
    
    # Look for table data or structured content
    print("\n=== LOOKING FOR TABLE/STRUCTURED DATA ===")
    
    # Check for tables
    tables = soup.find_all('table')
    print(f"Found {len(tables)} tables")
    
    # Check for data attributes
    data_elements = soup.find_all(attrs={"data-": True})
    print(f"Found {len(data_elements)} elements with data attributes")
    
    # Check for specific ESPN classes
    espn_classes = [
        'c-stat-compare',
        'c-stat-3bar',
        'c-overlap__stats',
        'c-card-event',
        'c-bio__field'
    ]
    
    for class_name in espn_classes:
        elements = soup.find_all(class_=class_name)
        if elements:
            print(f"Found {len(elements)} elements with class '{class_name}'")
    
    # Look for any JSON-like structures in the HTML
    print("\n=== SEARCHING FOR ANY JSON STRUCTURES ===")
    
    # Find all potential JSON objects
    json_pattern = r'\{[^{}]*"[^"]*"[^{}]*\}'
    json_matches = re.findall(json_pattern, html_content)
    print(f"Found {len(json_matches)} potential JSON objects")
    
    # Look for arrays
    array_pattern = r'\[[^\[\]]*"[^\[\]]*"[^\[\]]*\]'
    array_matches = re.findall(array_pattern, html_content)
    print(f"Found {len(array_matches)} potential JSON arrays")
    
    # Check if there are any hidden divs with data
    hidden_divs = soup.find_all('div', style='display: none')
    print(f"Found {len(hidden_divs)} hidden divs")
    
    # Look for any data in comments
    comments = soup.find_all(string=lambda text: isinstance(text, str) and '<!--' in text)
    print(f"Found {len(comments)} comments with potential data")

if __name__ == "__main__":
    deep_espn_analysis() 