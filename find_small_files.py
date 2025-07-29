#!/usr/bin/env python3
"""
Script to find all HTML files under 5KB in the A1-ESPN-Profiles FighterHTMLs folder
and generate a list of fighters to scrape
"""

import os
from pathlib import Path
import pandas as pd
from datetime import datetime

def find_small_html_files(html_folder="data/FighterHTMLs", size_limit_kb=5):
    """
    Find all HTML files under the specified size limit
    
    Args:
        html_folder: Path to the HTML files folder
        size_limit_kb: Size limit in KB (default 5KB)
    
    Returns:
        List of dictionaries with file info
    """
    html_path = Path(html_folder)
    small_files = []
    
    if not html_path.exists():
        print(f"❌ HTML folder not found: {html_path}")
        return small_files
    
    print(f"🔍 Scanning {html_path} for files under {size_limit_kb}KB...")
    
    # Get all HTML files
    html_files = list(html_path.glob("*.html"))
    print(f"📁 Found {len(html_files)} total HTML files")
    
    size_limit_bytes = size_limit_kb * 1024
    
    for html_file in html_files:
        try:
            file_size = html_file.stat().st_size
            if file_size < size_limit_bytes:
                # Get file modification time
                mtime = html_file.stat().st_mtime
                mod_time = datetime.fromtimestamp(mtime)
                
                # Read first few lines to check content
                with open(html_file, 'r', encoding='utf-8') as f:
                    first_lines = f.read(500)  # Read first 500 chars
                
                # Check if it's a real ESPN page or placeholder
                is_real_espn = "prtlCmnApiRsp" in first_lines
                
                small_files.append({
                    'filename': html_file.name,
                    'fighter_name': html_file.stem.replace('_', ' '),
                    'size_bytes': file_size,
                    'size_kb': round(file_size / 1024, 2),
                    'modified': mod_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'is_real_espn': is_real_espn,
                    'content_preview': first_lines[:100] + "..." if len(first_lines) > 100 else first_lines
                })
        except Exception as e:
            print(f"❌ Error processing {html_file.name}: {e}")
    
    return small_files

def generate_fighter_list(small_files):
    """
    Generate a list of fighter names from small files
    """
    fighter_names = []
    for file_info in small_files:
        fighter_name = file_info['fighter_name']
        # Clean up the name (remove extra underscores, etc.)
        fighter_name = fighter_name.replace('_', ' ').strip()
        if fighter_name and fighter_name not in fighter_names:
            fighter_names.append(fighter_name)
    
    return fighter_names

def main():
    print("=== A1-ESPN-Profiles HTML File Size Checker ===")
    print("Finding all HTML files under 5KB...\n")
    
    small_files = find_small_html_files()
    
    if not small_files:
        print("✅ No files found under 5KB!")
        return
    
    print(f"\n❌ Found {len(small_files)} files under 5KB:")
    print("=" * 80)
    
    # Create DataFrame for better display
    df = pd.DataFrame(small_files)
    
    # Sort by size (smallest first)
    df = df.sort_values('size_bytes')
    
    # Display results
    for idx, row in df.iterrows():
        status = "✅" if row['is_real_espn'] else "❌"
        print(f"{status} {row['fighter_name']:30} | {row['size_kb']:6.2f}KB | {row['modified']} | {row['filename']}")
    
    print("\n" + "=" * 80)
    print(f"📊 Summary:")
    print(f"   Total small files: {len(small_files)}")
    print(f"   Real ESPN data: {sum(df['is_real_espn'])}")
    print(f"   Placeholder files: {len(small_files) - sum(df['is_real_espn'])}")
    
    # Generate fighter list
    fighter_names = generate_fighter_list(small_files)
    
    print(f"\n🔄 Fighters that need rescraping ({len(fighter_names)}):")
    for i, fighter in enumerate(fighter_names, 1):
        print(f"   {i:3d}. {fighter}")
    
    # Save detailed report
    output_file = "small_html_files_report.csv"
    df.to_csv(output_file, index=False)
    print(f"\n💾 Detailed report saved to: {output_file}")
    
    # Save fighter list for scraping
    fighter_list_file = "fighters_to_scrape.csv"
    fighter_df = pd.DataFrame({'fighters': fighter_names})
    fighter_df.to_csv(fighter_list_file, index=False)
    print(f"💾 Fighter list saved to: {fighter_list_file}")
    
    # Also save as JSON for easy use
    import json
    fighter_json_file = "fighters_to_scrape.json"
    with open(fighter_json_file, 'w') as f:
        json.dump(fighter_names, f, indent=2)
    print(f"💾 Fighter list saved to: {fighter_json_file}")
    
    print(f"\n🎯 Ready to scrape {len(fighter_names)} fighters!")
    print("Use the fighter list files above to run the scraper.")

if __name__ == "__main__":
    main()