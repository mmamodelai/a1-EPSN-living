#!/usr/bin/env python3
"""
Script to find all HTML files under 5KB in the FighterHTMLs folder
"""

import os
from pathlib import Path
import pandas as pd

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
                from datetime import datetime
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

def main():
    print("=== HTML File Size Checker ===")
    print("Checking HTML file sizes...\n")
    
    # Check different size thresholds
    thresholds = [5, 10, 50, 100]
    
    for threshold in thresholds:
        small_files = find_small_html_files(size_limit_kb=threshold)
        
        if not small_files:
            print(f"✅ No files found under {threshold}KB!")
        else:
            print(f"\n❌ Found {len(small_files)} files under {threshold}KB:")
            
            # Create DataFrame for better display
            df = pd.DataFrame(small_files)
            
            # Sort by size (smallest first)
            df = df.sort_values('size_bytes')
            
            # Display first 5 results
            for idx, row in df.head(5).iterrows():
                status = "✅" if row['is_real_espn'] else "❌"
                print(f"   {status} {row['fighter_name']:25} | {row['size_kb']:6.2f}KB | {row['modified']}")
            
            if len(small_files) > 5:
                print(f"   ... and {len(small_files) - 5} more")
        
        print()
    
    # Get overall statistics
    html_path = Path("data/FighterHTMLs")
    if html_path.exists():
        html_files = list(html_path.glob("*.html"))
        sizes = []
        for html_file in html_files:
            try:
                file_size = html_file.stat().st_size
                sizes.append(file_size / 1024)  # Convert to KB
            except:
                pass
        
        if sizes:
            print("📊 Overall Statistics:")
            print(f"   Total files: {len(sizes)}")
            print(f"   Average size: {sum(sizes)/len(sizes):.1f}KB")
            print(f"   Min size: {min(sizes):.1f}KB")
            print(f"   Max size: {max(sizes):.1f}KB")
            
            # Count files in different size ranges
            ranges = [(0, 10), (10, 50), (50, 100), (100, 500), (500, 1000), (1000, float('inf'))]
            for min_size, max_size in ranges:
                count = sum(1 for size in sizes if min_size <= size < max_size)
                if count > 0:
                    if max_size == float('inf'):
                        print(f"   {min_size}KB+: {count} files")
                    else:
                        print(f"   {min_size}-{max_size}KB: {count} files")

if __name__ == "__main__":
    main()