#!/usr/bin/env python3

import pandas as pd
import os
from pathlib import Path
from bs4 import BeautifulSoup
import re
from typing import Dict, List, Optional

def extract_fighter_stats_from_html(html_file_path: str) -> Dict:
    """Extract detailed fighter statistics from ESPN HTML file."""
    try:
        with open(html_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # Initialize stats dictionary
        stats = {}
        
        # Extract basic info from the page
        name_elem = soup.find('h1', class_='PlayerHeader__Name')
        if name_elem:
            stats['Name'] = name_elem.get_text(strip=True)
        
        # Find all tables with fight statistics
        tables = soup.find_all('table')
        
        total_sig_strikes_landed = 0
        total_sig_strikes_attempted = 0
        total_takedowns_landed = 0
        total_takedowns_attempted = 0
        total_knockdowns = 0
        total_submissions = 0
        fight_count = 0
        
        for table in tables:
            rows = table.find_all('tr')
            if len(rows) < 2:  # Skip tables with no data
                continue
                
            # Check if this is a stats table by looking at headers
            headers = [th.get_text(strip=True) for th in rows[0].find_all('th')]
            
            # Process striking data
            if 'SSL' in headers or 'Significant Strikes Landed' in str(headers):
                for row in rows[1:]:  # Skip header row
                    cells = row.find_all('td')
                    if len(cells) >= 8:
                        try:
                            ssl = cells[7].get_text(strip=True)  # SSL column
                            ssa = cells[8].get_text(strip=True)  # SSA column
                            
                            if ssl != '-' and ssl != '':
                                total_sig_strikes_landed += int(ssl)
                            if ssa != '-' and ssa != '':
                                total_sig_strikes_attempted += int(ssa)
                            fight_count += 1
                        except (ValueError, IndexError):
                            continue
            
            # Process clinch data for takedowns
            if 'TDL' in headers or 'Takedowns Landed' in str(headers):
                for row in rows[1:]:
                    cells = row.find_all('td')
                    if len(cells) >= 13:
                        try:
                            tdl = cells[12].get_text(strip=True)  # TDL column
                            tda = cells[13].get_text(strip=True)  # TDA column
                            
                            if tdl != '-' and tdl != '':
                                total_takedowns_landed += int(tdl)
                            if tda != '-' and tda != '':
                                total_takedowns_attempted += int(tda)
                        except (ValueError, IndexError):
                            continue
            
            # Process striking data for knockdowns
            if 'KD' in headers or 'Knockdowns' in str(headers):
                for row in rows[1:]:
                    cells = row.find_all('td')
                    if len(cells) >= 13:
                        try:
                            kd = cells[12].get_text(strip=True)  # KD column
                            if kd != '-' and kd != '':
                                total_knockdowns += int(kd)
                        except (ValueError, IndexError):
                            continue
        
        # Calculate averages and percentages
        if fight_count > 0:
            stats['Sig. Strike'] = total_sig_strikes_landed
            stats['Sig. Str. La'] = total_sig_strikes_landed
            stats['Sig. Str. At'] = total_sig_strikes_attempted
            stats['Takedownr'] = total_takedowns_landed
            stats['Knockdow'] = total_knockdowns
            stats['Submissio'] = total_submissions
            
            # Calculate accuracy percentages
            if total_sig_strikes_attempted > 0:
                stats['Striking ac'] = round((total_sig_strikes_landed / total_sig_strikes_attempted) * 100, 1)
            else:
                stats['Striking ac'] = 0
                
            if total_takedowns_attempted > 0:
                stats['Takedown Accuracy'] = round((total_takedowns_landed / total_takedowns_attempted) * 100, 1)
            else:
                stats['Takedown Accuracy'] = 0
        
        return stats
        
    except Exception as e:
        print(f"Error processing {html_file_path}: {e}")
        return {}

def process_html_files_to_profiles():
    """Process all HTML files and update fighter profiles with detailed stats."""
    
    # Load existing fighter profiles
    profiles_file = Path('data/fighter_profiles.csv')
    if profiles_file.exists():
        profiles_df = pd.read_csv(profiles_file)
    else:
        print("No fighter profiles file found!")
        return
    
    print(f"Loaded {len(profiles_df)} fighter profiles")
    
    # Process HTML files
    html_dir = Path('data/FighterHTMLs')
    html_files = list(html_dir.glob('*.html'))
    
    print(f"Found {len(html_files)} HTML files to process")
    
    # Track updates
    updated_count = 0
    
    for html_file in html_files:
        html_name = html_file.stem  # e.g., "Aalon_Cruz"
        
        # Convert HTML name to CSV format (remove underscores and spaces)
        csv_name = html_name.replace('_', '').replace('-', '').replace('.', '')
        
        # Find matching fighter in profiles
        mask = profiles_df['Name'].str.lower() == csv_name.lower()
        if not mask.any():
            # Try with underscores as spaces
            space_name = html_name.replace('_', ' ')
            mask = profiles_df['Name'].str.lower() == space_name.lower()
            if not mask.any():
                # Try partial match as last resort
                first_name = html_name.split('_')[0]
                mask = profiles_df['Name'].str.contains(first_name, case=False, na=False)
                if not mask.any():
                    continue
        
        # Extract stats from HTML
        stats = extract_fighter_stats_from_html(str(html_file))
        if not stats:
            continue
        
        # Update the fighter profile
        for col, value in stats.items():
            if col in profiles_df.columns:
                profiles_df.loc[mask, col] = value
        
        updated_count += 1
        if updated_count % 100 == 0:
            print(f"Processed {updated_count} fighters...")
    
    # Save updated profiles
    profiles_df.to_csv('data/fighter_profiles.csv', index=False)
    print(f"Updated {updated_count} fighter profiles with detailed statistics")
    
    # Show sample of updated data
    print("\nSample updated fighter profiles:")
    sample_fighters = ['Robert Whittaker', 'Petr Yan', 'Nikita Krylov']
    for fighter in sample_fighters:
        mask = profiles_df['Name'].str.contains(fighter, case=False, na=False)
        if mask.any():
            fighter_data = profiles_df[mask].iloc[0]
            print(f"\n{fighter_data['Name']}:")
            print(f"  Record: {fighter_data['Division Rk']}")
            print(f"  Sig Strikes Landed: {fighter_data['Sig. Strike']}")
            print(f"  Sig Strikes Attempted: {fighter_data['Sig. Str. At']}")
            print(f"  Striking Accuracy: {fighter_data['Striking ac']}%")
            print(f"  Takedowns Landed: {fighter_data['Takedownr']}")
            print(f"  Knockdowns: {fighter_data['Knockdow']}")

if __name__ == "__main__":
    process_html_files_to_profiles() 