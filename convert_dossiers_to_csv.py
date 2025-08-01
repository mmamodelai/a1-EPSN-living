#!/usr/bin/env python3
"""
Dossier to CSV Converter
========================

This script converts Excel dossiers to CSV format for easier data analysis
and sharing. It handles all sheets in the dossier and creates separate CSV files.

Usage:
    python convert_dossiers_to_csv.py [dossier_file.xlsx]
"""

import pandas as pd
import sys
from pathlib import Path
import os

def convert_dossier_to_csv(excel_file, output_dir='csv_dossiers'):
    """Convert an Excel dossier to multiple CSV files."""
    
    print(f"🎯 Converting dossier: {excel_file}")
    
    # Create output directory
    Path(output_dir).mkdir(exist_ok=True)
    
    # Read Excel file
    try:
        xl = pd.ExcelFile(excel_file)
        print(f"✓ Found {len(xl.sheet_names)} sheets: {xl.sheet_names}")
    except Exception as e:
        print(f"❌ Error reading Excel file: {e}")
        return False
    
    # Convert each sheet to CSV
    csv_files = []
    for sheet_name in xl.sheet_names:
        try:
            # Read sheet
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            
            # Clean data - replace NaN with empty strings for text columns
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].fillna('')
            
            # Create CSV filename
            safe_sheet_name = sheet_name.replace(' ', '_').replace('-', '_')
            csv_filename = f"{output_dir}/{Path(excel_file).stem}_{safe_sheet_name}.csv"
            
            # Save to CSV
            df.to_csv(csv_filename, index=False)
            csv_files.append(csv_filename)
            
            print(f"  ✅ {sheet_name}: {df.shape[0]} rows, {df.shape[1]} columns → {csv_filename}")
            
        except Exception as e:
            print(f"  ❌ Error converting sheet {sheet_name}: {e}")
    
    print(f"\n✅ Successfully converted {len(csv_files)} sheets to CSV")
    return csv_files

def convert_all_dossiers(dossier_dir='dossiers', output_dir='csv_dossiers'):
    """Convert all Excel dossiers in a directory to CSV."""
    
    print(f"🎯 Converting all dossiers from: {dossier_dir}")
    
    # Find all Excel files
    excel_files = list(Path(dossier_dir).glob('*.xlsx'))
    
    if not excel_files:
        print(f"❌ No Excel files found in {dossier_dir}")
        return []
    
    print(f"✓ Found {len(excel_files)} Excel files")
    
    all_csv_files = []
    for excel_file in excel_files:
        print(f"\n--- Converting {excel_file.name} ---")
        csv_files = convert_dossier_to_csv(excel_file, output_dir)
        if csv_files:
            all_csv_files.extend(csv_files)
    
    print(f"\n🎉 Total CSV files created: {len(all_csv_files)}")
    return all_csv_files

def show_csv_summary(csv_dir='csv_dossiers'):
    """Show summary of created CSV files."""
    
    if not Path(csv_dir).exists():
        print(f"❌ CSV directory {csv_dir} not found")
        return
    
    csv_files = list(Path(csv_dir).glob('*.csv'))
    
    if not csv_files:
        print(f"❌ No CSV files found in {csv_dir}")
        return
    
    print(f"\n📊 CSV Files Summary ({csv_dir}):")
    print("=" * 50)
    
    for csv_file in sorted(csv_files):
        try:
            df = pd.read_csv(csv_file)
            print(f"📄 {csv_file.name}: {df.shape[0]} rows, {df.shape[1]} columns")
        except Exception as e:
            print(f"❌ {csv_file.name}: Error reading - {e}")

def main():
    """Main function."""
    
    if len(sys.argv) > 1:
        # Convert specific file
        excel_file = sys.argv[1]
        if not Path(excel_file).exists():
            print(f"❌ File not found: {excel_file}")
            return
        
        convert_dossier_to_csv(excel_file)
    else:
        # Convert all dossiers
        convert_all_dossiers()
    
    # Show summary
    show_csv_summary()

if __name__ == "__main__":
    main() 