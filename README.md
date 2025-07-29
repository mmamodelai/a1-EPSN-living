# Step 2: ESPN Data Processor (Simplified)

## Overview

This system processes ESPN MMA data with **simplified UPSERT logic** - it works **IN and OUT of the data folder** while preserving all existing data for main outputs.

## Key Features

- **Data Preservation**: Never overwrites existing main output data
- **UPSERT Logic**: Adds new records, preserves existing ones for main outputs
- **Folder-Level Overwrite**: Cleans temp/HTML folders completely
- **Comprehensive Logging**: Tracks all operations
- **Simple & Focused**: Only processes 4 main outputs

## Data Structure

The system works with these **5 main files** and **1 HTML directory** in the `data/` folder:

- `clinch_data_living.csv` - Clinch/grappling statistics (UPSERT)
- `ground_data_living.csv` - Ground fighting statistics (UPSERT)
- `striking_data_living.csv` - Striking statistics (UPSERT)
- `fighter_profiles.csv` - Fighter profiles (UPSERT)
- `fighters_name.csv` - **PULLED FROM A0-roosterwatch** (Cross-repo integration)
- `FighterHTMLs/` - **FIGHTER HTML FILES** (UPSERT - preserved in GitHub)

## How It Works

### 1. **PULL FIGHTERS LIST** from A0-roosterwatch
- Automatically pulls latest `fighters_name.csv` from A0-roosterwatch
- Ensures ESPN scraping uses current active fighter roster
- Runs before ESPN data processing

### 2. **PROCESS FIGHTER HTMLS** (UPSERT)
- Scrapes HTML pages for fighters from ESPN
- **Preserves existing HTML files** in GitHub
- **Adds new HTML files** for new fighters
- **Updates HTML files** only if content changed
- **2,897+ HTML files** currently stored in GitHub

### 3. **READ FROM** Data Folder
- Loads existing CSV files from `data/` folder
- Preserves all current main output data

### 4. **PROCESS** with UPSERT Logic
- Compares new data with existing data
- Adds only new records (no duplicates)
- Preserves manually added data

### 5. **WRITE BACK TO** Data Folder
- Saves enhanced data back to same files
- Maintains file structure and naming

### 6. **CLEAN TEMP FOLDERS** (OVERWRITE)
- Completely removes and recreates `temp/` and `html_cache/` folders
- Ensures clean slate for each run

## Usage

### Test the System
```bash
python test_data_processor.py
```

### Run Full Processing
```bash
python run_espn_processor.py
```

### Process Specific Data Type
```python
from src.espn_data_processor import ESPNDataProcessor

processor = ESPNDataProcessor()
processor.process_fight_data('clinch')  # Process clinch data only
```

## UPSERT Logic

The system uses composite keys to identify unique records:

- **Fight Data**: `Player + Date + Opponent`
- **Fighter Profiles**: `Fighter Name`

This ensures:
- ✅ **No duplicate fights** are added
- ✅ **Manually added fights** are preserved
- ✅ **New fights** are added automatically
- ✅ **Data integrity** is maintained

## Folder Policies

### UPSERT Policy (Main Outputs)
- `data/clinch_data_living.csv`
- `data/ground_data_living.csv`
- `data/striking_data_living.csv`
- `data/fighter_profiles.csv`

### PULL Policy (Cross-Repo Integration)
- `data/fighters_name.csv` - **Pulled from A0-roosterwatch** (overwrites local copy)

### UPSERT Policy (HTML Files)
- `data/FighterHTMLs/` - **Preserved in GitHub** (2,897+ files)
- New HTML files added for new fighters
- Existing HTML files updated only if content changed
- No HTML files deleted

### OVERWRITE Policy (Temp Folders)
- `temp/` - Completely cleaned and recreated
- `html_cache/` - Completely cleaned and recreated

## Safety Features

1. **Data Preservation**: Main outputs are never overwritten
2. **Error Handling**: Graceful failure with logging
3. **Data Validation**: Checks file integrity
4. **Logging**: Complete audit trail
5. **Clean Temp**: Prevents accumulation of old files

## File Structure

```
step 2 ufc and espn scrapers/
├── data/                          # Main output files (UPSERT)
│   ├── clinch_data_living.csv     # Preserved
│   ├── ground_data_living.csv     # Preserved
│   ├── striking_data_living.csv   # Preserved
│   └── fighter_profiles.csv       # Preserved
├── temp/                          # Temp files (OVERWRITE)
├── html_cache/                    # HTML cache (OVERWRITE)
├── src/
│   └── espn_data_processor.py     # Main processor
├── test_data_processor.py         # Test script
├── run_espn_processor.py          # Runner script
├── requirements.txt               # Dependencies
└── README.md                      # This file
```

## Data Flow

```
Main Output Files → Load → Process → UPSERT → Save → Enhanced Files
         ↑                                                      ↑
    Preserved as-is                                        Updated with new data

Temp Folders → Clean → Recreate → Fresh Start
```

## Cross-Repo Integration

### A0-roosterwatch → A1-ESPN-Profiles
- **Source**: `A0-roosterwatch/A0 optional activefighters/data/fighters_name.csv`
- **Destination**: `A1-ESPN-Profiles/data/fighters_name.csv`
- **Schedule**: Sunday 1 PM UTC (before ESPN scraping)
- **Method**: GitHub Actions cross-repo checkout

### Benefits
- ✅ **Single source of truth** for active fighters
- ✅ **Automatic synchronization** between repos
- ✅ **No manual maintenance** of fighter lists
- ✅ **Always current** fighter roster for ESPN scraping

### HTML Storage Benefits
- ✅ **Faster scraping** - No need to re-download existing HTML
- ✅ **Reduced API calls** - Less load on ESPN servers
- ✅ **Offline processing** - Can process data without internet
- ✅ **Historical data** - Keep old HTML for analysis
- ✅ **GitHub storage** - 2,897+ HTML files safely stored
- ✅ **UPSERT protection** - Never lose existing HTML files

## Next Steps

1. **Add ESPN scraping logic** to the processor
2. **Set up GitHub Actions** for automation
3. **Integrate with GOLDEN DATA** folder
4. **Add more data sources** as needed

## Safety First

- All existing main output data is preserved
- Temp folders are cleaned for fresh starts
- No main data is ever deleted unless explicitly requested
- Manual additions are protected

This system follows the **UPSERT-FIRST** philosophy with **simplified complexity**. 