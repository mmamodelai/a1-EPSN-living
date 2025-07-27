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

The system works with these **4 main output files** in the `data/` folder:

- `clinch_data_living.csv` - Clinch/grappling statistics (UPSERT)
- `ground_data_living.csv` - Ground fighting statistics (UPSERT)
- `striking_data_living.csv` - Striking statistics (UPSERT)
- `fighter_profiles.csv` - Fighter profiles (UPSERT)

## How It Works

### 1. **READ FROM** Data Folder
- Loads existing CSV files from `data/` folder
- Preserves all current main output data

### 2. **PROCESS** with UPSERT Logic
- Compares new data with existing data
- Adds only new records (no duplicates)
- Preserves manually added data

### 3. **WRITE BACK TO** Data Folder
- Saves enhanced data back to same files
- Maintains file structure and naming

### 4. **CLEAN TEMP FOLDERS** (OVERWRITE)
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