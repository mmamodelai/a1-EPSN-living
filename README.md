# Step 2: ESPN Data Processor

## Overview

This system processes ESPN MMA data with **UPSERT logic** - it works **IN and OUT of the data folder** while preserving all existing data.

## Key Features

- **Data Preservation**: Never overwrites existing data
- **UPSERT Logic**: Adds new records, preserves existing ones
- **Automatic Backups**: Creates timestamped backups before processing
- **Comprehensive Logging**: Tracks all operations
- **Safe Processing**: Works with your existing data files

## Data Structure

The system works with these files in the `data/` folder:

- `clinch_data_living.csv` - Clinch/grappling statistics
- `ground_data_living.csv` - Ground fighting statistics  
- `striking_data_living.csv` - Striking statistics
- `fighters_name.csv` - Fighter names
- `fighter_profiles.csv` - Fighter profiles

## How It Works

### 1. **READ FROM** Data Folder
- Loads existing CSV files from `data/` folder
- Preserves all current data

### 2. **PROCESS** with UPSERT Logic
- Compares new data with existing data
- Adds only new records (no duplicates)
- Preserves manually added data

### 3. **WRITE BACK TO** Data Folder
- Saves enhanced data back to same files
- Maintains file structure and naming

### 4. **BACKUP** Before Changes
- Creates timestamped backups in `data/backups/`
- Ensures data safety

## Usage

### Test the System
```bash
python test_data_processor.py
```

### Run Full Processing
```bash
python src/espn_data_processor.py
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
- **Fighter Data**: `Fighter Name`

This ensures:
- ✅ **No duplicate fights** are added
- ✅ **Manually added fights** are preserved
- ✅ **New fights** are added automatically
- ✅ **Data integrity** is maintained

## Safety Features

1. **Automatic Backups**: Every run creates a backup
2. **Error Handling**: Graceful failure with logging
3. **Data Validation**: Checks file integrity
4. **Logging**: Complete audit trail

## File Structure

```
step 2 ufc and espn scrapers/
├── data/                          # Your existing data files
│   ├── clinch_data_living.csv
│   ├── ground_data_living.csv
│   ├── striking_data_living.csv
│   ├── fighters_name.csv
│   ├── fighter_profiles.csv
│   └── backups/                   # Automatic backups
├── src/
│   └── espn_data_processor.py     # Main processor
├── test_data_processor.py         # Test script
├── requirements.txt               # Dependencies
└── README.md                      # This file
```

## Data Flow

```
Existing Data Files → Load → Process → UPSERT → Save → Enhanced Data Files
         ↑                                                      ↑
    Preserved as-is                                        Updated with new data
```

## Next Steps

1. **Test the system** with your existing data
2. **Add ESPN scraping logic** to the processor
3. **Set up GitHub Actions** for automation
4. **Integrate with GOLDEN DATA** folder

## Safety First

- All existing data is preserved
- Backups are created automatically
- No data is ever deleted unless explicitly requested
- Manual additions are protected

This system follows the **UPSERT-FIRST** philosophy from the Olive documentation. 