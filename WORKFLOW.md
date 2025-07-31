# ESPN MMA Data Workflow: HTML → Living Documents

## 🎯 **System Overview**

This system scrapes ESPN MMA fighter data and maintains "living documents" that are automatically updated with the latest fight data while preserving historical information.

## 📁 **File Structure**

```
data/
├── FighterHTMLs/           # 2,927 HTML files (preserved in GitHub)
├── fighter_profiles.csv    # Consolidated fighter profiles (1,775 with real stats)
├── striking_data_living.csv   # 37,396 fight records (living document)
├── clinch_data_living.csv     # 35,422 fight records (living document)
├── ground_data_living.csv     # 35,422 fight records (living document)
└── backups/               # Timestamped backups
```

## 🔄 **Workflow Process**

### **Step 1: HTML Scraping & UPSERT**
- **Input**: List of fighter names (`fighters_name.csv`)
- **Process**: Scrapes ESPN fighter pages
- **Output**: HTML files saved to `data/FighterHTMLs/`
- **UPSERT Logic**: Never overwrites existing HTML files

### **Step 2: Living Documents Generation**
- **Input**: HTML files from Step 1
- **Process**: Extracts fight-by-fight statistics
- **Output**: Three living documents:
  - `striking_data_living.csv` - Strike statistics per fight
  - `clinch_data_living.csv` - Clinch statistics per fight  
  - `ground_data_living.csv` - Ground statistics per fight

### **Step 3: Fighter Profiles Consolidation**
- **Input**: HTML files from Step 1
- **Process**: Extracts career totals and personal info
- **Output**: `fighter_profiles.csv` - One consolidated profile per fighter

## 🚀 **How to Run**

### **Quick Test (24 fighters)**
```bash
python test_small_fighter_list.py
```

### **Full System Run**
```bash
python run_espn_processor.py
```

### **Update Fighter Profiles Only**
```bash
python extract_stats_from_html.py
```

## 📊 **Current Status**

- **Total Fighters**: 2,927
- **Fighters with Real Stats**: 1,775 (60.6%)
- **Fighters with Zeros**: 1,152 (39.4%) - No fight data available
- **HTML Files**: 2,927 (preserved)
- **Living Documents**: 37,396+ fight records

## 🔧 **Key Features**

### **✅ HTML UPSERT System**
- Preserves all HTML files in GitHub
- Never overwrites existing data
- Cross-repo integration with fighter lists

### **✅ Living Documents**
- Automatically captures latest fight data
- Preserves historical fight records
- Real-time updates (e.g., Robert Whittaker's July 26, 2025 fight)

### **✅ Fighter Profiles**
- Consolidated career statistics
- Personal information (height, weight, age, etc.)
- Real accuracy percentages and totals

## 📈 **Example Output**

### **Robert Whittaker Profile**
```
Name: RobertWhittaker
Division: Middleweight
Sig. Strikes Landed: 1,620
Sig. Strikes Attempted: 3,516
Striking Accuracy: 46.1%
Takedowns Landed: 16
Knockdowns: 12
```

### **Living Document Records**
- Latest fight: Robert Whittaker vs Reinier de Ridder (July 26, 2025)
- UFC 308: Whittaker def. de Ridder via Decision
- Fight statistics captured in all three living documents

## 🛠 **Maintenance**

### **Adding New Fighters**
1. Add names to `fighters_name.csv`
2. Run `test_small_fighter_list.py` or `run_espn_processor.py`
3. System automatically scrapes and updates

### **Updating Existing Data**
1. Run `extract_stats_from_html.py` to refresh fighter profiles
2. Living documents update automatically during scraping

### **Backup System**
- Automatic timestamped backups created
- Files preserved in `data/backups/`
- Never lose historical data

## 🎯 **GitHub Ready**

This system is designed for GitHub with:
- ✅ HTML files preserved (2,927 files)
- ✅ Living documents that update automatically
- ✅ Comprehensive documentation
- ✅ Backup and UPSERT protection
- ✅ Cross-repo integration capability 