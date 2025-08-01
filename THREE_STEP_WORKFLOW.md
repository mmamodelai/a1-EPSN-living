# Three-Step ESPN MMA Data Pipeline

## 🎯 Overview

The ESPN MMA data system has been broken down into **3 separate GitHub Actions** for better modularity, error handling, and maintenance. Each step can be run independently or as part of the complete pipeline.

## 📋 The Three Steps

### **Step 1: Scrape ESPN** (`.github/workflows/1-scrape-espn.yml`)
**Purpose**: Scrape ESPN for fighter data and produce HTML files

**What it does**:
- Takes `fighters_name.csv` as input
- Scrapes ESPN for each fighter
- Produces HTML files in `data/FighterHTMLs/`
- Commits and pushes HTML files

**Schedule**: Monday 6:00 AM UTC
**Manual trigger**: ✅ Available with test mode option

**Output**: HTML files for each fighter

---

### **Step 2: Process Living Data** (`.github/workflows/2-process-living-data.yml`)
**Purpose**: Convert HTML files to living documents (striking, clinch, ground data)

**What it does**:
- Reads HTML files from Step 1
- Processes fight-by-fight data
- Creates living documents:
  - `striking_data_living.csv`
  - `clinch_data_living.csv`
  - `ground_data_living.csv`
- Creates backups
- Commits and pushes living documents

**Schedule**: Monday 6:30 AM UTC (30 minutes after Step 1)
**Manual trigger**: ✅ Available with test mode option

**Prerequisites**: Step 1 must be completed first
**Output**: Living documents with fight-by-fight data

---

### **Step 3: Process Fighter Profiles** (`.github/workflows/3-process-fighter-profiles.yml`)
**Purpose**: Create enhanced fighter profiles with detailed statistics

**What it does**:
- Reads HTML files and living documents
- Extracts detailed fighter statistics
- Enhances profiles with:
  - Rate calculations (per-minute, per-15-minute)
  - Defensive percentages
  - Win method breakdowns
  - Target breakdowns (head, body, legs)
- Creates enhanced `fighter_profiles.csv`
- Creates backups
- Commits and pushes fighter profiles

**Schedule**: Monday 7:00 AM UTC (1 hour after Step 1)
**Manual trigger**: ✅ Available with test mode option

**Prerequisites**: Steps 1 and 2 must be completed first
**Output**: Enhanced fighter profiles with detailed statistics

## 🚀 How to Use

### **Automatic Pipeline**
The workflows run automatically every Monday:
1. **6:00 AM**: Step 1 - Scrape ESPN
2. **6:30 AM**: Step 2 - Process Living Data
3. **7:00 AM**: Step 3 - Process Fighter Profiles

### **Manual Execution**
1. Go to **Actions** tab in GitHub
2. Select the workflow you want to run
3. Click **Run workflow**
4. Choose options (test mode, etc.)
5. Click **Run workflow**

### **Step-by-Step Manual Execution**
For complete control, run them in order:

1. **Run "1. Scrape ESPN"**
   - Wait for completion
   - Check that HTML files were created

2. **Run "2. Process Living Data"**
   - Wait for completion
   - Check that living documents were created

3. **Run "3. Process Fighter Profiles"**
   - Wait for completion
   - Check that fighter profiles were enhanced

## 🔧 Configuration

### **Test Mode**
Each workflow supports test mode:
- Uses `test_fighters_list.csv` (24 fighters)
- Faster execution for testing
- Recommended for first-time runs

### **Fighters File**
Step 1 allows custom fighters file:
- Default: `data/fighters_name.csv`
- Can specify custom path
- Supports full roster or test list

### **Scheduling**
To change the schedule, edit the `cron` expressions:

```yaml
# Current schedule (Mondays)
schedule:
  - cron: '0 6 * * 1'   # Step 1: 6:00 AM Monday
  - cron: '30 6 * * 1'  # Step 2: 6:30 AM Monday  
  - cron: '0 7 * * 1'   # Step 3: 7:00 AM Monday

# Alternative schedules
- cron: '0 6 * * *'     # Daily at 6 AM
- cron: '0 */6 * * *'   # Every 6 hours
- cron: '0 6,18 * * *'  # Twice daily at 6 AM and 6 PM
```

## 🛠️ Troubleshooting

### **Step 1 Fails**
- Check ESPN rate limiting
- Verify fighters file exists
- Check internet connectivity

### **Step 2 Fails**
- Ensure Step 1 completed successfully
- Check that HTML files exist
- Verify file permissions

### **Step 3 Fails**
- Ensure Steps 1 and 2 completed successfully
- Check that living documents exist
- Verify enhancement scripts are present

### **Common Issues**
- **Rate limiting**: ESPN may block requests
- **Missing files**: Previous steps may have failed
- **Permission errors**: Check GitHub Actions permissions

## 📊 Monitoring

### **Check Progress**
- Go to **Actions** tab
- View workflow runs
- Check logs for errors

### **Verify Output**
```bash
# Check HTML files
ls -la data/FighterHTMLs/ | wc -l

# Check living documents
wc -l data/*_living.csv

# Check fighter profiles
wc -l data/fighter_profiles.csv
```

### **Data Quality**
Each step includes verification:
- File counts
- Data validation
- Sample data display
- Backup creation

## 🔄 Integration

### **With Other Systems**
The living documents can be consumed by:
- Data analysis tools (Python, R, Excel)
- Database systems
- API endpoints
- Machine learning models

### **Webhook Notifications**
Add webhook notifications to any step:

```yaml
- name: Notify on completion
  if: always()
  run: |
    curl -X POST ${{ secrets.WEBHOOK_URL }} \
      -H "Content-Type: application/json" \
      -d '{"step": "1", "status": "${{ job.status }}"}'
```

## 📝 Benefits

### **Modularity**
- Each step can be run independently
- Easy to debug and fix issues
- Can skip steps if not needed

### **Reliability**
- Automatic backups at each step
- Prerequisite checking
- Detailed error reporting

### **Flexibility**
- Manual triggers available
- Test mode for quick testing
- Customizable scheduling

### **Maintainability**
- Clear separation of concerns
- Easy to modify individual steps
- Better error isolation

---

**Note**: The three-step workflow provides better control, reliability, and maintainability compared to a single monolithic workflow. 