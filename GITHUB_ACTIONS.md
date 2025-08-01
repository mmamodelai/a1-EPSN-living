# GitHub Actions for Living Files Updates

## 🤖 Automated Living Files Updates

This repository includes GitHub Actions that automatically update the ESPN MMA living files (striking, clinch, ground data) on a schedule.

## 📋 Available Workflows

### 1. Update Living Files (Simple)
**File**: `.github/workflows/update_living_files_simple.yml`

**What it does**:
- Runs ESPN scraper on test fighters
- Updates living documents (striking, clinch, ground data)
- Extracts detailed fighter statistics
- Creates backups of previous data
- Commits and pushes changes automatically

**Triggers**:
- **Scheduled**: Every Monday at 6 AM UTC (after weekend fights)
- **Manual**: Can be triggered manually from GitHub Actions tab
- **Push**: When changes are made to `src/` or `test_small_fighter_list.py`

### 2. Update Living Files (Full)
**File**: `.github/workflows/update_living_files.yml`

**What it does**:
- Everything from the simple version
- Plus generates detailed reports
- Creates GitHub releases with updated data
- Uploads artifacts for download

## 🚀 How to Use

### Manual Trigger
1. Go to **Actions** tab in GitHub
2. Select **Update Living Files (Simple)**
3. Click **Run workflow**
4. Choose branch (usually `main`)
5. Click **Run workflow**

### Scheduled Updates
The workflow runs automatically every Monday at 6 AM UTC to capture weekend fight data.

### Monitor Progress
- Check the **Actions** tab to see workflow status
- View logs for any errors or issues
- See commit history for automatic updates

## 📊 What Gets Updated

### Living Documents
- `data/striking_data_living.csv` - Fight-by-fight striking statistics
- `data/clinch_data_living.csv` - Fight-by-fight clinch statistics  
- `data/ground_data_living.csv` - Fight-by-fight ground statistics

### Fighter Profiles
- `data/fighter_profiles.csv` - Enhanced with detailed career statistics

### Backups
- `data/backups/backup_YYYYMMDD_HHMMSS/` - Timestamped backups of all data

## 🔧 Configuration

### Schedule
To change the update frequency, edit the `cron` expression in the workflow file:

```yaml
schedule:
  - cron: '0 6 * * 1'  # Every Monday at 6 AM UTC
```

Common cron patterns:
- `'0 6 * * 1'` - Every Monday at 6 AM
- `'0 6 * * *'` - Every day at 6 AM
- `'0 */6 * * *'` - Every 6 hours
- `'0 6,18 * * *'` - Twice daily at 6 AM and 6 PM

### Test Fighters
The workflow uses `test_small_fighter_list.py` which contains 24 fighters. To change the fighters:

1. Edit `test_fighters_list.csv`
2. Or modify `test_small_fighter_list.py` to use different fighters

## 🛠️ Troubleshooting

### Common Issues

**Workflow fails on scraping**:
- ESPN may have rate limiting
- Check logs for specific error messages
- May need to adjust timing or add delays

**No changes committed**:
- No new fight data available
- All fighters already up to date
- Check logs for "No changes to commit" message

**Permission errors**:
- Ensure `GITHUB_TOKEN` has write permissions
- Check repository settings for Actions permissions

### Debug Mode
To run with more verbose output, add this to the workflow:

```yaml
- name: Run ESPN scraper and update living files
  run: |
    echo "🎯 Starting ESPN scraper for living files update..."
    python test_small_fighter_list.py --verbose
```

## 📈 Monitoring

### Check Update Status
```bash
# View recent commits
git log --oneline -10

# Check living files timestamps
ls -la data/*.csv

# View backup directory
ls -la data/backups/
```

### Data Quality Checks
The workflow automatically:
- Creates backups before updates
- Validates data integrity
- Reports any processing errors

## 🔄 Integration

### With Other Systems
The living files can be consumed by:
- Data analysis tools (Python, R, Excel)
- Database systems
- API endpoints
- Machine learning models

### Webhook Notifications
Add webhook notifications to the workflow:

```yaml
- name: Notify on completion
  if: always()
  run: |
    # Add webhook call here
    curl -X POST ${{ secrets.WEBHOOK_URL }} \
      -H "Content-Type: application/json" \
      -d '{"status": "${{ job.status }}", "workflow": "update-living-files"}'
```

## 📝 Customization

### Add More Fighters
1. Update `test_fighters_list.csv` with more fighters
2. Or create a new script for different fighter lists
3. Modify the workflow to use your custom script

### Different Update Frequency
Edit the cron schedule in the workflow file to match your needs.

### Custom Reports
Add custom reporting by modifying the workflow to generate specific reports or analytics.

---

**Note**: The GitHub Actions require appropriate permissions to commit and push changes. Ensure the repository has the necessary settings enabled. 