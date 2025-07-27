# GitHub Actions Workflows

## Overview

This repository includes automated GitHub Actions workflows for ESPN data processing.

## Available Workflows

### 1. Weekly ESPN Data Update
**File:** `.github/workflows/weekly-update.yml`

**Schedule:** Every Sunday at 2 PM UTC
**Manual Trigger:** Available via GitHub Actions tab

**What it does:**
- Runs incremental ESPN data updates
- Preserves existing data with UPSERT logic
- Commits changes only if new data is found
- Runs automatically every week

**Usage:**
- **Automatic:** Runs every Sunday at 2 PM UTC
- **Manual:** Go to Actions tab → "Weekly ESPN Data Update" → "Run workflow"

### 2. Manual Full ESPN Scrape
**File:** `.github/workflows/manual-full-scrape.yml`

**Schedule:** Manual only
**Trigger:** Manual via GitHub Actions tab

**What it does:**
- Runs complete ESPN data scrape
- Uses `--full-scrape` flag for comprehensive processing
- Commits all changes found
- For testing and complete data updates

**Usage:**
- Go to Actions tab → "Manual Full ESPN Scrape" → "Run workflow"

### 3. Test ESPN Data Processor
**File:** `.github/workflows/test-workflow.yml`

**Schedule:** On push to main branch (for src files)
**Manual Trigger:** Available via GitHub Actions tab

**What it does:**
- Runs test suite
- Validates data processor functionality
- Tests data summary generation
- For development and validation

**Usage:**
- **Automatic:** Runs when you push changes to src files
- **Manual:** Go to Actions tab → "Test ESPN Data Processor" → "Run workflow"

## Workflow Features

### Smart Commits
- Only commits when changes are detected
- Prevents empty commits
- Clear commit messages with timestamps

### Error Handling
- Graceful failure handling
- Continues processing even if some steps fail
- Comprehensive logging

### Permissions
- `contents: write` - Required for commits
- Proper Git configuration for GitHub Actions

## Schedule Management

### Weekly Schedule
```yaml
cron: '0 14 * * 0'  # Sunday 2 PM UTC
```

### Timezone Notes
- GitHub Actions runs in UTC
- Sunday 2 PM UTC = Sunday 10 AM EST / 7 AM PST
- Adjust cron schedule as needed for your timezone

## Manual Triggers

To run workflows manually:

1. Go to your repository on GitHub
2. Click the "Actions" tab
3. Select the workflow you want to run
4. Click "Run workflow"
5. Choose branch (usually main)
6. Click "Run workflow"

## Monitoring

### Check Workflow Status
- Go to Actions tab to see all workflow runs
- Green checkmark = Success
- Red X = Failed
- Yellow dot = Running

### View Logs
- Click on any workflow run
- Click on individual steps to see detailed logs
- Logs show data processing progress and results

## Troubleshooting

### Common Issues

**Workflow fails with permission error:**
- Ensure repository has Actions enabled
- Check that workflow has `contents: write` permission

**No changes committed:**
- This is normal - means data is up to date
- Check logs to see processing results

**Python dependencies fail:**
- Check `requirements.txt` is up to date
- Verify all dependencies are available

### Debug Mode
To enable debug logging, add these secrets to your repository:
- `ACTIONS_RUNNER_DEBUG`: `true`
- `ACTIONS_STEP_DEBUG`: `true`

## Next Steps

1. **Test workflows** manually to ensure they work
2. **Monitor first weekly run** to verify automation
3. **Add ESPN scraping logic** to the processor
4. **Set up notifications** for workflow results

## Reference

These workflows follow the patterns established in:
- A0-roosterwatch: [https://github.com/mmamodelai/A0-roosterwatch](https://github.com/mmamodelai/A0-roosterwatch)
- A0-greco-updater: [https://github.com/mmamodelai/A0-greco-updater](https://github.com/mmamodelai/A0-greco-updater) 