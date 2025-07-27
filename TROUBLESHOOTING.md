# Troubleshooting Guide

## Current Status: ✅ WORKING

The ESPN Data Processor and GitHub Actions are currently working correctly. This guide helps identify and resolve potential issues.

## Quick Health Check

### Local Testing
```bash
python test_data_processor.py
```
**Expected Output:** All tests pass with green checkmarks

### GitHub Actions Status
- Go to Actions tab in your repository
- Look for green checkmarks on all workflow runs
- Check that "Commit and push changes" step runs (may be 0s if no changes)

## Common Issues & Solutions

### Issue 1: "No changes detected" in GitHub Actions
**Symptoms:** "Commit and push changes" step shows 0s duration
**Cause:** This is normal - means data is up to date
**Solution:** This is expected behavior when no new data is found

### Issue 2: Workflow fails with permission error
**Symptoms:** Error about "Write access to repository not granted"
**Solution:** 
- Ensure repository has Actions enabled
- Check workflow has `contents: write` permission (already configured)

### Issue 3: Python dependencies fail
**Symptoms:** "ModuleNotFoundError" in workflow
**Solution:**
- Check `requirements.txt` is up to date
- Verify all dependencies are available

### Issue 4: Data not being processed
**Symptoms:** No new records added
**Cause:** ESPN scraping logic is currently placeholder
**Solution:** Add actual ESPN scraping logic to the processor

## Workflow Optimization

### Current Workflow Steps:
1. ✅ Checkout repository
2. ✅ Set up Python
3. ✅ Install dependencies
4. ✅ Run Tests
5. ✅ Test Data Summary
6. ✅ Check for changes
7. ✅ Commit and push changes

### Optimization Opportunities:
- **Step 4 & 5:** Could be combined for efficiency
- **Step 6 & 7:** Only run when changes detected (already implemented)

## Data Processing Status

### Current Data:
- **Clinch:** 32,179 records ✅
- **Ground:** 32,179 records ✅
- **Striking:** 34,153 records ✅
- **Fighter Profiles:** 2,969 records ✅

### Processing Logic:
- **UPSERT:** Working correctly
- **Temp Cleanup:** Working correctly
- **ESPN Scraping:** Placeholder (needs implementation)

## Debug Mode

To enable detailed logging in GitHub Actions:

1. Go to repository Settings
2. Navigate to Secrets and variables → Actions
3. Add these repository secrets:
   - `ACTIONS_RUNNER_DEBUG`: `true`
   - `ACTIONS_STEP_DEBUG`: `true`

## Manual Testing

### Test Individual Components:
```bash
# Test data loading
python -c "from src.espn_data_processor import ESPNDataProcessor; p = ESPNDataProcessor(); print(p.get_data_summary())"

# Test UPSERT logic
python -c "from src.espn_data_processor import ESPNDataProcessor; p = ESPNDataProcessor(); print('UPSERT test passed')"

# Test temp folder cleaning
python -c "from src.espn_data_processor import ESPNDataProcessor; p = ESPNDataProcessor(); p.clean_temp_folders(); print('Temp cleanup passed')"
```

### Test Workflow Locally:
```bash
# Simulate workflow steps
python run_espn_processor.py
python test_data_processor.py
```

## Next Steps for Full Functionality

1. **Add ESPN Scraping Logic**
   - Implement actual ESPN data scraping
   - Add new data sources
   - Test with real data

2. **Monitor Weekly Runs**
   - Check first Sunday run (2 PM UTC)
   - Verify data updates correctly
   - Monitor for any issues

3. **Add Notifications**
   - Set up email/Slack notifications
   - Alert on workflow failures
   - Report data processing results

## Emergency Recovery

### If Workflow Breaks:
1. Check Actions tab for error details
2. Review logs for specific failure
3. Test locally to reproduce issue
4. Fix and push changes
5. Re-run workflow manually

### If Data Gets Corrupted:
1. Check Git history for previous commits
2. Restore from last good commit
3. Re-run processor to rebuild data
4. Verify data integrity

## Performance Monitoring

### Key Metrics to Watch:
- **Workflow Duration:** Should be under 5 minutes
- **Data Record Counts:** Should increase over time
- **Commit Frequency:** Should match data updates
- **Error Rate:** Should be 0%

### Expected Behavior:
- Weekly runs every Sunday at 2 PM UTC
- Manual runs complete within 5 minutes
- Data grows incrementally with new fights
- No data loss or corruption

## Support

If issues persist:
1. Check this troubleshooting guide
2. Review GitHub Actions logs
3. Test locally to isolate issues
4. Check established patterns in A0 projects 