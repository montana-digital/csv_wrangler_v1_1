# E2E UI Testing Guide

## Quick Start

### Prerequisites

1. Install Playwright browsers:
   ```powershell
   playwright install chromium
   ```

2. Ensure Streamlit app can run:
   ```powershell
   python run_app.bat
   ```

### Running Tests

**Option 1: Auto-start Streamlit (Recommended)**
```powershell
pytest src/tests/e2e/test_ui_comprehensive.py -v
```

**Option 2: Manual Streamlit + Tests**
```powershell
# Terminal 1: Start app
python run_app.bat

# Terminal 2: Run tests
pytest src/tests/e2e/test_ui_comprehensive.py -v
```

**Option 3: Use batch file**
```powershell
test_data\scripts\run_ui_tests.bat
```

## Test Coverage

### ✅ Comprehensive UI Testing

1. **First Launch & Profile Creation**
   - App loads correctly
   - Profile form appears
   - Can create profile

2. **Dataset Management**
   - Navigate to dataset pages
   - Initialize datasets
   - Upload files (CSV/Pickle)

3. **Data Operations**
   - View data
   - Search functionality
   - Filter unique records

4. **Export Features**
   - Export to CSV
   - Export to Pickle
   - Date range filtering

5. **Settings & Configuration**
   - View dataset details
   - Database information
   - Dataset deletion

6. **Navigation**
   - All pages accessible
   - Sidebar navigation works

7. **Error Handling**
   - Invalid inputs show errors
   - Validation messages appear

## Debugging

### View Browser During Tests
```powershell
pytest src/tests/e2e/test_ui_comprehensive.py --headed -v
```

### Slow Down Execution
```powershell
pytest src/tests/e2e/test_ui_comprehensive.py --slow-mo=1000 -v
```

### Run Single Test
```powershell
pytest src/tests/e2e/test_ui_comprehensive.py::TestFirstLaunchAndProfileCreation::test_app_loads -v
```

## Troubleshooting

### Port 8501 Already in Use
- Tests will detect if Streamlit is already running
- Manually start app: `python run_app.bat`
- Or kill existing process and let tests start it

### Browser Not Found
```powershell
playwright install chromium
```

### Tests Timeout
- Increase wait times in tests
- Check Streamlit app is responding
- Verify no firewall blocking

### Elements Not Found
- Streamlit renders dynamically - waits are built in
- Some tests check for element existence before asserting
- Check browser console for errors

## Test Structure

Tests are organized by user journey:
- Setup fixtures handle Streamlit app and browser
- Each test gets a fresh page
- Tests verify both element presence and functionality
- Error handling tests verify graceful failures

