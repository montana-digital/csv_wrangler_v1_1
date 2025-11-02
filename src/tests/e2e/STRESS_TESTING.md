# UI Stress Testing Guide

## Overview

Comprehensive stress tests for the Dataset Uploader UI flow using Playwright. These tests stress-test the complete user journey with various edge cases.

## Test Suite: `test_dataset_uploader_stress.py`

### Test Coverage

#### 1. Dataset Initialization Flow (`TestDatasetInitializationFlow`)
- ✅ **Complete initialization flow** - Full journey: navigate → upload → name → configure → submit
- ✅ **Missing name validation** - Upload file but don't enter name → error handling
- ✅ **Invalid file handling** - Upload invalid file → error display

#### 2. File Upload Flow (`TestFileUploadFlow`)
- ✅ **Sequential uploads** - Upload multiple files one after another
- ✅ **Duplicate filename detection** - Upload same filename twice → duplicate warning
- ✅ **Large file handling** - Upload 1000+ row CSV → performance testing
- ✅ **Pickle file upload** - Upload Pickle files to initialized dataset

#### 3. Form Validation Stress (`TestFormValidationStress`)
- ✅ **Empty form submission** - Submit without filling anything → validation errors
- ✅ **Partial form filling** - Fill only file OR only name → button behavior

#### 4. Column Configuration Stress (`TestColumnConfigurationStress`)
- ✅ **All column types** - Configure TEXT, INTEGER, REAL columns
- ✅ **Base64 detection** - Auto-detect Base64 image columns

## Running Stress Tests

### Prerequisites
```powershell
# Ensure Playwright browsers are installed
test_data\scripts\setup_playwright.bat

# Or manually
python -m playwright install chromium
```

### Run All Stress Tests
```powershell
pytest src/tests/e2e/test_dataset_uploader_stress.py -v
```

### Run Specific Test Class
```powershell
# Test initialization flow only
pytest src/tests/e2e/test_dataset_uploader_stress.py::TestDatasetInitializationFlow -v

# Test file upload flow only
pytest src/tests/e2e/test_dataset_uploader_stress.py::TestFileUploadFlow -v
```

### Run with Visible Browser (Debugging)
```powershell
pytest src/tests/e2e/test_dataset_uploader_stress.py --headed -v
```

### Run with Slow Motion (Step-by-step)
```powershell
pytest src/tests/e2e/test_dataset_uploader_stress.py --slow-mo=1000 -v
```

## Test Scenarios

### Happy Path
1. Navigate to dataset page
2. Upload CSV file
3. Enter dataset name
4. Configure column types
5. Select duplicate filter column
6. Click "Initialize Dataset"
7. Verify success message

### Edge Cases
- **Missing file**: Button disabled or shows error
- **Missing name**: Validation error appears
- **Invalid file**: Error message displayed
- **Duplicate upload**: Warning appears with confirmation
- **Large files**: Handles 1000+ rows gracefully
- **Form resets**: Session state persists correctly

## Known Issues Being Tested

1. **Button Availability**: Tests verify "Initialize Dataset" button becomes enabled when ready
2. **File Persistence**: Tests verify uploaded files persist across Streamlit reruns
3. **Form State**: Tests verify form state is maintained correctly
4. **Error Handling**: Tests verify proper error messages appear

## Continuous Integration

These tests can be integrated into CI/CD pipelines:

```yaml
# Example GitHub Actions
- name: Run UI Stress Tests
  run: |
    pytest src/tests/e2e/test_dataset_uploader_stress.py -v --junitxml=stress-test-results.xml
```

## Troubleshooting

### Tests Skipping
- Some tests skip if datasets aren't initialized - this is expected
- Ensure app is running: `streamlit run src/main.py`

### Flaky Tests
- Increase `sleep()` times if tests are timing out
- Use `--slow-mo=1000` to slow down execution
- Use `--headed` to see what's happening

### Browser Not Found
- Run `.\setup_playwright.bat` to install browsers
- Check `docs/chrome-win/chrome.exe` exists

