# Test Data and Scripts Directory

This directory contains test-related files, scripts, and data for CSV Wrangler.

## Structure

```
test_data/
├── config/              # Test configuration files
│   └── playwright.config.json
├── scripts/             # Test runner scripts
│   ├── run_all_tests.py
│   ├── run_ui_tests.bat
│   ├── run_ui_tests_smoke.bat
│   ├── run_ui_tests_debug.bat
│   ├── setup_playwright.bat
│   └── generate_test_datasets.py
├── knowledge_base_phone_carriers.csv  # Test data files
└── README.md            # This file
```

## Usage

### Running Tests

All test scripts should be run from the **project root**, not from this directory. The scripts automatically change to the project root.

**From project root:**

```batch
# Run all tests
python test_data/scripts/run_all_tests.py

# Run UI tests (full suite)
test_data\scripts\run_ui_tests.bat

# Quick smoke tests
test_data\scripts\run_ui_tests_smoke.bat

# Debug mode (visible browser, slow motion)
test_data\scripts\run_ui_tests_debug.bat

# Setup Playwright browsers
test_data\scripts\setup_playwright.bat
```

### Direct pytest usage

You can also run pytest directly from the project root:

```batch
# All tests
pytest

# Specific test type
pytest -m unit
pytest -m integration
pytest -m e2e

# UI tests
pytest src/tests/e2e/test_ui_comprehensive_suite.py -v
```

Note: `pytest.ini` and `pytest_ui.ini` are in the project root (pytest expects them there).

### Generating Test Data

Generate test datasets for testing:

```batch
python test_data/scripts/generate_test_datasets.py
```

This will create CSV and Pickle files in the `test_data/` directory.

## Configuration Files

- **playwright.config.json**: Playwright browser configuration (in `config/`)
- **pytest.ini**: Main pytest configuration (in project root)
- **pytest_ui.ini**: UI-specific pytest configuration (in project root)

## Notes

- Test scripts automatically navigate to the project root
- Test data files (CSV, Pickle) used by tests are stored here
- Coverage reports and test results are generated in project root (and gitignored)

