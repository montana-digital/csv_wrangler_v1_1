# CSV Wrangler

A standalone Python/Streamlit application for managing large CSV and Pickle file datasets with SQLite backend.

**Version**: 1.0.0

> 📌 **Note**: CSV Wrangler v1.0.0 - Initial release. See [Versioning Guide](docs/VERSIONING.md) for details.

## Features

- **Multi-Dataset Management**: Manage up to 5 independent dataset slots
- **File Format Support**: Import and export CSV and Pickle (.pkl) files
- **Data Configuration**: Configure column data types, image columns, and duplicate filtering
- **Export Functionality**: Export datasets with date range filtering
- **User Profiles**: Profile-based data management
- **Easy Setup**: Automated setup script detects Python and installs dependencies

## Requirements

- Python 3.12+
- Windows OS (primary target)

## Installation

### Quick Setup (Recommended)

1. **Run the setup script:**
   ```batch
   setup.bat
   ```
   This script will:
   - Detect all Python installations on your system
   - Let you select which Python to use (requires 3.12+)
   - Create a virtual environment (`venv/`)
   - Install all required dependencies

2. **Launch the application:**
   ```batch
   launch.bat
   ```
   The application will open in your default browser automatically.

### Manual Setup

If you prefer to set up manually:

1. Create a virtual environment:
   ```powershell
   python -m venv venv
   .\venv\Scripts\Activate.ps1
   ```

2. Install core dependencies:
   ```powershell
   pip install -r requirements.txt
   ```

3. (Optional) Install enhanced features:
   ```powershell
   pip install -r requirements-optional.txt
   ```

### Development Setup

For development work with test dependencies:

1. Follow manual setup above
2. Install development dependencies:
   ```powershell
   pip install -r requirements-dev.txt
   ```

## Development

### Running the Application

**For End Users:**
```batch
launch.bat
```
This will check prerequisites and launch the application in your browser.

**For Developers:**
```powershell
# Option 1: Use the launch script (recommended)
.\launch.bat

# Option 2: Use the Python run script
python run_app.py

# Option 3: Run directly with PYTHONPATH set
$env:PYTHONPATH="."; streamlit run src/main.py

# Option 4: Run from project root
python -m streamlit run src/main.py
```

### Running Tests

```powershell
# Run all tests
pytest

# Run with coverage
pytest --cov=src

# Run specific test type
pytest -m unit
pytest -m integration
pytest -m e2e

# Run comprehensive UI tests
pytest src/tests/e2e/test_ui_comprehensive_suite.py -v

# Run UI tests with visible browser (for debugging)
pytest src/tests/e2e/test_ui_comprehensive_suite.py --headed

# Use test runner scripts (from test_data/scripts/)
python test_data/scripts/run_all_tests.py
test_data\scripts\run_ui_tests.bat              # Full suite
test_data\scripts\run_ui_tests_smoke.bat        # Quick smoke tests
test_data\scripts\run_ui_tests_debug.bat        # Debug mode (headed + slow motion)
test_data\scripts\setup_playwright.bat          # Setup Playwright browsers
```

See [UI Testing Guide](docs/UI_TESTING_COMPREHENSIVE.md) for comprehensive UI testing documentation.

### Code Quality

```powershell
# Format code
black src/

# Lint code
flake8 src/

# Type checking
mypy src/
```

## Project Structure

```
csv_wrangler_v1/
├── src/                    # Source code
│   ├── config/             # Configuration
│   ├── database/           # Database models and connection
│   ├── services/           # Business logic
│   ├── ui/                 # Streamlit UI
│   ├── utils/              # Utilities
│   └── tests/              # Test suite
├── userdata/               # Runtime data (created automatically)
└── docs/                   # Documentation
```

## Building EXE

```powershell
python build_exe.py
```

## License

Proprietary - NASA project

