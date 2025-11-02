@echo off
REM Setup Playwright browsers for UI testing
REM If browsers are already in docs/chrome-win/, they will be used automatically
REM Change to project root (two levels up from test_data/scripts/)
cd /d "%~dp0\..\.."

echo Checking for existing browsers in docs/chrome-win/...
if exist "docs\chrome-win\chrome.exe" (
    echo Found browsers in docs/chrome-win/ - they will be used automatically!
    echo.
    echo To verify, run: pytest src/tests/e2e/test_ui_comprehensive.py::TestFirstLaunchAndProfileCreation::test_app_loads -v --no-cov
) else (
    echo No browsers found. Installing Playwright browsers...
    python -m playwright install chromium
    echo.
    echo Playwright setup complete!
)

echo.
echo You can now run UI tests with: pytest src/tests/e2e/test_ui_comprehensive.py -v
pause

