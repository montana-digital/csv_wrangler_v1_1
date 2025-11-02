@echo off
REM Batch script to run comprehensive UI tests for CSV Wrangler

REM Change to project root (two levels up from test_data/scripts/)
cd /d "%~dp0\..\.."

echo ========================================
echo CSV Wrangler UI Test Suite Runner
echo ========================================
echo.

REM Check if virtual environment is activated
python -c "import sys; sys.exit(0 if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix) else 1)" 2>nul
if %errorlevel% neq 0 (
    echo [WARNING] Virtual environment may not be activated
    echo.
)

REM Parse arguments
set TEST_MODE=full
set HEADED=false
set SLOWMO=0
set BROWSER=chromium

:parse_args
if "%1"=="" goto run_tests
if "%1"=="--smoke" set TEST_MODE=smoke & shift & goto parse_args
if "%1"=="--stress" set TEST_MODE=stress & shift & goto parse_args
if "%1"=="--headed" set HEADED=true & shift & goto parse_args
if "%1"=="--slowmo" set SLOWMO=%2 & shift & shift & goto parse_args
if "%1"=="--browser" set BROWSER=%2 & shift & shift & goto parse_args
shift
goto parse_args

:run_tests
echo Test Mode: %TEST_MODE%
echo Browser: %BROWSER%
if "%HEADED%"=="true" echo Running in HEADED mode (browser visible)
if "%SLOWMO%" neq "0" echo Slow motion: %SLOWMO%ms
echo.

REM Build pytest command
set PYTEST_CMD=pytest src/tests/e2e/test_ui_comprehensive_suite.py -v

if "%TEST_MODE%"=="smoke" (
    set PYTEST_CMD=%PYTEST_CMD%::TestNavigationAndPageLoads
) else if "%TEST_MODE%"=="stress" (
    set PYTEST_CMD=%PYTEST_CMD%::TestPerformanceAndStress
)

if "%HEADED%"=="true" (
    set PYTEST_CMD=%PYTEST_CMD% --headed
)

if "%SLOWMO%" neq "0" (
    set PYTEST_CMD=%PYTEST_CMD% --slowmo %SLOWMO%
)

set PYTEST_CMD=%PYTEST_CMD% --browser %BROWSER%

echo Running: %PYTEST_CMD%
echo.

REM Run tests
%PYTEST_CMD%

if %errorlevel% equ 0 (
    echo.
    echo ========================================
    echo ✅ All tests passed!
    echo ========================================
) else (
    echo.
    echo ========================================
    echo ❌ Some tests failed
    echo ========================================
    echo.
    echo Tips:
    echo - Run with --headed to see browser actions
    echo - Run with --slowmo 500 to slow down for debugging
    echo - Check test-results/ for screenshots on failures
)

exit /b %errorlevel%
