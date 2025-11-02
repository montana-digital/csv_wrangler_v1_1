@echo off
REM Debug mode - run tests with visible browser and slow motion
REM Change to script directory to call run_ui_tests.bat
cd /d "%~dp0"
call run_ui_tests.bat --headed --slowmo 500

