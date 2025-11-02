@echo off
REM Quick smoke tests - fast navigation tests only
REM Change to script directory to call run_ui_tests.bat
cd /d "%~dp0"
call run_ui_tests.bat --smoke

