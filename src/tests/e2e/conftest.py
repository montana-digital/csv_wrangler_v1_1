"""
Pytest configuration and fixtures for E2E UI tests.

Uses pytest-playwright plugin which provides browser automation fixtures.
We extend these to start Streamlit app and navigate to it.

If browsers are installed in docs/chrome-win/, they will be used automatically.
"""
import subprocess
import time
from pathlib import Path
from typing import Generator

import pytest
from playwright.sync_api import Page

# Configure Playwright to use browsers from docs/ if they exist
_project_root = Path(__file__).parent.parent.parent.parent
_chrome_exe = _project_root / "docs" / "chrome-win" / "chrome.exe"
_has_custom_browser = _chrome_exe.exists()


@pytest.fixture(scope="session")
def streamlit_app():
    """Start Streamlit app as a background process."""
    import sys
    import os
    import atexit

    # Get project root
    project_root = Path(__file__).parent.parent.parent.parent
    os.chdir(project_root)

    # Check if port is already in use (might be manually started)
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    port_in_use = sock.connect_ex(('localhost', 8501)) == 0
    sock.close()

    if port_in_use:
        # Port already in use, assume app is running manually
        yield None
        return

    # Start Streamlit app
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            "src/main.py",
            "--server.headless",
            "true",
            "--server.port",
            "8501",
            "--server.address",
            "localhost",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=project_root,
    )

    # Register cleanup
    def cleanup():
        if process.poll() is None:  # Process still running
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()

    atexit.register(cleanup)

    # Wait for app to start - check if it's responding
    max_attempts = 30
    for _ in range(max_attempts):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('localhost', 8501))
            sock.close()
            if result == 0:
                time.sleep(2)  # Additional wait for Streamlit to fully initialize
                break
        except Exception:
            pass
        time.sleep(1)
    else:
        raise RuntimeError("Streamlit app failed to start on port 8501")

    yield process

    # Cleanup
    cleanup()


@pytest.fixture(scope="session")
def browser_type_launch_args(browser_type_launch_args):
    """
    Customize browser launch to use chrome.exe from docs/ if available.
    
    Overrides pytest-playwright's browser launch args to use custom browser path.
    """
    if _has_custom_browser:
        # Use custom chrome.exe path
        browser_type_launch_args["executable_path"] = str(_chrome_exe)
        # Remove channel to use executable_path instead
        browser_type_launch_args.pop("channel", None)
    return browser_type_launch_args


@pytest.fixture
def app_page(page: Page, streamlit_app) -> Generator[Page, None, None]:
    """
    Page fixture that navigates to Streamlit app.
    
    Uses pytest-playwright's built-in 'page' fixture (provided by pytest-playwright plugin)
    and extends it to navigate to our Streamlit app and wait for it to be ready.
    """
    # Navigate to Streamlit app
    page.goto("http://localhost:8501", wait_until="domcontentloaded", timeout=30000)
    
    # Wait for Streamlit to be ready - wait for any Streamlit-specific element
    # Streamlit renders content dynamically, so we wait for either:
    # 1. Main content area (streamlit-specific class)
    # 2. Or just wait a bit for JS to execute
    try:
        # Wait for Streamlit to initialize (look for stApp or main content)
        page.wait_for_selector("body", timeout=5000, state="attached")
        # Give Streamlit time to render
        page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        # If that fails, just wait a bit and continue
        time.sleep(3)
    
    yield page

