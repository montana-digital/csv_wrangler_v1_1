"""
Unified test runner script for CSV Wrangler.

PowerShell-compatible script that activates virtual environment and runs all tests.
"""
import subprocess
import sys
from pathlib import Path


def main():
    """Run all tests using the master test suite."""
    # Get project root (two levels up from test_data/scripts/)
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent
    venv_activate = project_root / "venv" / "Scripts" / "Activate.ps1"
    python_exe = project_root / "venv" / "Scripts" / "python.exe"
    
    # Check if virtual environment exists
    if not python_exe.exists():
        print("Error: Virtual environment not found. Please create it first:")
        print("  python -m venv venv")
        sys.exit(1)
    
    # Run master test suite
    cmd = [
        str(python_exe),
        str(project_root / "src" / "tests" / "test_suite_master.py")
    ]
    
    print(f"Running: {' '.join(cmd)}")
    print()
    
    result = subprocess.run(cmd, cwd=project_root)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()

