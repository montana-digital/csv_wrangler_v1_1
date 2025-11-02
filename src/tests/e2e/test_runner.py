"""
Test Runner Script for Comprehensive UI Testing Suite.

This script provides utilities to run the full UI test suite with various options.
"""
import subprocess
import sys
from pathlib import Path


def run_tests(
    test_file: str = None,
    test_class: str = None,
    test_method: str = None,
    verbose: bool = True,
    parallel: bool = False,
    headed: bool = False,
    slow_mo: int = 0,
    browser: str = "chromium",
):
    """
    Run UI tests with specified options.
    
    Args:
        test_file: Specific test file to run (e.g., 'test_ui_comprehensive_suite.py')
        test_class: Specific test class to run (e.g., 'TestNavigationAndPageLoads')
        test_method: Specific test method to run
        verbose: Show verbose output
        parallel: Run tests in parallel
        headed: Run browser in headed mode (visible)
        slow_mo: Slow down operations by milliseconds
        browser: Browser to use ('chromium', 'firefox', 'webkit')
    """
    project_root = Path(__file__).parent.parent.parent.parent
    test_dir = project_root / "src" / "tests" / "e2e"
    
    # Build pytest command
    cmd = ["pytest"]
    
    # Add test path
    if test_method and test_class:
        cmd.append(f"{test_file}::{test_class}::{test_method}")
    elif test_class:
        cmd.append(f"{test_file}::{test_class}")
    elif test_file:
        cmd.append(str(test_dir / test_file))
    else:
        cmd.append(str(test_dir))
    
    # Add options
    if verbose:
        cmd.append("-v")
    
    if parallel:
        cmd.extend(["-n", "auto"])  # Requires pytest-xdist
    
    if headed:
        cmd.append("--headed")
    
    if slow_mo > 0:
        cmd.extend(["--slowmo", str(slow_mo)])
    
    cmd.extend(["--browser", browser])
    
    # Add pytest-playwright options
    cmd.extend([
        "--browser-context-arg",
        "--disable-web-security",  # Allow file uploads in tests
    ])
    
    print(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=project_root)
    return result.returncode


def run_smoke_tests():
    """Run quick smoke tests."""
    return run_tests(
        test_file="test_ui_comprehensive_suite.py",
        test_class="TestNavigationAndPageLoads",
        verbose=True,
    )


def run_full_suite():
    """Run the complete test suite."""
    return run_tests(
        test_file="test_ui_comprehensive_suite.py",
        verbose=True,
    )


def run_stress_tests():
    """Run stress and performance tests."""
    return run_tests(
        test_file="test_ui_comprehensive_suite.py",
        test_class="TestPerformanceAndStress",
        verbose=True,
    )


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run CSV Wrangler UI Tests")
    parser.add_argument("--file", help="Test file to run")
    parser.add_argument("--class", dest="test_class", help="Test class to run")
    parser.add_argument("--method", help="Test method to run")
    parser.add_argument("--smoke", action="store_true", help="Run smoke tests only")
    parser.add_argument("--full", action="store_true", help="Run full test suite")
    parser.add_argument("--stress", action="store_true", help="Run stress tests")
    parser.add_argument("--headed", action="store_true", help="Run browser in headed mode")
    parser.add_argument("--slowmo", type=int, default=0, help="Slow down operations (ms)")
    parser.add_argument("--browser", default="chromium", choices=["chromium", "firefox", "webkit"])
    
    args = parser.parse_args()
    
    if args.smoke:
        exit_code = run_smoke_tests()
    elif args.stress:
        exit_code = run_stress_tests()
    elif args.full:
        exit_code = run_full_suite()
    else:
        exit_code = run_tests(
            test_file=args.file,
            test_class=args.test_class,
            test_method=args.method,
            headed=args.headed,
            slow_mo=args.slowmo,
            browser=args.browser,
        )
    
    sys.exit(exit_code)

