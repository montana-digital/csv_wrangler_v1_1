"""
Master test suite orchestrator for CSV Wrangler.

Runs all test suites in stages:
1. Unit tests (fastest)
2. Integration tests
3. E2E tests (slowest)
"""
import sys
import subprocess
from pathlib import Path


def run_tests_with_marker(marker: str, verbose: bool = True) -> int:
    """Run tests with specific marker."""
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "src/tests",
        "-m", marker,
    ]
    
    if verbose:
        cmd.append("-v")
    
    result = subprocess.run(cmd)
    return result.returncode


def run_all_tests():
    """Run all test suites in order."""
    print("=" * 80)
    print("CSV Wrangler - Master Test Suite")
    print("=" * 80)
    print()
    
    exit_codes = {}
    
    # Stage 1: Unit tests
    print("Stage 1: Running Unit Tests...")
    print("-" * 80)
    exit_code = run_tests_with_marker("unit", verbose=True)
    exit_codes["unit"] = exit_code
    if exit_code != 0:
        print(f"⚠️  Unit tests failed with exit code {exit_code}")
    else:
        print("✅ Unit tests passed")
    print()
    
    # Stage 2: Integration tests
    print("Stage 2: Running Integration Tests...")
    print("-" * 80)
    exit_code = run_tests_with_marker("integration", verbose=True)
    exit_codes["integration"] = exit_code
    if exit_code != 0:
        print(f"⚠️  Integration tests failed with exit code {exit_code}")
    else:
        print("✅ Integration tests passed")
    print()
    
    # Stage 3: E2E tests
    print("Stage 3: Running E2E Tests...")
    print("-" * 80)
    exit_code = run_tests_with_marker("e2e", verbose=True)
    exit_codes["e2e"] = exit_code
    if exit_code != 0:
        print(f"⚠️  E2E tests failed with exit code {exit_code}")
    else:
        print("✅ E2E tests passed")
    print()
    
    # Summary
    print("=" * 80)
    print("Test Suite Summary")
    print("=" * 80)
    print(f"Unit Tests:       {'✅ PASSED' if exit_codes['unit'] == 0 else '❌ FAILED'}")
    print(f"Integration Tests: {'✅ PASSED' if exit_codes['integration'] == 0 else '❌ FAILED'}")
    print(f"E2E Tests:        {'✅ PASSED' if exit_codes['e2e'] == 0 else '❌ FAILED'}")
    print()
    
    # Overall result
    total_failed = sum(1 for code in exit_codes.values() if code != 0)
    if total_failed == 0:
        print("🎉 All test suites passed!")
        return 0
    else:
        print(f"❌ {total_failed} test suite(s) failed")
        return 1


if __name__ == "__main__":
    sys.exit(run_all_tests())

