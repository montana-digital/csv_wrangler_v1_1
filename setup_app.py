#!/usr/bin/env python3
"""
CSV Wrangler - Application Setup Script

This script:
1. Validates prerequisites (Python version, etc.)
2. Detects if already in virtual environment
3. Creates/validates virtual environment (if not already active)
4. Installs dependencies
5. Runs validation checks
6. Prepares application for launch

Usage:
    python setup_app.py [--skip-env] [--skip-checks] [--skip-deps] [--force]
                        [--run-tests] [--launch-app] [--python PATH] [--no-verbose]
"""

import argparse
import os
import platform
import shutil
import socket
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Colors for terminal output
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


def print_success(message: str):
    """Print success message."""
    print(f"{Colors.GREEN}[OK]{Colors.RESET} {message}")


def print_error(message: str):
    """Print error message."""
    print(f"{Colors.RED}[ERROR]{Colors.RESET} {message}")


def print_warning(message: str):
    """Print warning message."""
    print(f"{Colors.YELLOW}[WARN]{Colors.RESET} {message}")


def print_info(message: str):
    """Print info message."""
    print(f"{Colors.BLUE}[INFO]{Colors.RESET} {message}")


def print_header(message: str):
    """Print header."""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'=' * 60}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{message}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'=' * 60}{Colors.RESET}\n")


def run_command(cmd: list[str], check: bool = True, capture_output: bool = False) -> Tuple[int, str]:
    """Run a shell command and return exit code and output."""
    try:
        result = subprocess.run(
            cmd,
            check=check,
            capture_output=capture_output,
            text=True,
            encoding='utf-8'
        )
        output = result.stdout if capture_output else ""
        return result.returncode, output
    except subprocess.CalledProcessError as e:
        if capture_output:
            return e.returncode, e.stdout or e.stderr or ""
        return e.returncode, ""
    except FileNotFoundError:
        return 1, "Command not found"


def is_in_virtual_environment() -> bool:
    """Check if Python is running inside a virtual environment."""
    # Check common virtual environment indicators
    return (
        hasattr(sys, 'real_prefix') or  # virtualenv
        (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix) or  # venv
        os.environ.get('VIRTUAL_ENV') is not None  # VIRTUAL_ENV environment variable
    )


def get_virtual_env_info() -> dict:
    """Get information about current virtual environment."""
    info = {
        'active': is_in_virtual_environment(),
        'path': None,
        'python': sys.executable
    }
    
    if info['active']:
        # Get virtual environment path
        if 'VIRTUAL_ENV' in os.environ:
            info['path'] = os.environ['VIRTUAL_ENV']
        elif hasattr(sys, 'real_prefix'):
            info['path'] = sys.real_prefix
        elif hasattr(sys, 'base_prefix'):
            info['path'] = sys.prefix
    
    return info


def detect_python_installations() -> List[Dict[str, str]]:
    """
    Detect all Python installations on the system.
    
    Returns:
        List of dicts with keys: 'path', 'version', 'source'
    """
    installations = []
    seen_paths = set()
    
    # Always include current Python
    current_path = Path(sys.executable).resolve()
    if current_path.exists():
        seen_paths.add(str(current_path))
        exit_code, version_output = run_command(
            [str(current_path), "--version"],
            check=False,
            capture_output=True
        )
        version = version_output.strip() if exit_code == 0 else "Unknown"
        installations.append({
            'path': str(current_path),
            'version': version,
            'source': 'Current interpreter'
        })
    
    if platform.system() == "Windows":
        # Method 1: Python Launcher (py.exe)
        exit_code, output = run_command(
            ["py", "-0"],
            check=False,
            capture_output=True
        )
        if exit_code == 0:
            # Parse py -0 output
            # Format: -3.13-64    C:\Python313\python.exe
            for line in output.splitlines():
                line = line.strip()
                if line and not line.startswith("-V:") and "python.exe" in line.lower():
                    parts = line.split()
                    if len(parts) >= 2:
                        version_tag = parts[0]
                        python_path = parts[-1]
                        if Path(python_path).exists():
                            resolved_path = str(Path(python_path).resolve())
                            if resolved_path not in seen_paths:
                                seen_paths.add(resolved_path)
                                exit_code, version_output = run_command(
                                    [python_path, "--version"],
                                    check=False,
                                    capture_output=True
                                )
                                version = version_output.strip() if exit_code == 0 else version_tag
                                installations.append({
                                    'path': python_path,
                                    'version': version,
                                    'source': 'Python Launcher'
                                })
        
        # Method 2: Check PATH
        for cmd in ["python", "python3", "python.exe", "python3.exe"]:
            exit_code, output = run_command(
                ["where", cmd],
                check=False,
                capture_output=True
            )
            if exit_code == 0:
                for line in output.splitlines():
                    python_path = line.strip()
                    if python_path and Path(python_path).exists():
                        resolved_path = str(Path(python_path).resolve())
                        if resolved_path not in seen_paths:
                            seen_paths.add(resolved_path)
                            exit_code, version_output = run_command(
                                [python_path, "--version"],
                                check=False,
                                capture_output=True
                            )
                            version = version_output.strip() if exit_code == 0 else "Unknown"
                            installations.append({
                                'path': python_path,
                                'version': version,
                                'source': 'PATH'
                            })
        
        # Method 3: Check registry (HKCU and HKLM)
        try:
            import winreg
            for hkey_root, root_name in [(winreg.HKEY_CURRENT_USER, "HKCU"), 
                                         (winreg.HKEY_LOCAL_MACHINE, "HKLM")]:
                try:
                    key_path = r"Software\Python\PythonCore"
                    key = winreg.OpenKey(hkey_root, key_path)
                    try:
                        i = 0
                        while True:
                            try:
                                version_key = winreg.EnumKey(key, i)
                                install_path_key = winreg.OpenKey(
                                    key, f"{version_key}\\InstallPath"
                                )
                                try:
                                    python_exe = winreg.QueryValueEx(install_path_key, "ExecutablePath")[0]
                                    if python_exe and Path(python_exe).exists():
                                        resolved_path = str(Path(python_exe).resolve())
                                        if resolved_path not in seen_paths:
                                            seen_paths.add(resolved_path)
                                            exit_code, version_output = run_command(
                                                [python_exe, "--version"],
                                                check=False,
                                                capture_output=True
                                            )
                                            version = version_output.strip() if exit_code == 0 else version_key
                                            installations.append({
                                                'path': python_exe,
                                                'version': version,
                                                'source': f'Registry ({root_name})'
                                            })
                                finally:
                                    winreg.CloseKey(install_path_key)
                                i += 1
                            except OSError:
                                break
                    finally:
                        winreg.CloseKey(key)
                except FileNotFoundError:
                    pass
        except ImportError:
            pass  # winreg not available (shouldn't happen on Windows)
        except Exception:
            pass  # Registry access might fail
    
    else:
        # Linux/Mac
        # Method 1: Check PATH
        for cmd in ["python3", "python", "python3.13", "python3.12", "python3.11"]:
            exit_code, output = run_command(
                ["which", cmd],
                check=False,
                capture_output=True
            )
            if exit_code == 0:
                python_path = output.strip()
                if python_path and Path(python_path).exists():
                    resolved_path = str(Path(python_path).resolve())
                    if resolved_path not in seen_paths:
                        seen_paths.add(resolved_path)
                        exit_code, version_output = run_command(
                            [python_path, "--version"],
                            check=False,
                            capture_output=True
                        )
                        version = version_output.strip() if exit_code == 0 else "Unknown"
                        installations.append({
                            'path': python_path,
                            'version': version,
                            'source': 'PATH'
                        })
    
    return installations


def select_python_installation(installations: List[Dict[str, str]], 
                                current_python: Optional[str] = None) -> Optional[Path]:
    """
    Display Python installations and let user select one.
    
    Args:
        installations: List of Python installation dicts
        current_python: Path to current Python executable (will be marked as default)
    
    Returns:
        Selected Python path, or None if user cancels
    """
    if not installations:
        return None
    
    # Filter to only Python 3.12+ installations
    valid_installations = []
    for inst in installations:
        version_str = inst['version'].lower()
        # Extract version number (e.g., "Python 3.13.0" -> (3, 13))
        try:
            if 'python' in version_str:
                # Extract major.minor version
                parts = version_str.split()
                for part in parts:
                    if part.startswith('3.'):
                        version_parts = part.split('.')
                        major = int(version_parts[0])
                        minor = int(version_parts[1]) if len(version_parts) > 1 else 0
                        if (major, minor) >= (3, 12):
                            valid_installations.append(inst)
                            break
        except (ValueError, IndexError):
            # If version parsing fails, include it anyway (let user decide)
            valid_installations.append(inst)
    
    # If no valid installations found, show all (let user see what's available)
    if not valid_installations:
        print_warning("No Python 3.12+ installations detected. Showing all found installations:")
        valid_installations = installations
    
    print("\nFound Python installations:")
    print(f"{Colors.BOLD}{'=' * 80}{Colors.RESET}")
    
    for i, inst in enumerate(valid_installations, 1):
        marker = ""
        if current_python and str(inst['path']) == current_python:
            marker = f"{Colors.YELLOW}[Current]{Colors.RESET} "
        elif i == 1:
            marker = f"{Colors.GREEN}[Default]{Colors.RESET} "
        
        print(f"{marker}{i}. {inst['version']:20s} - {inst['path']}")
        print(f"   Source: {inst['source']}")
    
    print(f"{Colors.BOLD}{'=' * 80}{Colors.RESET}")
    
    # Get user selection
    while True:
        try:
            prompt = f"\nSelect Python to use (1-{len(valid_installations)}, or 'q' to quit): "
            choice = input(prompt).strip().lower()
            
            if choice == 'q':
                return None
            
            choice_num = int(choice)
            if 1 <= choice_num <= len(valid_installations):
                selected = valid_installations[choice_num - 1]
                selected_path = Path(selected['path'])
                
                # Validate the selection
                if not selected_path.exists():
                    print_error(f"Selected Python path does not exist: {selected_path}")
                    continue
                
                # Verify it's actually Python
                exit_code, version_output = run_command(
                    [str(selected_path), "--version"],
                    check=False,
                    capture_output=True
                )
                if exit_code != 0:
                    print_error(f"Selected path does not appear to be a valid Python: {selected_path}")
                    continue
                
                print_success(f"Selected: {selected['version']} at {selected_path}")
                return selected_path
            else:
                print_error(f"Please enter a number between 1 and {len(valid_installations)}")
        except ValueError:
            print_error("Please enter a valid number or 'q' to quit")
        except KeyboardInterrupt:
            print("\n")
            return None


def check_prerequisites() -> bool:
    """Check if all prerequisites are met."""
    print_header("Checking Prerequisites")
    
    all_ok = True
    
    # Check Python version
    print("Checking Python version...")
    if sys.version_info < (3, 12):
        print_error(f"Python 3.12+ required. Found: {sys.version}")
        all_ok = False
    else:
        print_success(f"Python {sys.version.split()[0]}")
    
    # Check if in virtual environment
    venv_info = get_virtual_env_info()
    if venv_info['active']:
        print_success(f"Virtual environment detected: {venv_info['path']}")
        print_info(f"Python executable: {venv_info['python']}")
    else:
        print_info("Not in a virtual environment (will create one if needed)")
    
    return all_ok


def setup_virtual_environment(force: bool = False, 
                               selected_python: Optional[Path] = None) -> Tuple[bool, Optional[Path]]:
    """
    Create or validate virtual environment.
    
    Args:
        force: Force recreate virtual environment
        selected_python: Python executable to use (if None, uses sys.executable)
    
    Returns:
        (success, python_executable_path)
    """
    print_header("Virtual Environment Setup")
    
    # Check if already in virtual environment
    venv_info = get_virtual_env_info()
    if venv_info['active']:
        print_success("Already in a virtual environment")
        print_info(f"Using existing environment: {venv_info['path']}")
        print_info(f"Python: {venv_info['python']}")
        return True, Path(venv_info['python'])
    
    # Determine which Python to use
    python_to_use = selected_python if selected_python else Path(sys.executable)
    
    # Not in virtual environment - check for local venv
    venv_path = Path("venv")
    venv_python = venv_path / "Scripts" / "python.exe"  # Windows
    
    if not venv_python.exists():
        venv_python = venv_path / "bin" / "python"  # Linux/Mac
    
    if venv_path.exists() and venv_python.exists():
        if force:
            print_warning("Removing existing virtual environment...")
            shutil.rmtree(venv_path)
        else:
            print_info("Virtual environment found in 'venv/' directory")
            
            # Validate it
            exit_code, output = run_command([str(venv_python), "--version"], capture_output=True, check=False)
            if exit_code == 0:
                print_success(f"Virtual environment is valid: {output.strip()}")
                print_info(f"Python: {venv_python}")
                return True, venv_python
            else:
                print_warning("Virtual environment may be corrupted")
    
    # Create new virtual environment
    print(f"Creating virtual environment in 'venv/' using {python_to_use}...")
    exit_code, _ = run_command([str(python_to_use), "-m", "venv", "venv"])
    if exit_code != 0:
        print_error("Failed to create virtual environment")
        return False, None
    
    # Update python path after creation
    venv_python = venv_path / "Scripts" / "python.exe"
    if not venv_python.exists():
        venv_python = venv_path / "bin" / "python"
    
    if not venv_python.exists():
        print_error("Virtual environment created but Python executable not found")
        return False, None
    
    print_success("Virtual environment created successfully")
    exit_code, output = run_command([str(venv_python), "--version"], capture_output=True)
    if exit_code == 0:
        print_success(f"Python: {output.strip()}")
    
    return True, venv_python


def install_dependencies(python_exe: Path, verbose: bool = True) -> bool:
    """
    Install project dependencies with verbose output.
    
    Args:
        python_exe: Path to Python executable
        verbose: Show detailed installation progress
    """
    print_header("Installing Dependencies")
    
    if not python_exe.exists():
        print_error("Python executable not found")
        return False
    
    requirements_file = Path("requirements.txt")
    if not requirements_file.exists():
        print_error("requirements.txt not found")
        return False
    
    # Show what packages will be installed
    print_info("Reading requirements from requirements.txt...")
    try:
        req_content = requirements_file.read_text()
        # Extract package names (lines that aren't comments)
        packages = []
        for line in req_content.splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                # Extract package name (before version specifiers)
                pkg_name = line.split(">=")[0].split("==")[0].split("~=")[0].strip()
                if pkg_name:
                    packages.append(pkg_name)
        
        if packages:
            print_info(f"Will install {len(packages)} packages: {', '.join(packages[:5])}")
            if len(packages) > 5:
                print_info(f"  ... and {len(packages) - 5} more packages")
    except Exception as e:
        print_warning(f"Could not parse requirements file: {e}")
    
    # Upgrade pip first
    print("\n" + "=" * 60)
    print("Step 1: Upgrading pip to latest version...")
    print("=" * 60)
    pip_cmd = [str(python_exe), "-m", "pip", "install", "--upgrade", "pip"]
    if verbose:
        pip_cmd.extend(["--verbose", "--progress-bar", "pretty"])
    
    exit_code, output = run_command(pip_cmd, capture_output=not verbose, check=False)
    if exit_code == 0:
        if verbose and output:
            print(output)
        print_success("pip upgraded successfully")
    else:
        print_warning("Failed to upgrade pip (continuing anyway)")
        if output and not verbose:
            print_info(output)
    
    # Install core dependencies
    print("\n" + "=" * 60)
    print("Step 2: Installing core dependencies from requirements.txt...")
    print("=" * 60)
    print_info("This may take several minutes depending on your internet connection...")
    print_info("Packages will be downloaded and installed from PyPI...")
    
    install_cmd = [str(python_exe), "-m", "pip", "install", "-r", str(requirements_file)]
    if verbose:
        install_cmd.extend(["--verbose", "--progress-bar", "pretty"])
    else:
        install_cmd.append("--progress-bar")
    
    print("\nStarting installation...")
    exit_code, output = run_command(install_cmd, capture_output=not verbose)
    
    if exit_code != 0:
        print_error("\nFailed to install core dependencies")
        if verbose and output:
            print_error("\nError output:")
            print_error(output)
        elif output:
            # Show last few lines of error
            error_lines = output.splitlines()[-20:]
            print_error("\nError details (last 20 lines):")
            for line in error_lines:
                print_error(f"  {line}")
        return False
    
    if verbose and output:
        # Show summary of what was installed
        lines = output.splitlines()
        installed_packages = []
        for line in lines:
            if "Successfully installed" in line:
                print_success(f"\n{line}")
            elif "Requirement already satisfied" in line:
                # Extract package name
                if " " in line:
                    pkg = line.split()[3] if len(line.split()) > 3 else ""
                    if pkg:
                        installed_packages.append(pkg)
    
    print_success("Core dependencies installed successfully")
    
    # Check for optional dependencies
    optional_req = Path("requirements-optional.txt")
    if optional_req.exists():
        print("\n" + "=" * 60)
        print("Step 3: Installing optional dependencies...")
        print("=" * 60)
        print_info("These packages enhance functionality but are not required for core features")
        
        try:
            opt_content = optional_req.read_text()
            opt_packages = []
            for line in opt_content.splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    pkg_name = line.split(">=")[0].split("==")[0].split("~=")[0].strip()
                    if pkg_name:
                        opt_packages.append(pkg_name)
            if opt_packages:
                print_info(f"Optional packages: {', '.join(opt_packages)}")
        except Exception:
            pass
        
        opt_cmd = [str(python_exe), "-m", "pip", "install", "-r", str(optional_req)]
        if verbose:
            opt_cmd.extend(["--verbose", "--progress-bar", "pretty"])
        else:
            opt_cmd.append("--progress-bar")
        
        exit_code, output = run_command(opt_cmd, capture_output=not verbose, check=False)
        if exit_code == 0:
            print_success("Optional dependencies installed successfully")
            if verbose and output:
                print(output)
        else:
            print_warning("Some optional dependencies failed to install (non-critical)")
            if output and not verbose:
                # Show error summary
                error_lines = output.splitlines()[-10:]
                print_warning("\nError details:")
                for line in error_lines:
                    print_warning(f"  {line}")
    
    # Verify key packages
    print("\n" + "=" * 60)
    print("Step 4: Verifying installation...")
    print("=" * 60)
    key_packages = ["streamlit", "pandas", "sqlalchemy"]
    all_installed = True
    
    for package in key_packages:
        exit_code, output = run_command(
            [str(python_exe), "-m", "pip", "show", package],
            check=False,
            capture_output=True
        )
        if exit_code == 0:
            # Extract version from output
            version = "unknown"
            for line in output.splitlines():
                if line.startswith("Version:"):
                    version = line.split(":", 1)[1].strip()
                    break
            print_success(f"{package} installed (version: {version})")
        else:
            print_error(f"{package} not found")
            all_installed = False
    
    return all_installed


def check_disk_space(minimum_mb: int = 500) -> bool:
    """Check if sufficient disk space is available."""
    try:
        project_root = Path.cwd()
        if sys.platform == "win32":
            import ctypes
            free_bytes = ctypes.c_ulonglong(0)
            ctypes.windll.kernel32.GetDiskFreeSpaceExW(
                ctypes.c_wchar_p(str(project_root)),
                ctypes.pointer(free_bytes),
                None,
                None
            )
            free_mb = free_bytes.value / (1024 * 1024)
        else:
            stat = shutil.disk_usage(project_root)
            free_mb = stat.free / (1024 * 1024)
        
        if free_mb < minimum_mb:
            print_error(f"Insufficient disk space: {free_mb:.1f} MB available, {minimum_mb} MB required")
            print_info("   SOLUTION: Free up disk space or install to a different drive")
            return False
        else:
            print_success(f"Disk space OK: {free_mb:.1f} MB available")
            return True
    except Exception as e:
        print_warning(f"Could not check disk space: {e}")
        return True  # Don't block on check failure


def check_write_permissions() -> bool:
    """Check if we have write permissions to project directory."""
    project_root = Path.cwd()
    test_file = project_root / ".write_test_tmp"
    
    try:
        # Try to create a test file
        test_file.write_text("test")
        test_file.unlink()  # Clean up
        print_success("Write permissions OK")
        return True
    except PermissionError:
        print_error(f"No write permission in project directory: {project_root}")
        print_info("   SOLUTION: Run with appropriate permissions or change directory ownership")
        return False
    except Exception as e:
        print_warning(f"Could not check write permissions: {e}")
        return True  # Don't block on check failure


def check_internet_connectivity() -> bool:
    """Check internet connectivity for PyPI access."""
    try:
        # Try to connect to pypi.org with a short timeout
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex(("pypi.org", 80))
        sock.close()
        
        if result == 0:
            print_success("Internet connectivity OK (PyPI accessible)")
            return True
        else:
            print_warning("Cannot reach PyPI - internet connectivity issue")
            print_info("   NOTE: Package installation may fail without internet access")
            return True  # Warn but don't block
    except Exception:
        print_warning("Could not verify internet connectivity")
        print_info("   NOTE: Package installation may fail without internet access")
        return True  # Warn but don't block


def check_port_availability(start_port: int = 8501, max_ports: int = 10) -> bool:
    """Check if common ports for the app are available."""
    available_ports = []
    checked_ports = []
    
    for port in range(start_port, start_port + max_ports):
        checked_ports.append(port)
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(("localhost", port))
            sock.close()
            
            if result != 0:  # Port is available
                available_ports.append(port)
        except Exception:
            pass
    
    if available_ports:
        print_success(f"Port availability OK: ports {available_ports[0]}-{available_ports[-1]} available")
        if start_port not in available_ports:
            print_info(f"   NOTE: Port {start_port} is in use, app will use port {available_ports[0]} automatically")
        return True
    else:
        print_warning(f"All checked ports ({checked_ports[0]}-{checked_ports[-1]}) are in use")
        print_info("   NOTE: App may have trouble finding an available port when launching")
        return True  # Warn but don't block


def test_database_init(python_exe: Path) -> bool:
    """Test that database can be initialized."""
    try:
        # Test SQLite import and basic operation
        test_script = """
import sqlite3
import tempfile
import os

# Test SQLite import
conn = sqlite3.connect(':memory:')
cursor = conn.cursor()
cursor.execute('CREATE TABLE test (id INTEGER PRIMARY KEY)')
cursor.execute('INSERT INTO test VALUES (1)')
result = cursor.fetchone()
conn.close()
print('OK')
"""
        exit_code, output = run_command(
            [str(python_exe), "-c", test_script],
            check=False,
            capture_output=True
        )
        
        if exit_code == 0 and "OK" in output:
            print_success("Database initialization OK (SQLite available)")
            return True
        else:
            print_error("Database initialization test failed")
            print_info("   SOLUTION: SQLite should be included with Python - reinstall Python if needed")
            return False
    except Exception as e:
        print_warning(f"Could not test database initialization: {e}")
        return True  # Don't block on check failure


def test_critical_imports(python_exe: Path) -> bool:
    """Test that critical modules can be imported."""
    critical_modules = ["streamlit", "pandas", "sqlalchemy"]
    all_ok = True
    
    for module in critical_modules:
        try:
            exit_code, output = run_command(
                [str(python_exe), "-c", f"import {module}; print('OK')"],
                check=False,
                capture_output=True
            )
            
            if exit_code == 0 and "OK" in output:
                print_success(f"Import {module} OK")
            else:
                print_error(f"Cannot import {module}")
                print_info(f"   SOLUTION: Install with: {python_exe} -m pip install {module}")
                all_ok = False
        except Exception:
            print_error(f"Could not test import of {module}")
            all_ok = False
    
    return all_ok


def check_path_length() -> bool:
    """Check if project path exceeds Windows MAX_PATH limit."""
    if sys.platform == "win32":
        project_path = Path.cwd().absolute()
        path_length = len(str(project_path))
        
        if path_length > 260:
            print_warning(f"Project path length ({path_length} chars) exceeds Windows MAX_PATH (260)")
            print_info("   NOTE: This may cause issues on Windows. Consider moving project to shorter path")
            print_info(f"   Current path: {project_path}")
            return True  # Warn but don't block
        else:
            print_success(f"Path length OK ({path_length} characters)")
            return True
    else:
        # Not relevant for non-Windows
        return True


def check_file_system_permissions(python_exe: Path) -> bool:
    """Check that userdata/ directory can be created and written."""
    project_root = Path.cwd()
    userdata_dir = project_root / "userdata"
    
    try:
        # Try to create the directory if it doesn't exist
        userdata_dir.mkdir(exist_ok=True)
        
        # Try to write a test file
        test_file = userdata_dir / ".write_test_tmp"
        test_file.write_text("test")
        test_file.unlink()
        
        print_success("File system permissions OK (userdata/ writable)")
        return True
    except PermissionError:
        print_error(f"Cannot create or write to userdata/ directory: {userdata_dir}")
        print_info("   SOLUTION: Check directory permissions or run with appropriate access")
        return False
    except Exception as e:
        print_warning(f"Could not verify file system permissions: {e}")
        return True  # Don't block on check failure


def check_python_path_validation(python_exe: Path) -> bool:
    """Verify src/ directory is importable and main.py syntax is valid."""
    all_ok = True
    
    # Check src/ directory exists
    src_dir = Path("src")
    if not src_dir.exists():
        print_error("src/ directory not found")
        print_info("   SOLUTION: Ensure you're running setup from project root directory")
        return False
    
    print_success("src/ directory exists")
    
    # Check main.py exists
    main_py = src_dir / "main.py"
    if not main_py.exists():
        print_error("src/main.py not found")
        print_info("   SOLUTION: Ensure main.py exists in src/ directory")
        return False
    
    # Check Python syntax of main.py
    exit_code, output = run_command(
        [str(python_exe), "-m", "py_compile", str(main_py)],
        check=False,
        capture_output=True
    )
    
    if exit_code == 0:
        print_success("src/main.py syntax OK")
    else:
        print_error("src/main.py has syntax errors")
        if output:
            print_error(output)
        print_info("   SOLUTION: Fix syntax errors in main.py before running setup")
        all_ok = False
    
    # Test that src can be imported
    try:
        project_root = Path.cwd().absolute()
        test_script = f"""
import sys
from pathlib import Path
sys.path.insert(0, r'{project_root}')
try:
    import src
    print('OK')
except Exception as e:
    print('FAIL: ' + str(e))
"""
        exit_code, output = run_command(
            [str(python_exe), "-c", test_script],
            check=False,
            capture_output=True
        )
        
        if exit_code == 0 and "OK" in output:
            print_success("src/ directory is importable")
        else:
            print_warning("Could not verify src/ importability")
            if output:
                print_info(output.strip())
    except Exception:
        print_warning("Could not test src/ importability")
    
    return all_ok


def run_validation_checks(python_exe: Path) -> bool:
    """Run comprehensive validation checks on the codebase."""
    print_header("Running Validation Checks")
    
    all_ok = True
    checks_run = []
    
    # System-level checks
    print("\n[System Checks]")
    checks_run.append(("Disk Space", check_disk_space()))
    checks_run.append(("Write Permissions", check_write_permissions()))
    checks_run.append(("Internet Connectivity", check_internet_connectivity()))
    checks_run.append(("Port Availability", check_port_availability()))
    checks_run.append(("Path Length", check_path_length()))
    
    # File system checks
    print("\n[File System Checks]")
    checks_run.append(("File System Permissions", check_file_system_permissions(python_exe)))
    checks_run.append(("Python Path Validation", check_python_path_validation(python_exe)))
    
    # Database checks
    print("\n[Database Checks]")
    checks_run.append(("Database Initialization", test_database_init(python_exe)))
    
    # Import checks
    print("\n[Import Checks]")
    import_result = test_critical_imports(python_exe)
    checks_run.append(("Critical Imports", import_result))
    
    # Configuration checks
    print("\n[Configuration Checks]")
    
    # Check if key files exist
    key_files = [
        ("requirements.txt", True),
        ("README.md", False),  # Warning only
    ]
    
    for file_path, required in key_files:
        if Path(file_path).exists():
            print_success(f"{file_path} exists")
        else:
            if required:
                print_error(f"{file_path} missing")
                all_ok = False
            else:
                print_warning(f"{file_path} missing (optional)")
    
    # Check .gitignore
    gitignore = Path(".gitignore")
    if gitignore.exists():
        try:
            content = gitignore.read_text()
            required_ignores = ["venv/", "userdata/", "*.db"]
            missing = [item for item in required_ignores if item not in content]
            if missing:
                print_warning(f".gitignore missing recommended entries: {', '.join(missing)}")
            else:
                print_success(".gitignore configured correctly")
        except Exception:
            print_warning("Could not read .gitignore")
    else:
        print_warning(".gitignore not found (recommended for git repositories)")
    
    # Summary
    print("\n[Check Summary]")
    failed_checks = [name for name, result in checks_run if not result]
    if failed_checks:
        print_error(f"Failed checks: {', '.join(failed_checks)}")
        all_ok = False
    else:
        print_success("All validation checks passed")
    
    return all_ok


def run_tests(python_exe: Path) -> bool:
    """
    Run test suite to verify installation.
    
    Returns:
        True if tests passed, False otherwise
    """
    print_header("Running Tests")
    print_info("Running test suite to verify installation...")
    print_info("This will run unit tests first (fastest), then integration tests...")
    
    # Run unit tests first (fastest)
    print("\n[Unit Tests]")
    print_info("Running unit tests...")
    exit_code, output = run_command(
        [str(python_exe), "-m", "pytest", "src/tests/unit", "-v", "--tb=short"],
        capture_output=False,
        check=False
    )
    
    if exit_code != 0:
        print_warning("Some unit tests failed")
        return False
    
    print_success("Unit tests passed")
    
    # Run integration tests
    print("\n[Integration Tests]")
    print_info("Running integration tests...")
    exit_code, output = run_command(
        [str(python_exe), "-m", "pytest", "src/tests/integration", "-v", "--tb=short"],
        capture_output=False,
        check=False
    )
    
    if exit_code != 0:
        print_warning("Some integration tests failed")
        return False
    
    print_success("Integration tests passed")
    
    print_success("\nAll tests passed! Installation verified.")
    return True


def launch_app(python_exe: Path) -> bool:
    """
    Launch the application.
    
    Returns:
        True if app started successfully, False otherwise
    """
    print_header("Launching Application")
    print_info("Starting CSV Wrangler application...")
    print_info("The app will open in your default browser automatically")
    print_info("Press Ctrl+C to stop the application\n")
    
    # Use run_app.py script
    run_app_script = Path("run_app.py")
    if not run_app_script.exists():
        print_error("run_app.py not found")
        return False
    
    try:
        # Run the app (this will block until user stops it)
        exit_code, _ = run_command(
            [str(python_exe), str(run_app_script)],
            capture_output=False,
            check=False
        )
        return exit_code == 0
    except KeyboardInterrupt:
        print("\n\nApplication stopped by user")
        return True
    except Exception as e:
        print_error(f"Failed to launch application: {e}")
        return False


def print_setup_summary(python_exe: Path, venv_active: bool):
    """Print setup summary and next steps."""
    print_header("Setup Complete!")
    
    print_success("Application is ready to run")
    
    print(f"\n{Colors.BOLD}Python Environment:{Colors.RESET}")
    print(f"  Python: {python_exe}")
    if venv_active:
        print(f"  Status: Using active virtual environment")
    else:
        print(f"  Status: Using local virtual environment (venv/)")
    
    print(f"\n{Colors.BOLD}Next Steps:{Colors.RESET}")
    if not venv_active:
        print("  1. Activate virtual environment:")
        if sys.platform == "win32":
            print("     .\\venv\\Scripts\\Activate.ps1")
        else:
            print("     source venv/bin/activate")
    
    print("  2. Launch the application:")
    print("     .\\launch.bat")
    print("     OR")
    print("     python run_app.py")
    print("     OR")
    print("     streamlit run src/main.py")
    
    print("  3. Run tests:")
    print("     pytest")
    print("     OR")
    print("     pytest -m unit")
    print("     OR")
    print("     pytest -m integration")
    
    print(f"\n{Colors.BOLD}Project Structure:{Colors.RESET}")
    print("  [OK] Source code: src/")
    print("  [OK] Tests: src/tests/")
    print("  [OK] Configuration: requirements.txt, pytest.ini")
    print("  [OK] Documentation: README.md")


def main():
    """Main setup workflow."""
    parser = argparse.ArgumentParser(
        description="CSV Wrangler - Application Setup Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python setup_app.py                    # Full setup
  python setup_app.py --skip-deps        # Skip dependency installation
  python setup_app.py --force            # Recreate virtual environment
  python setup_app.py --python PATH      # Use specific Python executable
  python setup_app.py --run-tests        # Run tests after setup
  python setup_app.py --launch-app       # Launch app after setup
  python setup_app.py --run-tests --launch-app  # Run tests then launch app
  python setup_app.py --no-verbose       # Less verbose dependency installation
        """
    )
    parser.add_argument(
        "--skip-env",
        action="store_true",
        help="Skip virtual environment setup (use current Python)"
    )
    parser.add_argument(
        "--skip-checks",
        action="store_true",
        help="Skip validation checks"
    )
    parser.add_argument(
        "--skip-deps",
        action="store_true",
        help="Skip dependency installation"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force recreate virtual environment"
    )
    parser.add_argument(
        "--python",
        type=str,
        metavar="PATH",
        help="Path to Python executable to use (bypasses detection and selection)"
    )
    parser.add_argument(
        "--select-python",
        action="store_true",
        help="Show Python selection menu even if only one installation found"
    )
    parser.add_argument(
        "--run-tests",
        action="store_true",
        help="Run test suite after setup to verify installation"
    )
    parser.add_argument(
        "--launch-app",
        action="store_true",
        help="Launch the application after setup completes"
    )
    parser.add_argument(
        "--no-verbose",
        action="store_true",
        dest="no_verbose",
        help="Reduce verbosity of dependency installation output"
    )
    
    args = parser.parse_args()
    
    print_header("CSV Wrangler - Application Setup")
    
    # Step 0: Python selection (if not skipping env setup)
    selected_python = None
    python_exe = Path(sys.executable)
    
    if not args.skip_env and not args.python:
        # Detect Python installations
        print_header("Python Installation Detection")
        print("Scanning for Python installations...")
        installations = detect_python_installations()
        
        if len(installations) > 1 or args.select_python:
            # Multiple installations found - let user select
            current_python = str(sys.executable)
            selected_python = select_python_installation(installations, current_python)
            
            if selected_python is None:
                print_error("\nPython selection cancelled by user.")
                sys.exit(1)
            
            python_exe = selected_python
            print(f"\nUsing Python: {python_exe}")
            exit_code, version_output = run_command(
                [str(python_exe), "--version"],
                check=False,
                capture_output=True
            )
            if exit_code == 0:
                print_success(f"Version: {version_output.strip()}")
        elif len(installations) == 1:
            # Only one installation found
            print_info(f"Using Python: {installations[0]['path']}")
            print_success(f"Version: {installations[0]['version']}")
            python_exe = Path(installations[0]['path'])
        else:
            # No installations found (unlikely, but use current)
            print_warning("Could not detect Python installations, using current interpreter")
            python_exe = Path(sys.executable)
    elif args.python:
        # User specified Python path
        specified_path = Path(args.python)
        if not specified_path.exists():
            print_error(f"Specified Python path does not exist: {specified_path}")
            sys.exit(1)
        python_exe = specified_path
        exit_code, version_output = run_command(
            [str(python_exe), "--version"],
            check=False,
            capture_output=True
        )
        if exit_code != 0:
            print_error(f"Specified path does not appear to be a valid Python: {specified_path}")
            sys.exit(1)
        print_success(f"Using specified Python: {python_exe}")
        print_success(f"Version: {version_output.strip()}")
        selected_python = python_exe
    
    print(f"\nCurrent Python: {python_exe}")
    exit_code, version_output = run_command(
        [str(python_exe), "--version"],
        check=False,
        capture_output=True
    )
    if exit_code == 0:
        print(f"Version: {version_output.strip()}\n")
    
    # Step 1: Check prerequisites (validate selected Python version)
    if not args.skip_env:
        # Validate Python version of selected/current Python
        exit_code, version_output = run_command(
            [str(python_exe), "--version"],
            check=False,
            capture_output=True
        )
        if exit_code == 0:
            # Parse version
            version_str = version_output.strip().lower()
            try:
                if 'python' in version_str:
                    parts = version_str.split()
                    for part in parts:
                        if part.startswith('3.'):
                            version_parts = part.split('.')
                            major = int(version_parts[0])
                            minor = int(version_parts[1]) if len(version_parts) > 1 else 0
                            if (major, minor) < (3, 12):
                                print_error(f"Python 3.12+ required. Selected Python version: {part}")
                                print_info("Please select a different Python installation.")
                                sys.exit(1)
                            break
            except (ValueError, IndexError):
                print_warning("Could not parse Python version, continuing anyway...")
    
    # Check prerequisites with current sys.executable (for initial check)
    if not check_prerequisites():
        print_error("\nPrerequisites check failed. Please fix the issues above.")
        sys.exit(1)
    
    # Step 2: Setup virtual environment
    venv_info = get_virtual_env_info()
    venv_active = venv_info['active']
    
    if args.skip_env:
        print_info("Skipping virtual environment setup (using current Python)")
        python_exe = Path(sys.executable)
    else:
        success, python_exe = setup_virtual_environment(force=args.force, selected_python=selected_python)
        if not success or python_exe is None:
            print_error("\nVirtual environment setup failed.")
            sys.exit(1)
    
    # Step 3: Install dependencies
    if not args.skip_deps:
        verbose_install = not args.no_verbose
        if not install_dependencies(python_exe, verbose=verbose_install):
            print_error("\nDependency installation failed.")
            sys.exit(1)
    else:
        print_info("Skipping dependency installation")
    
    # Step 4: Run validation checks
    if not args.skip_checks:
        if not run_validation_checks(python_exe):
            print_warning("\nSome validation checks failed, but setup completed.")
    else:
        print_info("Skipping validation checks")
    
    # Step 5: Print summary
    print_setup_summary(python_exe, venv_active)
    
    # Step 6: Run tests if requested
    if args.run_tests:
        print("\n")
        tests_passed = run_tests(python_exe)
        if not tests_passed:
            print_warning("\nSome tests failed, but setup completed.")
            print_info("You can run tests manually later with: pytest")
        else:
            print_success("\n✓ All tests passed - installation verified!")
    
    # Step 7: Launch app if requested
    if args.launch_app:
        print("\n")
        if not launch_app(python_exe):
            print_warning("\nFailed to launch application.")
            print_info("You can launch manually with: python run_app.py")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nSetup cancelled by user")
        sys.exit(1)
    except Exception as e:
        print_error(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

