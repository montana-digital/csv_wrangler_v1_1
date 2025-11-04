"""
Entry point for running CSV Wrangler Streamlit app.

This script:
1. Checks for virtual environment and activates it if found
2. Validates application setup (database, profile)
3. Ensures proper Python path setup before launching Streamlit
4. Automatically finds an available port if the default port is in use
"""
import os
import socket
import subprocess
import sys
from pathlib import Path
from typing import Optional

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


def find_virtual_environment() -> Optional[Path]:
    """
    Find virtual environment in project directory.
    
    Checks for common venv directory names: venv, env, .venv
    
    Returns:
        Path to virtual environment activation script, or None if not found
    """
    project_root = Path(__file__).parent
    
    # Common virtual environment directory names
    venv_names = ["venv", "env", ".venv"]
    
    for venv_name in venv_names:
        venv_path = project_root / venv_name
        if venv_path.exists() and venv_path.is_dir():
            # Check for activation script based on OS
            if sys.platform == "win32":
                activate_script = venv_path / "Scripts" / "activate.bat"
                if activate_script.exists():
                    return venv_path
            else:
                activate_script = venv_path / "bin" / "activate"
                if activate_script.exists():
                    return venv_path
    
    return None


def is_in_virtual_environment() -> bool:
    """Check if Python is running inside a virtual environment."""
    return (
        hasattr(sys, 'real_prefix') or  # virtualenv
        (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix) or  # venv
        os.environ.get('VIRTUAL_ENV') is not None  # VIRTUAL_ENV environment variable
    )


def check_app_initialization() -> tuple[bool, bool]:
    """
    Check if app is initialized (database and profile exist).
    
    Returns:
        Tuple of (database_exists, profile_exists)
    """
    try:
        from src.config.settings import DATABASE_PATH
        from src.services.profile_service import is_app_initialized
        
        # Check if database file exists
        database_exists = DATABASE_PATH.exists()
        
        if not database_exists:
            # If database doesn't exist, check if profile exists in a corrupted database
            # by trying to connect to a non-existent database
            return (False, False)
        
        # Try to connect to database and check for profile
        try:
            from src.database.connection import get_session
            
            with get_session() as session:
                profile_exists = is_app_initialized(session)
                return (True, profile_exists)
        except Exception:
            # Database exists but might be corrupted or not initialized
            return (True, False)
            
    except Exception:
        # Any error means not initialized
        return (False, False)


def delete_profile_from_database() -> bool:
    """
    Delete the user profile from the database.
    
    Returns:
        True if profile was deleted, False otherwise
    """
    try:
        from src.database.connection import get_session
        from src.database.models import UserProfile
        
        with get_session() as session:
            profile = session.query(UserProfile).first()
            if profile:
                session.delete(profile)
                # Session will commit via context manager
                return True
            return False
    except Exception as e:
        print(f"Error deleting profile: {e}")
        return False


def check_venv_dependencies(venv_python: Path) -> tuple[bool, list[str]]:
    """
    Check if required dependencies are installed in the virtual environment.
    
    Args:
        venv_python: Path to venv Python executable
        
    Returns:
        Tuple of (all_installed, missing_packages)
    """
    required_packages = ["streamlit", "pandas", "sqlalchemy"]
    missing_packages = []
    
    for package in required_packages:
        try:
            result = subprocess.run(
                [str(venv_python), "-m", "pip", "show", package],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode != 0:
                missing_packages.append(package)
        except Exception:
            missing_packages.append(package)
    
    return len(missing_packages) == 0, missing_packages


def install_venv_dependencies(venv_python: Path) -> bool:
    """
    Install required dependencies in the virtual environment.
    
    Args:
        venv_python: Path to venv Python executable
        
    Returns:
        True if installation successful, False otherwise
    """
    try:
        requirements_file = project_root / "requirements.txt"
        if not requirements_file.exists():
            print("Error: requirements.txt not found")
            return False
        
        print("\nInstalling dependencies from requirements.txt...")
        print("This may take a few minutes...")
        
        # Upgrade pip first
        subprocess.run(
            [str(venv_python), "-m", "pip", "install", "--upgrade", "pip"],
            check=False,
            capture_output=True
        )
        
        # Install requirements
        result = subprocess.run(
            [str(venv_python), "-m", "pip", "install", "-r", str(requirements_file)],
            capture_output=False
        )
        
        if result.returncode == 0:
            print("✓ Dependencies installed successfully")
            return True
        else:
            print("✗ Failed to install dependencies")
            return False
    except Exception as e:
        print(f"Error installing dependencies: {e}")
        return False


def initialize_app_quick() -> bool:
    """
    Quick initialization: create database and profile without full setup.
    
    This is a lightweight alternative to setup_app.py that only initializes
    the database and creates a user profile.
    
    Returns:
        True if initialization successful, False otherwise
    """
    try:
        print("\n" + "="*60)
        print("Quick App Initialization")
        print("="*60)
        
        # Step 1: Initialize database
        print("\nInitializing database...")
        from src.database.connection import init_database
        init_database()
        print("✓ Database initialized")
        
        # Step 2: Check if profile already exists
        from src.database.connection import get_session
        from src.services.profile_service import is_app_initialized, create_user_profile
        
        with get_session() as session:
            if is_app_initialized(session):
                print("✓ Profile already exists")
                return True
            
            # Step 3: Create profile - prompt for user name
            print("\nCreating user profile...")
            print("Please enter your name to create your profile.")
            
            while True:
                user_name = input("Enter your name: ").strip()
                if user_name:
                    try:
                        profile = create_user_profile(session, user_name)
                        print(f"✓ Profile created: {profile.name}")
                        return True
                    except Exception as e:
                        print(f"Error creating profile: {e}")
                        retry = input("Would you like to try again? (y/n): ").strip().lower()
                        if retry not in ['y', 'yes']:
                            return False
                else:
                    print("Please enter a valid name.")
                    retry = input("Would you like to try again? (y/n): ").strip().lower()
                    if retry not in ['y', 'yes']:
                        return False
        
    except Exception as e:
        print(f"\nError during initialization: {e}")
        import traceback
        traceback.print_exc()
        return False


def activate_virtual_environment(venv_path: Path) -> bool:
    """
    Activate virtual environment and update current process.
    
    Note: This modifies the current process environment.
    For Windows, we need to use a different approach.
    
    Returns:
        True if activation successful, False otherwise
    """
    if sys.platform == "win32":
        # On Windows, we need to modify the PATH and use the venv's Python
        scripts_dir = venv_path / "Scripts"
        if not scripts_dir.exists():
            return False
        
        # Add venv Scripts to PATH
        venv_scripts = str(scripts_dir)
        current_path = os.environ.get('PATH', '')
        if venv_scripts not in current_path:
            os.environ['PATH'] = f"{venv_scripts};{current_path}"
        
        # Update Python executable to use venv Python
        venv_python = scripts_dir / "python.exe"
        if venv_python.exists():
            # Note: We can't change sys.executable in the current process,
            # but we can ensure the venv's Python is in PATH
            return True
    else:
        # On Unix, modify PATH
        bin_dir = venv_path / "bin"
        if not bin_dir.exists():
            return False
        
        venv_bin = str(bin_dir)
        current_path = os.environ.get('PATH', '')
        if venv_bin not in current_path:
            os.environ['PATH'] = f"{venv_bin}:{current_path}"
        
        os.environ['VIRTUAL_ENV'] = str(venv_path)
        return True
    
    return False


def find_available_port(start_port: int = 8501, max_attempts: int = 100) -> int:
    """
    Find first available port starting from start_port.
    
    Args:
        start_port: Port number to start checking from (default: 8501)
        max_attempts: Maximum number of ports to check (default: 100)
    
    Returns:
        First available port number
    
    Raises:
        RuntimeError: If no available port is found in the range
    """
    for port in range(start_port, start_port + max_attempts):
        try:
            # Try to bind to the port
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind(("localhost", port))
                # If bind succeeds, port is available
                return port
        except OSError:
            # Port is in use, try next one
            continue
    
    # No port found
    raise RuntimeError(
        f"Could not find an available port in range {start_port}-{start_port + max_attempts - 1}. "
        "Please free up some ports or specify a different port range."
    )


def main():
    """Main entry point that checks setup and launches Streamlit."""
    default_port = 8501
    
    # Step 1: Check for virtual environment first
    venv_path = find_virtual_environment()
    venv_python = None
    
    if venv_path:
        if sys.platform == "win32":
            venv_python = venv_path / "Scripts" / "python.exe"
        else:
            venv_python = venv_path / "bin" / "python"
        
        if not venv_python.exists():
            venv_python = None
            venv_path = None
    
    if not venv_path or not venv_python:
        # No virtual environment found - must run setup_app.py first
        print("\n" + "="*60)
        print("Virtual Environment Not Found")
        print("="*60)
        print("\nNo virtual environment detected in project directory.")
        print("The application requires a virtual environment to run.")
        
        response = input("\nWould you like to run setup_app.py to create the environment? (y/n): ").strip().lower()
        
        if response in ['y', 'yes']:
            print("\nRunning setup_app.py...")
            try:
                setup_script = project_root / "setup_app.py"
                subprocess.run([sys.executable, str(setup_script)])
                
                # After setup, check for venv again
                venv_path = find_virtual_environment()
                if venv_path:
                    if sys.platform == "win32":
                        venv_python = venv_path / "Scripts" / "python.exe"
                    else:
                        venv_python = venv_path / "bin" / "python"
                    
                    if not venv_python.exists():
                        print("\nSetup completed but virtual environment not found.")
                        print("Please run setup_app.py manually and ensure it completes successfully.")
                        sys.exit(1)
                else:
                    print("\nSetup completed but virtual environment not found.")
                    print("Please run setup_app.py manually and ensure it completes successfully.")
                    sys.exit(1)
            except Exception as e:
                print(f"\nError running setup: {e}")
                print("Please run setup_app.py manually:")
                print(f"  python setup_app.py")
                sys.exit(1)
        else:
            print("\nExiting. Please run setup_app.py to create the virtual environment.")
            sys.exit(1)
    
    # Step 2: Activate/enter virtual environment if not already active
    if venv_path and venv_python:
        if not is_in_virtual_environment():
            print(f"Found virtual environment at: {venv_path}")
            print("Entering virtual environment...")
            
            # Try to activate by modifying PATH
            if activate_virtual_environment(venv_path):
                print("Virtual environment entered")
            else:
                print("Note: Virtual environment found - will use venv Python directly")
        else:
            print("Virtual environment already active")
        
        # Verify venv Python is accessible
        if venv_python.exists():
            try:
                result = subprocess.run(
                    [str(venv_python), "--version"],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                if result.returncode == 0:
                    version = result.stdout.strip()
                    print(f"Using Python from venv: {version}")
                else:
                    print("Warning: Could not verify venv Python version")
            except Exception as e:
                print(f"Warning: Could not verify venv Python: {e}")
        else:
            print("Error: venv Python executable not found")
            sys.exit(1)
        
        # Check if dependencies are installed in venv
        print("\nChecking dependencies in virtual environment...")
        deps_installed, missing_deps = check_venv_dependencies(venv_python)
        
        if not deps_installed:
            print(f"\n⚠️  Missing dependencies in virtual environment: {', '.join(missing_deps)}")
            print("\nOptions:")
            print("  1. Install dependencies now (quick)")
            print("  2. Run full setup (setup_app.py - includes validation checks)")
            print("  3. Exit - Install manually")
            
            response = input("\nEnter choice (1/2/3): ").strip()
            
            if response == '1':
                if install_venv_dependencies(venv_python):
                    print("\n✓ Dependencies installed successfully")
                    # Re-check
                    deps_installed, missing_deps = check_venv_dependencies(venv_python)
                    if not deps_installed:
                        print(f"\nSome dependencies still missing: {', '.join(missing_deps)}")
                        print("Please run setup_app.py for full setup.")
                        sys.exit(1)
                else:
                    print("\nFailed to install dependencies. Please run setup_app.py manually.")
                    sys.exit(1)
            elif response == '2':
                print("\nRunning full setup (setup_app.py)...")
                try:
                    setup_script = project_root / "setup_app.py"
                    subprocess.run([str(venv_python), str(setup_script)])
                    # Re-check dependencies after setup
                    deps_installed, missing_deps = check_venv_dependencies(venv_python)
                    if not deps_installed:
                        print(f"\nSome dependencies still missing: {', '.join(missing_deps)}")
                        print("Please ensure setup_app.py completed successfully.")
                        sys.exit(1)
                except Exception as e:
                    print(f"\nError running setup: {e}")
                    print("Please run setup_app.py manually:")
                    print(f"  {venv_python} setup_app.py")
                    sys.exit(1)
            else:
                print("\nExiting. Please install dependencies manually:")
                print(f"  {venv_python} -m pip install -r requirements.txt")
                sys.exit(1)
        else:
            print("✓ All dependencies installed")
    
    # Step 3: Check if app is initialized (using venv Python if available)
    database_exists, profile_exists = check_app_initialization()
    
    if not database_exists or not profile_exists:
        print("\n" + "="*60)
        print("Application not initialized!")
        print("="*60)
        
        # Case 1: Database exists but profile doesn't (inconsistent state)
        if database_exists and not profile_exists:
            print("\n⚠️  Inconsistent state detected:")
            print("   Database exists but user profile is missing.")
            print("   This may indicate a corrupted or partially initialized database.")
            
            print("\nOptions:")
            print("  1. Re-initialize app (quick) - Create profile only")
            print("  2. Run full setup - Run setup_app.py (checks dependencies, etc.)")
            print("  3. Exit - Resolve manually")
            
            response = input("\nEnter choice (1/2/3): ").strip()
            
            if response == '1':
                print("\nRe-initializing app (quick)...")
                # Delete the profile (if it somehow exists) and create new one
                # Use venv Python to run initialization
                if venv_python and venv_python.exists():
                    # Run initialization in a subprocess using venv Python
                    import tempfile
                    init_script = f"""
import sys
from pathlib import Path
sys.path.insert(0, r'{project_root}')

from run_app import delete_profile_from_database, initialize_app_quick
from src.database.connection import get_session

# Delete profile
delete_profile_from_database()

# Initialize
if initialize_app_quick():
    print("SUCCESS")
    sys.exit(0)
else:
    print("FAILED")
    sys.exit(1)
"""
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
                        f.write(init_script)
                        temp_script = Path(f.name)
                    
                    try:
                        result = subprocess.run(
                            [str(venv_python), str(temp_script)],
                            cwd=str(project_root),
                            capture_output=True,
                            text=True
                        )
                        print(result.stdout)
                        if result.stderr:
                            print(result.stderr)
                        
                        if result.returncode == 0:
                            print("\n✓ App initialized successfully!")
                            # Re-check
                            database_exists, profile_exists = check_app_initialization()
                            if not profile_exists:
                                print("\nInitialization completed but profile still missing.")
                                print("Please try initializing manually in the app.")
                                sys.exit(1)
                        else:
                            print("\nInitialization failed. Please try running full setup or resolve manually.")
                            sys.exit(1)
                    finally:
                        if temp_script.exists():
                            temp_script.unlink()
                else:
                    # Fallback: run in current process
                    delete_profile_from_database()
                    if initialize_app_quick():
                        print("\n✓ App initialized successfully!")
                        database_exists, profile_exists = check_app_initialization()
                        if not profile_exists:
                            print("\nInitialization completed but profile still missing.")
                            print("Please try initializing manually in the app.")
                            sys.exit(1)
                    else:
                        print("\nInitialization failed. Please try running full setup or resolve manually.")
                        sys.exit(1)
            elif response == '2':
                print("\nRunning full setup (setup_app.py)...")
                try:
                    setup_script = project_root / "setup_app.py"
                    if venv_python and venv_python.exists():
                        subprocess.run([str(venv_python), str(setup_script)])
                    else:
                        subprocess.run([sys.executable, str(setup_script)])
                    
                    # After setup, check again
                    database_exists, profile_exists = check_app_initialization()
                    if not database_exists or not profile_exists:
                        print("\nSetup completed but application still not initialized.")
                        print("Please run setup_app.py manually and ensure it completes successfully.")
                        sys.exit(1)
                except Exception as e:
                    print(f"\nError running setup: {e}")
                    print("Please run setup_app.py manually:")
                    print(f"  python setup_app.py")
                    sys.exit(1)
            else:
                print("\nExiting. Please resolve the profile issue manually.")
                sys.exit(1)
        
        # Case 2: Both missing (fresh installation)
        else:
            print("\nFirst-time setup required:")
            print("   Database: Not found")
            print("   User profile: Not found")
            
            print("\nOptions:")
            print("  1. Initialize app (quick) - Create database and profile only")
            print("  2. Run full setup - Run setup_app.py (checks dependencies, etc.)")
            print("  3. Exit - Run setup manually")
            
            response = input("\nEnter choice (1/2/3): ").strip()
            
            if response == '1':
                print("\nInitializing app (quick)...")
                # Use venv Python to run initialization
                if venv_python and venv_python.exists():
                    # Run initialization in a subprocess using venv Python
                    import tempfile
                    init_script = f"""
import sys
from pathlib import Path
sys.path.insert(0, r'{project_root}')

from run_app import initialize_app_quick

if initialize_app_quick():
    print("SUCCESS")
    sys.exit(0)
else:
    print("FAILED")
    sys.exit(1)
"""
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
                        f.write(init_script)
                        temp_script = Path(f.name)
                    
                    try:
                        result = subprocess.run(
                            [str(venv_python), str(temp_script)],
                            cwd=str(project_root),
                            capture_output=True,
                            text=True
                        )
                        print(result.stdout)
                        if result.stderr:
                            print(result.stderr)
                        
                        if result.returncode == 0:
                            print("\n✓ App initialized successfully!")
                            # Re-check
                            database_exists, profile_exists = check_app_initialization()
                            if not database_exists or not profile_exists:
                                print("\nInitialization completed but app still not ready.")
                                print("Please try running full setup or resolve manually.")
                                sys.exit(1)
                        else:
                            print("\nInitialization failed. Please try running full setup or resolve manually.")
                            sys.exit(1)
                    finally:
                        if temp_script.exists():
                            temp_script.unlink()
                else:
                    # Fallback: run in current process
                    if initialize_app_quick():
                        print("\n✓ App initialized successfully!")
                        database_exists, profile_exists = check_app_initialization()
                        if not database_exists or not profile_exists:
                            print("\nInitialization completed but app still not ready.")
                            print("Please try running full setup or resolve manually.")
                            sys.exit(1)
                    else:
                        print("\nInitialization failed. Please try running full setup or resolve manually.")
                        sys.exit(1)
            elif response == '2':
                print("\nRunning full setup (setup_app.py)...")
                try:
                    setup_script = project_root / "setup_app.py"
                    if venv_python and venv_python.exists():
                        subprocess.run([str(venv_python), str(setup_script)])
                    else:
                        subprocess.run([sys.executable, str(setup_script)])
                    
                    # After setup, check again
                    database_exists, profile_exists = check_app_initialization()
                    if not database_exists or not profile_exists:
                        print("\nSetup completed but application still not initialized.")
                        print("Please run setup_app.py manually and ensure it completes successfully.")
                        sys.exit(1)
                except Exception as e:
                    print(f"\nError running setup: {e}")
                    print("Please run setup_app.py manually:")
                    print(f"  python setup_app.py")
                    sys.exit(1)
            else:
                print("\nExiting. Please run setup_app.py before starting the application.")
                sys.exit(1)
    
    # Step 3: Launch Streamlit
    try:
        # Find available port
        available_port = find_available_port(start_port=default_port)
        
        # Inform user about port selection
        if available_port != default_port:
            print(f"\nPort {default_port} is in use, using port {available_port} instead")
        else:
            print(f"\nStarting application on port {available_port}")
        
        # Use venv Python if available (ensures correct dependencies)
        if venv_python and venv_python.exists():
            # Use venv Python to run Streamlit
            streamlit_args = [
                str(venv_python),
                "-m",
                "streamlit",
                "run",
                "src/main.py",
                "--server.port",
                str(available_port),
                "--server.address",
                "localhost",
            ]
            
            # Append any additional arguments passed to this script
            if len(sys.argv) > 1:
                streamlit_args.extend(sys.argv[1:])
            
            # Run Streamlit as subprocess using venv Python
            subprocess.run(streamlit_args)
            return
        
        # Fallback: use current Python (should not happen if venv check passed)
        print("Warning: Using current Python (venv Python not available)")
        # Prepare Streamlit command line arguments
        streamlit_args = [
            "streamlit",
            "run",
            "src/main.py",
            "--server.port",
            str(available_port),
            "--server.address",
            "localhost",
        ]
        
        # Append any additional arguments passed to this script
        if len(sys.argv) > 1:
            streamlit_args.extend(sys.argv[1:])
        
        # Replace sys.argv for Streamlit CLI
        sys.argv = streamlit_args
        
        # Now import and run Streamlit
        import streamlit.web.cli as stcli
        stcli.main()
        
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nApplication stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

