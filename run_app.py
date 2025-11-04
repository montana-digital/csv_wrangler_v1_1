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
    
    # Step 1: Check for virtual environment
    venv_path = find_virtual_environment()
    if venv_path:
        if not is_in_virtual_environment():
            print(f"Found virtual environment at: {venv_path}")
            print("Activating virtual environment...")
            if activate_virtual_environment(venv_path):
                print("Virtual environment activated")
            else:
                print("Warning: Could not activate virtual environment")
                print("Continuing with current Python environment...")
        else:
            print("Virtual environment already active")
    else:
        print("No virtual environment found in project directory")
        print("Checking if app is set up...")
    
    # Step 2: Check if app is initialized
    database_exists, profile_exists = check_app_initialization()
    
    if not database_exists or not profile_exists:
        print("\n" + "="*60)
        print("Application not initialized!")
        print("="*60)
        
        if not database_exists:
            print("\nDatabase not found. The application needs to be set up first.")
        if not profile_exists:
            print("\nUser profile not found. The application needs to be set up first.")
        
        print("\nPlease run the setup script first:")
        print(f"  python setup_app.py")
        print("\nOr if you're in a virtual environment:")
        print(f"  {sys.executable} setup_app.py")
        
        # Offer to run setup
        if sys.platform == "win32":
            response = input("\nWould you like to run setup_app.py now? (y/n): ").strip().lower()
        else:
            response = input("\nWould you like to run setup_app.py now? (y/n): ").strip().lower()
        
        if response in ['y', 'yes']:
            print("\nRunning setup_app.py...")
            try:
                setup_script = project_root / "setup_app.py"
                if sys.platform == "win32" and venv_path:
                    # On Windows with venv, use the venv's Python
                    venv_python = venv_path / "Scripts" / "python.exe"
                    if venv_python.exists():
                        subprocess.run([str(venv_python), str(setup_script)])
                    else:
                        subprocess.run([sys.executable, str(setup_script)])
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
        
        # On Windows, if we have a venv and it's not active, use venv's Python directly
        # This ensures we use the correct Python with all dependencies
        if sys.platform == "win32" and venv_path and not is_in_virtual_environment():
            venv_python = venv_path / "Scripts" / "python.exe"
            if venv_python.exists():
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
        
        # Default: use current Python (venv is active or no venv found)
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

