"""
Entry point for running CSV Wrangler Streamlit app.

This script:
1. Checks for virtual environment in the project directory
2. If found, launches the Streamlit app
3. If not found, informs the user and waits for key press to exit
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
        Path to virtual environment directory, or None if not found
    """
    # Common virtual environment directory names
    venv_names = ["venv", "env", ".venv"]
    
    for venv_name in venv_names:
        venv_path = project_root / venv_name
        if venv_path.exists() and venv_path.is_dir():
            # Check for Python executable based on OS
            if sys.platform == "win32":
                python_exe = venv_path / "Scripts" / "python.exe"
            else:
                python_exe = venv_path / "bin" / "python"
            
            if python_exe.exists():
                return venv_path
    
    return None


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


def activate_virtual_environment(venv_path: Path) -> bool:
    """
    Activate virtual environment by modifying PATH and environment variables.
    
    Args:
        venv_path: Path to virtual environment directory
        
    Returns:
        True if activation successful, False otherwise
    """
    try:
        if sys.platform == "win32":
            # Windows: Add Scripts directory to PATH
            scripts_dir = venv_path / "Scripts"
            if not scripts_dir.exists():
                return False
            
            venv_scripts = str(scripts_dir)
            current_path = os.environ.get('PATH', '')
            
            # Add to front of PATH if not already there
            if venv_scripts not in current_path:
                os.environ['PATH'] = f"{venv_scripts};{current_path}"
            
            # Set VIRTUAL_ENV variable
            os.environ['VIRTUAL_ENV'] = str(venv_path)
            return True
        else:
            # Unix/Linux/Mac: Add bin directory to PATH
            bin_dir = venv_path / "bin"
            if not bin_dir.exists():
                return False
            
            venv_bin = str(bin_dir)
            current_path = os.environ.get('PATH', '')
            
            # Add to front of PATH if not already there
            if venv_bin not in current_path:
                os.environ['PATH'] = f"{venv_bin}:{current_path}"
            
            # Set VIRTUAL_ENV variable
            os.environ['VIRTUAL_ENV'] = str(venv_path)
            return True
    except Exception:
        return False


def main():
    """Main entry point that checks for virtual environment, activates it, and launches Streamlit."""
    default_port = 8501
    
    # Check for virtual environment
    venv_path = find_virtual_environment()
    
    if not venv_path:
        print("\n" + "="*60)
        print("Virtual Environment Not Found")
        print("="*60)
        print("\nNo virtual environment detected in project directory.")
        print("The application requires a virtual environment to run.")
        print("\nPlease run setup_app.py to create the virtual environment first.")
        print("\nPress any key to exit...")
        
        # Wait for key press (cross-platform)
        try:
            if sys.platform == "win32":
                import msvcrt
                msvcrt.getch()
            else:
                import termios
                import tty
                fd = sys.stdin.fileno()
                old_settings = termios.tcgetattr(fd)
                try:
                    tty.setraw(sys.stdin.fileno())
                    sys.stdin.read(1)
                finally:
                    termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        except (ImportError, Exception):
            # Fallback: just wait for Enter key
            input()
        
        sys.exit(1)
    
    # Virtual environment found - get Python executable
    if sys.platform == "win32":
        venv_python = venv_path / "Scripts" / "python.exe"
    else:
        venv_python = venv_path / "bin" / "python"
    
    if not venv_python.exists():
        print("\n" + "="*60)
        print("Virtual Environment Found But Python Not Available")
        print("="*60)
        print(f"\nVirtual environment found at: {venv_path}")
        print("But Python executable not found.")
        print("\nPlease run setup_app.py to recreate the virtual environment.")
        print("\nPress any key to exit...")
        
        try:
            if sys.platform == "win32":
                import msvcrt
                msvcrt.getch()
            else:
                import termios
                import tty
                fd = sys.stdin.fileno()
                old_settings = termios.tcgetattr(fd)
                try:
                    tty.setraw(sys.stdin.fileno())
                    sys.stdin.read(1)
                finally:
                    termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        except (ImportError, Exception):
            input()
        
        sys.exit(1)
    
    # Activate virtual environment
    print(f"\nFound virtual environment at: {venv_path}")
    print("Activating virtual environment...")
    
    if activate_virtual_environment(venv_path):
        print("✓ Virtual environment activated")
    else:
        print("⚠ Warning: Could not activate virtual environment (will use venv Python directly)")
    
    # Launch Streamlit
    try:
        # Find available port
        available_port = find_available_port(start_port=default_port)
        
        # Inform user about port selection
        if available_port != default_port:
            print(f"\nPort {default_port} is in use, using port {available_port} instead")
        else:
            print(f"\nStarting application on port {available_port}")
        
        # Launch Streamlit using venv Python
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
        
        # Run Streamlit
        subprocess.run(streamlit_args)
        
    except KeyboardInterrupt:
        print("\nApplication stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"\nError launching application: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
