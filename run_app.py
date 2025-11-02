"""
Entry point for running CSV Wrangler Streamlit app.

This script ensures proper Python path setup before launching Streamlit.
Automatically finds an available port if the default port is in use.
"""
import socket
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


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
    """Main entry point that finds available port and launches Streamlit."""
    default_port = 8501
    
    try:
        # Find available port
        available_port = find_available_port(start_port=default_port)
        
        # Inform user about port selection
        if available_port != default_port:
            print(f"Port {default_port} is in use, using port {available_port} instead")
        else:
            print(f"Starting application on port {available_port}")
        
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

