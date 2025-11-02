"""
Diagnostics system for CSV Wrangler.

Provides system information, package versions, and environment diagnostics
to help with troubleshooting and support.
"""
import platform
import sys
from pathlib import Path
from typing import Any, Optional

from src.utils.package_check import (
    get_package_status_report,
    get_missing_optional_packages,
    get_package_version,
)
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def get_system_info() -> dict[str, Any]:
    """
    Get system information.

    Returns:
        Dictionary with system information
    """
    return {
        "platform": platform.platform(),
        "system": platform.system(),
        "release": platform.release(),
        "version": platform.version(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "python_version": sys.version,
        "python_executable": sys.executable,
        "architecture": platform.architecture()[0],
    }


def get_package_versions() -> dict[str, Optional[str]]:
    """
    Get versions of installed packages.

    Returns:
        Dictionary mapping package names to versions
    """
    packages = [
        "streamlit",
        "pandas",
        "sqlalchemy",
        "plotly",
        "pyarrow",
        "dateutil",
    ]

    versions = {}
    for pkg in packages:
        versions[pkg] = get_package_version(pkg)

    return versions


def get_environment_info() -> dict[str, Any]:
    """
    Get environment information.

    Returns:
        Dictionary with environment information
    """
    import os

    # Get application directory
    app_dir = Path(__file__).parent.parent.parent
    userdata_dir = app_dir / "userdata"

    return {
        "app_directory": str(app_dir),
        "userdata_directory": str(userdata_dir),
        "userdata_exists": userdata_dir.exists(),
        "userdata_writable": os.access(userdata_dir, os.W_OK) if userdata_dir.exists() else False,
        "working_directory": os.getcwd(),
        "environment_variables": {
            k: v for k, v in os.environ.items() if "CSV" in k.upper() or "WRANGLER" in k.upper()
        },
    }


def get_full_diagnostics() -> dict[str, Any]:
    """
    Get comprehensive diagnostics report.

    Returns:
        Dictionary with complete diagnostics information
    """
    return {
        "system": get_system_info(),
        "packages": get_package_status_report(),
        "environment": get_environment_info(),
        "missing_optional": get_missing_optional_packages(),
    }


def format_diagnostics_for_display(diagnostics: Optional[dict[str, Any]] = None) -> str:
    """
    Format diagnostics for user-friendly display.

    Args:
        diagnostics: Optional diagnostics dictionary (uses get_full_diagnostics if None)

    Returns:
        Formatted string for display
    """
    if diagnostics is None:
        diagnostics = get_full_diagnostics()

    lines = ["# CSV Wrangler Diagnostics Report\n"]

    # System Info
    lines.append("## System Information")
    sys_info = diagnostics["system"]
    lines.append(f"- Platform: {sys_info['platform']}")
    lines.append(f"- Python Version: {sys_info['python_version'].split()[0]}")
    lines.append(f"- Architecture: {sys_info['architecture']}")
    lines.append("")

    # Package Status
    lines.append("## Package Status")
    package_status = diagnostics["packages"]
    for pkg, status in package_status.items():
        available = "✅" if status["available"] else "❌"
        category = status["category"]
        version = status.get("version", "N/A")
        lines.append(f"- {available} {pkg} ({category}): {version}")
    lines.append("")

    # Missing Optional Packages
    missing = diagnostics["missing_optional"]
    if missing:
        lines.append("## Missing Optional Packages")
        for pkg in missing:
            lines.append(f"- {pkg} (install with: pip install {pkg})")
        lines.append("")

    # Environment
    lines.append("## Environment")
    env_info = diagnostics["environment"]
    lines.append(f"- App Directory: {env_info['app_directory']}")
    lines.append(f"- Userdata Directory: {env_info['userdata_directory']}")
    lines.append(f"- Userdata Writable: {'✅' if env_info['userdata_writable'] else '❌'}")
    lines.append("")

    return "\n".join(lines)


def export_diagnostics(file_path: Optional[Path] = None) -> Path:
    """
    Export diagnostics to a file.

    Args:
        file_path: Optional file path (defaults to diagnostics.txt in app directory)

    Returns:
        Path to exported file
    """
    if file_path is None:
        app_dir = Path(__file__).parent.parent.parent
        file_path = app_dir / "diagnostics.txt"

    diagnostics = get_full_diagnostics()
    formatted = format_diagnostics_for_display(diagnostics)

    # Also include raw JSON for programmatic access
    import json

    content = f"{formatted}\n\n## Raw JSON Data\n\n```json\n{json.dumps(diagnostics, indent=2)}\n```"

    file_path.write_text(content, encoding="utf-8")
    logger.info(f"Diagnostics exported to {file_path}")

    return file_path

