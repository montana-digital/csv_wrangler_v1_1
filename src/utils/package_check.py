"""
Package availability detection for CSV Wrangler.

Checks which optional packages are available at runtime and provides fallback functionality.
"""
from typing import Any, Optional
import importlib
import sys


class PackageAvailability:
    """Tracks availability of optional packages."""

    def __init__(self):
        self._cache: dict[str, Optional[bool]] = {}
        self._version_cache: dict[str, Optional[str]] = {}

    def is_available(self, package_name: str) -> bool:
        """
        Check if a package is available.

        Args:
            package_name: Name of the package to check (e.g., "plotly", "dateutil")

        Returns:
            True if package can be imported, False otherwise
        """
        if package_name in self._cache:
            return self._cache[package_name] is True

        try:
            # Try importing the package
            importlib.import_module(package_name)
            self._cache[package_name] = True
            return True
        except (ImportError, ModuleNotFoundError):
            self._cache[package_name] = False
            return False

    def get_version(self, package_name: str) -> Optional[str]:
        """
        Get version of installed package.

        Args:
            package_name: Name of the package

        Returns:
            Version string if available, None otherwise
        """
        if package_name in self._version_cache:
            return self._version_cache[package_name]

        if not self.is_available(package_name):
            self._version_cache[package_name] = None
            return None

        try:
            module = importlib.import_module(package_name)
            version = getattr(module, "__version__", None)
            self._version_cache[package_name] = version
            return version
        except Exception:
            self._version_cache[package_name] = None
            return None

    def get_missing_packages(self) -> list[str]:
        """Get list of commonly expected but missing packages."""
        optional_packages = ["plotly", "pyarrow", "dateutil"]
        return [pkg for pkg in optional_packages if not self.is_available(pkg)]


# Global instance
_package_checker = PackageAvailability()


def is_package_available(package_name: str) -> bool:
    """
    Check if a package is available.

    Convenience function for the global PackageAvailability instance.

    Args:
        package_name: Name of the package to check

    Returns:
        True if available, False otherwise
    """
    return _package_checker.is_available(package_name)


def get_package_version(package_name: str) -> Optional[str]:
    """
    Get version of installed package.

    Args:
        package_name: Name of the package

    Returns:
        Version string if available, None otherwise
    """
    return _package_checker.get_version(package_name)


def get_missing_optional_packages() -> list[str]:
    """
    Get list of missing optional packages.

    Returns:
        List of package names that are commonly expected but not installed
    """
    return _package_checker.get_missing_packages()


# Specific package checks
def has_plotly() -> bool:
    """Check if Plotly is available for visualizations."""
    return is_package_available("plotly")


def has_pyarrow() -> bool:
    """Check if PyArrow is available for fast CSV reading."""
    return is_package_available("pyarrow")


def has_dateutil() -> bool:
    """Check if python-dateutil is available for advanced date parsing."""
    return is_package_available("dateutil")


def get_package_status_report() -> dict[str, dict[str, Any]]:
    """
    Get status report of all packages.

    Returns:
        Dictionary with package status information
    """
    packages = {
        "required": ["streamlit", "pandas", "sqlalchemy"],
        "optional": ["plotly", "pyarrow", "dateutil"],
    }

    report = {}

    for category, package_list in packages.items():
        for pkg in package_list:
            available = is_package_available(pkg)
            version = get_package_version(pkg) if available else None

            report[pkg] = {
                "category": category,
                "available": available,
                "version": version,
                "status": "installed" if available else "missing",
            }

    return report

