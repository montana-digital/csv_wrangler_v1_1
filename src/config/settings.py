"""
Application configuration settings for CSV Wrangler.

Centralized configuration for paths, limits, and constants used throughout the application.
"""
import os
from pathlib import Path
from typing import Final

# Application paths
APP_ROOT: Final[Path] = Path(__file__).parent.parent.parent
USERDATA_DIR: Final[Path] = APP_ROOT / "userdata"
ORIGINALS_DIR: Final[Path] = USERDATA_DIR / "originals"
LOGO_DIR: Final[Path] = USERDATA_DIR / "logos"
ANALYSIS_RESULTS_DIR: Final[Path] = USERDATA_DIR / "analysis_results"
DATABASE_PATH: Final[Path] = USERDATA_DIR / "database.db"

# Dataset configuration
MAX_DATASET_SLOTS: Final[int] = 5
DEFAULT_PAGE_SIZE: Final[int] = 10000
MAX_PAGE_SIZE: Final[int] = 100000

# File upload limits
MAX_FILE_SIZE_MB: Final[int] = 500  # Maximum file size in MB
CHUNK_SIZE: Final[int] = 10000  # Rows per chunk for large file processing

# Database configuration
SQLITE_CHECK_SAME_THREAD: Final[bool] = False
SQLITE_TIMEOUT: Final[float] = 20.0  # seconds

# UUID Value configuration
UNIQUE_ID_COLUMN_NAME: Final[str] = "uuid_value"

# Export configuration
EXPORT_DATE_FORMAT: Final[str] = "%Y-%m-%d"
EXPORT_FILENAME_FORMAT: Final[str] = "export_{dataset_name}_{timestamp}.csv"

# Logging configuration
LOG_FORMAT: Final[str] = "json"
LOG_LEVEL: Final[str] = "INFO"

# Base64 image detection
BASE64_MIN_LENGTH: Final[int] = 100  # Minimum length to consider as Base64
BASE64_PATTERN_PREFIX: Final[str] = "data:image"

# Supported file types
SUPPORTED_CSV_EXTENSIONS: Final[list[str]] = [".csv"]
SUPPORTED_PICKLE_EXTENSIONS: Final[list[str]] = [".pkl", ".pickle"]
SUPPORTED_FILE_EXTENSIONS: Final[list[str]] = SUPPORTED_CSV_EXTENSIONS + SUPPORTED_PICKLE_EXTENSIONS

# CSV encoding detection
CSV_ENCODINGS: Final[list[str]] = ["utf-8", "utf-8-sig", "latin-1", "cp1252", "iso-8859-1"]


def ensure_userdata_directories() -> None:
    """Create userdata directories if they don't exist."""
    USERDATA_DIR.mkdir(parents=True, exist_ok=True)
    ORIGINALS_DIR.mkdir(parents=True, exist_ok=True)
    LOGO_DIR.mkdir(parents=True, exist_ok=True)
    ANALYSIS_RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def get_database_url() -> str:
    """Get SQLite database URL for SQLAlchemy."""
    ensure_userdata_directories()
    return f"sqlite:///{DATABASE_PATH.absolute()}"

