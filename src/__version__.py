"""
Version information for CSV Wrangler.

This module provides version information for the application.
Version format: MAJOR.MINOR.PATCH
- MAJOR: Breaking changes or major feature additions
- MINOR: New features, backward compatible
- PATCH: Bug fixes, backward compatible
"""
__version__ = "1.0.4"
__version_info__ = (1, 0, 4)

# Version history
VERSION_HISTORY = {
    "1.0.0": {
        "date": "2025-01-27",
        "status": "final",
        "description": "Base level application - CSV Wrangler v1 Final",
        "features": [
            "Multi-dataset management (5 slots)",
            "CSV and Pickle file support",
            "Dataset initialization with column configuration",
            "Data upload and viewing",
            "Export with date range filtering",
            "User profile management",
            "Settings and dataset management",
            "Comprehensive UI testing suite",
        ],
    },
    "1.0.1": {
        "date": "2025-01-28",
        "status": "final",
        "description": "Database Implementation Improvements - Critical fixes and enhancements",
        "features": [
            "Fixed Migration 6 database connection bug - proper transaction context",
            "Created EnrichedDatasetRepository following repository pattern",
            "Added missing database indexes for EnrichedDataset.source_dataset_id",
            "Added defensive None checks for JSON fields (columns_config, image_columns, etc.)",
            "Fixed SQL injection risks - all raw SQL now uses quote_identifier()",
            "Added operation-specific validation for operation_config",
            "Improved error handling in migrations with detailed logging",
            "Removed unnecessary pool_pre_ping for SQLite connections",
            "Enhanced transaction isolation documentation",
        ],
    },
    "1.0.2": {
        "date": "2025-11-04",
        "status": "final",
        "description": "Knowledge Base Fixes and Version History Display - Critical bug fixes and UX improvements",
        "features": [
            "Fixed Knowledge Base delete functionality - added explicit session commit",
            "Fixed Knowledge Search not showing enriched dataset results - improved column matching and type checking",
            "Fixed Knowledge Table upload duplicate Key_ID handling - deduplication within upload DataFrame",
            "Added comprehensive debug logging for Knowledge Search troubleshooting",
            "Added Version History section to Home page with expandable version details",
            "Improved error handling with SafeOperation context manager for Knowledge Base operations",
            "Enhanced duplicate detection to handle intra-file duplicates vs database duplicates separately",
        ],
    },
    "1.0.3": {
        "date": "2025-11-04",
        "status": "final",
        "description": "Version History Update - Documentation and versioning improvements",
        "features": [
            "Version history properly displayed on Home page",
            "All recent fixes documented in version history",
        ],
    },
    "1.0.4": {
        "date": "2025-01-27",
        "status": "final",
        "description": "Directory Dave Feature - Directory tree visualization and codebase cleanup",
        "features": [
            "Added Directory Dave page (16_directory_dave.py) for directory tree exploration",
            "ASCII tree visualization with Unicode box-drawing characters",
            "Configurable depth and item limits for performance",
            "Symlink cycle detection to prevent infinite loops",
            "Tree statistics display (total items, depth, generation time)",
            "Option to include files in tree visualization",
            "Download functionality for large directory trees",
            "Codebase cleanup: removed unused imports and redundant error handling",
            "Improved error handling consistency using SafeOperation pattern",
        ],
    },
}

def get_version():
    """Get current version string."""
    return __version__

def get_version_info():
    """Get version as tuple."""
    return __version_info__

def get_version_history():
    """Get version history."""
    return VERSION_HISTORY

