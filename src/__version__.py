"""
Version information for CSV Wrangler.

This module provides version information for the application.
Version format: MAJOR.MINOR.PATCH
- MAJOR: Breaking changes or major feature additions
- MINOR: New features, backward compatible
- PATCH: Bug fixes, backward compatible
"""
__version__ = "1.0.0"
__version_info__ = (1, 0, 0)

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
    "1.1.0": {
        "date": "2025-01-27",
        "status": "final",
        "description": "Enrichment Suite and DataFrame View - Data enrichment and advanced exploration",
        "features": [
            "Enrichment Suite page with 5 enrichment functions",
            "Phone number validation and formatting",
            "Web domain/URL validation and extraction",
            "Email validation",
            "Date-only field parsing",
            "DateTime field parsing",
            "Enriched dataset tracking and auto-sync",
            "DataFrame View page for advanced data exploration",
            "Advanced filtering and search capabilities",
            "Image column handling (hide by default)",
            "Large dataset support with pagination",
        ],
    },
    "1.2.0": {
        "date": "2025-01-27",
        "status": "final",
        "description": "Uniform Sidebar, Notes Feature, and Column Renaming - Enhanced UX and usability",
        "features": [
            "Uniform sidebar across all pages with username and version",
            "Notes/notepad feature in sidebar (persistent database storage)",
            "Username editing in Settings page",
            "Renamed unique_id column to uuid_value throughout application",
            "Database migration system for schema updates",
            "Logo display on main page",
            "Improved note deletion with confirmation",
            "Backward compatibility fixes for legacy datasets",
        ],
    },
    "1.3.0": {
        "date": "2025-11-01",
        "status": "final",
        "description": "Progress Indicators and Enhanced DataFrame View - Improved UX and data exploration",
        "features": [
            "Progress bars for CSV uploads and processing",
            "Large file support with chunked reading and progress indicators",
            "Step-by-step upload progress (parsing, generating IDs, inserting)",
            "Enriched datasets now available in DataFrame View selector",
            "Date range filtering in DataFrame View with automatic date column detection",
            "Multi-date column support with column selector",
            "Enhanced filtering capabilities combining date and advanced filters",
            "Improved user experience throughout the application",
        ],
    },
    "1.4.0": {
        "date": "2025-01-28",
        "status": "final",
        "description": "Knowledge Base Feature - Multi-table data linking and standardization",
        "features": [
            "Knowledge Base page for managing Knowledge Tables",
            "Multiple Knowledge Tables per data type (phone_numbers, emails, web_domains)",
            "Standardized Key_ID generation using enrichment functions",
            "Automatic linking between Knowledge Tables and enriched datasets",
            "1-to-many relationship model (Knowledge Table → multiple datasets)",
            "Per-table statistics (Top 20, Recently Added, Missing Values)",
            "Data upload and management for Knowledge Tables",
            "Comprehensive test suite (unit, integration, E2E)",
            "Multi-table architecture supporting white lists, black lists, and data sources",
        ],
    },
    "1.5.0": {
        "date": "2025-01-28",
        "status": "final",
        "description": "Knowledge Base Search - Fast indexed search across Knowledge Tables and enriched datasets",
        "features": [
            "Knowledge Base Search page with fast presence search",
            "Two-phase search approach (presence flags first, detailed retrieval on-demand)",
            "Indexed search queries for optimal performance (<100ms for 10 sources)",
            "Search across Knowledge Tables and enriched datasets simultaneously",
            "Protocol normalization for domain search (http:// and https:// treated same)",
            "Path normalization for domains (example.com/path → example.com)",
            "Drill-down data viewing with expandable sections",
            "Session state caching for search results and data",
            "Comprehensive test suite covering unit, integration, E2E, and performance tests",
            "Automatic indexing of enriched columns for fast search",
            "Migration script to normalize existing enriched domain data",
        ],
    },
    "1.6.0": {
        "date": "2025-11-02",
        "status": "final",
        "description": "Image Search Enhancements - Pinning and Selection Improvements",
        "features": [
            "Detail viewer pinning feature - pin multiple rows for persistent viewing",
            "Multiple selection warning instead of error",
            "Close button now properly deselects table row",
            "Collapsible detail viewers with expand/collapse",
            "Pinned details persist when selecting other rows",
            "Pins automatically cleared when switching tables",
            "Comprehensive test suite for pinning and selection features (16 tests)",
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

