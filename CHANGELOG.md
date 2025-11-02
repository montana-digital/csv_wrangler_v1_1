# Changelog

All notable changes to CSV Wrangler will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2025-01-27

### Added - Base Level Application
- Multi-dataset management system supporting 5 independent dataset slots
- CSV and Pickle file format support for import/export
- Dataset initialization with comprehensive column configuration
  - Column data type configuration (TEXT, INTEGER, REAL)
  - Automatic Base64 image column detection
  - Duplicate filtering configuration
- Data upload functionality with duplicate detection
- Data viewer with pagination, search, and unique record filtering
- Export functionality with CSV/Pickle formats and date range filtering
- User profile management system
- Settings page with dataset details, statistics, and deletion
- Comprehensive automated UI testing suite
  - 26+ Playwright-based E2E tests
  - Coverage of all major UI flows and user journeys
- Dark mode UI theme by default
- Multi-page navigation with Streamlit
- SQLite database backend with SQLAlchemy ORM
- Structured logging with correlation IDs
- Error handling with operational vs programmer error classification

### Technical Details
- Python 3.12+ support
- Streamlit multi-page application architecture
- SQLite database with separate tables per dataset
- Pandas for data manipulation
- Playwright for browser automation testing
- pytest test framework with comprehensive coverage

## [1.1.0] - 2025-01-27

### Added - Enrichment Suite
- New Enrichment Suite page for data enrichment
- Five enrichment functions:
  - Phone number validation and formatting
  - Web domain/URL validation and extraction
  - Email address validation
  - Date-only field parsing (various formats)
  - DateTime field parsing (various formats)
- Enriched dataset creation with automatic table copying
- Enriched dataset tracking with sync status
- Auto-sync mechanism when source datasets are updated
- Manual sync option for enriched datasets
- Enriched dataset deletion with table cleanup

### Added - DataFrame View
- New DataFrame View page for advanced data exploration
- Dataset selection and loading (default 10,000 rows, most recent first)
- Image column handling (hidden by default, can be enabled)
- Advanced filtering:
  - Global search across all columns
  - Column-specific filters (numeric and text)
  - Multiple filter combinations
- Dataset statistics display
- Download filtered data as CSV
- Efficient pagination for large datasets

### Technical
- New `EnrichedDataset` database model
- Table copy and management utilities
- Enrichment functions service with best-effort parsing
- DataFrame loading service with image column exclusion
- Integration with existing upload process for auto-sync

## [Unreleased]

### Planned Features
- Features and improvements will be documented here as they are developed

---

[1.1.0]: https://github.com/your-repo/csv_wrangler_v1/releases/tag/v1.1.0
[1.0.0]: https://github.com/your-repo/csv_wrangler_v1/releases/tag/v1.0.0

