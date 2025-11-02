# Versioning Strategy

CSV Wrangler follows [Semantic Versioning](https://semver.org/) (SemVer) principles.

## Version Format

**MAJOR.MINOR.PATCH** (e.g., `1.0.0`)

- **MAJOR**: Breaking changes, major feature additions, or architectural changes
- **MINOR**: New features, enhancements, backward compatible additions
- **PATCH**: Bug fixes, minor improvements, backward compatible changes

## Current Version

**v1.0.0** - Base Level Application (Final)

This is the base level application serving as the foundation for all future development.

## Version History

### v1.0.0 (2025-01-27) - Base Level Application
**Status**: Final  
**Tag**: `v1.0.0`

#### Features
- ✅ Multi-dataset management (5 independent dataset slots)
- ✅ CSV and Pickle file format support
- ✅ Dataset initialization with column configuration
  - Column data type configuration (TEXT, INTEGER, REAL)
  - Base64 image column detection
  - Duplicate filtering configuration
- ✅ Data upload and viewing
  - CSV/Pickle file upload
  - Data viewer with pagination and search
  - Unique record filtering
- ✅ Export functionality
  - CSV and Pickle export formats
  - Date range filtering
- ✅ User profile management
- ✅ Settings and dataset management
  - Dataset details and statistics
  - Dataset deletion
- ✅ Comprehensive automated UI testing suite
  - Playwright-based E2E tests
  - 26+ test scenarios
  - Coverage of all major UI flows

#### Technical Stack
- Python 3.12+
- Streamlit (multi-page application)
- SQLite database with SQLAlchemy ORM
- Pandas for data manipulation
- Playwright for UI testing
- pytest for test framework

## Version Upgrade Process

### When to Upgrade Version

#### PATCH (1.0.0 → 1.0.1)
- Bug fixes
- Minor UI improvements
- Performance optimizations
- Documentation updates
- Test improvements

**Example**: Fix file upload bug, improve error messages

#### MINOR (1.0.0 → 1.1.0)
- New features (backward compatible)
- New dataset slots
- New file format support
- New export options
- UI enhancements
- New utility functions

**Example**: Add JSON export, add dataset slot #6, add data visualization

#### MAJOR (1.0.0 → 2.0.0)
- Breaking changes to API
- Database schema changes
- Major architectural changes
- Removed features
- Incompatible changes

**Example**: Change database structure, redesign UI layout, remove dataset slots

### Version Upgrade Checklist

1. **Update Version Files**
   ```bash
   # Update VERSION file
   echo "1.1.0" > VERSION
   
   # Update src/__version__.py
   # - Update __version__
   # - Update __version_info__
   # - Add entry to VERSION_HISTORY
   ```

2. **Update Documentation**
   - Add entry to `docs/VERSIONING.md`
   - Update `README.md` if needed
   - Update `CHANGELOG.md` (if maintained)

3. **Git Tagging**
   ```bash
   git add -A
   git commit -m "Release v1.1.0: [Description]"
   git tag -a v1.1.0 -m "Version 1.1.0: [Description]"
   git push origin main --tags
   ```

4. **Update Application Display**
   - Version is automatically displayed in sidebar (from `src/__version__.py`)
   - Verify version appears correctly in UI

5. **Test**
   - Run full test suite
   - Verify all tests pass
   - Run UI tests
   - Manual testing of new features

## Version Display

The application displays the current version in:
- **Sidebar**: Version shown in footer
- **Code**: `src/__version__.py` contains version information
- **File**: `VERSION` file in project root

## Git Tags

Each version release should be tagged in Git:

```bash
# Create annotated tag
git tag -a v1.0.0 -m "Version 1.0.0: Base Level Application"

# Push tags
git push origin main --tags

# List tags
git tag -l

# View tag details
git show v1.0.0
```

## Version Comparison

To compare versions:

```python
from src import __version__

current = __version__.__version__
# Compare with version_info tuple
if __version__.__version_info__ >= (1, 1, 0):
    # New feature available
    pass
```

## Base Version (v1.0.0)

**v1.0.0 represents the base level application.** All future modifications will be version upgrades from this baseline.

### What Constitutes Base Level (v1.0.0)

- Core functionality working end-to-end
- All major features implemented
- Comprehensive test coverage
- Documentation complete
- Stable and production-ready

### Future Upgrades

All changes after v1.0.0 are considered upgrades:
- **v1.0.x**: Bug fixes and patches
- **v1.x.0**: New features and enhancements
- **v2.0.0**: Major changes or breaking updates

## Best Practices

1. **Always update version** when making changes
2. **Follow SemVer** principles strictly
3. **Document changes** in VERSION_HISTORY
4. **Tag releases** in Git
5. **Test thoroughly** before version bump
6. **Maintain backward compatibility** for MINOR and PATCH updates
7. **Communicate breaking changes** clearly for MAJOR updates

## Questions?

For questions about versioning:
- Check `src/__version__.py` for current version
- Review `docs/VERSIONING.md` for version history
- See Git tags for release history

