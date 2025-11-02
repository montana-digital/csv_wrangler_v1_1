# Versioning Guide - Quick Reference

## Current Version: 1.0.0 (Base Level Application)

## Version Files

- `VERSION` - Simple version string file
- `src/__version__.py` - Python module with version info
- `docs/VERSIONING.md` - Full versioning documentation
- `CHANGELOG.md` - Version history and changes

## Version Format

**MAJOR.MINOR.PATCH** (e.g., `1.0.0`)

- **PATCH** (1.0.0 → 1.0.1): Bug fixes, minor improvements
- **MINOR** (1.0.0 → 1.1.0): New features, backward compatible
- **MAJOR** (1.0.0 → 2.0.0): Breaking changes, major updates

## Quick Upgrade Steps

1. **Update version files**:
   ```bash
   # Update VERSION
   echo "1.1.0" > VERSION
   
   # Update src/__version__.py
   # - Change __version__ = "1.1.0"
   # - Change __version_info__ = (1, 1, 0)
   # - Add entry to VERSION_HISTORY
   ```

2. **Update documentation**:
   - Add entry to `docs/VERSIONING.md`
   - Update `CHANGELOG.md`
   - Update `README.md` if needed

3. **Commit and tag**:
   ```bash
   git add -A
   git commit -m "Release v1.1.0: [Description]"
   git tag -a v1.1.0 -m "Version 1.1.0: [Description]"
   git push origin main --tags
   ```

## Base Version (v1.0.0)

This is the **foundation** - all future changes are upgrades from this base.

See `docs/VERSIONING.md` for complete documentation.

