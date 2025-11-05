"""
Directory tree utilities for CSV Wrangler.

Provides functions for generating ASCII directory tree visualizations.
"""
import os
import time
from pathlib import Path

from src.utils.errors import DirectoryTreeError, FileNotFoundError
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def validate_directory_path(path: Path, safe_mode: bool = False) -> Path:
    """
    Validate directory path and optionally restrict to safe locations.
    
    Args:
        path: Path to validate
        safe_mode: If True, restrict to user's home directory
        
    Returns:
        Validated Path object
        
    Raises:
        DirectoryTreeError: If path is invalid
        FileNotFoundError: If path doesn't exist
    """
    try:
        # Convert to Path if string
        if isinstance(path, str):
            path = Path(path)
        
        # Resolve to absolute path
        path = path.resolve()
        
        # Check if exists
        if not path.exists():
            raise FileNotFoundError(str(path))
        
        # Check if it's a directory
        if not path.is_dir():
            raise DirectoryTreeError(
                f"Path is not a directory: {path}",
                path=str(path)
            )
        
        # Safe mode: restrict to user's home directory
        if safe_mode:
            home_dir = Path.home()
            try:
                # Check if path is within home directory
                path.relative_to(home_dir)
            except ValueError:
                raise DirectoryTreeError(
                    f"Path is outside safe directory (home: {home_dir}): {path}",
                    path=str(path)
                )
        
        return path
        
    except FileNotFoundError:
        raise
    except DirectoryTreeError:
        raise
    except Exception as e:
        raise DirectoryTreeError(
            f"Failed to validate directory path: {e}",
            path=str(path)
        ) from e


def generate_directory_tree(
    root: Path,
    include_files: bool = False,
    max_depth: int = 10,
    max_items: int = 1000,
) -> tuple[str, dict]:
    """
    Generate ASCII directory tree visualization.
    
    Args:
        root: Root directory path
        include_files: Whether to include files in the tree
        max_depth: Maximum depth to traverse (default: 10)
        max_items: Maximum items per directory before truncation (default: 1000)
        
    Returns:
        Tuple of (tree_string, statistics_dict)
        
    Raises:
        DirectoryTreeError: If tree generation fails
    """
    start_time = time.time()
    
    # Validate root path
    root = validate_directory_path(root)
    
    # Statistics tracking
    stats = {
        "total_items": 0,
        "total_dirs": 0,
        "total_files": 0,
        "max_depth_reached": 0,
        "items_truncated": 0,
        "directories_truncated": 0,
        "permission_errors": 0,
    }
    
    # Track visited paths to prevent symlink cycles
    visited_paths: set[Path] = set()
    
    def get_real_path(path: Path) -> Path:
        """Get real path resolving symlinks."""
        try:
            return Path(os.path.realpath(str(path)))
        except Exception:
            return path.resolve()
    
    def build_tree_recursive(
        path: Path,
        prefix: str = "",
        is_last: bool = True,
        depth: int = 0,
        parent_is_last_stack: list[bool] = None,
    ) -> str:
        """
        Recursively build tree string.
        
        Args:
            path: Current directory path
            prefix: Prefix string for current line
            is_last: Whether this is the last item in parent directory
            depth: Current depth level
            parent_is_last_stack: Stack of parent "is_last" flags for proper indentation
            
        Returns:
            Tree string for this subtree
        """
        if parent_is_last_stack is None:
            parent_is_last_stack = []
        
        # Check depth limit
        if depth > max_depth:
            stats["max_depth_reached"] = max(depth, stats["max_depth_reached"])
            return ""
        
        # Get real path to detect symlink cycles
        real_path = get_real_path(path)
        
        # Check for cycles
        if real_path in visited_paths:
            return f"{prefix}{'└── ' if is_last else '├── '}{path.name} [SYMLINK CYCLE]\n"
        
        # Mark as visited before processing
        visited_paths.add(real_path)
        
        # Build current line
        current_line = f"{prefix}{'└── ' if is_last else '├── '}{path.name}/\n"
        result = current_line
        stats["total_dirs"] += 1
        stats["total_items"] += 1
        
        # Update depth tracking
        stats["max_depth_reached"] = max(depth + 1, stats["max_depth_reached"])
        
        # Prepare for children
        try:
            # Get all items in directory
            items = []
            dirs = []
            files = []
            
            for item in path.iterdir():
                try:
                    if item.is_dir():
                        dirs.append(item)
                    elif include_files and item.is_file():
                        files.append(item)
                except (PermissionError, OSError) as e:
                    stats["permission_errors"] += 1
                    logger.warning(f"Permission denied accessing {item}: {e}")
                    continue
            
            # Sort directories and files
            dirs.sort(key=lambda p: p.name.lower())
            files.sort(key=lambda p: p.name.lower())
            
            # Combine: directories first, then files
            if include_files:
                items = dirs + files
            else:
                items = dirs
            
            # Apply max_items limit
            items_truncated = 0
            if len(items) > max_items:
                items_truncated = len(items) - max_items
                items = items[:max_items]
                stats["items_truncated"] += items_truncated
                stats["directories_truncated"] += 1
            
            # Process children
            for i, item in enumerate(items):
                is_item_last = (i == len(items) - 1) and items_truncated == 0
                
                # Determine prefix for child
                if is_last:
                    child_prefix = prefix + "    "
                else:
                    child_prefix = prefix + "│   "
                
                # Add to parent stack
                new_parent_stack = parent_is_last_stack + [is_last]
                
                if item.is_dir():
                    # Recursively process directory
                    child_result = build_tree_recursive(
                        item,
                        prefix=child_prefix,
                        is_last=is_item_last,
                        depth=depth + 1,
                        parent_is_last_stack=new_parent_stack,
                    )
                    result += child_result
                elif include_files and item.is_file():
                    # Add file line
                    file_prefix = f"{child_prefix}{'└── ' if is_item_last else '├── '}"
                    result += f"{file_prefix}{item.name}\n"
                    stats["total_files"] += 1
                    stats["total_items"] += 1
            
            # Add truncation message if needed
            if items_truncated > 0:
                truncation_prefix = prefix + ("    " if is_last else "│   ")
                result += f"{truncation_prefix}└── ... and {items_truncated} more items\n"
        
        except PermissionError as e:
            stats["permission_errors"] += 1
            logger.warning(f"Permission denied accessing {path}: {e}")
            result += f"{prefix}{'    ' if is_last else '│   '}└── [Permission Denied]\n"
        
        except Exception as e:
            logger.warning(f"Error processing {path}: {e}")
            result += f"{prefix}{'    ' if is_last else '│   '}└── [Error: {str(e)[:50]}]\n"
        
        finally:
            # Remove from visited set when done with subtree
            visited_paths.discard(real_path)
        
        return result
    
    try:
        # Start tree with root directory name
        root_name = root.name if root.name else str(root)
        tree = f"{root_name}/\n"
        
        # Build the tree recursively
        tree += build_tree_recursive(root, prefix="", is_last=True, depth=0)
        
        # Calculate generation time
        generation_time = time.time() - start_time
        
        # Finalize statistics
        stats["generation_time"] = generation_time
        stats["root_path"] = str(root)
        
        return tree, stats
        
    except Exception as e:
        raise DirectoryTreeError(
            f"Failed to generate directory tree: {e}",
            path=str(root)
        ) from e

