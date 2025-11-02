"""
File utilities for CSV Wrangler.

Handles file operations like copying originals, creating directories, etc.
"""
import shutil
from pathlib import Path
from typing import Optional

from src.config.settings import ORIGINALS_DIR
from src.utils.errors import FileNotFoundError, FileProcessingError
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def ensure_originals_directory() -> Path:
    """
    Ensure originals directory exists.
    
    Returns:
        Path to originals directory
    """
    ORIGINALS_DIR.mkdir(parents=True, exist_ok=True)
    return ORIGINALS_DIR


def copy_to_originals(
    source_file: Path,
    dataset_name: Optional[str] = None,
) -> Path:
    """
    Copy uploaded file to originals directory for record keeping.
    
    Generates unique filename if duplicate exists.
    
    Args:
        source_file: Source file to copy
        dataset_name: Optional dataset name for organization
        
    Returns:
        Path to copied file in originals directory
        
    Raises:
        FileNotFoundError: If source file doesn't exist
        FileProcessingError: If copy fails
    """
    if not source_file.exists():
        raise FileNotFoundError(str(source_file))

    ensure_originals_directory()

    # Generate destination filename
    # Include dataset name in subdirectory if provided
    if dataset_name:
        dataset_dir = ORIGINALS_DIR / dataset_name.lower().replace(" ", "_")
        dataset_dir.mkdir(parents=True, exist_ok=True)
        dest_dir = dataset_dir
    else:
        dest_dir = ORIGINALS_DIR

    # Generate unique filename
    base_name = source_file.name
    dest_file = dest_dir / base_name

    # If file exists, add counter
    counter = 1
    while dest_file.exists():
        stem = source_file.stem
        suffix = source_file.suffix
        dest_file = dest_dir / f"{stem}_{counter}{suffix}"
        counter += 1

    try:
        shutil.copy2(source_file, dest_file)
        logger.info(f"Copied {source_file.name} to originals: {dest_file}")

        return dest_file

    except Exception as e:
        logger.error(f"Failed to copy file to originals: {e}", exc_info=True)
        raise FileProcessingError(
            f"Failed to copy file to originals: {str(e)}", filename=source_file.name
        ) from e


def cleanup_dataset_originals(dataset_name: str) -> None:
    """
    Clean up originals directory for a deleted dataset.
    
    Args:
        dataset_name: Dataset name to clean up
    """
    dataset_dir = ORIGINALS_DIR / dataset_name.lower().replace(" ", "_")

    if dataset_dir.exists():
        try:
            shutil.rmtree(dataset_dir)
            logger.info(f"Cleaned up originals for dataset: {dataset_name}")
        except Exception as e:
            logger.warning(f"Failed to cleanup originals: {e}", exc_info=True)

