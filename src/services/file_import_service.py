"""
Unified file import service for CSV Wrangler.

Provides a single interface for importing both CSV and Pickle files.
"""
from pathlib import Path
from typing import Literal

import pandas as pd

from src.config.settings import (
    SUPPORTED_CSV_EXTENSIONS,
    SUPPORTED_PICKLE_EXTENSIONS,
)
from src.services.csv_service import parse_csv_file
from src.services.pickle_service import parse_pickle_file
from src.utils.errors import FileProcessingError, ValidationError
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

FileType = Literal["CSV", "PICKLE"]


def detect_file_type(file_path: Path) -> FileType:
    """
    Detect file type based on extension.
    
    Args:
        file_path: Path to file
        
    Returns:
        File type ("CSV" or "PICKLE")
        
    Raises:
        ValidationError: If file type is not supported
    """
    extension = file_path.suffix.lower()

    if extension in SUPPORTED_CSV_EXTENSIONS:
        return "CSV"
    elif extension in SUPPORTED_PICKLE_EXTENSIONS:
        return "PICKLE"
    else:
        raise ValidationError(
            f"Unsupported file type: {extension}. "
            f"Supported: {SUPPORTED_CSV_EXTENSIONS + SUPPORTED_PICKLE_EXTENSIONS}",
            field="file_type",
            value=extension,
        )


def import_file(file_path: Path, show_progress: bool = True) -> tuple[pd.DataFrame, FileType]:
    """
    Import file (CSV or Pickle) and return DataFrame.
    
    Unified interface for importing both file types.
    
    Args:
        file_path: Path to file
        
    Returns:
        Tuple of (DataFrame, file_type)
        
    Raises:
        ValidationError: If file type is not supported
        FileProcessingError: If file cannot be parsed
    """
    file_type = detect_file_type(file_path)

    try:
        if file_type == "CSV":
            df = parse_csv_file(file_path, show_progress=show_progress)
        elif file_type == "PICKLE":
            df = parse_pickle_file(file_path)
        else:
            raise ValidationError(f"Unsupported file type: {file_type}")

        logger.info(f"Imported {file_type} file: {file_path.name} ({len(df)} rows)")

        return df, file_type

    except (ValidationError, FileProcessingError):
        raise
    except Exception as e:
        logger.error(f"Failed to import file: {e}", exc_info=True)
        raise FileProcessingError(
            f"Failed to import file: {str(e)}", filename=file_path.name
        ) from e

