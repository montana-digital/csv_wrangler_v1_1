"""
Pickle file parsing and validation service for CSV Wrangler.

Handles Pickle (.pkl) file parsing, conversion to DataFrame, and validation.
"""
import pickle
from pathlib import Path
from typing import Any

import pandas as pd

from src.services.csv_service import detect_base64_image_columns
from src.utils.errors import FileProcessingError, ValidationError
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def parse_pickle_file(file_path: Path) -> pd.DataFrame:
    """
    Parse Pickle file and convert to DataFrame.
    
    Supports:
    - pandas DataFrame objects
    - Python dictionaries
    - Lists of dictionaries
    
    Args:
        file_path: Path to Pickle file
        
    Returns:
        Parsed DataFrame
        
    Raises:
        FileProcessingError: If file cannot be parsed or is unsupported type
    """
    if not file_path.exists():
        raise FileProcessingError(f"File not found: {file_path}", filename=file_path.name)

    if file_path.stat().st_size == 0:
        raise FileProcessingError("Pickle file is empty", filename=file_path.name)

    try:
        with open(file_path, "rb") as f:
            data = pickle.load(f)

        # Convert to DataFrame
        df = convert_pickle_to_dataframe(data)

        # Validate DataFrame
        validate_pickle_dataframe(df)

        logger.info(f"Successfully parsed Pickle file: {file_path.name}")
        return df

    except FileProcessingError:
        raise
    except (pickle.UnpicklingError, EOFError) as e:
        logger.error(f"Failed to unpickle file: {e}", exc_info=True)
        raise FileProcessingError(
            f"Invalid or corrupted Pickle file: {str(e)}", filename=file_path.name
        ) from e
    except Exception as e:
        logger.error(f"Unexpected error parsing Pickle file: {e}", exc_info=True)
        raise FileProcessingError(
            f"Failed to parse Pickle file: {str(e)}", filename=file_path.name
        ) from e


def convert_pickle_to_dataframe(data: Any) -> pd.DataFrame:
    """
    Convert various data types to pandas DataFrame.
    
    Args:
        data: Data to convert (DataFrame, dict, list of dicts)
        
    Returns:
        DataFrame
        
    Raises:
        FileProcessingError: If data type is not supported
    """
    if isinstance(data, pd.DataFrame):
        return data.copy()

    elif isinstance(data, dict):
        # Single dictionary -> DataFrame with one row
        return pd.DataFrame([data])

    elif isinstance(data, list):
        if len(data) == 0:
            raise FileProcessingError("Pickle data is empty list")

        # Check if list contains dictionaries
        if all(isinstance(item, dict) for item in data):
            return pd.DataFrame(data)
        else:
            raise FileProcessingError(
                "Pickle list must contain dictionaries. Found unsupported types."
            )

    else:
        raise FileProcessingError(
            f"Unsupported Pickle data type: {type(data).__name__}. "
            "Supported types: DataFrame, dict, list of dicts"
        )


def validate_pickle_dataframe(df: pd.DataFrame) -> None:
    """
    Validate that parsed data is a valid DataFrame.
    
    Args:
        df: DataFrame to validate
        
    Raises:
        ValidationError: If DataFrame is invalid
    """
    if not isinstance(df, pd.DataFrame):
        raise ValidationError(
            f"Expected DataFrame, got {type(df).__name__}",
            field="dataframe",
        )

    if df.empty:
        raise ValidationError("DataFrame is empty", field="dataframe")


def detect_base64_in_pickle_dataframe(df: pd.DataFrame) -> list[str]:
    """
    Detect Base64 image columns in pickle DataFrame.
    
    Uses the same detection logic as CSV service.
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        List of column names containing Base64 image data
    """
    return detect_base64_image_columns(df)

