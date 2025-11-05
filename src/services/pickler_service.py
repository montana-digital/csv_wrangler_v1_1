"""
Pickler service for CSV Wrangler.

Handles pickle file processing, filtering, and export operations.
"""
import pickle
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from src.services.export_service import filter_by_date_range
from src.services.pickle_service import parse_pickle_file
from src.utils.errors import FileProcessingError, ValidationError
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def process_pickle_file(file_path: Path) -> pd.DataFrame:
    """
    Parse and validate pickle file, return DataFrame for inspection.
    
    Uses existing parse_pickle_file from pickle_service.py which handles
    DataFrame, dict, and list of dicts formats.
    
    Args:
        file_path: Path to pickle file
        
    Returns:
        Parsed DataFrame (copy for safe manipulation)
        
    Raises:
        FileProcessingError: If file cannot be parsed or is unsupported type
        ValidationError: If DataFrame is empty
    """
    try:
        # Use existing pickle parsing service
        df = parse_pickle_file(file_path)
        
        # Additional validation: ensure DataFrame is not empty
        if df.empty:
            raise ValidationError(
                "DataFrame is empty",
                field="dataframe",
            )
        
        # Return copy for safe manipulation
        logger.info(f"Successfully processed pickle file: {file_path.name} ({len(df)} rows, {len(df.columns)} columns)")
        return df.copy()
        
    except (FileProcessingError, ValidationError):
        # Re-raise existing errors
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing pickle file: {e}", exc_info=True)
        raise FileProcessingError(
            f"Failed to process pickle file: {str(e)}",
            filename=file_path.name,
        ) from e


def filter_pickle_dataframe(
    df: pd.DataFrame,
    columns: list[str],
    date_column: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Filter DataFrame by selected columns and optional date range.
    
    Args:
        df: DataFrame to filter
        columns: List of column names to keep (must not be empty)
        date_column: Optional date column name for filtering
        start_date: Optional start date for filtering (inclusive)
        end_date: Optional end date for filtering (inclusive)
        
    Returns:
        Filtered DataFrame
        
    Raises:
        ValidationError: If columns are invalid, empty, or date column doesn't exist
    """
    # Validate at least one column selected
    if not columns:
        raise ValidationError(
            "At least one column must be selected",
            field="columns",
            value=columns,
        )
    
    # Validate all selected columns exist in DataFrame
    missing_columns = [col for col in columns if col not in df.columns]
    if missing_columns:
        raise ValidationError(
            f"Columns not found in DataFrame: {', '.join(missing_columns)}",
            field="columns",
            value=missing_columns,
        )
    
    # Start with column filtering
    filtered_df = df[columns].copy()
    
    # Apply date range filtering if date column and dates are provided
    if date_column:
        if date_column not in filtered_df.columns:
            raise ValidationError(
                f"Date column '{date_column}' not found in selected columns",
                field="date_column",
                value=date_column,
            )
        
        # Use existing filter_by_date_range function
        # Note: start_date and end_date should already be datetime objects
        # If they're date objects, they should be converted in the UI layer
        filtered_df = filter_by_date_range(
            filtered_df,
            date_column,
            start_date,
            end_date,
        )
        
        logger.info(
            f"Applied date range filter: {date_column} from {start_date} to {end_date} "
            f"({len(filtered_df)} rows remaining)"
        )
    
    logger.info(
        f"Filtered DataFrame: {len(filtered_df)} rows, {len(columns)} columns selected"
    )
    
    return filtered_df


def export_filtered_pickle(df: pd.DataFrame, output_path: Path) -> Path:
    """
    Export filtered DataFrame to pickle file.
    
    Args:
        df: DataFrame to export (must not be empty)
        output_path: Path where pickle file should be saved
        
    Returns:
        Path to exported pickle file
        
    Raises:
        ValidationError: If DataFrame is empty
        FileProcessingError: If export fails
    """
    # Validate DataFrame is not empty
    if df.empty:
        raise ValidationError(
            "Cannot export empty DataFrame",
            field="dataframe",
        )
    
    try:
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Export to pickle file
        with open(output_path, "wb") as f:
            pickle.dump(df, f)
        
        logger.info(
            f"Successfully exported filtered pickle file: {output_path} "
            f"({len(df)} rows, {len(df.columns)} columns)"
        )
        
        return output_path
        
    except PermissionError as e:
        logger.error(f"Permission denied writing pickle file: {e}", exc_info=True)
        raise FileProcessingError(
            f"Permission denied: Cannot write to {output_path}",
            filename=output_path.name,
        ) from e
    except OSError as e:
        logger.error(f"OS error writing pickle file: {e}", exc_info=True)
        raise FileProcessingError(
            f"Failed to write pickle file: {str(e)}",
            filename=output_path.name,
        ) from e
    except Exception as e:
        logger.error(f"Unexpected error exporting pickle file: {e}", exc_info=True)
        raise FileProcessingError(
            f"Failed to export pickle file: {str(e)}",
            filename=output_path.name,
        ) from e

