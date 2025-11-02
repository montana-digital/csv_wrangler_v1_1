"""
CSV parsing and validation service for CSV Wrangler.

Handles CSV file parsing, encoding detection, Base64 image detection,
and column validation.
"""
import base64
import re
from pathlib import Path
from typing import Optional

import pandas as pd

from src.config.settings import (
    BASE64_MIN_LENGTH,
    BASE64_PATTERN_PREFIX,
    CSV_ENCODINGS,
    UNIQUE_ID_COLUMN_NAME,
)
from src.utils.errors import FileProcessingError, SchemaMismatchError
from src.utils.logging_config import get_logger
from src.utils.package_check import has_pyarrow

logger = get_logger(__name__)


def parse_csv_file(file_path: Path, encoding: Optional[str] = None, show_progress: bool = True) -> pd.DataFrame:
    """
    Parse CSV file with automatic encoding detection.
    
    Args:
        file_path: Path to CSV file
        encoding: Optional encoding (if None, will auto-detect)
        
    Returns:
        Parsed DataFrame
        
    Raises:
        FileProcessingError: If file cannot be parsed
    """
    if not file_path.exists():
        raise FileProcessingError(f"File not found: {file_path}")

    if file_path.stat().st_size == 0:
        raise FileProcessingError("CSV file is empty")

    # Try specified encoding first, then auto-detect
    encodings_to_try = [encoding] if encoding else CSV_ENCODINGS

    last_error: Optional[Exception] = None

    # Estimate file size for progress tracking
    file_size_mb = file_path.stat().st_size / (1024 * 1024)
    use_chunked_reading = file_size_mb > 5.0  # Use chunked reading for files > 5MB
    
    for enc in encodings_to_try:
        try:
            # Read all columns as strings to preserve data exactly as provided
            # User will configure data types during dataset initialization
            import warnings
            with warnings.catch_warnings():
                # Suppress parser warnings for malformed lines (we handle them with on_bad_lines="skip")
                warnings.filterwarnings("ignore", category=pd.errors.ParserWarning)
                
                if use_chunked_reading and show_progress:
                    # For large files, read in chunks with progress indicator
                    import streamlit as st
                    from src.utils.progress import estimate_row_count
                    
                    st.info(f"📊 Reading large file ({file_size_mb:.1f} MB)...")
                    estimated_rows = estimate_row_count(file_path)
                    
                    chunks = []
                    chunk_size = 10000
                    row_count = 0
                    
                    from src.utils.progress import progress_bar
                    # Use 'c' engine for compatibility - pyarrow has issues with chunked reading
                    with progress_bar(estimated_rows, "Reading CSV file", key=f"parse_{file_path.name}") as update_progress:
                        for chunk_df in pd.read_csv(
                            file_path,
                            encoding=enc,
                            on_bad_lines="skip",
                            dtype=str,
                            keep_default_na=False,
                            index_col=False,
                            chunksize=chunk_size,
                            engine="c",  # Use 'c' engine for compatibility
                        ):
                            chunks.append(chunk_df)
                            row_count += len(chunk_df)
                            update_progress(row_count, f"Read {row_count:,} rows")
                    
                    # Combine all chunks
                    df = pd.concat(chunks, ignore_index=True)
                else:
                    # Standard reading for smaller files
                    if show_progress and file_size_mb > 1.0:
                        import streamlit as st
                        st.info(f"📊 Reading file ({file_size_mb:.1f} MB)...")
                    
                    # Use engine based on pyarrow availability for better performance
                    # Note: PyArrow engine doesn't support all pandas read_csv parameters
                    # so we use 'c' engine for consistency and compatibility
                    # PyArrow can still be used by pandas internally if available for performance
                    df = pd.read_csv(
                        file_path,
                        encoding=enc,
                        on_bad_lines="skip",  # Skip malformed lines
                        dtype=str,  # Read all columns as strings
                        keep_default_na=False,  # Don't convert empty strings to NaN
                        index_col=False,  # Don't use any column as index
                        engine="c",  # Use 'c' engine for compatibility (pyarrow may cause issues with some params)
                    )

            # Check if DataFrame is empty (all rows skipped)
            if df.empty:
                raise FileProcessingError(
                    "CSV file contains no valid data rows",
                    filename=file_path.name,
                )

            # Strip BOM from column names if present
            df.columns = df.columns.str.replace("\ufeff", "", regex=False)

            logger.info(f"Successfully parsed CSV file: {file_path.name} ({enc} encoding)")
            return df

        except UnicodeDecodeError as e:
            last_error = e
            logger.debug(f"Failed to parse with encoding {enc}: {e}")
            continue
        except pd.errors.EmptyDataError:
            raise FileProcessingError("CSV file is empty", filename=file_path.name)
        except Exception as e:
            logger.error(f"Unexpected error parsing CSV: {e}", exc_info=True)
            raise FileProcessingError(
                f"Failed to parse CSV file: {str(e)}", filename=file_path.name
            ) from e

    # If we get here, all encodings failed
    raise FileProcessingError(
        f"Failed to parse CSV file with any encoding. Last error: {last_error}",
        filename=file_path.name,
    )


def detect_base64_image_columns(df: pd.DataFrame) -> list[str]:
    """
    Detect columns containing Base64-encoded image data.
    
    Looks for columns with values starting with "data:image" prefix
    and having sufficient length to be Base64 image data.
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        List of column names containing Base64 image data
    """
    image_columns: list[str] = []

    for column in df.columns:
        # Check if column contains string data
        if df[column].dtype != "object":
            continue

        # Sample non-null values to check
        sample_values = df[column].dropna().head(100)

        if len(sample_values) == 0:
            continue

        # Check if values match Base64 image pattern
        matches_pattern = sample_values.astype(str).str.startswith(
            BASE64_PATTERN_PREFIX, na=False
        )

        if matches_pattern.any():
            # Additional check: check if matching values have sufficient length
            matching_values = sample_values[matches_pattern]
            if len(matching_values) > 0:
                # Check if at least some matching values meet minimum length
                matching_lengths = matching_values.astype(str).str.len()
                # If majority of matching values meet length threshold, consider it an image column
                # OR if any matching value is very long (likely real image data)
                if matching_lengths.max() >= BASE64_MIN_LENGTH or (
                    len(matching_values) >= 2 and matching_lengths.mean() >= BASE64_MIN_LENGTH * 0.5
                ):
                    image_columns.append(column)
                    logger.debug(f"Detected image column: {column}")
                # Also accept if pattern matches even with shorter values (could be small images)
                elif matches_pattern.sum() >= len(sample_values) * 0.5:  # 50%+ of values match pattern
                    image_columns.append(column)
                    logger.debug(f"Detected image column: {column} (pattern match)")

    return image_columns


def generate_unique_ids(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate unique IDs for each row in DataFrame.
    
    Adds a 'uuid_value' column with UUID-based unique identifiers.
    
    Args:
        df: DataFrame to add IDs to
        
    Returns:
        DataFrame with uuid_value column added
    """
    import uuid

    df_copy = df.copy()
    df_copy[UNIQUE_ID_COLUMN_NAME] = [str(uuid.uuid4()) for _ in range(len(df_copy))]

    return df_copy


def validate_column_matching(
    expected_columns: list[str], actual_columns: list[str]
) -> None:
    """
    Validate that actual columns match expected columns.
    
    Args:
        expected_columns: Expected column names
        actual_columns: Actual column names from CSV
        
    Raises:
        SchemaMismatchError: If columns don't match
    """
    expected_set = set(expected_columns)
    actual_set = set(actual_columns)

    if expected_set != actual_set:
        missing = expected_set - actual_set
        extra = actual_set - expected_set

        error_parts = []
        if missing:
            error_parts.append(f"Missing columns: {sorted(missing)}")
        if extra:
            error_parts.append(f"Extra columns: {sorted(extra)}")

        message = f"Column mismatch. {', '.join(error_parts)}"

        raise SchemaMismatchError(
            message,
            expected_columns=expected_columns,
            actual_columns=actual_columns,
        )

    # Check order as well (columns must be in same order)
    if expected_columns != actual_columns:
        raise SchemaMismatchError(
            "Column order mismatch. Columns must be in the same order.",
            expected_columns=expected_columns,
            actual_columns=actual_columns,
        )

