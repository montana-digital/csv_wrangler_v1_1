"""
Export service for CSV Wrangler.

Handles exporting datasets to CSV and Pickle formats with date filtering.
"""
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from src.config.settings import EXPORT_DATE_FORMAT, EXPORT_FILENAME_FORMAT
from src.database.models import DatasetConfig
from src.database.repository import DatasetRepository
from src.utils.errors import DatabaseError, ValidationError
from src.utils.logging_config import get_logger
from src.utils.validation import quote_identifier

logger = get_logger(__name__)


def filter_by_date_range(
    df: pd.DataFrame,
    date_column: str,
    start_date: Optional[datetime],
    end_date: Optional[datetime],
) -> pd.DataFrame:
    """
    Filter DataFrame by date range.
    
    Args:
        df: DataFrame to filter
        date_column: Name of date column
        start_date: Start date (inclusive) or None for no lower bound
        end_date: End date (inclusive) or None for no upper bound
        
    Returns:
        Filtered DataFrame
        
    Raises:
        ValidationError: If date column doesn't exist
    """
    if date_column not in df.columns:
        raise ValidationError(
            f"Date column '{date_column}' not found in DataFrame",
            field="date_column",
            value=date_column,
        )

    filtered_df = df.copy()

    # Convert date column to datetime if needed
    if not pd.api.types.is_datetime64_any_dtype(filtered_df[date_column]):
        filtered_df[date_column] = pd.to_datetime(filtered_df[date_column])

    # Apply filters
    if start_date is not None:
        filtered_df = filtered_df[filtered_df[date_column] >= start_date]

    if end_date is not None:
        filtered_df = filtered_df[filtered_df[date_column] <= end_date]

    return filtered_df


def export_dataset_to_csv(
    session,
    dataset_id: int,
    output_path: Path,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> Path:
    """
    Export dataset to CSV file with optional date filtering.
    
    Args:
        session: Database session
        dataset_id: Dataset ID
        output_path: Path where CSV should be saved
        start_date: Optional start date for filtering
        end_date: Optional end date for filtering
        
    Returns:
        Path to exported CSV file
        
    Raises:
        ValidationError: If dataset not found
        DatabaseError: If export fails
    """
    repo = DatasetRepository(session)
    dataset = repo.get_by_id(dataset_id)

    if not dataset:
        raise ValidationError(f"Dataset with ID {dataset_id} not found")

    try:
        # Load all data from table
        from sqlalchemy import text

        # Quote table name for safety
        quoted_table = quote_identifier(dataset.table_name)
        query = text(f"SELECT * FROM {quoted_table}")
        result = session.execute(query)
        rows = result.fetchall()

        # Convert to DataFrame
        if len(rows) == 0:
            from src.config.settings import UNIQUE_ID_COLUMN_NAME
            # Filter out unique_id from columns_config if it exists (legacy data)
            if not dataset.columns_config:
                raise ValidationError(
                    f"Dataset {dataset_id} has invalid columns_config (None or empty)",
                    field="columns_config",
                    value=dataset_id,
                )
            config_columns = [
                col for col in dataset.columns_config.keys() 
                if col != "unique_id" and col != UNIQUE_ID_COLUMN_NAME
            ]
            df = pd.DataFrame(columns=config_columns + [UNIQUE_ID_COLUMN_NAME])
        else:
            df = pd.DataFrame(rows, columns=result.keys())

        # Filter by date range if provided
        if start_date is not None or end_date is not None:
            # Use upload_date from UploadLog - need to join or filter separately
            # For now, filter on any date column if it exists
            date_columns = [
                col for col in df.columns
                if "date" in col.lower() or "time" in col.lower()
            ]

            if date_columns:
                date_column = date_columns[0]
                df = filter_by_date_range(df, date_column, start_date, end_date)

        # Export to CSV
        df.to_csv(output_path, index=False, encoding="utf-8-sig")

        logger.info(
            f"Exported dataset {dataset_id} to CSV: {output_path} "
            f"({len(df)} rows)"
        )

        return output_path

    except Exception as e:
        logger.error(f"Failed to export dataset to CSV: {e}", exc_info=True)
        raise DatabaseError(f"Failed to export dataset: {e}", operation="export_csv") from e


def export_dataset_to_pickle(
    session,
    dataset_id: int,
    output_path: Path,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> Path:
    """
    Export dataset to Pickle file with optional date filtering.
    
    Args:
        session: Database session
        dataset_id: Dataset ID
        output_path: Path where Pickle file should be saved
        start_date: Optional start date for filtering
        end_date: Optional end date for filtering
        
    Returns:
        Path to exported Pickle file
        
    Raises:
        ValidationError: If dataset not found
        DatabaseError: If export fails
    """
    import pickle

    repo = DatasetRepository(session)
    dataset = repo.get_by_id(dataset_id)

    if not dataset:
        raise ValidationError(f"Dataset with ID {dataset_id} not found")

    try:
        # Load all data from table
        from sqlalchemy import text

        # Quote table name for safety
        quoted_table = quote_identifier(dataset.table_name)
        query = text(f"SELECT * FROM {quoted_table}")
        result = session.execute(query)
        rows = result.fetchall()

        # Convert to DataFrame
        if len(rows) == 0:
            from src.config.settings import UNIQUE_ID_COLUMN_NAME
            # Filter out unique_id from columns_config if it exists (legacy data)
            if not dataset.columns_config:
                raise ValidationError(
                    f"Dataset {dataset_id} has invalid columns_config (None or empty)",
                    field="columns_config",
                    value=dataset_id,
                )
            config_columns = [
                col for col in dataset.columns_config.keys() 
                if col != "unique_id" and col != UNIQUE_ID_COLUMN_NAME
            ]
            df = pd.DataFrame(columns=config_columns + [UNIQUE_ID_COLUMN_NAME])
        else:
            df = pd.DataFrame(rows, columns=result.keys())

        # Filter by date range if provided
        if start_date is not None or end_date is not None:
            date_columns = [
                col for col in df.columns
                if "date" in col.lower() or "time" in col.lower()
            ]

            if date_columns:
                date_column = date_columns[0]
                df = filter_by_date_range(df, date_column, start_date, end_date)

        # Export to Pickle
        with open(output_path, "wb") as f:
            pickle.dump(df, f)

        logger.info(
            f"Exported dataset {dataset_id} to Pickle: {output_path} "
            f"({len(df)} rows)"
        )

        return output_path

    except Exception as e:
        logger.error(f"Failed to export dataset to Pickle: {e}", exc_info=True)
        raise DatabaseError(
            f"Failed to export dataset: {e}", operation="export_pickle"
        ) from e


def generate_export_filename(
    dataset_name: str,
    file_type: str = "csv",
) -> str:
    """
    Generate export filename with timestamp.
    
    Args:
        dataset_name: Dataset name
        file_type: File type extension (csv or pkl)
        
    Returns:
        Generated filename
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    sanitized_name = dataset_name.lower().replace(" ", "_").replace("-", "_")
    return f"export_{sanitized_name}_{timestamp}.{file_type}"

