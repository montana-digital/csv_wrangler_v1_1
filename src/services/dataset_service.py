"""
Dataset service for CSV Wrangler.

Handles dataset initialization, CSV uploads, statistics, and deletion.
"""
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from sqlalchemy import Column, Integer, MetaData, String, Table, Text, create_engine, inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.config.settings import MAX_DATASET_SLOTS, UNIQUE_ID_COLUMN_NAME
from src.database.models import DatasetConfig, UploadLog
from src.services.csv_service import (
    generate_unique_ids,
    parse_csv_file,
    validate_column_matching,
)
from src.utils.errors import (
    DatabaseError,
    DuplicateFileError,
    SchemaMismatchError,
    ValidationError,
)
from src.utils.logging_config import get_logger
from src.utils.validation import (
    handle_integrity_error,
    quote_identifier,
    table_exists,
    validate_columns_config,
    validate_image_columns,
    validate_string_length,
)

logger = get_logger(__name__)

# SQLite type mapping
SQLITE_TYPE_MAP = {
    "TEXT": Text,
    "INTEGER": Integer,
    "REAL": None,  # Float - SQLite handles this
    "BLOB": None,  # Binary - SQLite handles this
}


def initialize_dataset(
    session: Session,
    name: str,
    slot_number: int,
    columns_config: dict[str, dict[str, Any]],
    image_columns: list[str],
    duplicate_filter_column: Optional[str] = None,  # Deprecated, kept for backward compatibility
) -> DatasetConfig:
    """
    Initialize a new dataset.
    
    Creates a database table based on the column configuration and saves
    metadata to DatasetConfig.
    
    Args:
        session: Database session
        name: Dataset name
        slot_number: Dataset slot number (1-5)
        columns_config: Column configuration {"col_name": {"type": "...", "is_image": bool}}
        image_columns: List of column names containing Base64 images
        duplicate_filter_column: Deprecated - kept for backward compatibility only
        
    Returns:
        Created DatasetConfig instance
        
    Raises:
        ValidationError: If slot number is invalid or name is duplicate
        DatabaseError: If table creation fails
    """
    # Validate inputs
    # Validate slot number
    if slot_number < 1 or slot_number > MAX_DATASET_SLOTS:
        raise ValidationError(
            f"Slot number must be between 1 and {MAX_DATASET_SLOTS}",
            field="slot_number",
            value=slot_number,
        )

    # Validate string lengths
    name = validate_string_length(name, 255, "Dataset name")
    if duplicate_filter_column:
        duplicate_filter_column = validate_string_length(duplicate_filter_column, 255, "duplicate_filter_column")

    # Validate JSON structures
    columns_config = validate_columns_config(columns_config, allow_empty=False)
    image_columns = validate_image_columns(image_columns, columns_config=columns_config)

    # Check if slot is already occupied
    existing = (
        session.query(DatasetConfig)
        .filter_by(slot_number=slot_number)
        .first()
    )
    if existing:
        raise ValidationError(
            f"Slot {slot_number} is already occupied by dataset '{existing.name}'",
            field="slot_number",
            value=slot_number,
        )

    # Check if name is duplicate
    existing_name = session.query(DatasetConfig).filter_by(name=name).first()
    if existing_name:
        raise ValidationError(
            f"Dataset name '{name}' already exists",
            field="name",
            value=name,
        )

    # Generate table name (sanitized)
    table_name = f"dataset_{slot_number}_{name.lower().replace(' ', '_').replace('-', '_')}"
    # Remove special characters
    table_name = "".join(c if c.isalnum() or c == "_" else "" for c in table_name)
    
    # Validate and truncate table name if needed (max 255 chars)
    table_name = validate_string_length(table_name, 255, "table_name", allow_truncate=True)
    
    # Check if table name already exists (collision check)
    # Use retry logic to handle race conditions
    from src.utils.validation import retry_with_backoff
    
    def check_table_name_collision():
        """Check if table name exists and generate unique one if needed."""
        counter = 1
        base_name = table_name
        current_name = base_name
        
        while table_exists(session, current_name):
            counter += 1
            # Truncate base name if needed to make room for version suffix
            suffix = f"_{counter}"
            max_base_length = 255 - len(suffix)
            if len(base_name) > max_base_length:
                base_name = base_name[:max_base_length]
            current_name = f"{base_name}{suffix}"
            
            if counter > 100:
                raise DatabaseError(
                    f"Unable to generate unique table name after 100 attempts",
                    operation="initialize_dataset"
                )
        return current_name
    
    try:
        table_name = retry_with_backoff(
            check_table_name_collision,
            max_attempts=3,
            initial_delay=0.1,
            max_delay=1.0,
        )
    except DatabaseError:
        raise
    except Exception as e:
        raise DatabaseError(
            f"Failed to generate unique table name: {e}",
            operation="initialize_dataset"
        ) from e

    table_created = False
    try:
        # Create table dynamically
        metadata = MetaData()
        columns = [
            Column(UNIQUE_ID_COLUMN_NAME, String(36), primary_key=True),  # UUID
        ]

        # Add columns based on configuration
        # Skip unique_id if it's in columns_config since it's already added above
        for col_name, col_config in columns_config.items():
            # Skip unique_id column - it's always added automatically
            if col_name == UNIQUE_ID_COLUMN_NAME:
                logger.warning(f"Skipping '{UNIQUE_ID_COLUMN_NAME}' from columns_config - it's automatically added")
                continue
                
            col_type = col_config.get("type", "TEXT")
            if col_type == "TEXT":
                columns.append(Column(col_name, Text))
            elif col_type == "INTEGER":
                columns.append(Column(col_name, Integer))
            else:
                # Default to TEXT for other types
                columns.append(Column(col_name, Text))

        table = Table(table_name, metadata, *columns)
        metadata.create_all(bind=session.bind)
        table_created = True

        logger.info(f"Created table {table_name} for dataset '{name}'")

        # Create DatasetConfig record
        dataset = DatasetConfig(
            name=name,
            slot_number=slot_number,
            table_name=table_name,
            columns_config=columns_config,
            duplicate_filter_column=duplicate_filter_column,
            image_columns=image_columns,
        )

        session.add(dataset)
        session.commit()
        session.refresh(dataset)

        logger.info(f"Initialized dataset '{name}' in slot {slot_number}")

        return dataset

    except IntegrityError as e:
        session.rollback()
        # Clean up orphaned table if it was created
        if table_created:
            try:
                quoted_table = quote_identifier(table_name)
                session.execute(text(f"DROP TABLE IF EXISTS {quoted_table}"))
                session.commit()
                logger.warning(f"Cleaned up orphaned table {table_name} after IntegrityError")
            except Exception as cleanup_error:
                logger.warning(f"Failed to cleanup orphaned table during error handling: {cleanup_error}")
        
        # Convert to ValidationError with clear message
        context = {
            "name": "Dataset name",
            "slot_number": "Slot number",
            "table_name": "Table name",
        }
        raise handle_integrity_error(e, context) from e

    except Exception as e:
        session.rollback()
        # Clean up orphaned table if it was created
        if table_created:
            try:
                quoted_table = quote_identifier(table_name)
                session.execute(text(f"DROP TABLE IF EXISTS {quoted_table}"))
                session.commit()
                logger.warning(f"Cleaned up orphaned table {table_name} after error")
            except Exception as cleanup_error:
                logger.warning(f"Failed to cleanup orphaned table during error handling: {cleanup_error}")
        
        logger.error(f"Failed to initialize dataset: {e}", exc_info=True)
        raise DatabaseError(f"Failed to initialize dataset: {e}", operation="initialize_dataset") from e


def check_duplicate_filename(
    session: Session,
    dataset_id: int,
    filename: str,
) -> None:
    """
    Check if filename has been uploaded to this dataset before.
    
    Args:
        session: Database session
        dataset_id: Dataset ID
        filename: Filename to check
        
    Raises:
        DuplicateFileError: If filename already exists for this dataset
    """
    existing = (
        session.query(UploadLog)
        .filter_by(dataset_id=dataset_id, filename=filename)
        .first()
    )

    if existing:
        raise DuplicateFileError(filename=filename, dataset_id=dataset_id)


def upload_csv_to_dataset(
    session: Session,
    dataset_id: int,
    csv_file: Path,
    filename: str,
    show_progress: bool = True,
) -> UploadLog:
    """
    Upload CSV file to existing dataset.
    
    Validates column matching, adds unique IDs, and inserts data into table.
    
    Args:
        session: Database session
        dataset_id: Dataset ID
        csv_file: Path to CSV file
        filename: Original filename
        
    Returns:
        UploadLog instance
        
    Raises:
        SchemaMismatchError: If columns don't match
        DuplicateFileError: If filename already uploaded
        DatabaseError: If upload fails
    """
    # Get dataset configuration
    dataset = session.get(DatasetConfig, dataset_id)
    if not dataset:
        raise ValidationError(f"Dataset with ID {dataset_id} not found")

    # Check for duplicate filename
    check_duplicate_filename(session, dataset_id, filename)

    # Parse CSV with progress indicator
    if show_progress:
        import streamlit as st
        st.info(f"📄 Parsing CSV file: {filename}")
    
    df = parse_csv_file(csv_file, show_progress=show_progress)

    # Validate column matching
    if not dataset.columns_config:
        raise ValidationError(
            f"Dataset {dataset_id} has invalid columns_config (None or empty)",
            field="columns_config",
            value=dataset_id,
        )
    expected_columns = list(dataset.columns_config.keys())
    actual_columns = list(df.columns)
    validate_column_matching(expected_columns, actual_columns)

    # Generate unique IDs
    if show_progress:
        import streamlit as st
        st.info("🔑 Generating unique IDs...")
    
    df_with_ids = generate_unique_ids(df)

    try:
        # Insert data into table
        # Convert DataFrame to list of dicts for insertion
        if show_progress:
            import streamlit as st
            st.info("💾 Preparing data for database insertion...")
        
        records = df_with_ids.to_dict("records")

        # Build INSERT statement
        table = Table(dataset.table_name, MetaData(), autoload_with=session.bind)
        
        # Insert in chunks for large datasets
        chunk_size = 1000
        total_rows = len(records)
        
        if show_progress:
            import streamlit as st
            from src.utils.progress import progress_bar
            
            with progress_bar(total_rows, f"Uploading {filename}", key=f"upload_{dataset_id}") as update_progress:
                for i in range(0, total_rows, chunk_size):
                    chunk = records[i : i + chunk_size]
                    session.execute(table.insert(), chunk)
                    update_progress(min(i + chunk_size, total_rows), f"Inserted {min(i + chunk_size, total_rows):,} rows")
        else:
            for i in range(0, total_rows, chunk_size):
                chunk = records[i : i + chunk_size]
                session.execute(table.insert(), chunk)

        # Create upload log
        upload_log = UploadLog(
            dataset_id=dataset_id,
            filename=filename,
            file_type="CSV",
            row_count=total_rows,
        )

        session.add(upload_log)
        
        # Update dataset's updated_at timestamp
        dataset.updated_at = datetime.now()
        
        session.commit()
        session.refresh(upload_log)
        session.refresh(dataset)

        logger.info(f"Uploaded {total_rows} rows from {filename} to dataset {dataset_id}")

        # Trigger enriched dataset sync (v1.1 feature)
        try:
            from src.services.enrichment_service import sync_all_enriched_datasets_for_source
            sync_results = sync_all_enriched_datasets_for_source(session, dataset_id)
            if sync_results:
                logger.info(
                    f"Synced {len(sync_results)} enriched datasets for source dataset {dataset_id}"
                )
        except Exception as sync_error:
            # Don't fail upload if sync fails - log and continue
            logger.warning(
                f"Failed to sync enriched datasets after upload: {sync_error}",
                exc_info=True,
            )

        return upload_log

    except Exception as e:
        session.rollback()
        logger.error(f"Failed to upload CSV: {e}", exc_info=True)
        raise DatabaseError(f"Failed to upload CSV: {e}", operation="upload_csv") from e


def get_dataset_statistics(
    session: Session,
    dataset_id: int,
) -> dict[str, Any]:
    """
    Get statistics for a dataset.
    
    Args:
        session: Database session
        dataset_id: Dataset ID
        
    Returns:
        Dictionary with statistics:
        - total_rows: Total number of rows in dataset
        - total_uploads: Number of CSV files uploaded
        - column_names: List of column names
        - column_types: Dictionary of column names to types
        - first_upload: First upload date (ISO format)
        - last_upload: Last upload date (ISO format)
        - image_columns: List of image column names
    """
    dataset = session.get(DatasetConfig, dataset_id)
    if not dataset:
        raise ValidationError(f"Dataset with ID {dataset_id} not found")

    # Get row count
    from src.utils.validation import quote_identifier
    quoted_table = quote_identifier(dataset.table_name)
    result = session.execute(
        text(f"SELECT COUNT(*) FROM {quoted_table}")
    )
    total_rows = result.scalar() or 0

    # Get upload statistics
    upload_logs = (
        session.query(UploadLog)
        .filter_by(dataset_id=dataset_id)
        .order_by(UploadLog.upload_date)
        .all()
    )

    total_uploads = len(upload_logs)
    first_upload = upload_logs[0].upload_date.isoformat() if upload_logs else None
    last_upload = upload_logs[-1].upload_date.isoformat() if upload_logs else None

    # Extract column info
    if not dataset.columns_config:
        raise ValidationError(
            f"Dataset {dataset_id} has invalid columns_config (None or empty)",
            field="columns_config",
            value=dataset_id,
        )
    column_names = list(dataset.columns_config.keys())
    column_types = {
        name: config.get("type", "TEXT")
        for name, config in dataset.columns_config.items()
    }

    return {
        "total_rows": total_rows,
        "total_uploads": total_uploads,
        "column_names": column_names,
        "column_types": column_types,
        "first_upload": first_upload,
        "last_upload": last_upload,
        "image_columns": dataset.image_columns,
    }


def delete_dataset(
    session: Session,
    dataset_id: int,
) -> None:
    """
    Delete a dataset.
    
    Drops the table, removes metadata, cleans up upload logs, and drops
    all associated enriched tables to prevent orphaned tables.
    
    Args:
        session: Database session
        dataset_id: Dataset ID
        
    Raises:
        ValidationError: If dataset not found
        DatabaseError: If deletion fails
    """
    dataset = session.get(DatasetConfig, dataset_id)
    if not dataset:
        raise ValidationError(f"Dataset with ID {dataset_id} not found")

    table_name = dataset.table_name

    try:
        # First, drop all enriched tables associated with this dataset
        # Foreign key CASCADE will delete EnrichedDataset records, but we need to drop the tables
        from src.database.models import EnrichedDataset
        from src.utils.validation import quote_identifier
        
        enriched_datasets = (
            session.query(EnrichedDataset)
            .filter_by(source_dataset_id=dataset_id)
            .all()
        )
        
        for enriched_dataset in enriched_datasets:
            try:
                # Drop the enriched table before deleting the record
                quoted_table = quote_identifier(enriched_dataset.enriched_table_name)
                session.execute(text(f"DROP TABLE IF EXISTS {quoted_table}"))
                logger.info(f"Dropped enriched table {enriched_dataset.enriched_table_name}")
            except Exception as e:
                # Log warning but continue - table might not exist or already dropped
                logger.warning(f"Failed to drop enriched table {enriched_dataset.enriched_table_name}: {e}")
        
        # Drop source table
        quoted_source_table = quote_identifier(table_name)
        session.execute(text(f"DROP TABLE IF EXISTS {quoted_source_table}"))

        # Delete dataset config (cascade will delete upload logs and EnrichedDataset records)
        session.delete(dataset)
        session.commit()

        logger.info(f"Deleted dataset {dataset_id} and table {table_name}")

    except Exception as e:
        session.rollback()
        logger.error(f"Failed to delete dataset: {e}", exc_info=True)
        raise DatabaseError(f"Failed to delete dataset: {e}", operation="delete_dataset") from e

