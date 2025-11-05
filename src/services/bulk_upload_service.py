"""
Bulk upload service for CSV Wrangler.

Handles batch processing of multiple file uploads with validation,
duplicate detection, and error reporting.
"""
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy.orm import Session

from src.database.models import DatasetConfig, UploadLog
from src.services.csv_service import validate_column_matching
from src.services.dataset_service import check_duplicate_filename, upload_csv_to_dataset
from src.services.file_import_service import FileType, import_file
from src.utils.errors import (
    DuplicateFileError,
    FileProcessingError,
    SchemaMismatchError,
    ValidationError,
)
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class FileUploadResult:
    """Result of a single file upload attempt."""

    filename: str
    success: bool
    row_count: Optional[int] = None
    error_reason: Optional[str] = None
    error_type: Optional[str] = None  # "schema_mismatch", "duplicate", "parse_error", etc.


@dataclass
class BulkUploadResult:
    """Result of bulk upload operation."""

    dataset_id: int
    dataset_name: str
    total_files: int
    successful: list[FileUploadResult] = field(default_factory=list)
    skipped: list[FileUploadResult] = field(default_factory=list)
    total_rows_added: int = 0


def validate_file_for_dataset(
    session: Session,
    dataset_id: int,
    file_path: Path,
    filename: str,
    batch_filenames: set[str],
) -> tuple[bool, Optional[str], Optional[str]]:
    """
    Validate file against dataset requirements.
    
    Checks:
    1. File can be parsed (CSV or Pickle)
    2. No duplicate within current batch
    3. No duplicate in database
    4. Column schema matches dataset
    
    Args:
        session: Database session
        dataset_id: Dataset ID
        file_path: Path to file to validate
        filename: Filename
        batch_filenames: Set of filenames in current batch (for duplicate check)
        
    Returns:
        Tuple of (is_valid, error_type, error_reason)
        error_type: "parse_error", "duplicate_in_batch", "duplicate_in_db", "schema_mismatch", None
    """
    # Step 1: Parse file
    try:
        df, file_type = import_file(file_path, show_progress=False)
    except (FileProcessingError, ValidationError) as e:
        return False, "parse_error", str(e)
    except Exception as e:
        logger.error(f"Unexpected error parsing file {filename}: {e}", exc_info=True)
        return False, "parse_error", f"Failed to parse file: {str(e)}"

    # Step 2: Check duplicate within batch
    if filename in batch_filenames:
        return False, "duplicate_in_batch", f"'{filename}' appears multiple times in this batch"

    # Step 3: Check duplicate in database
    try:
        check_duplicate_filename(session, dataset_id, filename)
    except DuplicateFileError:
        return False, "duplicate_in_db", f"'{filename}' has already been uploaded to this dataset"

    # Step 4: Validate column schema
    dataset = session.get(DatasetConfig, dataset_id)
    if not dataset:
        return False, "validation_error", f"Dataset with ID {dataset_id} not found"

    if not dataset.columns_config:
        return False, "validation_error", f"Dataset {dataset_id} has invalid columns_config (None or empty)"

    expected_columns = list(dataset.columns_config.keys())
    actual_columns = list(df.columns)

    try:
        validate_column_matching(expected_columns, actual_columns)
    except SchemaMismatchError as e:
        return False, "schema_mismatch", str(e)

    return True, None, None


def upload_file_to_dataset(
    session: Session,
    dataset_id: int,
    file_path: Path,
    filename: str,
    file_type: FileType,
) -> int:
    """
    Upload a validated file to dataset.
    
    Args:
        session: Database session
        dataset_id: Dataset ID
        file_path: Path to file
        filename: Filename
        file_type: File type ("CSV" or "PICKLE")
        
    Returns:
        Number of rows added
        
    Raises:
        DatabaseError: If upload fails
    """
    # For CSV files, use existing upload_csv_to_dataset function
    if file_type == "CSV":
        upload_log = upload_csv_to_dataset(
            session=session,
            dataset_id=dataset_id,
            csv_file=file_path,
            filename=filename,
            show_progress=False,
        )
        return upload_log.row_count
    else:
        # For Pickle files, we need to manually handle the upload
        # since upload_csv_to_dataset is CSV-specific
        from src.services.csv_service import generate_unique_ids
        from sqlalchemy import MetaData, Table
        from datetime import datetime

        dataset = session.get(DatasetConfig, dataset_id)
        if not dataset:
            raise ValidationError(f"Dataset with ID {dataset_id} not found")

        # Parse Pickle file
        df, _ = import_file(file_path, show_progress=False)

        # Validate columns (should already be validated, but double-check)
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
        df_with_ids = generate_unique_ids(df)

        # Insert data into table
        records = df_with_ids.to_dict("records")
        table = Table(dataset.table_name, MetaData(), autoload_with=session.bind)

        # Insert in chunks
        chunk_size = 1000
        total_rows = len(records)

        for i in range(0, total_rows, chunk_size):
            chunk = records[i : i + chunk_size]
            session.execute(table.insert(), chunk)

        # Create upload log
        upload_log = UploadLog(
            dataset_id=dataset_id,
            filename=filename,
            file_type="PICKLE",
            row_count=total_rows,
        )

        session.add(upload_log)

        # Update dataset's updated_at timestamp
        dataset.updated_at = datetime.now()

        session.commit()
        session.refresh(upload_log)
        session.refresh(dataset)

        logger.info(f"Uploaded {total_rows} rows from {filename} (Pickle) to dataset {dataset_id}")

        return total_rows


def process_bulk_upload(
    session: Session,
    dataset_id: int,
    files: list[tuple[Path, str]],  # List of (file_path, filename) tuples
    show_progress: bool = True,
) -> BulkUploadResult:
    """
    Process multiple file uploads to a dataset.
    
    Validates each file, skips invalid ones, and uploads valid files.
    
    Args:
        session: Database session
        dataset_id: Dataset ID
        files: List of (file_path, filename) tuples
        show_progress: Whether to show progress indicators
        
    Returns:
        BulkUploadResult with success/skip details
    """
    # Get dataset info
    dataset = session.get(DatasetConfig, dataset_id)
    if not dataset:
        raise ValidationError(f"Dataset with ID {dataset_id} not found")

    result = BulkUploadResult(
        dataset_id=dataset_id,
        dataset_name=dataset.name,
        total_files=len(files),
    )

    # Track filenames we've processed to avoid duplicate processing within batch
    processed_files: set[str] = set()

    if show_progress:
        import streamlit as st

    for idx, (file_path, filename) in enumerate(files, 1):
        if show_progress:
            st.info(f"Processing file {idx}/{len(files)}: {filename}")

        # Skip if already processed (duplicate in batch)
        if filename in processed_files:
            result.skipped.append(
                FileUploadResult(
                    filename=filename,
                    success=False,
                    error_type="duplicate_in_batch",
                    error_reason=f"'{filename}' appears multiple times in this batch",
                )
            )
            continue

        # Mark as processed before validation (prevents reprocessing if it appears again in batch)
        processed_files.add(filename)

        # Validate file - don't check batch duplicates here since we handle it above
        is_valid, error_type, error_reason = validate_file_for_dataset(
            session=session,
            dataset_id=dataset_id,
            file_path=file_path,
            filename=filename,
            batch_filenames=set(),  # Empty set since we handle duplicates in this function
        )

        if not is_valid:
            result.skipped.append(
                FileUploadResult(
                    filename=filename,
                    success=False,
                    error_type=error_type,
                    error_reason=error_reason,
                )
            )
            continue

        # File is valid - upload it
        try:
            # Determine file type
            from src.services.file_import_service import detect_file_type

            file_type = detect_file_type(file_path)

            # Upload file
            row_count = upload_file_to_dataset(
                session=session,
                dataset_id=dataset_id,
                file_path=file_path,
                filename=filename,
                file_type=file_type,
            )

            result.successful.append(
                FileUploadResult(
                    filename=filename,
                    success=True,
                    row_count=row_count,
                )
            )
            result.total_rows_added += row_count

            # Add to processed set (tracks files we've attempted to process)
            processed_files.add(filename)

            logger.info(f"Successfully uploaded {filename}: {row_count} rows")

        except Exception as e:
            # Unexpected error during upload
            logger.error(f"Failed to upload {filename}: {e}", exc_info=True)
            result.skipped.append(
                FileUploadResult(
                    filename=filename,
                    success=False,
                    error_type="upload_error",
                    error_reason=f"Upload failed: {str(e)}",
                )
            )
            processed_files.add(filename)

    logger.info(
        f"Bulk upload complete: {len(result.successful)} successful, "
        f"{len(result.skipped)} skipped, {result.total_rows_added} total rows"
    )

    return result

