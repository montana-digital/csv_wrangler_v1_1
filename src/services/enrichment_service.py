"""
Enrichment service for CSV Wrangler v1.1.

Manages enriched datasets: creation, synchronization, and updates.
"""
from datetime import datetime
from typing import Any, Optional

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.config.settings import UNIQUE_ID_COLUMN_NAME
from src.database.models import DatasetConfig, EnrichedDataset
from src.services.enrichment_functions import get_enrichment_function
from src.services.table_service import (
    add_column_to_table,
    copy_table_data,
    copy_table_structure,
    create_index_on_column,
    get_new_rows_since_sync,
    insert_dataframe_to_table,
    update_enriched_column_values,
)
from src.utils.errors import DatabaseError, ValidationError
from src.utils.logging_config import get_logger
from src.utils.validation import (
    handle_integrity_error,
    quote_identifier,
    sanitize_column_name,
    validate_enrichment_config,
    validate_foreign_key,
    validate_string_length,
)

logger = get_logger(__name__)


def create_enriched_dataset(
    session: Session,
    source_dataset_id: int,
    name: str,
    enrichment_config: dict[str, str],  # {"column_name": "function_name"}
) -> EnrichedDataset:
    """
    Create a new enriched dataset from a source dataset.
    
    Creates a copy of the source table and applies enrichment functions
    to specified columns, adding new enriched columns.
    
    Args:
        session: Database session
        source_dataset_id: ID of source dataset
        name: Name for enriched dataset
        enrichment_config: Dictionary mapping column names to enrichment functions
        
    Returns:
        Created EnrichedDataset instance
        
    Raises:
        ValidationError: If source dataset not found or invalid config
        DatabaseError: If enrichment creation fails
    """
    # Validate foreign key
    validate_foreign_key(session, DatasetConfig, source_dataset_id, "source_dataset_id")
    
    # Get source dataset (now guaranteed to exist)
    source_dataset = session.get(DatasetConfig, source_dataset_id)
    
    # Validate string lengths
    name = validate_string_length(name, 255, "Enriched dataset name")
    
    # Validate enrichment config structure and function names
    enrichment_config = validate_enrichment_config(enrichment_config)
    
    # Get actual table columns from database (more reliable than columns_config)
    inspector = inspect(session.bind)
    try:
        table_columns = [col["name"] for col in inspector.get_columns(source_dataset.table_name)]
    except Exception as e:
        logger.warning(f"Could not inspect table columns, falling back to columns_config: {e}")
        # Fallback to columns_config if inspection fails
        # Filter out unique_id from columns_config if it exists (legacy data)
        if not source_dataset.columns_config:
            raise ValidationError(
                f"Source dataset {source_dataset_id} has invalid columns_config (None or empty) and table inspection failed",
                field="columns_config",
                value=source_dataset_id,
            )
        config_columns = [
            col for col in source_dataset.columns_config.keys() 
            if col != "unique_id" and col != UNIQUE_ID_COLUMN_NAME
        ]
        table_columns = config_columns + [UNIQUE_ID_COLUMN_NAME]
    
    # Validate columns exist in actual table
    for col_name in enrichment_config.keys():
        # Prevent enriching unique_id column (it's automatically added and shouldn't be enriched)
        if col_name == UNIQUE_ID_COLUMN_NAME:
            raise ValidationError(
                f"Column '{UNIQUE_ID_COLUMN_NAME}' cannot be enriched (it's automatically added to all datasets)",
                field="enrichment_config"
            )
        
        if col_name not in table_columns:
            raise ValidationError(
                f"Column '{col_name}' not found in source dataset table '{source_dataset.table_name}'",
                field="enrichment_config"
            )
        
        # Warn about column names with spaces (will be sanitized for enriched column names)
        if " " in col_name or "-" in col_name:
            logger.warning(
                f"Column name '{col_name}' contains spaces or hyphens. "
                f"Enriched column name will be sanitized (spaces/hyphens replaced with underscores)."
            )
    
    # Generate enriched table name with collision handling
    # Use retry logic to handle race conditions
    from src.utils.validation import retry_with_backoff
    
    def check_and_generate_table_name():
        """Check if table name exists and generate unique one if needed."""
        counter = 1
        current_name = f"enriched_{source_dataset.table_name}_v1"
        # For sqlite_master queries, table name is a string literal, not identifier
        while session.execute(
            text(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{current_name.replace("'", "''")}'")
        ).fetchone():
            counter += 1
            current_name = f"enriched_{source_dataset.table_name}_v{counter}"
            if counter > 100:
                raise DatabaseError(
                    f"Unable to generate unique enriched table name after 100 attempts",
                    operation="create_enriched_dataset"
                )
        return current_name
    
    # Retry with exponential backoff to handle race conditions
    try:
        enriched_table_name = retry_with_backoff(
            check_and_generate_table_name,
            max_attempts=3,
            initial_delay=0.1,
            max_delay=1.0,
            exceptions=(DatabaseError,),
        )
    except DatabaseError:
        # Re-raise DatabaseError as-is
        raise
    except Exception as e:
        # Wrap other exceptions
        raise DatabaseError(
            f"Failed to generate unique table name: {e}",
            operation="create_enriched_dataset"
        ) from e
    
    # Note: This operation involves multiple commits because helper functions (copy_table_structure,
    # copy_table_data, add_column_to_table, etc.) commit internally. This is by design for modularity,
    # but means we can have partial state if the final EnrichedDataset record creation fails.
    # We handle this by cleaning up orphaned tables in the exception handler below.
    try:
        # Step 1: Copy table structure (commits internally)
        copy_table_structure(
            session,
            source_dataset.table_name,
            enriched_table_name,
        )
        
        # Step 2: Copy existing data (commits internally)
        rows_copied = copy_table_data(
            session,
            source_dataset.table_name,
            enriched_table_name,
        )
        
        # Step 3: Add enriched columns and populate them
        # Note: These operations commit internally, so partial state is possible
        # but we handle errors and ensure cleanup
        columns_added = []
        for col_name, function_name in enrichment_config.items():
            # Generate enriched column name - sanitize source column name to ensure valid SQL identifier
            # This prevents errors with spaces/special characters in source column names
            sanitized_col_name = sanitize_column_name(col_name)
            enriched_col_name = f"{sanitized_col_name}_enriched_{function_name}"
            columns_added.append(enriched_col_name)
            
            # Add column to table (commits internally)
            add_column_to_table(session, enriched_table_name, enriched_col_name, "TEXT")
            
            # Create index on enriched column for fast search queries (commits internally)
            try:
                create_index_on_column(
                    session,
                    enriched_table_name,
                    enriched_col_name,
                    include_not_null_filter=True,  # Optimized index for NOT NULL values only
                )
                logger.info(f"Created index on enriched column {enriched_col_name}")
            except Exception as index_error:
                # Log warning but don't fail - index creation is optional for functionality
                logger.warning(
                    f"Failed to create index on {enriched_col_name}: {index_error}. "
                    f"Table will still function but search may be slower."
                )
            
            # Load source column data - quote identifiers to handle spaces in column names
            quoted_col_name = quote_identifier(col_name)
            quoted_unique_id = quote_identifier(UNIQUE_ID_COLUMN_NAME)
            quoted_source_table = quote_identifier(source_dataset.table_name)
            query = text(f"SELECT {quoted_col_name}, {quoted_unique_id} FROM {quoted_source_table}")
            result = session.execute(query)
            rows = result.fetchall()
            
            if rows:
                df = pd.DataFrame(rows, columns=[col_name, UNIQUE_ID_COLUMN_NAME])
                
                # Apply enrichment function
                enrichment_func = get_enrichment_function(function_name)
                enriched_series = enrichment_func(df[col_name])
                
                # Add enriched values to DataFrame
                df[enriched_col_name] = enriched_series
                
                # Update enriched column values (commits internally)
                update_enriched_column_values(
                    session,
                    enriched_table_name,
                    enriched_col_name,
                    df[[UNIQUE_ID_COLUMN_NAME, enriched_col_name]],
                )
        
        # Step 4: Create EnrichedDataset record
        # This is the final commit point - if this fails, we have partial state
        # but the table operations above have already committed
        # Validate table name length
        enriched_table_name = validate_string_length(enriched_table_name, 255, "enriched_table_name", allow_truncate=True)
        source_table_name = validate_string_length(source_dataset.table_name, 255, "source_table_name", allow_truncate=True)
        
        enriched_dataset = EnrichedDataset(
            name=name,
            source_dataset_id=source_dataset_id,
            enriched_table_name=enriched_table_name,
            source_table_name=source_table_name,
            enrichment_config=enrichment_config,
            columns_added=columns_added,
            last_sync_date=datetime.now(),
        )
        
        session.add(enriched_dataset)
        session.commit()
        session.refresh(enriched_dataset)
        
        logger.info(
            f"Created enriched dataset '{name}' from dataset {source_dataset_id} "
            f"with {len(columns_added)} enriched columns"
        )
        
        return enriched_dataset
        
    except IntegrityError as e:
        # Rollback any uncommitted changes
        session.rollback()
        
        # Try to clean up partially created table if EnrichedDataset record wasn't created
        try:
            from src.utils.validation import table_exists, quote_identifier
            if table_exists(session, enriched_table_name):
                # Check if EnrichedDataset record exists - if not, table is orphaned
                existing_enriched = session.query(EnrichedDataset).filter_by(
                    enriched_table_name=enriched_table_name
                ).first()
                if not existing_enriched:
                    # Orphaned table - drop it
                    quoted_table = quote_identifier(enriched_table_name)
                    session.execute(text(f"DROP TABLE IF EXISTS {quoted_table}"))
                    session.commit()
                    logger.warning(f"Cleaned up orphaned enriched table {enriched_table_name}")
        except Exception as cleanup_error:
            logger.warning(f"Failed to cleanup orphaned table during error handling: {cleanup_error}")
        
        # Convert to ValidationError with clear message
        context = {
            "name": "Enriched dataset name",
            "enriched_table_name": "Enriched table name",
        }
        raise handle_integrity_error(e, context) from e
        
    except Exception as e:
        # Rollback any uncommitted changes
        session.rollback()
        
        # Try to clean up partially created table if EnrichedDataset record wasn't created
        # (table operations above may have already committed, so we can't fully rollback)
        try:
            from src.utils.validation import table_exists, quote_identifier
            if table_exists(session, enriched_table_name):
                # Check if EnrichedDataset record exists - if not, table is orphaned
                existing_enriched = session.query(EnrichedDataset).filter_by(
                    enriched_table_name=enriched_table_name
                ).first()
                if not existing_enriched:
                    # Orphaned table - drop it
                    quoted_table = quote_identifier(enriched_table_name)
                    session.execute(text(f"DROP TABLE IF EXISTS {quoted_table}"))
                    session.commit()
                    logger.warning(f"Cleaned up orphaned enriched table {enriched_table_name}")
        except Exception as cleanup_error:
            logger.warning(f"Failed to cleanup orphaned table during error handling: {cleanup_error}")
        
        logger.error(f"Failed to create enriched dataset: {e}", exc_info=True)
        raise DatabaseError(
            f"Failed to create enriched dataset: {e}", operation="create_enriched_dataset"
        ) from e


def sync_enriched_dataset(
    session: Session,
    enriched_dataset_id: int,
) -> int:
    """
    Synchronize enriched dataset with source dataset.
    
    Applies enrichment functions to new rows added to source since last sync.
    
    Args:
        session: Database session
        enriched_dataset_id: ID of enriched dataset
        
    Returns:
        Number of rows synced
        
    Raises:
        ValidationError: If enriched dataset not found
        DatabaseError: If sync fails
    """
    enriched_dataset = session.get(EnrichedDataset, enriched_dataset_id)
    if not enriched_dataset:
        raise ValidationError(f"Enriched dataset with ID {enriched_dataset_id} not found")
    
    # Validate that source dataset still exists
    source_dataset = session.get(DatasetConfig, enriched_dataset.source_dataset_id)
    if not source_dataset:
        raise ValidationError(
            f"Source dataset with ID {enriched_dataset.source_dataset_id} no longer exists. "
            f"The enriched dataset '{enriched_dataset.name}' is orphaned.",
            field="source_dataset_id",
            value=enriched_dataset.source_dataset_id,
        )
    
    # Validate that enriched table still exists
    from src.utils.validation import table_exists
    if not table_exists(session, enriched_dataset.enriched_table_name):
        raise ValidationError(
            f"Enriched table '{enriched_dataset.enriched_table_name}' no longer exists. "
            f"The enriched dataset '{enriched_dataset.name}' may have been corrupted.",
            field="enriched_table_name",
            value=enriched_dataset.enriched_table_name,
        )
    
    try:
        # Get new rows from source
        from src.config.settings import UNIQUE_ID_COLUMN_NAME
        new_rows_df = get_new_rows_since_sync(
            session,
            enriched_dataset.source_table_name,
            enriched_dataset.enriched_table_name,
            unique_id_column=UNIQUE_ID_COLUMN_NAME,
        )
        
        if new_rows_df.empty:
            logger.info(f"No new rows to sync for enriched dataset {enriched_dataset_id}")
            enriched_dataset.last_sync_date = datetime.now()
            session.commit()
            return 0
        
        # Insert new rows into enriched table (without enriched columns yet)
        insert_dataframe_to_table(
            session,
            enriched_dataset.enriched_table_name,
            new_rows_df,
        )
        
        # Apply enrichment functions to new rows
        rows_synced = len(new_rows_df)
        
        # Ensure enrichment_config exists
        if not enriched_dataset.enrichment_config:
            raise ValidationError(
                f"Enriched dataset {enriched_dataset_id} has invalid enrichment_config (None or empty)",
                field="enrichment_config",
                value=enriched_dataset_id,
            )
        
        for col_name, function_name in enriched_dataset.enrichment_config.items():
            # Sanitize source column name to match enriched column naming convention
            sanitized_col_name = sanitize_column_name(col_name)
            enriched_col_name = f"{sanitized_col_name}_enriched_{function_name}"
            
            # Apply enrichment function
            enrichment_func = get_enrichment_function(function_name)
            enriched_series = enrichment_func(new_rows_df[col_name])
            
            # Update enriched column values
            enriched_df = new_rows_df[[UNIQUE_ID_COLUMN_NAME]].copy()
            enriched_df[enriched_col_name] = enriched_series
            
            update_enriched_column_values(
                session,
                enriched_dataset.enriched_table_name,
                enriched_col_name,
                enriched_df,
            )
            
            # Ensure index exists on enriched column (in case it was created before indexes were added)
            # This is idempotent - won't create duplicate indexes
            try:
                create_index_on_column(
                    session,
                    enriched_dataset.enriched_table_name,
                    enriched_col_name,
                    include_not_null_filter=True,
                )
            except Exception:
                # Index may already exist or column may not exist - ignore
                pass
        
        # Update last sync date
        enriched_dataset.last_sync_date = datetime.now()
        session.commit()
        
        logger.info(
            f"Synced {rows_synced} new rows to enriched dataset {enriched_dataset_id}"
        )
        
        return rows_synced
        
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to sync enriched dataset: {e}", exc_info=True)
        raise DatabaseError(
            f"Failed to sync enriched dataset: {e}", operation="sync_enriched_dataset"
        ) from e


def sync_all_enriched_datasets_for_source(
    session: Session,
    source_dataset_id: int,
) -> dict[str, int]:
    """
    Sync all enriched datasets for a source dataset.
    
    Called automatically when source dataset is updated.
    
    Args:
        session: Database session
        source_dataset_id: ID of source dataset
        
    Returns:
        Dictionary mapping enriched dataset IDs to rows synced
    """
    enriched_datasets = (
        session.query(EnrichedDataset)
        .filter_by(source_dataset_id=source_dataset_id)
        .all()
    )
    
    results = {}
    for enriched_dataset in enriched_datasets:
        try:
            rows_synced = sync_enriched_dataset(session, enriched_dataset.id)
            results[str(enriched_dataset.id)] = rows_synced
        except Exception as e:
            logger.error(
                f"Failed to sync enriched dataset {enriched_dataset.id}: {e}",
                exc_info=True,
            )
            results[str(enriched_dataset.id)] = -1  # Error indicator
    
    return results


def get_enriched_datasets(
    session: Session,
    source_dataset_id: Optional[int] = None,
) -> list[EnrichedDataset]:
    """
    Get enriched datasets, optionally filtered by source.
    
    Args:
        session: Database session
        source_dataset_id: Optional source dataset ID to filter by
        
    Returns:
        List of EnrichedDataset instances
    """
    query = session.query(EnrichedDataset)
    
    if source_dataset_id:
        query = query.filter_by(source_dataset_id=source_dataset_id)
    
    return query.order_by(EnrichedDataset.created_at.desc()).all()


def delete_enriched_dataset(
    session: Session,
    enriched_dataset_id: int,
) -> None:
    """
    Delete enriched dataset and its table.
    
    Args:
        session: Database session
        enriched_dataset_id: ID of enriched dataset to delete
        
    Raises:
        ValidationError: If enriched dataset not found
        DatabaseError: If deletion fails
    """
    enriched_dataset = session.get(EnrichedDataset, enriched_dataset_id)
    if not enriched_dataset:
        raise ValidationError(f"Enriched dataset with ID {enriched_dataset_id} not found")
    
    try:
        # Drop the enriched table - quote identifier for safety
        table_name = enriched_dataset.enriched_table_name
        quoted_table = quote_identifier(table_name)
        session.execute(text(f"DROP TABLE IF EXISTS {quoted_table}"))
        
        # Delete the record (cascade will handle relationships)
        session.delete(enriched_dataset)
        session.commit()
        
        logger.info(f"Deleted enriched dataset {enriched_dataset_id} and table {table_name}")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to delete enriched dataset: {e}", exc_info=True)
        raise DatabaseError(
            f"Failed to delete enriched dataset: {e}", operation="delete_enriched_dataset"
        ) from e


def find_enriched_columns_by_function_type(
    session: Session,
    function_type: str,
) -> list[tuple[int, str, str]]:
    """
    Find all enriched columns using a specific enrichment function type.
    
    Args:
        session: Database session
        function_type: Enrichment function type (phone_numbers, emails, web_domains, etc.)
        
    Returns:
        List of (source_dataset_id, enriched_table_name, enriched_column_name) tuples
    """
    all_enriched = session.query(EnrichedDataset).all()
    
    result = []
    for enriched_dataset in all_enriched:
        # Check if any enrichment config uses matching function
        matching_columns = [
            col_name
            for col_name, func_name in enriched_dataset.enrichment_config.items()
            if func_name == function_type
        ]
        
        for col_name in matching_columns:
            enriched_col_name = f"{col_name}_enriched_{function_type}"
            if enriched_col_name in enriched_dataset.columns_added:
                result.append(
                    (
                        enriched_dataset.source_dataset_id,
                        enriched_dataset.enriched_table_name,
                        enriched_col_name,
                    )
                )
    
    return result
