"""
Enrichment service for CSV Wrangler v1.1.

Manages enriched datasets: creation, synchronization, and updates.
"""
from datetime import datetime
from typing import Any, Optional

import pandas as pd
from sqlalchemy import inspect, text
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
    # Get source dataset
    source_dataset = session.get(DatasetConfig, source_dataset_id)
    if not source_dataset:
        raise ValidationError(f"Source dataset with ID {source_dataset_id} not found")
    
    # Validate enrichment config
    if not enrichment_config:
        raise ValidationError("Enrichment config cannot be empty")
    
    # Get actual table columns from database (more reliable than columns_config)
    inspector = inspect(session.bind)
    try:
        table_columns = [col["name"] for col in inspector.get_columns(source_dataset.table_name)]
    except Exception as e:
        logger.warning(f"Could not inspect table columns, falling back to columns_config: {e}")
        # Fallback to columns_config if inspection fails
        # Filter out unique_id from columns_config if it exists (legacy data)
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
    
    # Generate enriched table name
    enriched_table_name = f"enriched_{source_dataset.table_name}_v1"
    # Ensure uniqueness by appending number if needed
    counter = 1
    while session.execute(
        text(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{enriched_table_name}'")
    ).fetchone():
        counter += 1
        enriched_table_name = f"enriched_{source_dataset.table_name}_v{counter}"
    
    try:
        # Step 1: Copy table structure
        copy_table_structure(
            session,
            source_dataset.table_name,
            enriched_table_name,
        )
        
        # Step 2: Copy existing data
        rows_copied = copy_table_data(
            session,
            source_dataset.table_name,
            enriched_table_name,
        )
        
        # Step 3: Add enriched columns and populate them
        columns_added = []
        for col_name, function_name in enrichment_config.items():
            # Generate enriched column name
            enriched_col_name = f"{col_name}_enriched_{function_name}"
            columns_added.append(enriched_col_name)
            
            # Add column to table
            add_column_to_table(session, enriched_table_name, enriched_col_name, "TEXT")
            
            # Create index on enriched column for fast search queries
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
            
            # Load source column data
            query = text(f"SELECT {col_name}, {UNIQUE_ID_COLUMN_NAME} FROM {source_dataset.table_name}")
            result = session.execute(query)
            rows = result.fetchall()
            
            if rows:
                df = pd.DataFrame(rows, columns=[col_name, UNIQUE_ID_COLUMN_NAME])
                
                # Apply enrichment function
                enrichment_func = get_enrichment_function(function_name)
                enriched_series = enrichment_func(df[col_name])
                
                # Add enriched values to DataFrame
                df[enriched_col_name] = enriched_series
                
                # Update enriched column values
                update_enriched_column_values(
                    session,
                    enriched_table_name,
                    enriched_col_name,
                    df[[UNIQUE_ID_COLUMN_NAME, enriched_col_name]],
                )
        
        # Step 4: Create EnrichedDataset record
        enriched_dataset = EnrichedDataset(
            name=name,
            source_dataset_id=source_dataset_id,
            enriched_table_name=enriched_table_name,
            source_table_name=source_dataset.table_name,
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
        
    except Exception as e:
        session.rollback()
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
        for col_name, function_name in enriched_dataset.enrichment_config.items():
            enriched_col_name = f"{col_name}_enriched_{function_name}"
            
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
        # Drop the enriched table
        table_name = enriched_dataset.enriched_table_name
        session.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        
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
