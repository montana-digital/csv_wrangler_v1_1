"""
DataFrame loading service for CSV Wrangler v1.1.

Provides utilities for loading dataset data into DataFrames
with image column handling and efficient pagination.
"""
import os
from typing import Optional

import pandas as pd
import streamlit as st
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.database.models import DatasetConfig, EnrichedDataset
from src.utils.errors import DatabaseError, ValidationError
from src.utils.logging_config import get_logger
from src.utils.validation import quote_identifier
from src.utils.cache_manager import get_cache_version

logger = get_logger(__name__)

# Detect if we're running in tests
def _is_test_mode() -> bool:
    """Check if we're running in test mode."""
    return (
        "pytest" in os.environ.get("_", "") or
        os.environ.get("PYTEST_CURRENT_TEST") is not None or
        "PYTEST" in os.environ
    )


@st.cache_data(
    ttl=300,  # 5 minutes
    show_spinner=False,
    max_entries=100,  # Limit cache size
)
def _load_dataset_dataframe_cached(
    table_name: str,
    columns_to_load: list[str],
    dataset_id: int,
    limit: int,
    offset: int,
    order_by_recent: bool,
    cache_version: int,
) -> pd.DataFrame:
    """
    Internal cached function for loading dataset DataFrame.
    
    This function is cached and takes only hashable parameters.
    The session is obtained inside this function.
    """
    from src.database.connection import get_session
    
    with get_session() as session:
        # Build SELECT query
        quoted_columns = [quote_identifier(col) for col in columns_to_load]
        columns_str = ", ".join(quoted_columns)
        quoted_table = quote_identifier(table_name)
        query = f"SELECT {columns_str} FROM {quoted_table}"
        
        # Add ordering
        if order_by_recent:
            query += " ORDER BY rowid DESC"
        
        # Add limit and offset
        query += f" LIMIT {limit}"
        if offset > 0:
            query += f" OFFSET {offset}"
        
        # Execute query
        result = session.execute(text(query))
        rows = result.fetchall()
        
        if not rows:
            return pd.DataFrame(columns=columns_to_load)
        
        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=columns_to_load)
        
        logger.debug(
            f"Cached load: {len(df)} rows from dataset {dataset_id} "
            f"(limit={limit}, offset={offset})"
        )
        
        return df


@st.cache_data(
    ttl=300,  # 5 minutes
    show_spinner=False,
    max_entries=100,  # Limit cache size
)
def _get_dataset_row_count_cached(
    table_name: str,
    dataset_id: int,
    cache_version: int,
) -> int:
    """Internal cached function for getting row count."""
    from src.database.connection import get_session
    from src.utils.validation import table_exists
    
    with get_session() as session:
        if not table_exists(session, table_name):
            logger.warning(f"Table '{table_name}' does not exist. Returning 0 rows.")
            return 0
        
        try:
            quoted_table = quote_identifier(table_name)
            query = text(f"SELECT COUNT(*) FROM {quoted_table}")
            result = session.execute(query)
            count = result.scalar() or 0
            return count
        except Exception as e:
            logger.error(f"Failed to get row count: {e}", exc_info=True)
            return 0


def load_dataset_dataframe(
    session: Session,
    dataset_id: int,
    limit: int = 10000,
    offset: int = 0,
    include_image_columns: bool = False,
    order_by_recent: bool = True,
) -> pd.DataFrame:
    """
    Load dataset data into DataFrame.
    
    Loads data from database table with options for:
    - Limiting rows (pagination)
    - Excluding image columns by default
    - Ordering by most recent
    
    Note: This function uses Streamlit caching for performance.
    Cache is automatically invalidated when data changes.
    
    Args:
        session: Database session
        dataset_id: Dataset ID
        limit: Maximum number of rows to load (default 10,000)
        offset: Number of rows to skip (for pagination)
        include_image_columns: Whether to include image columns (default False)
        order_by_recent: Order by rowid DESC (most recent first, default True)
        
    Returns:
        DataFrame with dataset data
        
    Raises:
        ValidationError: If dataset not found
        DatabaseError: If loading fails
    """
    dataset = session.get(DatasetConfig, dataset_id)
    if not dataset:
        raise ValidationError(f"Dataset with ID {dataset_id} not found")
    
    try:
        # Get column names
        from src.config.settings import UNIQUE_ID_COLUMN_NAME
        # Ensure columns_config exists (with mutable default fix, it should always be set)
        if not dataset.columns_config:
            raise ValidationError(
                f"Dataset {dataset_id} has invalid columns_config (None or empty)",
                field="columns_config",
                value=dataset_id,
            )
        
        # Filter out unique_id from columns_config if it exists (legacy data)
        config_columns = [
            col for col in dataset.columns_config.keys() 
            if col != "unique_id" and col != UNIQUE_ID_COLUMN_NAME
        ]
        all_columns = config_columns + [UNIQUE_ID_COLUMN_NAME]
        
        # Exclude image columns if requested
        columns_to_load = all_columns.copy()
        if not include_image_columns and dataset.image_columns:
            columns_to_load = [
                col for col in columns_to_load if col not in dataset.image_columns
            ]
        
        # In test mode, bypass caching and use the passed session directly
        if _is_test_mode():
            # Build SELECT query
            quoted_columns = [quote_identifier(col) for col in columns_to_load]
            columns_str = ", ".join(quoted_columns)
            quoted_table = quote_identifier(dataset.table_name)
            query = f"SELECT {columns_str} FROM {quoted_table}"
            
            # Add ordering
            if order_by_recent:
                query += " ORDER BY rowid DESC"
            
            # Add limit and offset
            query += f" LIMIT {limit}"
            if offset > 0:
                query += f" OFFSET {offset}"
            
            # Execute query using the passed session
            result = session.execute(text(query))
            rows = result.fetchall()
            
            if not rows:
                df = pd.DataFrame(columns=columns_to_load)
            else:
                df = pd.DataFrame(rows, columns=columns_to_load)
        else:
            # Get cache version for invalidation
            cache_version = get_cache_version(dataset_id=dataset_id)
            
            # Call cached function
            df = _load_dataset_dataframe_cached(
                table_name=dataset.table_name,
                columns_to_load=columns_to_load,
                dataset_id=dataset_id,
                limit=limit,
                offset=offset,
                order_by_recent=order_by_recent,
                cache_version=cache_version,
            )
        
        logger.info(
            f"Loaded {len(df)} rows from dataset {dataset_id} "
            f"(limit={limit}, offset={offset}, images={include_image_columns})"
        )
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to load dataset DataFrame: {e}", exc_info=True)
        raise DatabaseError(
            f"Failed to load dataset DataFrame: {e}", operation="load_dataset_dataframe"
        ) from e


def get_dataset_row_count(session: Session, dataset_id: int) -> int:
    """
    Get total row count for dataset.
    
    Note: This function uses Streamlit caching for performance.
    
    Args:
        session: Database session
        dataset_id: Dataset ID
        
    Returns:
        Total number of rows (0 if table doesn't exist)
    """
    dataset = session.get(DatasetConfig, dataset_id)
    if not dataset:
        return 0
    
    # In test mode, bypass caching and use the passed session directly
    if _is_test_mode():
        from src.utils.validation import table_exists
        
        if not table_exists(session, dataset.table_name):
            logger.warning(f"Table '{dataset.table_name}' does not exist. Returning 0 rows.")
            return 0
        
        try:
            quoted_table = quote_identifier(dataset.table_name)
            query = text(f"SELECT COUNT(*) FROM {quoted_table}")
            result = session.execute(query)
            count = result.scalar() or 0
            return count
        except Exception as e:
            logger.error(f"Failed to get row count: {e}", exc_info=True)
            return 0
    else:
        # Get cache version for invalidation
        cache_version = get_cache_version(dataset_id=dataset_id)
        
        # Call cached function
        return _get_dataset_row_count_cached(
            table_name=dataset.table_name,
            dataset_id=dataset_id,
            cache_version=cache_version,
        )


def get_dataset_columns(
    session: Session,
    dataset_id: int,
    include_image_columns: bool = False,
) -> list[str]:
    """
    Get list of column names for dataset.
    
    Args:
        session: Database session
        dataset_id: Dataset ID
        include_image_columns: Whether to include image columns
        
    Returns:
        List of column names
    """
    dataset = session.get(DatasetConfig, dataset_id)
    if not dataset:
        return []
    
    from src.config.settings import UNIQUE_ID_COLUMN_NAME
    
    # Ensure columns_config exists
    if not dataset.columns_config:
        raise ValidationError(
            f"Dataset {dataset_id} has invalid columns_config (None or empty)",
            field="columns_config",
            value=dataset_id,
        )
    
    # Filter out unique_id from columns_config if it exists (legacy data)
    config_columns = [
        col for col in dataset.columns_config.keys() 
        if col != "unique_id" and col != UNIQUE_ID_COLUMN_NAME
    ]
    columns = config_columns + [UNIQUE_ID_COLUMN_NAME]
    
    if not include_image_columns and dataset.image_columns:
        columns = [col for col in columns if col not in dataset.image_columns]
    
    return columns


@st.cache_data(
    ttl=300,  # 5 minutes
    show_spinner=False,
    max_entries=100,  # Limit cache size
)
def _load_enriched_dataset_dataframe_cached(
    table_name: str,
    columns_to_load: list[str],
    enriched_dataset_id: int,
    limit: int,
    offset: int,
    order_by_recent: bool,
    cache_version: int,
) -> pd.DataFrame:
    """
    Internal cached function for loading enriched dataset DataFrame.
    
    This function is cached and takes only hashable parameters.
    The session is obtained inside this function.
    """
    from src.database.connection import get_session
    
    with get_session() as session:
        # Build SELECT query
        quoted_columns = [quote_identifier(col) for col in columns_to_load]
        columns_str = ", ".join(quoted_columns)
        quoted_table = quote_identifier(table_name)
        query = f"SELECT {columns_str} FROM {quoted_table}"
        
        # Add ordering
        if order_by_recent:
            query += " ORDER BY rowid DESC"
        
        # Add limit and offset
        query += f" LIMIT {limit}"
        if offset > 0:
            query += f" OFFSET {offset}"
        
        # Execute query
        result = session.execute(text(query))
        rows = result.fetchall()
        
        if not rows:
            return pd.DataFrame(columns=columns_to_load)
        
        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=columns_to_load)
        
        logger.debug(
            f"Cached load: {len(df)} rows from enriched dataset {enriched_dataset_id} "
            f"(limit={limit}, offset={offset})"
        )
        
        return df


@st.cache_data(
    ttl=300,  # 5 minutes
    show_spinner=False,
    max_entries=100,  # Limit cache size
)
def _get_enriched_dataset_row_count_cached(
    table_name: str,
    enriched_dataset_id: int,
    cache_version: int,
) -> int:
    """Internal cached function for getting enriched dataset row count."""
    from src.database.connection import get_session
    from src.utils.validation import table_exists
    
    with get_session() as session:
        if not table_exists(session, table_name):
            logger.warning(f"Table '{table_name}' does not exist. Returning 0 rows.")
            return 0
        
        try:
            quoted_table = quote_identifier(table_name)
            query = text(f"SELECT COUNT(*) FROM {quoted_table}")
            result = session.execute(query)
            count = result.scalar() or 0
            return count
        except Exception as e:
            logger.error(f"Failed to get row count: {e}", exc_info=True)
            return 0


def load_enriched_dataset_dataframe(
    session: Session,
    enriched_dataset_id: int,
    limit: int = 10000,
    offset: int = 0,
    include_image_columns: bool = False,
    order_by_recent: bool = True,
) -> pd.DataFrame:
    """
    Load enriched dataset data into DataFrame.
    
    Loads data from enriched table with options for:
    - Limiting rows (pagination)
    - Excluding image columns by default
    - Ordering by most recent
    
    Note: This function uses Streamlit caching for performance.
    Cache is automatically invalidated when data changes.
    
    Args:
        session: Database session
        enriched_dataset_id: Enriched dataset ID
        limit: Maximum number of rows to load (default 10,000)
        offset: Number of rows to skip (for pagination)
        include_image_columns: Whether to include image columns (default False)
        order_by_recent: Order by rowid DESC (most recent first, default True)
        
    Returns:
        DataFrame with enriched dataset data
        
    Raises:
        ValidationError: If enriched dataset not found
        DatabaseError: If loading fails
    """
    enriched_dataset = session.get(EnrichedDataset, enriched_dataset_id)
    if not enriched_dataset:
        raise ValidationError(f"Enriched dataset with ID {enriched_dataset_id} not found")
    
    try:
        # Get all columns from the enriched table
        from sqlalchemy import inspect
        inspector = inspect(session.bind)
        table_columns = [col["name"] for col in inspector.get_columns(enriched_dataset.enriched_table_name)]
        
        # Exclude image columns if requested
        columns_to_load = table_columns.copy()
        if not include_image_columns:
            # Get source dataset to check image columns
            source_dataset = session.get(DatasetConfig, enriched_dataset.source_dataset_id)
            if source_dataset and source_dataset.image_columns and source_dataset.columns_config:
                columns_to_load = [
                    col for col in columns_to_load if col not in source_dataset.image_columns
                ]
        
        # In test mode, bypass caching and use the passed session directly
        if _is_test_mode():
            # Build SELECT query
            quoted_columns = [quote_identifier(col) for col in columns_to_load]
            columns_str = ", ".join(quoted_columns)
            quoted_table = quote_identifier(enriched_dataset.enriched_table_name)
            query = f"SELECT {columns_str} FROM {quoted_table}"
            
            # Add ordering
            if order_by_recent:
                query += " ORDER BY rowid DESC"
            
            # Add limit and offset
            query += f" LIMIT {limit}"
            if offset > 0:
                query += f" OFFSET {offset}"
            
            # Execute query using the passed session
            result = session.execute(text(query))
            rows = result.fetchall()
            
            if not rows:
                df = pd.DataFrame(columns=columns_to_load)
            else:
                df = pd.DataFrame(rows, columns=columns_to_load)
        else:
            # Get cache version for invalidation
            cache_version = get_cache_version(enriched_dataset_id=enriched_dataset_id)
            
            # Call cached function
            df = _load_enriched_dataset_dataframe_cached(
                table_name=enriched_dataset.enriched_table_name,
                columns_to_load=columns_to_load,
                enriched_dataset_id=enriched_dataset_id,
                limit=limit,
                offset=offset,
                order_by_recent=order_by_recent,
                cache_version=cache_version,
            )
        
        logger.info(
            f"Loaded {len(df)} rows from enriched dataset {enriched_dataset_id} "
            f"(limit={limit}, offset={offset}, images={include_image_columns})"
        )
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to load enriched dataset DataFrame: {e}", exc_info=True)
        raise DatabaseError(
            f"Failed to load enriched dataset DataFrame: {e}", operation="load_enriched_dataset_dataframe"
        ) from e


def get_enriched_dataset_row_count(session: Session, enriched_dataset_id: int) -> int:
    """
    Get total row count for enriched dataset.
    
    Note: This function uses Streamlit caching for performance.
    
    Args:
        session: Database session
        enriched_dataset_id: Enriched dataset ID
        
    Returns:
        Total number of rows (0 if table doesn't exist)
    """
    enriched_dataset = session.get(EnrichedDataset, enriched_dataset_id)
    if not enriched_dataset:
        return 0
    
    # In test mode, bypass caching and use the passed session directly
    if _is_test_mode():
        from src.utils.validation import table_exists
        
        if not table_exists(session, enriched_dataset.enriched_table_name):
            logger.warning(f"Table '{enriched_dataset.enriched_table_name}' does not exist. Returning 0 rows.")
            return 0
        
        try:
            quoted_table = quote_identifier(enriched_dataset.enriched_table_name)
            query = text(f"SELECT COUNT(*) FROM {quoted_table}")
            result = session.execute(query)
            count = result.scalar() or 0
            return count
        except Exception as e:
            logger.error(f"Failed to get row count: {e}", exc_info=True)
            return 0
    else:
        # Get cache version for invalidation
        cache_version = get_cache_version(enriched_dataset_id=enriched_dataset_id)
        
        # Call cached function
        return _get_enriched_dataset_row_count_cached(
            table_name=enriched_dataset.enriched_table_name,
            enriched_dataset_id=enriched_dataset_id,
            cache_version=cache_version,
        )


def get_enriched_dataset_columns(
    session: Session,
    enriched_dataset_id: int,
    include_image_columns: bool = False,
) -> list[str]:
    """
    Get list of column names for enriched dataset.
    
    Args:
        session: Database session
        enriched_dataset_id: Enriched dataset ID
        include_image_columns: Whether to include image columns
        
    Returns:
        List of column names
    """
    enriched_dataset = session.get(EnrichedDataset, enriched_dataset_id)
    if not enriched_dataset:
        return []
    
    try:
        from sqlalchemy import inspect
        inspector = inspect(session.bind)
        columns = [col["name"] for col in inspector.get_columns(enriched_dataset.enriched_table_name)]
        
        if not include_image_columns:
            # Get source dataset to check image columns
            source_dataset = session.get(DatasetConfig, enriched_dataset.source_dataset_id)
            if source_dataset and source_dataset.image_columns:
                columns = [col for col in columns if col not in source_dataset.image_columns]
        
        return columns
    except Exception as e:
        logger.error(f"Failed to get enriched dataset columns: {e}", exc_info=True)
        return []

