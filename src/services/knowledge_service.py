"""
Knowledge service for CSV Wrangler.

Handles Knowledge Table initialization, uploads, statistics, and deletion.
Knowledge Tables store standardized Key_ID values for linking enriched data.
"""
import uuid
from datetime import datetime
from typing import Any, Optional

import pandas as pd
from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    text,
)
from sqlalchemy.orm import Session
from sqlalchemy.sql import func

from src.config.settings import UNIQUE_ID_COLUMN_NAME
from src.database.models import EnrichedDataset, KnowledgeTable
from src.database.repository import KnowledgeTableRepository
from src.services.enrichment_functions import get_enrichment_function
from src.services.file_import_service import import_file
from src.services.table_service import insert_dataframe_to_table
from src.utils.errors import DatabaseError, ValidationError
from src.utils.logging_config import get_logger
from src.utils.validation import (
    handle_integrity_error,
    quote_identifier,
    sanitize_table_name,
    validate_columns_config,
    validate_image_columns,
    validate_string_length,
)

logger = get_logger(__name__)

# Valid data types for Knowledge Tables
VALID_DATA_TYPES = ["phone_numbers", "emails", "web_domains"]


def standardize_key_value(value: Any, data_type: str) -> Optional[str]:
    """
    Standardize a single value using enrichment functions.
    
    Args:
        value: Value to standardize
        data_type: Data type (phone_numbers, emails, web_domains)
        
    Returns:
        Standardized value or None if standardization fails
    """
    if pd.isna(value):
        return None
    
    try:
        enrichment_func = get_enrichment_function(data_type)
        series = pd.Series([value])
        result = enrichment_func(series)
        
        if len(result) > 0 and not pd.isna(result.iloc[0]):
            return str(result.iloc[0])
        return None
    except Exception as e:
        logger.warning(f"Failed to standardize value {value}: {e}")
        return None


def generate_key_ids_for_dataframe(
    df: pd.DataFrame, primary_key_column: str, data_type: str
) -> tuple[pd.DataFrame, list[int]]:
    """
    Generate Key_ID column for entire DataFrame.
    
    Args:
        df: DataFrame to process
        primary_key_column: Column name to use as source
        data_type: Data type for standardization function
        
    Returns:
        Tuple of (DataFrame with Key_ID column, list of skipped row indices)
    """
    if primary_key_column not in df.columns:
        raise ValidationError(
            f"Primary key column '{primary_key_column}' not found in DataFrame",
            field="primary_key_column",
            value=primary_key_column,
        )
    
    df_copy = df.copy()
    enrichment_func = get_enrichment_function(data_type)
    
    # Apply standardization
    standardized_series = enrichment_func(df_copy[primary_key_column])
    
    # Create Key_ID column
    df_copy["Key_ID"] = standardized_series
    
    # Find skipped rows (where standardization failed)
    skipped_indices = df_copy[df_copy["Key_ID"].isna()].index.tolist()
    
    return df_copy, skipped_indices


def initialize_knowledge_table(
    session: Session,
    name: str,
    data_type: str,
    primary_key_column: str,
    columns_config: dict[str, dict[str, Any]],
    image_columns: list[str],
    initial_data_df: Optional[pd.DataFrame] = None,
) -> KnowledgeTable:
    """
    Initialize a new Knowledge Table.
    
    Creates a database table based on the column configuration and processes
    initial data if provided.
    
    Args:
        session: Database session
        name: Knowledge Table name (must be unique globally)
        data_type: Data type (phone_numbers, emails, web_domains)
        primary_key_column: Column name from uploaded file for Key_ID generation
        columns_config: Column configuration {"col_name": {"type": "...", "is_image": bool}}
        image_columns: List of column names containing Base64 images
        initial_data_df: Optional DataFrame with initial data to upload
        
    Returns:
        Created KnowledgeTable instance
        
    Raises:
        ValidationError: If name is duplicate or invalid data_type
        DatabaseError: If table creation fails
    """
    # Validate inputs
    # Validate string lengths
    name = validate_string_length(name, 255, "Knowledge Table name")
    primary_key_column = validate_string_length(primary_key_column, 255, "primary_key_column")
    
    # Validate data_type
    if data_type not in VALID_DATA_TYPES:
        raise ValidationError(
            f"Invalid data_type: {data_type}. Must be one of {VALID_DATA_TYPES}",
            field="data_type",
            value=data_type,
        )
    
    # Validate JSON structures
    columns_config = validate_columns_config(columns_config, allow_empty=False)
    image_columns = validate_image_columns(image_columns, columns_config=columns_config)
    
    # Validate primary_key_column exists in columns_config
    if primary_key_column not in columns_config:
        raise ValidationError(
            f"Primary key column '{primary_key_column}' not found in columns_config",
            field="primary_key_column",
            value=primary_key_column,
        )
    
    # Check if name is duplicate
    repo = KnowledgeTableRepository(session)
    existing = repo.get_by_name(name)
    if existing:
        raise ValidationError(
            f"Knowledge Table name '{name}' already exists",
            field="name",
            value=name,
        )
    
    # Generate table name with collision handling
    sanitized_name = sanitize_table_name(name)
    base_table_name = f"knowledge_{data_type}_{sanitized_name}"
    table_name = f"{base_table_name}_v1"
    
    # Use retry logic to handle race conditions
    from src.utils.validation import retry_with_backoff
    
    def check_and_generate_table_name():
        """Check if table name exists and generate unique one if needed."""
        counter = 1
        current_name = f"{base_table_name}_v1"
        # For sqlite_master queries, table name is a string literal, not identifier
        while session.execute(
            text(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{current_name.replace("'", "''")}'")
        ).fetchone():
            counter += 1
            current_name = f"{base_table_name}_v{counter}"
            if counter > 100:
                raise DatabaseError(
                    f"Unable to generate unique table name for '{name}' after 100 attempts",
                    operation="initialize_knowledge_table",
                )
        return current_name
    
    # Retry with exponential backoff to handle race conditions
    try:
        table_name = retry_with_backoff(
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
            operation="initialize_knowledge_table"
        ) from e
    
    # Validate and truncate table name if needed (max 255 chars)
    table_name = validate_string_length(table_name, 255, "table_name", allow_truncate=True)
    
    table_created = False
    try:
        # Create table dynamically
        metadata = MetaData()
        columns = [
            Column(UNIQUE_ID_COLUMN_NAME, String(36), primary_key=True),  # UUID
            Column("Key_ID", Text, nullable=False, unique=True),  # Standardized key, UNIQUE
            Column("created_at", DateTime, nullable=False, server_default=func.now()),
        ]
        
        # Add columns based on configuration
        for col_name, col_config in columns_config.items():
            # Skip uuid_value if it's in columns_config
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
        
        # Create indexes for performance
        session.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_key_id ON {table_name}(Key_ID)"))
        session.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_created_at ON {table_name}(created_at)"))
        session.commit()
        
        logger.info(f"Created Knowledge Table {table_name} for '{name}'")
        
        # Create KnowledgeTable record
        knowledge_table = KnowledgeTable(
            name=name,
            data_type=data_type,
            table_name=table_name,
            primary_key_column=primary_key_column,
            columns_config=columns_config,
            key_id_column="Key_ID",
        )
        
        repo.create(knowledge_table)
        
        # Process initial data if provided
        if initial_data_df is not None and not initial_data_df.empty:
            upload_result = upload_to_knowledge_table(
                session,
                knowledge_table.id,
                initial_data_df,
            )
            logger.info(
                f"Initial upload processed: {upload_result['added']} rows added, "
                f"{upload_result['skipped_invalid']} invalid, "
                f"{upload_result['skipped_duplicates']} duplicates"
            )
        
        logger.info(f"Initialized Knowledge Table '{name}' with data_type '{data_type}'")
        
        return knowledge_table
        
    except IntegrityError as e:
        # Rollback any uncommitted changes
        session.rollback()
        
        # Clean up orphaned table if it was created
        if table_created:
            try:
                quoted_table = quote_identifier(table_name)
                session.execute(text(f"DROP TABLE IF EXISTS {quoted_table}"))
                session.commit()
                logger.warning(f"Cleaned up orphaned Knowledge Table {table_name} after IntegrityError")
            except Exception as cleanup_error:
                logger.warning(f"Failed to cleanup orphaned table during error handling: {cleanup_error}")
        
        # Convert to ValidationError with clear message
        context = {
            "name": "Knowledge Table name",
            "table_name": "Table name",
        }
        raise handle_integrity_error(e, context) from e
        
    except Exception as e:
        # Rollback table creation if KnowledgeTable record creation fails
        if table_created:
            try:
                # Quote table name for safety
                quoted_table = quote_identifier(table_name)
                session.execute(text(f"DROP TABLE IF EXISTS {quoted_table}"))
                session.commit()
                logger.warning(f"Cleaned up orphaned Knowledge Table {table_name} after error")
            except Exception as cleanup_error:
                logger.warning(f"Failed to cleanup orphaned table during error handling: {cleanup_error}")
        
        if isinstance(e, (ValidationError, DatabaseError)):
            raise
        logger.error(f"Failed to initialize Knowledge Table: {e}", exc_info=True)
        raise DatabaseError(
            f"Failed to initialize Knowledge Table: {e}",
            operation="initialize_knowledge_table",
        ) from e


def upload_to_knowledge_table(
    session: Session,
    knowledge_table_id: int,
    df: pd.DataFrame,
) -> dict[str, Any]:
    """
    Upload DataFrame to Knowledge Table.
    
    Generates Key_IDs, validates, and inserts new records.
    
    Args:
        session: Database session
        knowledge_table_id: Knowledge Table ID
        df: DataFrame to upload
        
    Returns:
        Dictionary with upload statistics
    """
    repo = KnowledgeTableRepository(session)
    knowledge_table = repo.get_by_id(knowledge_table_id)
    
    if not knowledge_table:
        raise ValidationError(
            f"Knowledge Table with ID {knowledge_table_id} not found",
            field="knowledge_table_id",
            value=knowledge_table_id,
        )
    
    # Validate required columns are present
    if not knowledge_table.columns_config:
        raise ValidationError(
            f"Knowledge Table {knowledge_table_id} has invalid columns_config (None or empty)",
            field="columns_config",
            value=knowledge_table_id,
        )
    required_columns = set(knowledge_table.columns_config.keys())
    upload_columns = set(df.columns)
    
    missing_columns = required_columns - upload_columns
    if missing_columns:
        raise ValidationError(
            f"Missing required columns: {', '.join(missing_columns)}",
            field="columns",
            value=list(upload_columns),
        )
    
    # Generate Key_IDs
    try:
        df_with_keys, skipped_indices = generate_key_ids_for_dataframe(
            df, knowledge_table.primary_key_column, knowledge_table.data_type
        )
    except Exception as e:
        raise ValidationError(
            f"Failed to generate Key_IDs: {e}",
            field="primary_key_column",
            value=knowledge_table.primary_key_column,
        ) from e
    
    # Filter out rows with failed standardization
    valid_df = df_with_keys[~df_with_keys["Key_ID"].isna()].copy()
    invalid_df = df_with_keys[df_with_keys["Key_ID"].isna()].copy()
    
    if valid_df.empty:
        return {
            "total_rows": len(df),
            "processed": 0,
            "added": 0,
            "skipped_duplicates": 0,
            "skipped_invalid": len(df),
            "skipped_list": [
                {
                    "row_index": int(idx),
                    "reason": "Standardization failed",
                    "key_value": str(df.loc[idx, knowledge_table.primary_key_column]),
                }
                for idx in skipped_indices
            ],
        }
    
    # Check for existing Key_IDs (duplicate detection)
    from src.utils.validation import quote_identifier
    quoted_table = quote_identifier(knowledge_table.table_name)
    quoted_key_id = quote_identifier("Key_ID")
    existing_keys_query = text(
        f"SELECT DISTINCT {quoted_key_id} FROM {quoted_table} WHERE {quoted_key_id} IS NOT NULL"
    )
    existing_keys_result = session.execute(existing_keys_query)
    existing_keys = {row[0] for row in existing_keys_result.fetchall()}
    
    # Filter out duplicates
    valid_df["is_duplicate"] = valid_df["Key_ID"].isin(existing_keys)
    new_df = valid_df[~valid_df["is_duplicate"]].copy()
    duplicate_df = valid_df[valid_df["is_duplicate"]].copy()
    
    # Prepare data for insertion
    if not new_df.empty:
        # Add uuid_value (created_at is handled by database default)
        new_df[UNIQUE_ID_COLUMN_NAME] = [str(uuid.uuid4()) for _ in range(len(new_df))]
        
        # Select columns that exist in both DataFrame and table
        # Table has: uuid_value (PK), Key_ID, created_at (auto), plus columns from columns_config
        if not knowledge_table.columns_config:
            raise ValidationError(
                f"Knowledge Table {knowledge_table.id} has invalid columns_config (None or empty)",
                field="columns_config",
                value=knowledge_table.id,
            )
        config_columns = list(knowledge_table.columns_config.keys())
        required_columns = config_columns + ["Key_ID", UNIQUE_ID_COLUMN_NAME]
        
        # Filter to only columns that exist in DataFrame
        columns_to_insert = [col for col in required_columns if col in new_df.columns]
        insert_df = new_df[columns_to_insert].copy()
        
        # Insert new records
        rows_added = insert_dataframe_to_table(session, knowledge_table.table_name, insert_df)
    else:
        rows_added = 0
    
    # Build skipped list
    skipped_list = []
    
    # Invalid rows
    for idx in invalid_df.index:
        skipped_list.append({
            "row_index": int(idx),
            "reason": "Standardization failed",
            "key_value": str(df.loc[idx, knowledge_table.primary_key_column]),
        })
    
    # Duplicate rows
    for idx in duplicate_df.index:
        skipped_list.append({
            "row_index": int(idx),
            "reason": "Key_ID already exists",
            "key_value": str(duplicate_df.loc[idx, "Key_ID"]),
        })
    
    # Update knowledge_table timestamp
    knowledge_table.updated_at = datetime.now()
    repo.update(knowledge_table)
    
    return {
        "total_rows": len(df),
        "processed": len(valid_df),
        "added": rows_added,
        "skipped_duplicates": len(duplicate_df),
        "skipped_invalid": len(invalid_df),
        "skipped_list": skipped_list,
    }


def get_knowledge_table_stats(
    session: Session,
    knowledge_table_id: int,
) -> dict[str, Any]:
    """
    Calculate statistics for a Knowledge Table.
    
    Args:
        session: Database session
        knowledge_table_id: Knowledge Table ID
        
    Returns:
        Dictionary with statistics (top_20, recently_added, missing_values)
    """
    repo = KnowledgeTableRepository(session)
    knowledge_table = repo.get_by_id(knowledge_table_id)
    
    if not knowledge_table:
        raise ValidationError(
            f"Knowledge Table with ID {knowledge_table_id} not found",
            field="knowledge_table_id",
            value=knowledge_table_id,
        )
    
    # Get all enriched datasets with matching enrichment function
    all_enriched = session.query(EnrichedDataset).all()
    matching_enriched = [
        ed
        for ed in all_enriched
        if knowledge_table.data_type in ed.enrichment_config.values()
    ]
    
    # Collect Key_ID counts from enriched columns
    key_id_counts = {}
    
    for enriched_dataset in matching_enriched:
        # Find enriched columns using matching function
        if not enriched_dataset.enrichment_config:
            continue  # Skip if no enrichment config
        matching_columns = [
            col_name
            for col_name, func_name in enriched_dataset.enrichment_config.items()
            if func_name == knowledge_table.data_type
        ]
        
        for enriched_col in matching_columns:
            enriched_col_name = f"{enriched_col}_enriched_{knowledge_table.data_type}"
            
            if enriched_dataset.columns_added and enriched_col_name in enriched_dataset.columns_added:
                # Query enriched values
                query = text(
                    f"SELECT DISTINCT uuid_value, {enriched_col_name} "
                    f"FROM {enriched_dataset.enriched_table_name} "
                    f"WHERE {enriched_col_name} IS NOT NULL"
                )
                
                result = session.execute(query)
                rows = result.fetchall()
                
                # Count unique uuid_value per Key_ID (deduplicate within row)
                for uuid_val, enriched_value in rows:
                    if enriched_value in key_id_counts:
                        if uuid_val not in key_id_counts[enriched_value]:
                            key_id_counts[enriched_value].add(uuid_val)
                    else:
                        key_id_counts[enriched_value] = {uuid_val}
    
    # Get existing Key_IDs for filtering
    existing_key_ids = get_existing_key_ids(session, knowledge_table.table_name)
    
    # Convert to counts and get top 20 (only for Key_IDs that exist in Knowledge Table)
    top_20_data = [
        {"Key_ID": key_id, "count": len(uuids)}
        for key_id, uuids in key_id_counts.items()
        if key_id in existing_key_ids
    ]
    top_20_data.sort(key=lambda x: x["count"], reverse=True)
    top_20 = pd.DataFrame(top_20_data[:20]) if top_20_data else pd.DataFrame(columns=["Key_ID", "count"])
    
    # Recently added (last 20 records)
    from src.utils.validation import quote_identifier
    quoted_table = quote_identifier(knowledge_table.table_name)
    recently_query = text(
        f"SELECT * FROM {quoted_table} "
        f"ORDER BY created_at DESC LIMIT 20"
    )
    recently_result = session.execute(recently_query)
    recently_rows = recently_result.fetchall()
    
    if recently_rows:
        recently_columns = list(recently_result.keys())
        recently_added = pd.DataFrame(recently_rows, columns=recently_columns)
    else:
        recently_added = pd.DataFrame()
    
    # Missing values (values in enriched datasets but not in Knowledge Table)
    all_enriched_values = set()
    for enriched_dataset in matching_enriched:
        matching_columns = [
            col_name
            for col_name, func_name in enriched_dataset.enrichment_config.items()
            if func_name == knowledge_table.data_type
        ]
        
        for enriched_col in matching_columns:
            enriched_col_name = f"{enriched_col}_enriched_{knowledge_table.data_type}"
            
            if enriched_col_name in enriched_dataset.columns_added:
                query = text(
                    f"SELECT DISTINCT {enriched_col_name} "
                    f"FROM {enriched_dataset.enriched_table_name} "
                    f"WHERE {enriched_col_name} IS NOT NULL"
                )
                result = session.execute(query)
                all_enriched_values.update(row[0] for row in result.fetchall())
    
    existing_key_ids = get_existing_key_ids(session, knowledge_table.table_name)
    missing_values = sorted(list(all_enriched_values - existing_key_ids))[:1000]  # Limit to 1000
    missing_values_df = pd.DataFrame({"missing_key_id": missing_values}) if missing_values else pd.DataFrame(columns=["missing_key_id"])
    
    return {
        "top_20": top_20,
        "recently_added": recently_added,
        "missing_values": missing_values_df,
    }


def get_existing_key_ids(session: Session, table_name: str) -> set[str]:
    """Get all existing Key_IDs from a Knowledge Table."""
    # Quote identifiers to handle spaces in table/column names
    quoted_table = quote_identifier(table_name)
    quoted_key_id = quote_identifier("Key_ID")
    query = text(f"SELECT DISTINCT {quoted_key_id} FROM {quoted_table} WHERE {quoted_key_id} IS NOT NULL")
    result = session.execute(query)
    return {row[0] for row in result.fetchall()}


def delete_knowledge_table(session: Session, knowledge_table_id: int) -> None:
    """
    Delete a Knowledge Table and its database table.
    
    Args:
        session: Database session
        knowledge_table_id: Knowledge Table ID
        
    Raises:
        ValidationError: If Knowledge Table not found
        DatabaseError: If deletion fails
    """
    repo = KnowledgeTableRepository(session)
    knowledge_table = repo.get_by_id(knowledge_table_id)
    
    if not knowledge_table:
        raise ValidationError(
            f"Knowledge Table with ID {knowledge_table_id} not found",
            field="knowledge_table_id",
            value=knowledge_table_id,
        )
    
    table_name = knowledge_table.table_name
    
    try:
        # Drop database table - quote table name for safety
        quoted_table = quote_identifier(table_name)
        session.execute(text(f"DROP TABLE IF EXISTS {quoted_table}"))
        session.commit()
        logger.info(f"Dropped Knowledge Table: {table_name}")
        
        # Delete KnowledgeTable record
        repo.delete(knowledge_table_id)
        
        logger.info(f"Deleted Knowledge Table: {knowledge_table.name} (ID: {knowledge_table_id})")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to delete Knowledge Table: {e}", exc_info=True)
        raise DatabaseError(
            f"Failed to delete Knowledge Table: {e}",
            operation="delete_knowledge_table",
        ) from e


def get_all_knowledge_tables(session: Session) -> list[KnowledgeTable]:
    """
    Get all Knowledge Tables.
    
    Args:
        session: Database session
        
    Returns:
        List of all KnowledgeTable instances
    """
    repo = KnowledgeTableRepository(session)
    return repo.get_all()


def get_knowledge_tables_by_type(session: Session, data_type: str) -> list[KnowledgeTable]:
    """
    Get all Knowledge Tables of matching data_type.
    
    Args:
        session: Database session
        data_type: Data type (phone_numbers, emails, web_domains)
        
    Returns:
        List of KnowledgeTable instances
    """
    repo = KnowledgeTableRepository(session)
    return repo.get_by_data_type(data_type)


def find_enriched_columns_by_type(
    session: Session,
    data_type: str,
) -> list[tuple[int, str, str]]:
    """
    Find all enriched columns of matching type across datasets 1-5.
    
    Args:
        session: Database session
        data_type: Data type to match (phone_numbers, emails, web_domains)
        
    Returns:
        List of (dataset_id, enriched_table_name, enriched_column_name) tuples
    """
    all_enriched = session.query(EnrichedDataset).all()
    
    result = []
    for enriched_dataset in all_enriched:
        # Check if any enrichment config uses matching function
        matching_columns = [
            col_name
            for col_name, func_name in enriched_dataset.enrichment_config.items()
            if func_name == data_type
        ]
        
        for col_name in matching_columns:
            enriched_col_name = f"{col_name}_enriched_{data_type}"
            if enriched_col_name in enriched_dataset.columns_added:
                result.append(
                    (
                        enriched_dataset.source_dataset_id,
                        enriched_dataset.enriched_table_name,
                        enriched_col_name,
                    )
                )
    
    return result

