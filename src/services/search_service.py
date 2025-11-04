"""
Search service for Knowledge Base.

Provides fast, indexed search across Knowledge Tables and enriched datasets.
Implements two-phase search: presence flags first, detailed retrieval on-demand.
"""
import time
from typing import Any, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.database.models import EnrichedDataset, KnowledgeTable
from src.database.repository import KnowledgeTableRepository
from src.services.knowledge_service import standardize_key_value
from src.utils.errors import ValidationError
from src.utils.logging_config import get_logger
from src.utils.validation import quote_identifier, sanitize_column_name

logger = get_logger(__name__)

# Valid data types for search
VALID_DATA_TYPES = ["phone_numbers", "emails", "web_domains"]


def search_knowledge_base(
    session: Session,
    search_value: str,
    data_type: str,
    source_filters: Optional[list[str]] = None,
) -> dict[str, Any]:
    """
    Phase 1: Fast presence search across all Knowledge Tables and enriched datasets.
    
    Searches for standardized Key_ID across matching data_type sources.
    Returns presence flags only - no full data retrieval.
    
    Args:
        session: Database session
        search_value: Raw search value (will be standardized)
        data_type: Data type to search (phone_numbers, emails, web_domains)
        source_filters: Optional list of Knowledge Table names or dataset IDs to filter by
        
    Returns:
        Dictionary with:
        - presence: {
            "knowledge_tables": [
                {"table_name": str, "table_id": int, "name": str, "row_count": int, "has_data": bool}
            ],
            "enriched_datasets": [
                {"dataset_id": int, "name": str, "enriched_table_name": str, 
                 "source_column": str, "enriched_column": str, "row_count": int}
            ]
          }
        - standardized_key_id: str or None
        - search_stats: {"total_sources": int, "matched_sources": int, "search_time_ms": float}
        
    Raises:
        ValidationError: If invalid data_type or search_value
    """
    start_time = time.time()
    
    # Validate data_type
    if data_type not in VALID_DATA_TYPES:
        raise ValidationError(
            f"Invalid data_type: {data_type}. Must be one of {VALID_DATA_TYPES}",
            field="data_type",
            value=data_type,
        )
    
    # Standardize search value
    standardized_key_id = standardize_key_value(search_value, data_type)
    
    if standardized_key_id is None:
        # Invalid search value - return empty results
        return {
            "presence": {
                "knowledge_tables": [],
                "enriched_datasets": [],
            },
            "standardized_key_id": None,
            "search_stats": {
                "total_sources": 0,
                "matched_sources": 0,
                "search_time_ms": (time.time() - start_time) * 1000,
            },
        }
    
    # Get all Knowledge Tables of matching data_type
    repo = KnowledgeTableRepository(session)
    all_knowledge_tables = repo.get_by_data_type(data_type)
    
    # Apply source filters if provided
    if source_filters:
        # Filter by Knowledge Table names (assumes source_filters contains names)
        all_knowledge_tables = [
            kt for kt in all_knowledge_tables if kt.name in source_filters
        ]
    
    # Search Knowledge Tables (fast - uses Key_ID index)
    knowledge_table_results = []
    for kt in all_knowledge_tables:
        try:
            # Use indexed Key_ID column for fast lookup
            quoted_table = quote_identifier(kt.table_name)
            quoted_key_id = quote_identifier("Key_ID")
            count_query = text(
                f"SELECT COUNT(*) FROM {quoted_table} WHERE {quoted_key_id} = :key_id"
            )
            result = session.execute(count_query, {"key_id": standardized_key_id})
            row_count = result.scalar() or 0
            
            knowledge_table_results.append({
                "table_name": kt.table_name,
                "table_id": kt.id,
                "name": kt.name,
                "row_count": int(row_count),
                "has_data": row_count > 0,
            })
        except Exception as e:
            logger.warning(f"Failed to search Knowledge Table {kt.name}: {e}")
            continue
    
    # Get all enriched datasets with matching function type
    all_enriched = session.query(EnrichedDataset).all()
    matching_enriched = [
        ed
        for ed in all_enriched
        if data_type in ed.enrichment_config.values()
    ]
    
    # Apply source filters for enriched datasets if provided
    if source_filters:
        # Filter by dataset IDs (assuming source_filters can contain IDs as strings)
        matching_enriched = [
            ed for ed in matching_enriched
            if str(ed.id) in source_filters or ed.name in source_filters
        ]
    
    # Search enriched datasets (fast - uses enriched column indexes)
    enriched_dataset_results = []
    for enriched_dataset in matching_enriched:
        # Find enriched columns using matching function
        matching_columns = [
            col_name
            for col_name, func_name in enriched_dataset.enrichment_config.items()
            if func_name == data_type
        ]
        
        for col_name in matching_columns:
            # Sanitize column name to match enriched column naming convention
            # (enriched columns are created with sanitized names)
            sanitized_col_name = sanitize_column_name(col_name)
            enriched_col_name = f"{sanitized_col_name}_enriched_{data_type}"
            
            if enriched_dataset.columns_added and enriched_col_name in enriched_dataset.columns_added:
                try:
                    # Use indexed enriched column for fast lookup
                    # Note: enriched_col_name is already sanitized (no spaces), but quote for safety
                    quoted_table = quote_identifier(enriched_dataset.enriched_table_name)
                    quoted_col = quote_identifier(enriched_col_name)
                    count_query = text(
                        f"SELECT COUNT(*) FROM {quoted_table} "
                        f"WHERE {quoted_col} = :key_id"
                    )
                    result = session.execute(count_query, {"key_id": standardized_key_id})
                    row_count = result.scalar() or 0
                    
                    enriched_dataset_results.append({
                        "dataset_id": enriched_dataset.id,
                        "name": enriched_dataset.name,
                        "enriched_table_name": enriched_dataset.enriched_table_name,
                        "source_column": col_name,
                        "enriched_column": enriched_col_name,
                        "row_count": int(row_count),
                    })
                except Exception as e:
                    logger.warning(
                        f"Failed to search enriched dataset {enriched_dataset.name} "
                        f"column {enriched_col_name}: {e}"
                    )
                    continue
    
    # Calculate statistics
    total_sources = len(knowledge_table_results) + len(enriched_dataset_results)
    matched_sources = sum(1 for kt in knowledge_table_results if kt["has_data"]) + sum(
        1 for ed in enriched_dataset_results if ed["row_count"] > 0
    )
    search_time_ms = (time.time() - start_time) * 1000
    
    return {
        "presence": {
            "knowledge_tables": knowledge_table_results,
            "enriched_datasets": enriched_dataset_results,
        },
        "standardized_key_id": standardized_key_id,
        "search_stats": {
            "total_sources": total_sources,
            "matched_sources": matched_sources,
            "search_time_ms": search_time_ms,
        },
    }


def get_knowledge_table_data_for_key(
    session: Session,
    knowledge_table_id: int,
    key_id: str,
    limit: int = 1000,
) -> pd.DataFrame:
    """
    Phase 2: Retrieve full data from specific Knowledge Table for a Key_ID.
    
    Fast lookup using Key_ID index. Returns all columns for matching rows.
    
    Args:
        session: Database session
        knowledge_table_id: Knowledge Table ID
        key_id: Standardized Key_ID to search for
        limit: Maximum number of rows to return (default 1000)
        
    Returns:
        DataFrame with all columns for matching rows
        
    Raises:
        ValidationError: If Knowledge Table not found
    """
    repo = KnowledgeTableRepository(session)
    knowledge_table = repo.get_by_id(knowledge_table_id)
    
    if not knowledge_table:
        raise ValidationError(
            f"Knowledge Table with ID {knowledge_table_id} not found",
            field="knowledge_table_id",
            value=knowledge_table_id,
        )
    
    try:
        # Fast lookup using Key_ID index
        quoted_table = quote_identifier(knowledge_table.table_name)
        quoted_key_id = quote_identifier("Key_ID")
        query = text(
            f"SELECT * FROM {quoted_table} "
            f"WHERE {quoted_key_id} = :key_id "
            f"LIMIT :limit"
        )
        result = session.execute(
            query, {"key_id": key_id, "limit": limit}
        )
        rows = result.fetchall()
        
        if rows:
            columns = list(result.keys())
            df = pd.DataFrame(rows, columns=columns)
            return df
        else:
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(
            f"Failed to retrieve data from Knowledge Table {knowledge_table_id}: {e}",
            exc_info=True,
        )
        raise ValidationError(
            f"Failed to retrieve data: {e}",
            field="knowledge_table_id",
            value=knowledge_table_id,
        ) from e


def get_enriched_dataset_data_for_key(
    session: Session,
    enriched_dataset_id: int,
    enriched_column: str,
    key_id: str,
    limit: int = 1000,
) -> pd.DataFrame:
    """
    Phase 2: Retrieve full data from enriched dataset matching a Key_ID.
    
    Fast lookup using enriched column index. Returns all columns for matching rows.
    
    Args:
        session: Database session
        enriched_dataset_id: Enriched Dataset ID
        enriched_column: Name of enriched column to search (e.g., "phone_enriched_phone_numbers")
        key_id: Standardized Key_ID to search for
        limit: Maximum number of rows to return (default 1000)
        
    Returns:
        DataFrame with all columns for matching rows
        
    Raises:
        ValidationError: If enriched dataset not found or column doesn't exist
    """
    enriched_dataset = session.get(EnrichedDataset, enriched_dataset_id)
    
    if not enriched_dataset:
        raise ValidationError(
            f"Enriched dataset with ID {enriched_dataset_id} not found",
            field="enriched_dataset_id",
            value=enriched_dataset_id,
        )
    
    if not enriched_dataset.columns_added:
        raise ValidationError(
            f"Enriched dataset {enriched_dataset_id} has invalid columns_added (None or empty)",
            field="columns_added",
            value=enriched_dataset_id,
        )
    if enriched_column not in enriched_dataset.columns_added:
        raise ValidationError(
            f"Enriched column '{enriched_column}' not found in dataset {enriched_dataset_id}",
            field="enriched_column",
            value=enriched_column,
        )
    
    try:
        # Fast lookup using enriched column index
        quoted_table = quote_identifier(enriched_dataset.enriched_table_name)
        quoted_col = quote_identifier(enriched_column)
        query = text(
            f"SELECT * FROM {quoted_table} "
            f"WHERE {quoted_col} = :key_id "
            f"LIMIT :limit"
        )
        result = session.execute(
            query, {"key_id": key_id, "limit": limit}
        )
        rows = result.fetchall()
        
        if rows:
            columns = list(result.keys())
            df = pd.DataFrame(rows, columns=columns)
            return df
        else:
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(
            f"Failed to retrieve data from enriched dataset {enriched_dataset_id}: {e}",
            exc_info=True,
        )
        raise ValidationError(
            f"Failed to retrieve data: {e}",
            field="enriched_dataset_id",
            value=enriched_dataset_id,
        ) from e

