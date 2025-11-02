"""
Table management service for CSV Wrangler v1.1.

Provides utilities for copying table structures and data,
and managing enriched tables.
"""
from typing import Any, Optional

import pandas as pd
from sqlalchemy import Column, Float, Integer, MetaData, Table, Text, inspect, text
from sqlalchemy.orm import Session

from src.utils.errors import DatabaseError
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def copy_table_structure(
    session: Session,
    source_table_name: str,
    target_table_name: str,
    additional_columns: list[tuple[str, type]] = None,
) -> None:
    """
    Copy table structure from source to target.
    
    Creates a new table with the same structure as source,
    optionally adding additional columns.
    
    Args:
        session: Database session
        source_table_name: Name of source table
        target_table_name: Name of target table to create
        additional_columns: List of (column_name, column_type) tuples to add
        
    Raises:
        DatabaseError: If table creation fails
    """
    try:
        inspector = inspect(session.bind)
        
        # Get source table columns
        source_columns = inspector.get_columns(source_table_name)
        
        # Create metadata for new table
        metadata = MetaData()
        columns = []
        
        # Copy all columns from source
        for col in source_columns:
            col_name = col["name"]
            col_type = col["type"]
            
            # Create column (SQLite mostly uses Text, Integer, Real)
            if "INTEGER" in str(col_type).upper():
                from sqlalchemy import Integer
                new_col = Column(col_name, Integer)
            elif "REAL" in str(col_type).upper() or "FLOAT" in str(col_type).upper():
                from sqlalchemy import Float
                new_col = Column(col_name, Float)
            else:
                # Default to Text for SQLite
                new_col = Column(col_name, Text)
            
            # Preserve primary key
            if col.get("primary_key"):
                new_col.primary_key = True
            
            columns.append(new_col)
        
        # Add additional columns if provided
        if additional_columns:
            for col_name, col_type in additional_columns:
                if col_type == int:
                    from sqlalchemy import Integer
                    columns.append(Column(col_name, Integer))
                elif col_type == float:
                    from sqlalchemy import Float
                    columns.append(Column(col_name, Float))
                else:
                    columns.append(Column(col_name, Text))
        
        # Create table
        new_table = Table(target_table_name, metadata, *columns)
        metadata.create_all(bind=session.bind)
        
        logger.info(
            f"Created table {target_table_name} with structure from {source_table_name}"
        )
        
    except Exception as e:
        logger.error(f"Failed to copy table structure: {e}", exc_info=True)
        raise DatabaseError(
            f"Failed to copy table structure: {e}", operation="copy_table_structure"
        ) from e


def copy_table_data(
    session: Session,
    source_table_name: str,
    target_table_name: str,
) -> int:
    """
    Copy all data from source table to target table.
    
    Both tables must have the same structure.
    
    Args:
        session: Database session
        source_table_name: Name of source table
        target_table_name: Name of target table
        
    Returns:
        Number of rows copied
        
    Raises:
        DatabaseError: If data copy fails
    """
    try:
        # Use SQL INSERT INTO ... SELECT for efficient copy
        query = text(f"INSERT INTO {target_table_name} SELECT * FROM {source_table_name}")
        result = session.execute(query)
        rows_copied = result.rowcount
        
        session.commit()
        
        logger.info(
            f"Copied {rows_copied} rows from {source_table_name} to {target_table_name}"
        )
        
        return rows_copied
        
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to copy table data: {e}", exc_info=True)
        raise DatabaseError(
            f"Failed to copy table data: {e}", operation="copy_table_data"
        ) from e


def add_column_to_table(
    session: Session,
    table_name: str,
    column_name: str,
    column_type: str = "TEXT",
) -> None:
    """
    Add a new column to an existing table.
    
    Args:
        session: Database session
        table_name: Name of table
        column_name: Name of new column
        column_type: SQLite type (TEXT, INTEGER, REAL)
        
    Raises:
        DatabaseError: If column addition fails
    """
    try:
        # SQLite ALTER TABLE ADD COLUMN
        query = text(
            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
        )
        session.execute(query)
        session.commit()
        
        logger.info(f"Added column {column_name} ({column_type}) to table {table_name}")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to add column: {e}", exc_info=True)
        raise DatabaseError(
            f"Failed to add column: {e}", operation="add_column_to_table"
        ) from e


def create_index_on_column(
    session: Session,
    table_name: str,
    column_name: str,
    index_name: Optional[str] = None,
    include_not_null_filter: bool = False,
) -> None:
    """
    Create an index on a table column.
    
    Args:
        session: Database session
        table_name: Name of table
        column_name: Name of column to index
        index_name: Optional custom index name (auto-generated if None)
        include_not_null_filter: If True, create filtered index for NOT NULL values only
        
    Raises:
        DatabaseError: If index creation fails
    """
    try:
        # Generate index name if not provided
        if index_name is None:
            # Sanitize table and column names for index name
            safe_table = table_name.replace(".", "_").replace("-", "_")
            safe_column = column_name.replace(".", "_").replace("-", "_")
            index_suffix = "_not_null" if include_not_null_filter else ""
            index_name = f"idx_{safe_table}_{safe_column}{index_suffix}"
        
        # Create index with optional WHERE clause for NOT NULL filter
        if include_not_null_filter:
            query = text(
                f"CREATE INDEX IF NOT EXISTS {index_name} "
                f"ON {table_name}({column_name}) "
                f"WHERE {column_name} IS NOT NULL"
            )
        else:
            query = text(
                f"CREATE INDEX IF NOT EXISTS {index_name} "
                f"ON {table_name}({column_name})"
            )
        
        session.execute(query)
        session.commit()
        
        logger.info(f"Created index {index_name} on {table_name}({column_name})")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to create index: {e}", exc_info=True)
        raise DatabaseError(
            f"Failed to create index: {e}", operation="create_index_on_column"
        ) from e


def get_table_row_count(session: Session, table_name: str) -> int:
    """
    Get row count for a table.
    
    Args:
        session: Database session
        table_name: Name of table
        
    Returns:
        Number of rows in table
    """
    try:
        query = text(f"SELECT COUNT(*) FROM {table_name}")
        result = session.execute(query)
        count = result.scalar() or 0
        return count
    except Exception as e:
        logger.error(f"Failed to get row count: {e}", exc_info=True)
        return 0


def get_new_rows_since_sync(
    session: Session,
    source_table_name: str,
    enriched_table_name: str,
    unique_id_column: str = "uuid_value",
) -> pd.DataFrame:
    """
    Get rows from source table that don't exist in enriched table.
    
    Compares by unique_id to find new rows.
    
    Args:
        session: Database session
        source_table_name: Name of source table
        enriched_table_name: Name of enriched table
        unique_id_column: Column name for unique ID comparison
        
    Returns:
        DataFrame with new rows
    """
    try:
        # Get all unique_ids from enriched table
        enriched_query = text(
            f"SELECT {unique_id_column} FROM {enriched_table_name}"
        )
        enriched_result = session.execute(enriched_query)
        enriched_ids = {row[0] for row in enriched_result.fetchall()}
        
        if not enriched_ids:
            # Enriched table is empty, return all rows from source
            source_query = text(f"SELECT * FROM {source_table_name}")
            source_result = session.execute(source_query)
            rows = source_result.fetchall()
            if rows:
                return pd.DataFrame(rows, columns=source_result.keys())
            return pd.DataFrame()
        
        # Get rows from source that aren't in enriched
        # SQLite doesn't support parameterized table names, so we need to construct query
        source_query = text(f"SELECT * FROM {source_table_name}")
        source_result = session.execute(source_query)
        rows = source_result.fetchall()
        
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame(rows, columns=source_result.keys())
        
        # Filter to only new rows
        if unique_id_column in df.columns:
            new_df = df[~df[unique_id_column].isin(enriched_ids)]
            return new_df
        
        return pd.DataFrame()
        
    except Exception as e:
        logger.error(f"Failed to get new rows: {e}", exc_info=True)
        return pd.DataFrame()


def insert_dataframe_to_table(
    session: Session,
    table_name: str,
    df: pd.DataFrame,
) -> int:
    """
    Insert DataFrame rows into table.
    
    Args:
        session: Database session
        table_name: Name of target table
        df: DataFrame to insert
        
    Returns:
        Number of rows inserted
    """
    try:
        if df.empty:
            return 0
        
        # Use pandas to_sql for efficient insertion
        rows_inserted = df.to_sql(
            table_name,
            session.bind,
            if_exists="append",
            index=False,
            method="multi",
        )
        
        session.commit()
        
        logger.info(f"Inserted {rows_inserted} rows into {table_name}")
        
        return rows_inserted
        
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to insert DataFrame: {e}", exc_info=True)
        raise DatabaseError(
            f"Failed to insert DataFrame: {e}", operation="insert_dataframe_to_table"
        ) from e


def update_enriched_column_values(
    session: Session,
    table_name: str,
    column_name: str,
    df: pd.DataFrame,
    unique_id_column: str = "uuid_value",
) -> int:
    """
    Update enriched column values for existing rows.
    
    Matches rows by unique_id and updates the enriched column.
    
    Args:
        session: Database session
        table_name: Name of table
        column_name: Name of enriched column to update
        df: DataFrame with unique_id and enriched values
        unique_id_column: Column name for unique ID
        
    Returns:
        Number of rows updated
    """
    try:
        if df.empty or unique_id_column not in df.columns or column_name not in df.columns:
            return 0
        
        updated_count = 0
        
        # Update row by row (SQLite doesn't have great bulk update support)
        for _, row in df.iterrows():
            unique_id = row[unique_id_column]
            enriched_value = row[column_name]
            
            # Handle None/NaN values
            if pd.isna(enriched_value):
                value_str = "NULL"
            else:
                value_str = f"'{str(enriched_value).replace("'", "''")}'"
            
            query = text(
                f"UPDATE {table_name} SET {column_name} = {value_str} "
                f"WHERE {unique_id_column} = '{unique_id}'"
            )
            result = session.execute(query)
            updated_count += result.rowcount
        
        session.commit()
        
        logger.info(f"Updated {updated_count} rows in {table_name}.{column_name}")
        
        return updated_count
        
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to update enriched values: {e}", exc_info=True)
        raise DatabaseError(
            f"Failed to update enriched values: {e}",
            operation="update_enriched_column_values",
        ) from e

