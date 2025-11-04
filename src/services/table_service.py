"""
Table management service for CSV Wrangler v1.1.

Provides utilities for copying table structures and data,
and managing enriched tables.
"""
from typing import Any, Optional

import pandas as pd
from sqlalchemy import Column, Float, Integer, MetaData, Table, Text, inspect, text
from sqlalchemy.orm import Session

from src.utils.errors import DatabaseError, ValidationError
from src.utils.logging_config import get_logger
from src.utils.validation import column_exists, quote_identifier, table_exists

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
        # Validate source table exists
        if not table_exists(session, source_table_name):
            raise ValidationError(
                f"Source table '{source_table_name}' does not exist",
                field="source_table_name",
                value=source_table_name,
            )
        
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
        # Quote table names for safety (handles edge cases)
        quoted_target = quote_identifier(target_table_name)
        quoted_source = quote_identifier(source_table_name)
        query = text(f"INSERT INTO {quoted_target} SELECT * FROM {quoted_source}")
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
        # Validate table exists
        if not table_exists(session, table_name):
            raise ValidationError(
                f"Table '{table_name}' does not exist",
                field="table_name",
                value=table_name,
            )
        
        # Check if column already exists
        if column_exists(session, table_name, column_name):
            raise ValidationError(
                f"Column '{column_name}' already exists in table '{table_name}'",
                field="column_name",
                value=column_name,
            )
        
        # SQLite ALTER TABLE ADD COLUMN
        # Quote identifiers to handle spaces and special characters
        quoted_table = quote_identifier(table_name)
        quoted_column = quote_identifier(column_name)
        query = text(
            f"ALTER TABLE {quoted_table} ADD COLUMN {quoted_column} {column_type}"
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
        # Validate table and column exist
        if not table_exists(session, table_name):
            raise ValidationError(
                f"Table '{table_name}' does not exist",
                field="table_name",
                value=table_name,
            )
        
        if not column_exists(session, table_name, column_name):
            raise ValidationError(
                f"Column '{column_name}' does not exist in table '{table_name}'",
                field="column_name",
                value=column_name,
            )
        
        # Generate index name if not provided
        if index_name is None:
            # Sanitize table and column names for index name
            safe_table = table_name.replace(".", "_").replace("-", "_")
            safe_column = column_name.replace(".", "_").replace("-", "_")
            index_suffix = "_not_null" if include_not_null_filter else ""
            index_name = f"idx_{safe_table}_{safe_column}{index_suffix}"
        
        # Create index with optional WHERE clause for NOT NULL filter
        # Quote identifiers to handle spaces and special characters
        quoted_table = quote_identifier(table_name)
        quoted_column = quote_identifier(column_name)
        if include_not_null_filter:
            query = text(
                f"CREATE INDEX IF NOT EXISTS {index_name} "
                f"ON {quoted_table}({quoted_column}) "
                f"WHERE {quoted_column} IS NOT NULL"
            )
        else:
            query = text(
                f"CREATE INDEX IF NOT EXISTS {index_name} "
                f"ON {quoted_table}({quoted_column})"
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
        # Quote table name for safety
        quoted_table = quote_identifier(table_name)
        query = text(f"SELECT COUNT(*) FROM {quoted_table}")
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
        # Quote identifiers to handle spaces and special characters
        quoted_unique_id = quote_identifier(unique_id_column)
        quoted_enriched_table = quote_identifier(enriched_table_name)
        enriched_query = text(
            f"SELECT {quoted_unique_id} FROM {quoted_enriched_table}"
        )
        enriched_result = session.execute(enriched_query)
        enriched_ids = {row[0] for row in enriched_result.fetchall()}
        
        if not enriched_ids:
            # Enriched table is empty, return all rows from source
            quoted_source_table = quote_identifier(source_table_name)
            source_query = text(f"SELECT * FROM {quoted_source_table}")
            source_result = session.execute(source_query)
            rows = source_result.fetchall()
            if rows:
                return pd.DataFrame(rows, columns=source_result.keys())
            return pd.DataFrame()
        
        # Get rows from source that aren't in enriched
        # SQLite doesn't support parameterized table names, so we need to construct query
        quoted_source_table = quote_identifier(source_table_name)
        source_query = text(f"SELECT * FROM {quoted_source_table}")
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
        # Quote identifiers to handle spaces and special characters
        quoted_table = quote_identifier(table_name)
        quoted_column = quote_identifier(column_name)
        quoted_unique_id = quote_identifier(unique_id_column)
        
        for _, row in df.iterrows():
            unique_id = row[unique_id_column]
            enriched_value = row[column_name]
            
            # Handle None/NaN values
            if pd.isna(enriched_value):
                value_str = "NULL"
            else:
                # Escape single quotes in value
                value_str = f"'{str(enriched_value).replace("'", "''")}'"
            
            # Escape single quotes in unique_id
            unique_id_escaped = str(unique_id).replace("'", "''")
            query = text(
                f"UPDATE {quoted_table} SET {quoted_column} = {value_str} "
                f"WHERE {quoted_unique_id} = '{unique_id_escaped}'"
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

