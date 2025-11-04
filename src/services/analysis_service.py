"""
Analysis service for CSV Wrangler Data Geek page.

Handles data analysis operations: GroupBy, Pivot, Merge, Join, Concat, Apply, Map.
Results are stored as parquet files and tracked in database.
"""
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from sqlalchemy.orm import Session

from src.config.settings import ANALYSIS_RESULTS_DIR, UNIQUE_ID_COLUMN_NAME
from src.database.models import DataAnalysis, DatasetConfig
from src.database.repository import DataAnalysisRepository, DatasetRepository
from src.services.dataframe_service import load_dataset_dataframe
from src.services.export_service import filter_by_date_range
from src.utils.errors import DatabaseError, ValidationError
from src.utils.logging_config import get_logger
from src.utils.validation import validate_file_path, validate_foreign_key, validate_string_length

logger = get_logger(__name__)


def load_filtered_dataset(
    session: Session,
    dataset_id: int,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    date_column: Optional[str] = None,
) -> pd.DataFrame:
    """
    Load dataset with optional date range filtering.
    
    Args:
        session: Database session
        dataset_id: Dataset ID
        start_date: Optional start date for filtering
        end_date: Optional end date for filtering
        date_column: Optional date column name (auto-detected if None)
        
    Returns:
        Filtered DataFrame
    """
    # Load dataset (without limit for analysis operations)
    df = load_dataset_dataframe(
        session=session,
        dataset_id=dataset_id,
        limit=1000000,  # Large limit for analysis
        offset=0,
        include_image_columns=False,
        order_by_recent=False,
    )
    
    # Apply date filtering if requested
    if (start_date is not None or end_date is not None) and date_column:
        if date_column not in df.columns:
            raise ValidationError(
                f"Date column '{date_column}' not found in dataset",
                field="date_column",
                value=date_column,
            )
        df = filter_by_date_range(df, date_column, start_date, end_date)
    
    return df


def detect_date_columns(df: pd.DataFrame) -> list[str]:
    """
    Detect date/datetime columns in DataFrame.
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        List of column names that appear to be dates
    """
    date_columns = []
    
    for col in df.columns:
        if col == UNIQUE_ID_COLUMN_NAME:
            continue
        
        # Check if already datetime type
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            date_columns.append(col)
            continue
        
        # Check column name patterns
        col_lower = col.lower()
        if any(pattern in col_lower for pattern in ["date", "time", "created", "updated", "timestamp"]):
            # Try to convert sample values
            try:
                sample = df[col].dropna().head(10)
                if len(sample) > 0:
                    pd.to_datetime(sample.iloc[0], errors="raise")
                    date_columns.append(col)
            except (ValueError, TypeError):
                pass
    
    return date_columns


def execute_groupby(
    df: pd.DataFrame,
    group_columns: list[str],
    aggregations: dict[str, list[str]],
) -> pd.DataFrame:
    """
    Execute GroupBy operation.
    
    Args:
        df: Source DataFrame
        group_columns: Columns to group by
        aggregations: Dict mapping column names to list of aggregation functions
                     e.g., {"amount": ["sum", "mean"], "quantity": ["count"]}
        
    Returns:
        Grouped and aggregated DataFrame
        
    Raises:
        ValidationError: If columns don't exist or invalid configuration
    """
    # Validate columns exist
    for col in group_columns:
        if col not in df.columns:
            raise ValidationError(
                f"Group column '{col}' not found in dataset",
                field="group_columns",
                value=col,
            )
    
    for col, funcs in aggregations.items():
        if col not in df.columns:
            raise ValidationError(
                f"Aggregation column '{col}' not found in dataset",
                field="aggregations",
                value=col,
            )
    
    try:
        # Perform groupby
        grouped = df.groupby(group_columns)
        
        # Build aggregation dictionary
        agg_dict = {}
        for col, funcs in aggregations.items():
            agg_dict[col] = funcs
        
        result = grouped.agg(agg_dict)
        
        # Flatten column names if multi-level
        if isinstance(result.columns, pd.MultiIndex):
            result.columns = [
                f"{col}_{func}" if col != func else col
                for col, func in result.columns
            ]
        
        result = result.reset_index()
        
        logger.info(f"GroupBy executed: {len(result)} groups from {len(df)} rows")
        return result
        
    except Exception as e:
        logger.error(f"Failed to execute GroupBy: {e}", exc_info=True)
        raise ValidationError(
            f"Failed to execute GroupBy operation: {e}",
            field="groupby",
            value=str(e),
        ) from e


def execute_pivot(
    df: pd.DataFrame,
    index: str,
    columns: str,
    values: str,
    aggfunc: str = "sum",
) -> pd.DataFrame:
    """
    Execute Pivot Table operation.
    
    Args:
        df: Source DataFrame
        index: Column to use as index
        columns: Column to use as columns
        values: Column to aggregate
        aggfunc: Aggregation function (sum, mean, count, etc.)
        
    Returns:
        Pivot table DataFrame
        
    Raises:
        ValidationError: If columns don't exist or invalid configuration
    """
    # Validate columns exist
    for col_name, col_type in [(index, "index"), (columns, "columns"), (values, "values")]:
        if col_name not in df.columns:
            raise ValidationError(
                f"Pivot {col_type} column '{col_name}' not found in dataset",
                field=col_type,
                value=col_name,
            )
    
    try:
        # Perform pivot
        result = df.pivot_table(
            index=index,
            columns=columns,
            values=values,
            aggfunc=aggfunc,
            fill_value=0,
        )
        
        result = result.reset_index()
        
        logger.info(f"Pivot executed: {len(result)} rows")
        return result
        
    except Exception as e:
        logger.error(f"Failed to execute Pivot: {e}", exc_info=True)
        raise ValidationError(
            f"Failed to execute Pivot operation: {e}",
            field="pivot",
            value=str(e),
        ) from e


def execute_merge(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    left_on: list[str],
    right_on: list[str],
    how: str = "inner",
) -> pd.DataFrame:
    """
    Execute Merge operation.
    
    Args:
        df1: Left DataFrame
        df2: Right DataFrame
        left_on: Columns from df1 to join on
        right_on: Columns from df2 to join on
        how: Join type (inner, left, right, outer)
        
    Returns:
        Merged DataFrame
        
    Raises:
        ValidationError: If columns don't exist or invalid configuration
    """
    # Validate columns exist
    for col in left_on:
        if col not in df1.columns:
            raise ValidationError(
                f"Left join column '{col}' not found in source dataset",
                field="left_on",
                value=col,
            )
    
    for col in right_on:
        if col not in df2.columns:
            raise ValidationError(
                f"Right join column '{col}' not found in secondary dataset",
                field="right_on",
                value=col,
            )
    
    if len(left_on) != len(right_on):
        raise ValidationError(
            "Number of left and right join columns must match",
            field="join_keys",
            value=f"left: {len(left_on)}, right: {len(right_on)}",
        )
    
    try:
        # Perform merge
        result = pd.merge(
            df1,
            df2,
            left_on=left_on,
            right_on=right_on,
            how=how,
            suffixes=("_left", "_right"),
        )
        
        logger.info(f"Merge executed: {len(result)} rows from {len(df1)} + {len(df2)}")
        return result
        
    except Exception as e:
        logger.error(f"Failed to execute Merge: {e}", exc_info=True)
        raise ValidationError(
            f"Failed to execute Merge operation: {e}",
            field="merge",
            value=str(e),
        ) from e


def execute_concat(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    axis: int = 0,
    ignore_index: bool = True,
) -> pd.DataFrame:
    """
    Execute Concat operation.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        axis: 0 for vertical (rows), 1 for horizontal (columns)
        ignore_index: Whether to ignore index when concatenating
        
    Returns:
        Concatenated DataFrame
        
    Raises:
        ValidationError: If invalid configuration
    """
    try:
        # Perform concat
        result = pd.concat([df1, df2], axis=axis, ignore_index=ignore_index)
        
        logger.info(f"Concat executed: {len(result)} rows/cols from {len(df1)} + {len(df2)}")
        return result
        
    except Exception as e:
        logger.error(f"Failed to execute Concat: {e}", exc_info=True)
        raise ValidationError(
            f"Failed to execute Concat operation: {e}",
            field="concat",
            value=str(e),
        ) from e


def save_analysis_result(df: pd.DataFrame, analysis_id: int) -> Path:
    """
    Save analysis result DataFrame to parquet file.
    
    Args:
        df: DataFrame to save
        analysis_id: Analysis ID for filename
        
    Returns:
        Path to saved parquet file
        
    Raises:
        DatabaseError: If save fails
    """
    try:
        # Ensure directory exists
        ANALYSIS_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        
        # Generate file path
        file_path = ANALYSIS_RESULTS_DIR / f"analysis_{analysis_id}.parquet"
        
        # Remove placeholder file if it exists
        placeholder_path = ANALYSIS_RESULTS_DIR / "temp_placeholder.parquet"
        if placeholder_path.exists():
            try:
                placeholder_path.unlink()
            except Exception:
                pass  # Ignore errors deleting placeholder
        
        # Save to parquet
        df.to_parquet(file_path, index=False, engine="pyarrow")
        
        logger.info(f"Saved analysis result to {file_path}")
        return file_path
        
    except Exception as e:
        logger.error(f"Failed to save analysis result: {e}", exc_info=True)
        raise DatabaseError(
            f"Failed to save analysis result: {e}", operation="save_analysis_result"
        ) from e


def load_analysis_result(file_path: str) -> pd.DataFrame:
    """
    Load analysis result DataFrame from parquet file.
    
    Args:
        file_path: Path to parquet file
        
    Returns:
        Loaded DataFrame
        
    Raises:
        DatabaseError: If load fails
    """
    try:
        path = Path(file_path)
        if not path.exists():
            raise ValidationError(
                f"Analysis result file not found: {file_path}",
                field="file_path",
                value=file_path,
            )
        
        df = pd.read_parquet(path, engine="pyarrow")
        
        logger.info(f"Loaded analysis result from {file_path}: {len(df)} rows")
        return df
        
    except ValidationError:
        raise
    except Exception as e:
        logger.error(f"Failed to load analysis result: {e}", exc_info=True)
        raise DatabaseError(
            f"Failed to load analysis result: {e}", operation="load_analysis_result"
        ) from e


def create_analysis(
    session: Session,
    name: str,
    operation_type: str,
    source_dataset_id: int,
    operation_config: dict[str, Any],
    visualization_config: Optional[dict[str, Any]] = None,
    secondary_dataset_id: Optional[int] = None,
    date_range_start: Optional[datetime] = None,
    date_range_end: Optional[datetime] = None,
    date_column: Optional[str] = None,
) -> DataAnalysis:
    """
    Create a new data analysis and execute the operation.
    
    Args:
        session: Database session
        name: Analysis name
        operation_type: Type of operation (groupby, pivot, merge, join, concat, apply, map)
        source_dataset_id: Source dataset ID
        operation_config: Operation configuration (varies by operation type)
        visualization_config: Optional visualization configuration
        secondary_dataset_id: Optional secondary dataset ID (for merge/join/concat)
        date_range_start: Optional start date for filtering
        date_range_end: Optional end date for filtering
        date_column: Optional date column name
        
    Returns:
        Created DataAnalysis instance
        
    Raises:
        ValidationError: If configuration is invalid
        DatabaseError: If creation fails
    """
    # Validate inputs
    name = validate_string_length(name, 255, "Analysis name")
    if date_column:
        date_column = validate_string_length(date_column, 255, "date_column")
    
    # Validate operation type
    valid_operations = ["groupby", "pivot", "merge", "join", "concat", "apply", "map"]
    if operation_type not in valid_operations:
        raise ValidationError(
            f"Invalid operation type: {operation_type}. Must be one of {valid_operations}",
            field="operation_type",
            value=operation_type,
        )
    
    # Validate foreign keys
    validate_foreign_key(session, DatasetConfig, source_dataset_id, "source_dataset_id")
    if secondary_dataset_id is not None:
        validate_foreign_key(session, DatasetConfig, secondary_dataset_id, "secondary_dataset_id")
    
    # Get source dataset (now guaranteed to exist)
    dataset_repo = DatasetRepository(session)
    source_dataset = dataset_repo.get_by_id(source_dataset_id)
    
    # Validate date_column exists in source dataset if provided
    if date_column:
        if not source_dataset.columns_config or date_column not in source_dataset.columns_config:
            raise ValidationError(
                f"Date column '{date_column}' not found in source dataset '{source_dataset.name}'",
                field="date_column",
                value=date_column,
            )
    
    # Validate operation_config structure based on operation_type
    source_columns = list(source_dataset.columns_config.keys()) if source_dataset.columns_config else []
    from src.utils.validation import validate_operation_config
    operation_config = validate_operation_config(operation_type, operation_config, source_dataset_columns=source_columns)
    
    # Load and filter source data
    df_source = load_filtered_dataset(
        session, source_dataset_id, date_range_start, date_range_end, date_column
    )
    
    # Execute operation based on type
    result_df = None
    
    if operation_type == "groupby":
        group_columns = operation_config.get("group_columns", [])
        aggregations = operation_config.get("aggregations", {})
        result_df = execute_groupby(df_source, group_columns, aggregations)
        
    elif operation_type == "pivot":
        index = operation_config.get("index")
        columns = operation_config.get("columns")
        values = operation_config.get("values")
        aggfunc = operation_config.get("aggfunc", "sum")
        result_df = execute_pivot(df_source, index, columns, values, aggfunc)
        
    elif operation_type in ["merge", "join"]:
        if not secondary_dataset_id:
            raise ValidationError(
                "Secondary dataset required for merge/join operations",
                field="secondary_dataset_id",
                value=secondary_dataset_id,
            )
        
        secondary_dataset = dataset_repo.get_by_id(secondary_dataset_id)
        if not secondary_dataset:
            raise ValidationError(
                f"Secondary dataset with ID {secondary_dataset_id} not found",
                field="secondary_dataset_id",
                value=secondary_dataset_id,
            )
        
        # Load secondary dataset
        df_secondary = load_filtered_dataset(
            session, secondary_dataset_id, date_range_start, date_range_end, date_column
        )
        
        left_on = operation_config.get("left_on", [])
        right_on = operation_config.get("right_on", [])
        how = operation_config.get("how", "inner")
        result_df = execute_merge(df_source, df_secondary, left_on, right_on, how)
        
    elif operation_type == "concat":
        if not secondary_dataset_id:
            raise ValidationError(
                "Secondary dataset required for concat operation",
                field="secondary_dataset_id",
                value=secondary_dataset_id,
            )
        
        secondary_dataset = dataset_repo.get_by_id(secondary_dataset_id)
        if not secondary_dataset:
            raise ValidationError(
                f"Secondary dataset with ID {secondary_dataset_id} not found",
                field="secondary_dataset_id",
                value=secondary_dataset_id,
            )
        
        # Load secondary dataset
        df_secondary = load_filtered_dataset(
            session, secondary_dataset_id, date_range_start, date_range_end, date_column
        )
        
        axis = operation_config.get("axis", 0)
        ignore_index = operation_config.get("ignore_index", True)
        result_df = execute_concat(df_source, df_secondary, axis, ignore_index)
        
    elif operation_type in ["apply", "map"]:
        # TODO: Implement apply/map operations
        raise ValidationError(
            "Apply/Map operations not yet implemented",
            field="operation_type",
            value=operation_type,
        )
    
    if result_df is None or result_df.empty:
        raise ValidationError(
            "Operation produced no results",
            field="operation",
            value=operation_type,
        )
    
    # Generate temporary file path (will be updated after we get the ID)
    # We need to create the record first to get the ID, but we can't save the file yet
    # So we'll use a placeholder and update it immediately after
    temp_file_path = str(ANALYSIS_RESULTS_DIR / "temp_placeholder.parquet")
    
    # Create analysis record with temporary path
    analysis = DataAnalysis(
        name=name,
        operation_type=operation_type,
        source_dataset_id=source_dataset_id,
        secondary_dataset_id=secondary_dataset_id,
        operation_config=operation_config,
        visualization_config=visualization_config,
        date_range_start=date_range_start,
        date_range_end=date_range_end,
        date_column=date_column,
        result_file_path=temp_file_path,  # Temporary placeholder
        source_updated_at=source_dataset.updated_at,
    )
    
    # Save to database first to get ID
    repo = DataAnalysisRepository(session)
    analysis = repo.create(analysis)
    
    # Now save result to parquet with actual ID
    result_file_path = save_analysis_result(result_df, analysis.id)
    result_file_path_str = str(result_file_path)
    
    # Validate file path before storing
    result_file_path_str = validate_file_path(result_file_path_str, max_length=500, check_exists=True, field_name="result_file_path")
    analysis.result_file_path = result_file_path_str
    
    # Update with actual file path
    analysis = repo.update(analysis)
    
    logger.info(f"Created analysis: {name} (ID: {analysis.id}, Type: {operation_type})")
    
    return analysis


def refresh_analysis(session: Session, analysis_id: int) -> DataAnalysis:
    """
    Refresh analysis by recalculating from current source data.
    
    Args:
        session: Database session
        analysis_id: Analysis ID to refresh
        
    Returns:
        Updated DataAnalysis instance
        
    Raises:
        ValidationError: If analysis not found
        DatabaseError: If refresh fails
    """
    repo = DataAnalysisRepository(session)
    analysis = repo.get_by_id(analysis_id)
    
    if not analysis:
        raise ValidationError(
            f"Analysis with ID {analysis_id} not found",
            field="analysis_id",
            value=analysis_id,
        )
    
    # Get current source dataset
    dataset_repo = DatasetRepository(session)
    source_dataset = dataset_repo.get_by_id(analysis.source_dataset_id)
    if not source_dataset:
        raise ValidationError(
            f"Source dataset not found for analysis {analysis_id}",
            field="source_dataset_id",
            value=analysis.source_dataset_id,
        )
    
    # Reload and filter source data
    df_source = load_filtered_dataset(
        session,
        analysis.source_dataset_id,
        analysis.date_range_start,
        analysis.date_range_end,
        analysis.date_column,
    )
    
    # Ensure operation_config exists
    if not analysis.operation_config:
        raise ValidationError(
            f"Analysis {analysis.id} has invalid operation_config (None or empty)",
            field="operation_config",
            value=analysis.id,
        )
    
    # Re-execute operation
    result_df = None
    
    if analysis.operation_type == "groupby":
        group_columns = analysis.operation_config.get("group_columns", [])
        aggregations = analysis.operation_config.get("aggregations", {})
        result_df = execute_groupby(df_source, group_columns, aggregations)
        
    elif analysis.operation_type == "pivot":
        index = analysis.operation_config.get("index")
        columns = analysis.operation_config.get("columns")
        values = analysis.operation_config.get("values")
        aggfunc = analysis.operation_config.get("aggfunc", "sum")
        result_df = execute_pivot(df_source, index, columns, values, aggfunc)
        
    elif analysis.operation_type in ["merge", "join"]:
        if not analysis.secondary_dataset_id:
            raise ValidationError(
                "Secondary dataset missing for merge/join operation",
                field="secondary_dataset_id",
                value=None,
            )
        
        df_secondary = load_filtered_dataset(
            session,
            analysis.secondary_dataset_id,
            analysis.date_range_start,
            analysis.date_range_end,
            analysis.date_column,
        )
        
        left_on = analysis.operation_config.get("left_on", [])
        right_on = analysis.operation_config.get("right_on", [])
        how = analysis.operation_config.get("how", "inner")
        result_df = execute_merge(df_source, df_secondary, left_on, right_on, how)
        
    elif analysis.operation_type == "concat":
        if not analysis.secondary_dataset_id:
            raise ValidationError(
                "Secondary dataset missing for concat operation",
                field="secondary_dataset_id",
                value=None,
            )
        
        df_secondary = load_filtered_dataset(
            session,
            analysis.secondary_dataset_id,
            analysis.date_range_start,
            analysis.date_range_end,
            analysis.date_column,
        )
        
        axis = analysis.operation_config.get("axis", 0)
        ignore_index = analysis.operation_config.get("ignore_index", True)
        result_df = execute_concat(df_source, df_secondary, axis, ignore_index)
    
    if result_df is None or result_df.empty:
        raise ValidationError(
            "Refresh produced no results",
            field="refresh",
            value=analysis.operation_type,
        )
    
    # Update file path (delete old file, save new)
    old_path = Path(analysis.result_file_path)
    if old_path.exists():
        old_path.unlink()
    
    result_file_path = save_analysis_result(result_df, analysis.id)
    result_file_path_str = str(result_file_path)
    
    # Validate file path before storing
    result_file_path_str = validate_file_path(result_file_path_str, max_length=500, check_exists=True, field_name="result_file_path")
    analysis.result_file_path = result_file_path_str
    analysis.last_refreshed_at = datetime.now()
    analysis.source_updated_at = source_dataset.updated_at
    
    # Update in database
    analysis = repo.update(analysis)
    
    logger.info(f"Refreshed analysis: {analysis.name} (ID: {analysis.id})")
    
    return analysis


def check_refresh_needed(session: Session, analysis: DataAnalysis) -> bool:
    """
    Check if analysis needs refresh based on source dataset updates.
    
    Args:
        session: Database session
        analysis: DataAnalysis instance to check
        
    Returns:
        True if refresh is needed, False otherwise
    """
    dataset_repo = DatasetRepository(session)
    source_dataset = dataset_repo.get_by_id(analysis.source_dataset_id)
    
    if not source_dataset:
        return False
    
    # Refresh the dataset to get latest updated_at
    session.refresh(source_dataset)
    
    # Check if source dataset was updated after last refresh
    # Use >= to account for microsecond precision issues
    return source_dataset.updated_at >= analysis.last_refreshed_at and source_dataset.updated_at > analysis.source_updated_at


def delete_analysis(session: Session, analysis_id: int) -> None:
    """
    Delete analysis and its parquet file.
    
    Args:
        session: Database session
        analysis_id: Analysis ID to delete
        
    Raises:
        ValidationError: If analysis not found
        DatabaseError: If deletion fails
    """
    repo = DataAnalysisRepository(session)
    analysis = repo.get_by_id(analysis_id)
    
    if not analysis:
        raise ValidationError(
            f"Analysis with ID {analysis_id} not found",
            field="analysis_id",
            value=analysis_id,
        )
    
    # Delete parquet file
    file_path = Path(analysis.result_file_path)
    if file_path.exists():
        try:
            file_path.unlink()
            logger.info(f"Deleted analysis result file: {file_path}")
        except Exception as e:
            logger.warning(f"Failed to delete analysis file {file_path}: {e}")
    
    # Delete database record
    repo.delete(analysis_id)
    
    logger.info(f"Deleted analysis: {analysis.name} (ID: {analysis_id})")


def get_all_analyses(session: Session) -> list[DataAnalysis]:
    """
    Get all analyses, ordered by creation date (newest first).
    
    Args:
        session: Database session
        
    Returns:
        List of DataAnalysis instances
    """
    repo = DataAnalysisRepository(session)
    return repo.get_all()

