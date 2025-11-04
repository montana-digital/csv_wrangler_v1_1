"""
Validation utilities for CSV Wrangler.

Provides input validation and sanitization functions.
"""
import re
from typing import Any, Optional

from sqlalchemy.exc import IntegrityError

from src.utils.errors import ValidationError
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# SQL injection patterns to check for
SQL_INJECTION_PATTERNS = [
    r"(?i)(\b(SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|EXEC|EXECUTE)\b)",
    r"(?i)(\b(UNION|OR|AND)\s+\d+)",
    r"(--|#|/\*|\*/)",
    r"('|;|\\)",
]

# Valid table/column name pattern (alphanumeric and underscore only)
VALID_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def validate_table_name(name: str) -> None:
    """
    Validate table name to prevent SQL injection.
    
    Args:
        name: Table name to validate
        
    Raises:
        ValidationError: If name is invalid or contains SQL injection patterns
    """
    if not name or not name.strip():
        raise ValidationError("Table name cannot be empty", field="table_name")

    name = name.strip()

    # Check for SQL injection patterns
    for pattern in SQL_INJECTION_PATTERNS:
        if re.search(pattern, name):
            raise ValidationError(
                f"Table name contains invalid characters or SQL keywords: {name}",
                field="table_name",
                value=name,
            )

    # Check for valid identifier pattern
    if not VALID_NAME_PATTERN.match(name):
        raise ValidationError(
            f"Table name must start with letter or underscore and contain only "
            f"alphanumeric characters and underscores: {name}",
            field="table_name",
            value=name,
        )


def validate_column_name(name: str) -> None:
    """
    Validate column name to prevent SQL injection.
    
    Args:
        name: Column name to validate
        
    Raises:
        ValidationError: If name is invalid
    """
    if not name or not name.strip():
        raise ValidationError("Column name cannot be empty", field="column_name")

    name = name.strip()

    # Check for SQL injection patterns
    for pattern in SQL_INJECTION_PATTERNS:
        if re.search(pattern, name):
            raise ValidationError(
                f"Column name contains invalid characters or SQL keywords: {name}",
                field="column_name",
                value=name,
            )

    # Column names can be more flexible than table names
    # Allow spaces, hyphens, etc. but sanitize them
    if len(name) > 255:
        raise ValidationError(
            "Column name exceeds maximum length of 255 characters",
            field="column_name",
            value=name,
        )


def sanitize_table_name(name: str) -> str:
    """
    Sanitize table name for safe use in SQL.
    
    Args:
        name: Table name to sanitize
        
    Returns:
        Sanitized table name
    """
    # Replace spaces and hyphens with underscores
    sanitized = re.sub(r"[-\s]+", "_", name)

    # Remove invalid characters
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "", sanitized)

    # Ensure it starts with letter or underscore
    if sanitized and not sanitized[0].isalpha() and sanitized[0] != "_":
        sanitized = "_" + sanitized

    return sanitized.lower()


def sanitize_column_name(name: str) -> str:
    """
    Sanitize column name for safe use in SQL identifiers.
    
    Replaces spaces and special characters with underscores to create
    a valid SQL identifier. Useful when creating new column names
    (e.g., enriched columns).
    
    Args:
        name: Column name to sanitize
        
    Returns:
        Sanitized column name suitable for SQL identifiers
    """
    if not name:
        return ""
    
    # Replace spaces, hyphens, and common special chars with underscores
    sanitized = re.sub(r"[-\s.]+", "_", name)
    
    # Replace multiple consecutive underscores with single underscore
    sanitized = re.sub(r"_+", "_", sanitized)
    
    # Remove invalid characters (keep alphanumeric and underscores)
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "", sanitized)
    
    # Remove leading/trailing underscores
    sanitized = sanitized.strip("_")
    
    # Ensure it starts with letter or underscore
    if sanitized and not sanitized[0].isalpha() and sanitized[0] != "_":
        sanitized = "_" + sanitized
    
    # If empty after sanitization, use default
    if not sanitized:
        sanitized = "column"
    
    return sanitized


def quote_identifier(identifier: str) -> str:
    """
    Quote SQLite identifier for safe use in SQL queries.
    
    SQLite identifiers with spaces or special characters must be quoted
    with double quotes. This function properly escapes embedded quotes.
    
    Args:
        identifier: Table or column name to quote
        
    Returns:
        Quoted identifier (e.g., '"column name"' or '"table""name"')
        
    Example:
        >>> quote_identifier("column name")
        '"column name"'
        >>> quote_identifier('table"name')
        '"table""name"'
    """
    if not identifier:
        raise ValueError("Identifier cannot be empty")
    
    # Escape double quotes by doubling them (SQLite standard)
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def quote_identifier_if_needed(identifier: str) -> str:
    """
    Quote SQLite identifier only if it contains spaces or special characters.
    
    If the identifier is already a valid SQL identifier (alphanumeric + underscore),
    returns it unquoted. Otherwise, quotes it.
    
    Args:
        identifier: Table or column name to conditionally quote
        
    Returns:
        Quoted identifier if needed, unquoted otherwise
        
    Example:
        >>> quote_identifier_if_needed("column_name")
        'column_name'
        >>> quote_identifier_if_needed("column name")
        '"column name"'
    """
    if not identifier:
        raise ValueError("Identifier cannot be empty")
    
    # Check if identifier is valid SQL identifier (alphanumeric, underscore, no spaces)
    if VALID_NAME_PATTERN.match(identifier):
        return identifier
    
    # Contains spaces or special characters, needs quoting
    return quote_identifier(identifier)


def validate_data_type(data_type: str) -> None:
    """
    Validate data type string.
    
    Args:
        data_type: Data type to validate
        
    Raises:
        ValidationError: If data type is invalid
    """
    valid_types = ["TEXT", "INTEGER", "REAL", "BLOB"]

    if data_type.upper() not in valid_types:
        raise ValidationError(
            f"Invalid data type: {data_type}. Valid types: {valid_types}",
            field="data_type",
            value=data_type,
        )


def validate_slot_number(slot_number: int) -> None:
    """
    Validate dataset slot number.
    
    Args:
        slot_number: Slot number to validate
        
    Raises:
        ValidationError: If slot number is invalid
    """
    from src.config.settings import MAX_DATASET_SLOTS

    if not isinstance(slot_number, int):
        raise ValidationError(
            f"Slot number must be an integer, got {type(slot_number).__name__}",
            field="slot_number",
            value=slot_number,
        )

    if slot_number < 1 or slot_number > MAX_DATASET_SLOTS:
        raise ValidationError(
            f"Slot number must be between 1 and {MAX_DATASET_SLOTS}",
            field="slot_number",
            value=slot_number,
        )


def table_exists(session, table_name: str) -> bool:
    """
    Check if a table exists in the database.
    
    Args:
        session: Database session
        table_name: Name of table to check
        
    Returns:
        True if table exists, False otherwise
    """
    from sqlalchemy import inspect, text
    from sqlalchemy.orm import Session as SQLASession
    
    if not isinstance(session, SQLASession):
        return False
    
    try:
        inspector = inspect(session.bind)
        return table_name in inspector.get_table_names()
    except Exception:
        # Fallback: query sqlite_master directly using parameterized query
        try:
            result = session.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name=:table_name"),
                {"table_name": table_name}
            )
            return result.fetchone() is not None
        except Exception:
            return False


def column_exists(session, table_name: str, column_name: str) -> bool:
    """
    Check if a column exists in a table.
    
    Args:
        session: Database session
        table_name: Name of table
        column_name: Name of column to check
        
    Returns:
        True if column exists, False otherwise
        
    Raises:
        ValidationError: If table does not exist
    """
    from sqlalchemy import inspect, text
    from sqlalchemy.orm import Session as SQLASession
    
    if not isinstance(session, SQLASession):
        return False
    
    # First check if table exists
    if not table_exists(session, table_name):
        raise ValidationError(
            f"Table '{table_name}' does not exist",
            field="table_name",
            value=table_name,
        )
    
    try:
        inspector = inspect(session.bind)
        columns = inspector.get_columns(table_name)
        return any(col["name"] == column_name for col in columns)
    except Exception:
        # Fallback: use PRAGMA table_info
        try:
            quoted_table = quote_identifier(table_name)
            result = session.execute(text(f"PRAGMA table_info({quoted_table})"))
            columns = result.fetchall()
            return any(col[1] == column_name for col in columns)  # Column name is at index 1
        except Exception:
            return False


def retry_with_backoff(
    func,
    max_attempts: int = 3,
    initial_delay: float = 0.1,
    max_delay: float = 2.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,),
):
    """
    Retry a function with exponential backoff.
    
    Useful for handling race conditions in database operations like table creation.
    
    Args:
        func: Function to retry (callable)
        max_attempts: Maximum number of attempts (default 3)
        initial_delay: Initial delay in seconds (default 0.1)
        max_delay: Maximum delay in seconds (default 2.0)
        backoff_factor: Factor to multiply delay by each retry (default 2.0)
        exceptions: Tuple of exceptions to catch and retry on (default: all exceptions)
        
    Returns:
        Result of func()
        
    Raises:
        Last exception if all attempts fail
    """
    import time
    
    delay = initial_delay
    last_exception = None
    
    for attempt in range(max_attempts):
        try:
            return func()
        except exceptions as e:
            last_exception = e
            if attempt < max_attempts - 1:  # Don't sleep on last attempt
                time.sleep(min(delay, max_delay))
                delay *= backoff_factor
            else:
                # Last attempt failed, raise the exception
                raise
    
    # Should never reach here, but just in case
    if last_exception:
        raise last_exception
    raise Exception("All retry attempts failed")


def handle_integrity_error(e: IntegrityError, context: dict[str, Any] | None = None) -> ValidationError:
    """
    Convert SQLAlchemy IntegrityError to ValidationError with clear message.
    
    Analyzes the error message to determine which unique constraint failed
    and provides a user-friendly error message.
    
    Args:
        e: IntegrityError exception
        context: Optional context dict with field names to check (e.g., {"name": "Dataset name", "slot_number": "Slot number"})
        
    Returns:
        ValidationError with clear message
    """
    error_msg = str(e.orig) if hasattr(e, 'orig') else str(e)
    context = context or {}
    
    # Check for unique constraint violations
    if "UNIQUE constraint failed" in error_msg or "UNIQUE constraint" in error_msg:
        # Try to identify which field failed
        if "name" in error_msg.lower() or "dataset_config.name" in error_msg:
            field = "name"
            field_label = context.get("name", "Name")
            message = f"{field_label} already exists. Please choose a different name."
        elif "slot_number" in error_msg.lower() or "dataset_config.slot_number" in error_msg:
            field = "slot_number"
            field_label = context.get("slot_number", "Slot number")
            message = f"{field_label} is already occupied. Please choose a different slot."
        elif "table_name" in error_msg.lower():
            field = "table_name"
            field_label = context.get("table_name", "Table name")
            message = f"{field_label} already exists. This may be due to a name collision. Please try again."
        elif "enriched_table_name" in error_msg.lower() or "enriched_dataset.enriched_table_name" in error_msg:
            field = "enriched_table_name"
            field_label = context.get("enriched_table_name", "Enriched table name")
            message = f"{field_label} already exists. Please try again."
        elif "knowledge_table.name" in error_msg or "knowledge_table.table_name" in error_msg:
            if "name" in error_msg:
                field = "name"
                field_label = context.get("name", "Knowledge Table name")
            else:
                field = "table_name"
                field_label = context.get("table_name", "Table name")
            message = f"{field_label} already exists. Please choose a different name."
        else:
            # Generic unique constraint message
            field = None
            message = "A unique constraint violation occurred. This value already exists in the database."
        
        return ValidationError(
            message,
            field=field,
            value=context.get(field) if field else None,
        )
    
    # Generic integrity error
    return ValidationError(
        "Database integrity constraint violation. This may indicate duplicate data or invalid relationships.",
        details={"error": error_msg}
    )


def validate_string_length(value: str, max_length: int, field_name: str, allow_truncate: bool = False) -> str:
    """
    Validate string length and optionally truncate.
    
    Args:
        value: String value to validate
        max_length: Maximum allowed length
        field_name: Name of field for error messages
        allow_truncate: If True, truncate and warn instead of raising error
        
    Returns:
        Validated (and optionally truncated) string
        
    Raises:
        ValidationError: If value exceeds max_length and allow_truncate is False
    """
    if not isinstance(value, str):
        raise ValidationError(
            f"{field_name} must be a string, got {type(value).__name__}",
            field=field_name,
            value=value,
        )
    
    if len(value) > max_length:
        if allow_truncate:
            logger.warning(
                f"{field_name} exceeded maximum length of {max_length} characters. "
                f"Truncating from {len(value)} to {max_length} characters."
            )
            return value[:max_length]
        else:
            raise ValidationError(
                f"{field_name} exceeds maximum length of {max_length} characters (got {len(value)})",
                field=field_name,
                value=value[:100] + "..." if len(value) > 100 else value,
            )
    
    return value


def validate_columns_config(columns_config: Any, allow_empty: bool = False) -> dict[str, dict[str, Any]]:
    """
    Validate columns_config JSON structure.
    
    Expected format: {"col_name": {"type": "TEXT|INTEGER|REAL|BLOB", "is_image": bool}}
    
    Args:
        columns_config: Value to validate
        allow_empty: If True, allow empty dict
        
    Returns:
        Validated columns_config dict
        
    Raises:
        ValidationError: If structure is invalid
    """
    if not isinstance(columns_config, dict):
        raise ValidationError(
            f"columns_config must be a dictionary, got {type(columns_config).__name__}",
            field="columns_config",
            value=str(columns_config)[:100],
        )
    
    if not allow_empty and len(columns_config) == 0:
        raise ValidationError(
            "columns_config cannot be empty",
            field="columns_config",
        )
    
    from src.config.settings import UNIQUE_ID_COLUMN_NAME
    
    for col_name, col_config in columns_config.items():
        if not isinstance(col_name, str):
            raise ValidationError(
                f"Column names must be strings, got {type(col_name).__name__}",
                field="columns_config",
                value=col_name,
            )
        
        if col_name == UNIQUE_ID_COLUMN_NAME:
            logger.warning(f"Column '{UNIQUE_ID_COLUMN_NAME}' in columns_config will be ignored (it's automatically added)")
            continue
        
        if not isinstance(col_config, dict):
            raise ValidationError(
                f"Column config for '{col_name}' must be a dictionary, got {type(col_config).__name__}",
                field="columns_config",
                value=col_name,
            )
        
        # Validate "type" field
        if "type" in col_config:
            col_type = col_config["type"]
            if not isinstance(col_type, str):
                raise ValidationError(
                    f"Column type for '{col_name}' must be a string, got {type(col_type).__name__}",
                    field="columns_config",
                    value=col_name,
                )
            validate_data_type(col_type)
        
        # Validate "is_image" field
        if "is_image" in col_config:
            is_image = col_config["is_image"]
            if not isinstance(is_image, bool):
                raise ValidationError(
                    f"is_image for '{col_name}' must be a boolean, got {type(is_image).__name__}",
                    field="columns_config",
                    value=col_name,
                )
    
    return columns_config


def validate_enrichment_config(enrichment_config: Any) -> dict[str, str]:
    """
    Validate enrichment_config JSON structure and function names.
    
    Expected format: {"column_name": "function_name"}
    
    Args:
        enrichment_config: Value to validate
        
    Returns:
        Validated enrichment_config dict
        
    Raises:
        ValidationError: If structure is invalid or function names are invalid
    """
    if not isinstance(enrichment_config, dict):
        raise ValidationError(
            f"enrichment_config must be a dictionary, got {type(enrichment_config).__name__}",
            field="enrichment_config",
            value=str(enrichment_config)[:100],
        )
    
    if len(enrichment_config) == 0:
        raise ValidationError(
            "enrichment_config cannot be empty",
            field="enrichment_config",
        )
    
    # Get valid function names
    from src.services.enrichment_functions import ENRICHMENT_FUNCTIONS
    valid_functions = list(ENRICHMENT_FUNCTIONS.keys())
    
    for col_name, function_name in enrichment_config.items():
        if not isinstance(col_name, str):
            raise ValidationError(
                f"Column names in enrichment_config must be strings, got {type(col_name).__name__}",
                field="enrichment_config",
                value=col_name,
            )
        
        if not isinstance(function_name, str):
            raise ValidationError(
                f"Function names in enrichment_config must be strings, got {type(function_name).__name__}",
                field="enrichment_config",
                value=col_name,
            )
        
        # Validate function name exists
        if function_name not in valid_functions:
            raise ValidationError(
                f"Unknown enrichment function '{function_name}' for column '{col_name}'. "
                f"Valid functions: {valid_functions}",
                field="enrichment_config",
                value=col_name,
            )
    
    return enrichment_config


def validate_image_columns(image_columns: Any, columns_config: dict[str, dict[str, Any]] | None = None) -> list[str]:
    """
    Validate image_columns JSON structure and references.
    
    Expected format: ["column_name1", "column_name2", ...]
    
    Args:
        image_columns: Value to validate
        columns_config: Optional columns_config to validate references against
        
    Returns:
        Validated image_columns list
        
    Raises:
        ValidationError: If structure is invalid or references don't exist
    """
    if not isinstance(image_columns, list):
        raise ValidationError(
            f"image_columns must be a list, got {type(image_columns).__name__}",
            field="image_columns",
            value=str(image_columns)[:100],
        )
    
    validated = []
    for idx, col_name in enumerate(image_columns):
        if not isinstance(col_name, str):
            raise ValidationError(
                f"All items in image_columns must be strings, got {type(col_name).__name__} at index {idx}",
                field="image_columns",
                value=col_name,
            )
        
        # Validate reference exists in columns_config if provided
        if columns_config is not None:
            if col_name not in columns_config:
                raise ValidationError(
                    f"Image column '{col_name}' not found in columns_config",
                    field="image_columns",
                    value=col_name,
                )
        
        validated.append(col_name)
    
    return validated


def validate_columns_added(columns_added: Any) -> list[str]:
    """
    Validate columns_added JSON structure.
    
    Expected format: ["column_name1", "column_name2", ...]
    
    Args:
        columns_added: Value to validate
        
    Returns:
        Validated columns_added list
        
    Raises:
        ValidationError: If structure is invalid
    """
    if not isinstance(columns_added, list):
        raise ValidationError(
            f"columns_added must be a list, got {type(columns_added).__name__}",
            field="columns_added",
            value=str(columns_added)[:100],
        )
    
    validated = []
    for idx, col_name in enumerate(columns_added):
        if not isinstance(col_name, str):
            raise ValidationError(
                f"All items in columns_added must be strings, got {type(col_name).__name__} at index {idx}",
                field="columns_added",
                value=col_name,
            )
        validated.append(col_name)
    
    return validated


def validate_foreign_key(session, model_class: type, key_id: int, field_name: str) -> None:
    """
    Validate that a foreign key reference exists.
    
    Args:
        session: Database session
        model_class: SQLAlchemy model class to check
        key_id: Foreign key ID to validate
        field_name: Field name for error messages
        
    Raises:
        ValidationError: If foreign key doesn't exist
    """
    if not isinstance(key_id, int):
        raise ValidationError(
            f"{field_name} must be an integer, got {type(key_id).__name__}",
            field=field_name,
            value=key_id,
        )
    
    record = session.get(model_class, key_id)
    if not record:
        model_name = model_class.__name__
        raise ValidationError(
            f"{field_name} references non-existent {model_name} with ID {key_id}",
            field=field_name,
            value=key_id,
        )


def validate_file_path(file_path: str, max_length: int = 500, check_exists: bool = False, field_name: str = "file_path") -> str:
    """
    Validate file path for length, format, and optionally existence.
    
    Args:
        file_path: File path to validate
        max_length: Maximum allowed path length (default 500)
        check_exists: If True, check that file exists
        field_name: Field name for error messages
        
    Returns:
        Validated file path
        
    Raises:
        ValidationError: If path is invalid
    """
    if not isinstance(file_path, str):
        raise ValidationError(
            f"{field_name} must be a string, got {type(file_path).__name__}",
            field=field_name,
            value=file_path,
        )
    
    if not file_path or not file_path.strip():
        raise ValidationError(
            f"{field_name} cannot be empty",
            field=field_name,
        )
    
    file_path = file_path.strip()
    
    # Validate length
    if len(file_path) > max_length:
        raise ValidationError(
            f"{field_name} exceeds maximum length of {max_length} characters (got {len(file_path)})",
            field=field_name,
            value=file_path[:100] + "..." if len(file_path) > 100 else file_path,
        )
    
    # Validate path format (basic check)
    try:
        from pathlib import Path
        path_obj = Path(file_path)
        # Check for invalid characters (basic validation)
        if any(char in file_path for char in ['<', '>', '|', '?', '*']):
            raise ValidationError(
                f"{field_name} contains invalid characters",
                field=field_name,
                value=file_path,
            )
    except Exception as e:
        raise ValidationError(
            f"{field_name} is not a valid path format: {str(e)}",
            field=field_name,
            value=file_path,
        ) from e
    
    # Optionally check if file exists
    if check_exists:
        path_obj = Path(file_path)
        if not path_obj.exists():
            raise ValidationError(
                f"{field_name} references a file that does not exist: {file_path}",
                field=field_name,
                value=file_path,
            )
    
    return file_path


def validate_operation_config(operation_type: str, operation_config: Any, source_dataset_columns: Optional[list[str]] = None) -> dict[str, Any]:
    """
    Validate operation_config structure based on operation_type.
    
    Args:
        operation_type: Type of operation (groupby, pivot, merge, join, concat)
        operation_config: Operation configuration to validate
        source_dataset_columns: Optional list of source dataset column names for validation
        
    Returns:
        Validated operation_config dict
        
    Raises:
        ValidationError: If configuration is invalid
    """
    if not isinstance(operation_config, dict):
        raise ValidationError(
            f"operation_config must be a dictionary, got {type(operation_config).__name__}",
            field="operation_config",
            value=str(operation_config)[:100],
        )
    
    if operation_type == "groupby":
        # Required: group_columns (list), aggregations (dict)
        if "group_columns" not in operation_config:
            raise ValidationError(
                "operation_config for groupby must include 'group_columns' (list of column names)",
                field="operation_config",
            )
        if not isinstance(operation_config["group_columns"], list):
            raise ValidationError(
                "operation_config.group_columns must be a list",
                field="operation_config",
            )
        if len(operation_config["group_columns"]) == 0:
            raise ValidationError(
                "operation_config.group_columns cannot be empty",
                field="operation_config",
            )
        
        if "aggregations" not in operation_config:
            raise ValidationError(
                "operation_config for groupby must include 'aggregations' (dict mapping columns to aggregation functions)",
                field="operation_config",
            )
        if not isinstance(operation_config["aggregations"], dict):
            raise ValidationError(
                "operation_config.aggregations must be a dictionary",
                field="operation_config",
            )
        if len(operation_config["aggregations"]) == 0:
            raise ValidationError(
                "operation_config.aggregations cannot be empty",
                field="operation_config",
            )
        
        # Validate columns exist in source dataset if provided
        if source_dataset_columns:
            for col in operation_config["group_columns"]:
                if col not in source_dataset_columns:
                    raise ValidationError(
                        f"Group column '{col}' not found in source dataset",
                        field="operation_config",
                        value=col,
                    )
            for col in operation_config["aggregations"].keys():
                if col not in source_dataset_columns:
                    raise ValidationError(
                        f"Aggregation column '{col}' not found in source dataset",
                        field="operation_config",
                        value=col,
                    )
    
    elif operation_type == "pivot":
        # Required: index, columns, values
        if "index" not in operation_config:
            raise ValidationError(
                "operation_config for pivot must include 'index' (column name)",
                field="operation_config",
            )
        if "columns" not in operation_config:
            raise ValidationError(
                "operation_config for pivot must include 'columns' (column name)",
                field="operation_config",
            )
        if "values" not in operation_config:
            raise ValidationError(
                "operation_config for pivot must include 'values' (column name)",
                field="operation_config",
            )
        
        # Validate columns exist in source dataset if provided
        if source_dataset_columns:
            for field in ["index", "columns", "values"]:
                col = operation_config[field]
                if col not in source_dataset_columns:
                    raise ValidationError(
                        f"Pivot {field} column '{col}' not found in source dataset",
                        field="operation_config",
                        value=col,
                    )
        
        # Validate aggfunc if provided
        if "aggfunc" in operation_config:
            valid_aggfuncs = ["sum", "mean", "count", "min", "max", "first", "last"]
            if operation_config["aggfunc"] not in valid_aggfuncs:
                raise ValidationError(
                    f"Invalid aggfunc '{operation_config['aggfunc']}'. Must be one of {valid_aggfuncs}",
                    field="operation_config",
                    value=operation_config["aggfunc"],
                )
    
    elif operation_type in ["merge", "join"]:
        # Required: left_on, right_on (both lists)
        if "left_on" not in operation_config:
            raise ValidationError(
                "operation_config for merge/join must include 'left_on' (list of column names)",
                field="operation_config",
            )
        if not isinstance(operation_config["left_on"], list):
            raise ValidationError(
                "operation_config.left_on must be a list",
                field="operation_config",
            )
        if len(operation_config["left_on"]) == 0:
            raise ValidationError(
                "operation_config.left_on cannot be empty",
                field="operation_config",
            )
        
        if "right_on" not in operation_config:
            raise ValidationError(
                "operation_config for merge/join must include 'right_on' (list of column names)",
                field="operation_config",
            )
        if not isinstance(operation_config["right_on"], list):
            raise ValidationError(
                "operation_config.right_on must be a list",
                field="operation_config",
            )
        if len(operation_config["right_on"]) == 0:
            raise ValidationError(
                "operation_config.right_on cannot be empty",
                field="operation_config",
            )
        
        if len(operation_config["left_on"]) != len(operation_config["right_on"]):
            raise ValidationError(
                "operation_config.left_on and right_on must have the same length",
                field="operation_config",
            )
        
        # Validate how if provided
        if "how" in operation_config:
            valid_hows = ["inner", "left", "right", "outer"]
            if operation_config["how"] not in valid_hows:
                raise ValidationError(
                    f"Invalid how '{operation_config['how']}'. Must be one of {valid_hows}",
                    field="operation_config",
                    value=operation_config["how"],
                )
    
    elif operation_type == "concat":
        # Optional: axis (0 or 1), ignore_index (bool)
        if "axis" in operation_config:
            if operation_config["axis"] not in [0, 1]:
                raise ValidationError(
                    "operation_config.axis must be 0 (vertical) or 1 (horizontal)",
                    field="operation_config",
                    value=operation_config["axis"],
                )
        
        if "ignore_index" in operation_config:
            if not isinstance(operation_config["ignore_index"], bool):
                raise ValidationError(
                    "operation_config.ignore_index must be a boolean",
                    field="operation_config",
                    value=operation_config["ignore_index"],
                )
    
    elif operation_type in ["apply", "map"]:
        # These are not yet implemented, but we can validate the structure
        if "function" not in operation_config:
            raise ValidationError(
                "operation_config for apply/map must include 'function' (function name or lambda)",
                field="operation_config",
            )
    
    return operation_config

