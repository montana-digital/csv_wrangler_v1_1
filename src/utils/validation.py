"""
Validation utilities for CSV Wrangler.

Provides input validation and sanitization functions.
"""
import re
from typing import Any

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

