"""
Custom error classes for CSV Wrangler.

Following operational vs programmer error classification:
- Operational Errors: Expected during normal operations, handle gracefully
- Programmer Errors: Bugs in code, crash with full stack trace
"""
from typing import Any, Optional


class CSVWranglerError(Exception):
    """Base exception for all CSV Wrangler errors."""

    def __init__(
        self,
        message: str,
        code: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ):
        super().__init__(message)
        self.message = message
        self.code = code or self.__class__.__name__
        self.details = details or {}
        self.cause = cause
        self.is_operational = isinstance(self, OperationalError)


class OperationalError(CSVWranglerError):
    """
    Operational errors - expected during normal operations.
    
    These should be handled gracefully:
    - Network timeouts
    - Invalid user input
    - File not found
    - Validation errors
    - Rate limits exceeded
    
    Handle: Log as WARN/ERROR, return friendly message, continue running.
    """

    pass


class ProgrammerError(CSVWranglerError):
    """
    Programmer errors - bugs in code.
    
    These indicate bugs that need fixing:
    - Null pointer exceptions
    - Type errors
    - Array out of bounds
    - Wrong function arguments
    - Database corruption
    
    Handle: Log as FATAL with full stack trace, crash fast (process.exit(1)),
    let supervisor restart. System stays in unknown state otherwise.
    """

    pass


# Operational Errors (handle gracefully)


class ValidationError(OperationalError):
    """Invalid input or data validation failure."""

    def __init__(
        self,
        message: str,
        field: Optional[str] = None,
        value: Optional[Any] = None,
        **kwargs: Any,
    ):
        details = kwargs.pop("details", {})
        if field:
            details["field"] = field
        if value is not None:
            details["value"] = str(value)
        super().__init__(message, code="VALIDATION_ERROR", details=details, **kwargs)


class FileNotFoundError(OperationalError):
    """File not found error."""

    def __init__(self, filepath: str, **kwargs: Any):
        message = f"File not found: {filepath}"
        super().__init__(
            message, code="FILE_NOT_FOUND", details={"filepath": filepath}, **kwargs
        )


class DuplicateFileError(OperationalError):
    """Duplicate filename detected."""

    def __init__(self, filename: str, dataset_id: Optional[int] = None, **kwargs: Any):
        message = f"Duplicate filename detected: {filename}"
        details = {"filename": filename}
        if dataset_id:
            details["dataset_id"] = dataset_id
        super().__init__(message, code="DUPLICATE_FILE", details=details, **kwargs)


class SchemaMismatchError(OperationalError):
    """CSV/Pickle schema doesn't match existing dataset schema."""

    def __init__(
        self,
        message: str,
        expected_columns: Optional[list[str]] = None,
        actual_columns: Optional[list[str]] = None,
        **kwargs: Any,
    ):
        details = kwargs.pop("details", {})
        if expected_columns:
            details["expected_columns"] = expected_columns
        if actual_columns:
            details["actual_columns"] = actual_columns
        super().__init__(message, code="SCHEMA_MISMATCH", details=details, **kwargs)


class FileProcessingError(OperationalError):
    """Error processing file (corrupt, invalid format, etc.)."""

    def __init__(self, message: str, filename: Optional[str] = None, **kwargs: Any):
        details = kwargs.pop("details", {})
        if filename:
            details["filename"] = filename
        super().__init__(message, code="FILE_PROCESSING_ERROR", details=details, **kwargs)


class DirectoryTreeError(OperationalError):
    """Error generating directory tree (invalid path, permission denied, etc.)."""

    def __init__(self, message: str, path: Optional[str] = None, **kwargs: Any):
        details = kwargs.pop("details", {})
        if path:
            details["path"] = path
        super().__init__(message, code="DIRECTORY_TREE_ERROR", details=details, **kwargs)


# Programmer Errors (crash with logging)


class DatabaseError(ProgrammerError):
    """Database operation failed - indicates bug or corruption."""

    def __init__(self, message: str, operation: Optional[str] = None, **kwargs: Any):
        details = kwargs.pop("details", {})
        if operation:
            details["operation"] = operation
        super().__init__(message, code="DATABASE_ERROR", details=details, **kwargs)


class ConfigurationError(ProgrammerError):
    """Invalid configuration - indicates bug in setup."""

    def __init__(self, message: str, config_key: Optional[str] = None, **kwargs: Any):
        details = kwargs.pop("details", {})
        if config_key:
            details["config_key"] = config_key
        super().__init__(message, code="CONFIGURATION_ERROR", details=details, **kwargs)

