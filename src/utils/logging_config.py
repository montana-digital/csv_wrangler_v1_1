"""
Structured logging configuration for CSV Wrangler.

Implements JSON logging with correlation IDs for distributed debugging.
"""
import json
import logging
import sys
import uuid
from datetime import datetime
from typing import Any, Optional

from src.config.settings import LOG_FORMAT, LOG_LEVEL


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add correlation ID if present
        if hasattr(record, "request_id"):
            log_data["requestId"] = record.request_id

        # Add user ID if present
        if hasattr(record, "user_id"):
            log_data["userId"] = record.user_id

        # Add any extra fields
        if hasattr(record, "extra_data"):
            log_data.update(record.extra_data)

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "traceback": self.formatException(record.exc_info),
            }

        return json.dumps(log_data)


class CorrelationIDFilter(logging.Filter):
    """Filter to add correlation ID to log records."""

    def __init__(self, request_id: Optional[str] = None):
        super().__init__()
        self.request_id = request_id or str(uuid.uuid4())

    def filter(self, record: logging.LogRecord) -> bool:
        """Add correlation ID to log record."""
        record.request_id = self.request_id
        return True


def setup_logging(
    level: Optional[str] = None,
    format_type: Optional[str] = None,
    request_id: Optional[str] = None,
) -> logging.Logger:
    """
    Setup structured logging for the application.
    
    Args:
        level: Log level (DEBUG, INFO, WARN, ERROR, FATAL)
        format_type: Log format ('json' or 'text')
        request_id: Correlation ID for request tracking
        
    Returns:
        Configured logger instance
    """
    log_level = level or LOG_LEVEL
    log_format = format_type or LOG_FORMAT

    # Get root logger
    logger = logging.getLogger("csv_wrangler")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Remove existing handlers
    logger.handlers.clear()

    # Create handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)

    # Set formatter
    if log_format.lower() == "json":
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

    handler.setFormatter(formatter)

    # Add correlation ID filter
    if request_id:
        handler.addFilter(CorrelationIDFilter(request_id))
    else:
        handler.addFilter(CorrelationIDFilter())

    logger.addHandler(handler)

    # Prevent propagation to root logger
    logger.propagate = False

    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get logger instance with correlation ID support.
    
    Args:
        name: Logger name (defaults to 'csv_wrangler')
        
    Returns:
        Logger instance
    """
    logger_name = name or "csv_wrangler"
    logger = logging.getLogger(logger_name)

    # If logger has no handlers, setup default logging
    if not logger.handlers:
        setup_logging()

    return logger


class ErrorContext:
    """Context manager for error tracking with correlation IDs."""

    def __init__(self, operation: str, request_id: Optional[str] = None):
        self.operation = operation
        self.request_id = request_id or str(uuid.uuid4())
        self.logger = get_logger()

    def __enter__(self):
        """Enter context."""
        # Add request ID to all log records in this context
        for handler in self.logger.handlers:
            handler.addFilter(CorrelationIDFilter(self.request_id))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context."""
        if exc_type:
            # Log error with full context
            self.logger.error(
                f"Error in {self.operation}",
                extra={
                    "operation": self.operation,
                    "requestId": self.request_id,
                    "exception_type": exc_type.__name__ if exc_type else None,
                    "exception_message": str(exc_val) if exc_val else None,
                },
                exc_info=(exc_type, exc_val, exc_tb),
            )
        return False  # Don't suppress exceptions

