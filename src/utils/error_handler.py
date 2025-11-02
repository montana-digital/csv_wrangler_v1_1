"""
Error handling utilities for CSV Wrangler.

Provides decorators and context managers for consistent error handling.
"""
from typing import Callable, Any, Optional
import functools
import streamlit as st

from src.utils.user_messages import format_error_for_user
from src.utils.error_context import get_error_context, record_action
from src.utils.logging_config import get_logger


logger = get_logger(__name__)


def handle_streamlit_error(
    error_code: str = "UNKNOWN_ERROR",
    show_troubleshooting: bool = True,
    custom_message: Optional[str] = None,
    log_error: bool = True,
    record_action_on_error: bool = True,
):
    """
    Decorator for handling errors in Streamlit UI functions.

    Wraps a function to catch exceptions and display user-friendly error messages.

    Args:
        error_code: Default error code to use if exception doesn't match known types
        show_troubleshooting: Whether to show troubleshooting tips
        custom_message: Optional custom message
        log_error: Whether to log errors
        record_action_on_error: Whether to record action on error

    Example:
        @handle_streamlit_error("FILE_PROCESSING_ERROR")
        def upload_file(file_path):
            # ... code that might raise exceptions
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                # Record action start
                if record_action_on_error:
                    record_action(
                        action=f"start_{func.__name__}",
                        details={"args_count": len(args), "kwargs_keys": list(kwargs.keys())},
                    )

                result = func(*args, **kwargs)

                # Record successful action
                if record_action_on_error:
                    record_action(action=f"complete_{func.__name__}", details={"success": True})

                return result

            except Exception as e:
                # Record failed action
                if record_action_on_error:
                    record_action(
                        action=f"error_{func.__name__}",
                        details={"error_type": type(e).__name__, "error_message": str(e)},
                    )

                # Determine error code from exception type
                actual_error_code = error_code
                if hasattr(e, "code"):
                    actual_error_code = e.code
                elif hasattr(e, "__class__"):
                    exception_name = e.__class__.__name__
                    # Map common exception types to error codes
                    if "FileNotFound" in exception_name or "NotFound" in exception_name:
                        actual_error_code = "FILE_NOT_FOUND"
                    elif "Validation" in exception_name:
                        actual_error_code = "VALIDATION_ERROR"
                    elif "Database" in exception_name:
                        actual_error_code = "DATABASE_ERROR"
                    elif "Permission" in exception_name:
                        actual_error_code = "PERMISSION_ERROR"

                # Log error
                if log_error:
                    logger.error(
                        f"Error in {func.__name__}: {e}",
                        exc_info=True,
                        extra={"error_code": actual_error_code},
                    )

                # Display user-friendly error
                error_info = format_error_for_user(
                    error_code=actual_error_code,
                    exception=e,
                    custom_message=custom_message,
                )

                st.error(f"**{error_info['title']}**\n\n{error_info['message']}")

                if show_troubleshooting and error_info.get("troubleshooting"):
                    with st.expander("💡 Troubleshooting Tips", expanded=False):
                        for tip in error_info["troubleshooting"]:
                            st.markdown(f"• {tip}")

                # Return None or re-raise based on context
                # For UI functions, typically we want to return None to continue execution
                return None

        return wrapper

    return decorator


class SafeOperation:
    """Context manager for safe operations with error handling."""

    def __init__(
        self,
        operation_name: str,
        error_code: str = "UNKNOWN_ERROR",
        show_troubleshooting: bool = True,
        log_error: bool = True,
        suppress_error: bool = False,
    ):
        """
        Initialize safe operation context manager.

        Args:
            operation_name: Name of the operation (for logging/context)
            error_code: Error code for error messages
            show_troubleshooting: Whether to show troubleshooting tips
            log_error: Whether to log errors
            suppress_error: If True, suppress error and return None; if False, re-raise
        """
        self.operation_name = operation_name
        self.error_code = error_code
        self.show_troubleshooting = show_troubleshooting
        self.log_error = log_error
        self.suppress_error = suppress_error
        self.exception: Optional[Exception] = None

    def __enter__(self):
        """Enter context."""
        record_action(f"start_{self.operation_name}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context and handle any exceptions."""
        if exc_type is None:
            # No exception, record success
            record_action(f"complete_{self.operation_name}", details={"success": True})
            return False

        # Exception occurred
        self.exception = exc_val

        # Record error
        record_action(
            f"error_{self.operation_name}",
            details={
                "error_type": exc_type.__name__,
                "error_message": str(exc_val),
            },
        )

        # Log error
        if self.log_error:
            logger.error(
                f"Error in {self.operation_name}: {exc_val}",
                exc_info=True,
                extra={"error_code": self.error_code},
            )

        # Display user-friendly error (if in Streamlit context)
        try:
            error_info = format_error_for_user(
                error_code=self.error_code,
                exception=exc_val,
            )

            st.error(f"**{error_info['title']}**\n\n{error_info['message']}")

            if self.show_troubleshooting and error_info.get("troubleshooting"):
                with st.expander("💡 Troubleshooting Tips", expanded=False):
                    for tip in error_info["troubleshooting"]:
                        st.markdown(f"• {tip}")

        except Exception:
            # If Streamlit isn't available, just log
            pass

        # Suppress or re-raise based on configuration
        return self.suppress_error


def safe_call(
    func: Callable,
    error_code: str = "UNKNOWN_ERROR",
    default_return: Any = None,
    show_troubleshooting: bool = True,
    log_error: bool = True,
) -> Any:
    """
    Safely call a function with error handling.

    Args:
        func: Function to call
        error_code: Error code for error messages
        default_return: Value to return on error (if None, will raise)
        show_troubleshooting: Whether to show troubleshooting tips
        log_error: Whether to log errors

    Returns:
        Function result or default_return on error

    Example:
        result = safe_call(process_file, "FILE_PROCESSING_ERROR", default_return={})
    """
    try:
        return func()
    except Exception as e:
        # Record error
        record_action(
            f"error_{func.__name__ if hasattr(func, '__name__') else 'unknown'}",
            details={"error_type": type(e).__name__, "error_message": str(e)},
        )

        # Log error
        if log_error:
            logger.error(f"Error in safe_call: {e}", exc_info=True, extra={"error_code": error_code})

        # Display error (if in Streamlit context)
        try:
            error_info = format_error_for_user(error_code=error_code, exception=e)
            st.error(f"**{error_info['title']}**\n\n{error_info['message']}")

            if show_troubleshooting and error_info.get("troubleshooting"):
                with st.expander("💡 Troubleshooting Tips", expanded=False):
                    for tip in error_info["troubleshooting"]:
                        st.markdown(f"• {tip}")

        except Exception:
            # Not in Streamlit context, skip UI display
            pass

        # Return default or re-raise
        if default_return is not None:
            return default_return
        raise

