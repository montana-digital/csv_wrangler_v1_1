"""
Error display component for CSV Wrangler.

Provides user-friendly error display with troubleshooting tips and recovery actions.
"""
import streamlit as st
from typing import Any, Optional

from src.utils.user_messages import get_error_message, format_error_for_user
from src.utils.error_context import get_error_context


def display_error(
    error_code: str,
    exception: Optional[Exception] = None,
    custom_message: Optional[str] = None,
    show_troubleshooting: bool = True,
    show_technical: bool = False,
    recovery_callback: Optional[callable] = None,
) -> None:
    """
    Display user-friendly error message with troubleshooting tips.

    Args:
        error_code: Error code (e.g., "FILE_NOT_FOUND")
        exception: Optional exception object
        custom_message: Optional custom message to override default
        show_troubleshooting: Whether to show troubleshooting tips
        show_technical: Whether to show technical details (for debugging)
        recovery_callback: Optional callback function for recovery action button
    """
    # Format error for display
    error_info = format_error_for_user(
        error_code=error_code,
        exception=exception,
        custom_message=custom_message,
        include_technical=show_technical,
    )

    # Display error in Streamlit
    with st.container():
        # Error title and message
        st.error(f"**{error_info['title']}**\n\n{error_info['message']}")

        # Troubleshooting tips (expandable)
        if show_troubleshooting and error_info.get("troubleshooting"):
            with st.expander("💡 Troubleshooting Tips", expanded=False):
                for tip in error_info["troubleshooting"]:
                    st.markdown(f"• {tip}")

        # Recovery action button
        if error_info.get("recovery_action") and recovery_callback:
            st.button(
                error_info["recovery_action"],
                key=f"recovery_{error_code}",
                on_click=recovery_callback,
            )

        # Technical details (only if explicitly requested)
        if show_technical and error_info.get("technical_details"):
            with st.expander("🔧 Technical Details (for support)", expanded=False):
                st.code(error_info["technical_details"], language="text")

                # Also include error context if available
                if exception:
                    context = get_error_context(exception)
                    st.json(context)


def display_warning(
    message: str,
    troubleshooting: Optional[list[str]] = None,
    dismissible: bool = True,
) -> None:
    """
    Display warning message with optional troubleshooting tips.

    Args:
        message: Warning message
        troubleshooting: Optional list of troubleshooting tips
        dismissible: Whether warning can be dismissed
    """
    st.warning(message)

    if troubleshooting:
        with st.expander("💡 Suggestions", expanded=False):
            for tip in troubleshooting:
                st.markdown(f"• {tip}")


def display_info_with_help(
    message: str,
    help_text: Optional[str] = None,
) -> None:
    """
    Display info message with optional help text.

    Args:
        message: Info message
        help_text: Optional help text to display
    """
    st.info(message)

    if help_text:
        with st.expander("ℹ️ More Information", expanded=False):
            st.markdown(help_text)


def handle_exception(
    exception: Exception,
    error_code: str = "UNKNOWN_ERROR",
    context_message: Optional[str] = None,
    show_troubleshooting: bool = True,
    log_error: bool = True,
) -> None:
    """
    Handle an exception and display user-friendly error.

    Args:
        exception: The exception that occurred
        error_code: Error code for message lookup
        context_message: Optional additional context message
        show_troubleshooting: Whether to show troubleshooting tips
        log_error: Whether to log the error (default True)
    """
    # Log error if requested
    if log_error:
        from src.utils.logging_config import get_logger

        logger = get_logger(__name__)
        logger.error(
            f"Error {error_code}: {exception}",
            exc_info=True,
            extra={"error_code": error_code, "context": context_message},
        )

    # Build custom message if context provided
    custom_message = None
    if context_message:
        error_msg = get_error_message(error_code)
        custom_message = f"{error_msg.message} {context_message}"

    # Display error to user
    display_error(
        error_code=error_code,
        exception=exception,
        custom_message=custom_message,
        show_troubleshooting=show_troubleshooting,
        show_technical=False,  # Don't show technical details to end users
    )

