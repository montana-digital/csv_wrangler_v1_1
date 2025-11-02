"""
Error context tracking for CSV Wrangler.

Tracks user actions and application state to help diagnose errors without exposing
technical details to end users.
"""
from typing import Any, Optional
from datetime import datetime
from collections import deque
import traceback


class ErrorContext:
    """Tracks context around errors for diagnostics."""

    def __init__(self, max_history: int = 50):
        """
        Initialize error context tracker.

        Args:
            max_history: Maximum number of actions to track
        """
        self.max_history = max_history
        self.action_history: deque[dict[str, Any]] = deque(maxlen=max_history)
        self.current_page: Optional[str] = None
        self.current_dataset: Optional[int] = None
        self.current_operation: Optional[str] = None

    def record_action(
        self,
        action: str,
        details: Optional[dict[str, Any]] = None,
        page: Optional[str] = None,
    ) -> None:
        """
        Record a user action.

        Args:
            action: Description of the action (e.g., "upload_file", "create_dataset")
            details: Optional dictionary with action details
            page: Optional page name where action occurred
        """
        action_record = {
            "timestamp": datetime.now().isoformat(),
            "action": action,
            "page": page or self.current_page,
            "dataset_id": self.current_dataset,
            "operation": self.current_operation,
            "details": details or {},
        }
        self.action_history.append(action_record)

    def set_current_page(self, page: str) -> None:
        """Set the current page."""
        self.current_page = page

    def set_current_dataset(self, dataset_id: Optional[int]) -> None:
        """Set the current dataset ID."""
        self.current_dataset = dataset_id

    def set_current_operation(self, operation: Optional[str]) -> None:
        """Set the current operation."""
        self.current_operation = operation

    def get_context_for_error(self, error: Exception) -> dict[str, Any]:
        """
        Get relevant context for an error.

        Args:
            error: The exception that occurred

        Returns:
            Dictionary with error context information
        """
        # Get recent actions (last 10)
        recent_actions = list(self.action_history)[-10:]

        return {
            "error_type": type(error).__name__,
            "error_message": str(error),
            "timestamp": datetime.now().isoformat(),
            "current_page": self.current_page,
            "current_dataset": self.current_dataset,
            "current_operation": self.current_operation,
            "recent_actions": recent_actions,
            "action_count": len(self.action_history),
        }

    def get_recent_actions(self, count: int = 5) -> list[dict[str, Any]]:
        """
        Get recent actions.

        Args:
            count: Number of recent actions to return

        Returns:
            List of recent action records
        """
        return list(self.action_history)[-count:]

    def clear_history(self) -> None:
        """Clear action history."""
        self.action_history.clear()


# Global error context instance
_error_context = ErrorContext()


def record_action(
    action: str,
    details: Optional[dict[str, Any]] = None,
    page: Optional[str] = None,
) -> None:
    """
    Record a user action.

    Convenience function for the global ErrorContext instance.
    """
    _error_context.record_action(action, details, page)


def set_current_page(page: str) -> None:
    """Set the current page."""
    _error_context.set_current_page(page)


def set_current_dataset(dataset_id: Optional[int]) -> None:
    """Set the current dataset ID."""
    _error_context.set_current_dataset(dataset_id)


def set_current_operation(operation: Optional[str]) -> None:
    """Set the current operation."""
    _error_context.set_current_operation(operation)


def get_error_context(error: Exception) -> dict[str, Any]:
    """
    Get context for an error.

    Args:
        error: The exception that occurred

    Returns:
        Dictionary with error context
    """
    return _error_context.get_context_for_error(error)


def get_recent_actions(count: int = 5) -> list[dict[str, Any]]:
    """Get recent actions."""
    return _error_context.get_recent_actions(count)

