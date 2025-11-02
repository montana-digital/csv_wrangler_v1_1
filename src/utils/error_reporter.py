"""
Error reporting system for CSV Wrangler.

Formats errors for support tickets, anonymizes sensitive data, and exports error reports.
"""
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from src.utils.diagnostics import get_full_diagnostics
from src.utils.error_context import get_error_context
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def format_error_report(
    error: Exception,
    error_code: Optional[str] = None,
    context: Optional[dict[str, Any]] = None,
    include_diagnostics: bool = True,
    anonymize: bool = True,
) -> dict[str, Any]:
    """
    Format error for support ticket or report.

    Args:
        error: The exception that occurred
        error_code: Optional error code
        context: Optional additional context
        include_diagnostics: Whether to include full diagnostics
        anonymize: Whether to anonymize sensitive data (paths, user names, etc.)

    Returns:
        Dictionary with formatted error report
    """
    error_context = get_error_context(error)

    report = {
        "timestamp": datetime.now().isoformat(),
        "error_type": type(error).__name__,
        "error_message": str(error),
        "error_code": error_code or getattr(error, "code", None),
        "context": error_context,
        "additional_context": context or {},
    }

    if include_diagnostics:
        report["diagnostics"] = get_full_diagnostics()

    if anonymize:
        report = _anonymize_report(report)

    return report


def _anonymize_report(report: dict[str, Any]) -> dict[str, Any]:
    """
    Anonymize sensitive data in error report.

    Args:
        report: Error report dictionary

    Returns:
        Anonymized report
    """
    import re

    # Create a copy to avoid modifying original
    anonymized = report.copy()

    # Anonymize paths (replace with placeholders)
    def anonymize_paths(text: str) -> str:
        if not isinstance(text, str):
            return text

        # Replace Windows paths
        text = re.sub(r"[A-Z]:\\[^\\]+", "[PATH]", text)
        # Replace Unix paths
        text = re.sub(r"/[^/]+", "[PATH]", text)
        # Replace usernames in paths
        text = re.sub(r"Users\\[^\\]+", "Users\\[USER]", text)
        text = re.sub(r"/home/[^/]+", "/home/[USER]", text)

        return text

    # Recursively anonymize
    def anonymize_dict(d: dict) -> dict:
        result = {}
        for k, v in d.items():
            if isinstance(v, str):
                result[k] = anonymize_paths(v)
            elif isinstance(v, dict):
                result[k] = anonymize_dict(v)
            elif isinstance(v, list):
                result[k] = [anonymize_dict(item) if isinstance(item, dict) else anonymize_paths(item) if isinstance(item, str) else item for item in v]
            else:
                result[k] = v
        return result

    return anonymize_dict(anonymized)


def export_error_report(
    error: Exception,
    error_code: Optional[str] = None,
    context: Optional[dict[str, Any]] = None,
    file_path: Optional[Path] = None,
    include_diagnostics: bool = True,
) -> Path:
    """
    Export error report to a file.

    Args:
        error: The exception that occurred
        error_code: Optional error code
        context: Optional additional context
        file_path: Optional file path (defaults to error_report_TIMESTAMP.json)
        include_diagnostics: Whether to include full diagnostics

    Returns:
        Path to exported file
    """
    if file_path is None:
        app_dir = Path(__file__).parent.parent.parent
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = app_dir / f"error_report_{timestamp}.json"

    report = format_error_report(
        error=error,
        error_code=error_code,
        context=context,
        include_diagnostics=include_diagnostics,
        anonymize=True,
    )

    import json

    file_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    logger.info(f"Error report exported to {file_path}")

    return file_path


def format_error_report_for_display(report: dict[str, Any]) -> str:
    """
    Format error report for user-friendly display.

    Args:
        report: Error report dictionary

    Returns:
        Formatted string for display
    """
    lines = ["# Error Report\n"]

    lines.append(f"**Timestamp:** {report.get('timestamp', 'N/A')}")
    lines.append(f"**Error Type:** {report.get('error_type', 'N/A')}")
    lines.append(f"**Error Code:** {report.get('error_code', 'N/A')}")
    lines.append("")
    lines.append(f"**Error Message:**")
    lines.append(f"```")
    lines.append(str(report.get("error_message", "N/A")))
    lines.append(f"```")
    lines.append("")

    if "context" in report:
        ctx = report["context"]
        lines.append("## Context")
        if "current_page" in ctx:
            lines.append(f"- Page: {ctx['current_page']}")
        if "current_operation" in ctx:
            lines.append(f"- Operation: {ctx['current_operation']}")
        lines.append("")

    return "\n".join(lines)

