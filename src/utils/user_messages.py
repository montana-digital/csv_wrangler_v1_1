"""
User-friendly error messages for CSV Wrangler.

Provides non-technical error messages with troubleshooting tips for end users.
"""
from typing import Any, Optional


class ErrorMessage:
    """Structured error message with troubleshooting information."""

    def __init__(
        self,
        title: str,
        message: str,
        troubleshooting: Optional[list[str]] = None,
        recovery_action: Optional[str] = None,
        technical_details: Optional[str] = None,
    ):
        self.title = title
        self.message = message
        self.troubleshooting = troubleshooting or []
        self.recovery_action = recovery_action
        self.technical_details = technical_details


# Dictionary mapping error codes to user-friendly messages
ERROR_MESSAGES: dict[str, ErrorMessage] = {
    "FILE_NOT_FOUND": ErrorMessage(
        title="File Not Found",
        message="We couldn't find the file you're trying to open.",
        troubleshooting=[
            "Check that the file path is correct",
            "Ensure the file hasn't been moved or deleted",
            "Verify you have permission to access the file",
            "Try selecting the file again using the file browser",
        ],
        recovery_action="Try selecting the file again",
    ),
    "DATABASE_ERROR": ErrorMessage(
        title="Database Issue",
        message="There was a problem accessing the database.",
        troubleshooting=[
            "Try restarting the application",
            "Check if the database file is locked by another program",
            "Ensure you have write permissions in the application folder",
            "Contact support if the problem persists",
        ],
        recovery_action="Restart the application",
    ),
    "VALIDATION_ERROR": ErrorMessage(
        title="Invalid Input",
        message="The information you entered isn't valid.",
        troubleshooting=[
            "Check that all required fields are filled in",
            "Verify the format of your data matches what's expected",
            "Look for highlighted fields with error messages",
            "Refer to the field descriptions for correct formats",
        ],
        recovery_action="Check the highlighted fields and correct any errors",
    ),
    "DUPLICATE_FILE": ErrorMessage(
        title="File Already Uploaded",
        message="This file has already been uploaded to this dataset.",
        troubleshooting=[
            "Check the upload history to confirm",
            "Rename the file if you want to upload it again",
            "Use a different file or dataset",
        ],
        recovery_action="Use a different file or upload to a different dataset",
    ),
    "SCHEMA_MISMATCH": ErrorMessage(
        title="File Format Doesn't Match",
        message="The columns in this file don't match the dataset structure.",
        troubleshooting=[
            "Check that the file has the same columns as the dataset",
            "Column names must match exactly (including capitalization)",
            "Ensure all required columns are present",
            "Review the dataset configuration in Settings",
        ],
        recovery_action="Use a file with matching columns or reconfigure the dataset",
    ),
    "FILE_PROCESSING_ERROR": ErrorMessage(
        title="File Processing Failed",
        message="We couldn't read or process this file.",
        troubleshooting=[
            "Check that the file isn't corrupted",
            "Ensure the file is a valid CSV or Pickle file",
            "Try opening the file in another program to verify it's valid",
            "If it's a CSV, ensure it uses UTF-8 encoding",
        ],
        recovery_action="Try a different file or convert your file to CSV format",
    ),
    "DATASET_NOT_FOUND": ErrorMessage(
        title="Dataset Not Found",
        message="The dataset you're trying to access doesn't exist.",
        troubleshooting=[
            "Check that you've initialized the dataset",
            "The dataset may have been deleted",
            "Try refreshing the page",
        ],
        recovery_action="Initialize a new dataset or check other dataset slots",
    ),
    "UPLOAD_FAILED": ErrorMessage(
        title="Upload Failed",
        message="We couldn't upload your file.",
        troubleshooting=[
            "Check that the file isn't too large (maximum 500MB)",
            "Ensure you have a stable internet connection if applicable",
            "Try closing and reopening the file",
            "Check that you have enough disk space",
        ],
        recovery_action="Try uploading again or use a smaller file",
    ),
    "ENRICHMENT_ERROR": ErrorMessage(
        title="Enrichment Failed",
        message="We couldn't enrich the selected columns.",
        troubleshooting=[
            "Check that the source dataset has data",
            "Verify the column names are correct",
            "Ensure the enrichment function is appropriate for the data type",
            "Try enriching one column at a time",
        ],
        recovery_action="Check your dataset and try again",
    ),
    "EXPORT_FAILED": ErrorMessage(
        title="Export Failed",
        message="We couldn't export your data.",
        troubleshooting=[
            "Check that you have write permissions in the export location",
            "Ensure there's enough disk space available",
            "Try a different export location",
            "Close the file if it's open in another program",
        ],
        recovery_action="Try exporting to a different location",
    ),
    "SEARCH_ERROR": ErrorMessage(
        title="Search Failed",
        message="We couldn't perform the search.",
        troubleshooting=[
            "Check that you've entered search terms",
            "Try using different search keywords",
            "Clear filters and try again",
        ],
        recovery_action="Adjust your search terms and try again",
    ),
    "VISUALIZATION_ERROR": ErrorMessage(
        title="Visualization Unavailable",
        message="We couldn't create the visualization.",
        troubleshooting=[
            "Check that you have data selected",
            "Ensure the data columns are appropriate for the chart type",
            "The Plotly package may not be installed (visualizations are optional)",
        ],
        recovery_action="Install Plotly for visualization features: pip install plotly",
    ),
    "MISSING_PACKAGE": ErrorMessage(
        title="Feature Unavailable",
        message="This feature requires an optional package that isn't installed.",
        troubleshooting=[
            "The feature will work with limited functionality",
            "Install the optional package for full features",
            "Check the Help page for installation instructions",
        ],
        recovery_action="Install the optional package or use alternative features",
    ),
    "PERMISSION_ERROR": ErrorMessage(
        title="Permission Denied",
        message="You don't have permission to perform this action.",
        troubleshooting=[
            "Check file and folder permissions",
            "Ensure you're not trying to access a locked file",
            "Try running the application as administrator if needed",
            "Check that the userdata folder is writable",
        ],
        recovery_action="Check permissions or contact your system administrator",
    ),
    "MEMORY_ERROR": ErrorMessage(
        title="Out of Memory",
        message="The operation requires more memory than is available.",
        troubleshooting=[
            "Try using a smaller file",
            "Close other applications to free up memory",
            "Export some data to reduce dataset size",
            "Restart the application",
        ],
        recovery_action="Use smaller files or free up system memory",
    ),
    "NETWORK_ERROR": ErrorMessage(
        title="Network Issue",
        message="We couldn't connect to the required service.",
        troubleshooting=[
            "Check your internet connection",
            "Verify firewall settings aren't blocking the connection",
            "Try again in a few moments",
        ],
        recovery_action="Check your connection and try again",
    ),
    "CONFIGURATION_ERROR": ErrorMessage(
        title="Configuration Problem",
        message="There's a problem with the application configuration.",
        troubleshooting=[
            "Try restarting the application",
            "Check that all required files are present",
            "Contact support if the problem persists",
        ],
        recovery_action="Restart the application",
    ),
    "UNKNOWN_ERROR": ErrorMessage(
        title="Something Went Wrong",
        message="An unexpected error occurred.",
        troubleshooting=[
            "Try the action again",
            "Restart the application",
            "Check the logs for more details",
            "Contact support if the problem continues",
        ],
        recovery_action="Try again or restart the application",
    ),
}


def get_error_message(error_code: str, custom_message: Optional[str] = None) -> ErrorMessage:
    """
    Get user-friendly error message by error code.

    Args:
        error_code: Error code (e.g., "FILE_NOT_FOUND")
        custom_message: Optional custom message to override default

    Returns:
        ErrorMessage object with title, message, and troubleshooting tips
    """
    error_msg = ERROR_MESSAGES.get(error_code, ERROR_MESSAGES["UNKNOWN_ERROR"])

    # If custom message provided, create new ErrorMessage with it
    if custom_message:
        return ErrorMessage(
            title=error_msg.title,
            message=custom_message,
            troubleshooting=error_msg.troubleshooting,
            recovery_action=error_msg.recovery_action,
            technical_details=error_msg.technical_details,
        )

    return error_msg


def format_error_for_user(
    error_code: str,
    exception: Optional[Exception] = None,
    custom_message: Optional[str] = None,
    include_technical: bool = False,
) -> dict[str, Any]:
    """
    Format error for display to user.

    Args:
        error_code: Error code
        exception: Optional exception object for technical details
        custom_message: Optional custom message
        include_technical: Whether to include technical details

    Returns:
        Dictionary with formatted error information
    """
    error_msg = get_error_message(error_code, custom_message)

    result = {
        "title": error_msg.title,
        "message": error_msg.message,
        "troubleshooting": error_msg.troubleshooting,
        "recovery_action": error_msg.recovery_action,
    }

    if include_technical and exception:
        result["technical_details"] = str(exception)
        if error_msg.technical_details:
            result["technical_details"] += f"\n{error_msg.technical_details}"

    return result

