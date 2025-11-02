"""
Progress tracking utilities for Streamlit operations.

Provides context managers and helpers for showing progress bars during
long-running operations like CSV parsing and database uploads.
"""
from contextlib import contextmanager
from typing import Optional

import streamlit as st


@contextmanager
def progress_bar(
    total: int,
    label: str = "Processing",
    show_count: bool = True,
    key: Optional[str] = None,
):
    """
    Context manager for displaying a progress bar.
    
    Usage:
        with progress_bar(100, "Uploading data") as update_progress:
            for i in range(100):
                update_progress(i + 1)
                # ... do work ...
    
    Args:
        total: Total number of items/steps
        label: Label to display above progress bar
        show_count: Whether to show "X / Y" count
        key: Optional unique key for this progress bar
        
    Yields:
        Function to call with current progress (0 to total)
    """
    progress_container = st.empty()
    status_container = st.empty()
    
    def update_progress(current: int, status: Optional[str] = None):
        """Update progress bar and status."""
        if current < 0:
            current = 0
        if current > total:
            current = total
            
        progress = current / total if total > 0 else 0
        
        # Build status text
        if show_count:
            status_text = f"{label}: {current:,} / {total:,}"
        else:
            status_text = label
            
        if status:
            status_text += f" - {status}"
            
        # Show progress bar
        progress_container.progress(progress, text=status_text)
        
        # Show additional status if provided
        if status:
            status_container.caption(status)
        else:
            status_container.empty()
    
    try:
        # Initialize progress bar
        update_progress(0)
        yield update_progress
    finally:
        # Clear progress bar on completion
        progress_container.empty()
        status_container.empty()


def show_upload_progress(
    current_step: int,
    total_steps: int,
    step_name: str,
    details: Optional[str] = None,
    key: Optional[str] = None,
):
    """
    Show upload progress with step-by-step indication.
    
    Args:
        current_step: Current step number (1-based)
        total_steps: Total number of steps
        step_name: Name of current step
        details: Optional additional details
        key: Optional unique key
    """
    progress = current_step / total_steps
    
    status_text = f"Step {current_step}/{total_steps}: {step_name}"
    if details:
        status_text += f" - {details}"
    
    st.progress(progress, text=status_text)


def estimate_row_count(file_path, chunk_size: int = 10000) -> int:
    """
    Estimate row count for a CSV file without loading it all.
    
    Args:
        file_path: Path to CSV file
        chunk_size: Number of rows to read for estimation
        
    Returns:
        Estimated row count
    """
    import pandas as pd
    
    try:
        # Read first chunk to get row size estimate
        chunk = pd.read_csv(file_path, nrows=chunk_size)
        if len(chunk) < chunk_size:
            # File is smaller than chunk size, return actual count
            return len(chunk)
        
        # Estimate based on file size
        file_size = file_path.stat().st_size
        bytes_per_row = file_size / len(chunk)
        
        # Read file size and estimate
        total_rows = int(file_size / bytes_per_row)
        return total_rows
    except Exception:
        # If estimation fails, return a default
        return 0

