"""
Bulk uploader UI component for CSV Wrangler.

Provides UI for selecting dataset and uploading multiple files.
"""
import os
import tempfile
from pathlib import Path
from typing import Optional

import streamlit as st

from src.database.models import DatasetConfig
from src.services.bulk_upload_service import BulkUploadResult, FileUploadResult
from src.utils.file_utils import copy_to_originals


def render_dataset_selector(session, datasets: list[DatasetConfig]) -> Optional[int]:
    """
    Render dataset selector dropdown.
    
    Args:
        session: Database session (unused but kept for consistency)
        datasets: List of available datasets
        
    Returns:
        Selected dataset ID or None
    """
    if not datasets:
        st.warning("⚠️ No datasets available. Please initialize a dataset first.")
        return None

    # Create display options
    options = {f"{ds.name} (Slot {ds.slot_number})": ds.id for ds in datasets}
    display_names = list(options.keys())

    # Get current selection from session state or use first dataset
    selected_key = "bulk_upload_selected_dataset"
    if selected_key not in st.session_state:
        st.session_state[selected_key] = display_names[0] if display_names else None

    selected_display = st.selectbox(
        "📊 Select Dataset",
        options=display_names,
        index=0 if display_names else None,
        key=selected_key,
        help="Choose which dataset to upload files to",
    )

    if selected_display:
        return options[selected_display]
    return None


def render_multi_file_uploader(dataset_name: str) -> list[tuple[Path, str]]:
    """
    Render multi-file uploader.
    
    Args:
        dataset_name: Name of selected dataset
        
    Returns:
        List of (file_path, filename) tuples, or empty list
    """
    st.subheader("📤 Upload Multiple Files")

    uploaded_files = st.file_uploader(
        "Drag and drop multiple CSV or Pickle files here",
        type=["csv", "pkl", "pickle"],
        accept_multiple_files=True,
        key="bulk_file_uploader",
        help=f"Select multiple files to upload to '{dataset_name}'",
    )

    if uploaded_files:
        st.info(f"📄 **{len(uploaded_files)} file(s) selected**")

        # Display file list
        with st.expander("📋 View Selected Files", expanded=False):
            for file in uploaded_files:
                file_size = len(file.getvalue()) / 1024  # KB
                st.text(f"  • {file.name} ({file_size:.2f} KB)")

        # Process button
        process_key = "bulk_upload_process_trigger"
        if st.button("🚀 Process Uploads", type="primary", key="bulk_process_btn"):
            st.session_state[process_key] = True

    # Process files if triggered
    process_key = "bulk_upload_process_trigger"
    if uploaded_files and st.session_state.get(process_key, False):
        # Save files to temporary locations
        file_paths = []
        temp_files_key = "bulk_upload_temp_files"

        if temp_files_key not in st.session_state:
            st.session_state[temp_files_key] = []

        try:
            for uploaded_file in uploaded_files:
                # Create temporary file
                suffix = os.path.splitext(uploaded_file.name)[1]
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_file:
                    tmp_file.write(uploaded_file.getvalue())
                    tmp_path = Path(tmp_file.name)

                # Copy to originals directory
                try:
                    copy_to_originals(tmp_path, dataset_name)
                except Exception as e:
                    st.warning(f"Could not copy {uploaded_file.name} to originals: {e}")

                file_paths.append((tmp_path, uploaded_file.name))
                st.session_state[temp_files_key].append(str(tmp_path))

            # Store pending files and return them
            pending_files_key = "bulk_upload_pending_files"
            st.session_state[pending_files_key] = file_paths
            return file_paths

        except Exception as e:
            st.error(f"Error preparing files: {e}")
            # Cleanup on error
            for file_path, _ in file_paths:
                if file_path.exists():
                    os.unlink(file_path)
            return []

    # Check for pending files from previous rerun
    pending_files_key = "bulk_upload_pending_files"
    if st.session_state.get(pending_files_key):
        pending = st.session_state[pending_files_key]
        return pending

    return []


def render_upload_results(result: BulkUploadResult):
    """
    Render bulk upload results summary.
    
    Args:
        result: BulkUploadResult instance
    """
    st.markdown("---")
    st.subheader("📊 Upload Results")

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Files", result.total_files)
    col2.metric("✅ Successful", len(result.successful))
    col3.metric("⚠️ Skipped", len(result.skipped))
    col4.metric("📈 Total Rows", f"{result.total_rows_added:,}")

    st.markdown("---")

    # Successful uploads
    if result.successful:
        with st.expander(f"✅ Successful Uploads ({len(result.successful)})", expanded=True):
            success_data = {
                "Filename": [r.filename for r in result.successful],
                "Rows Added": [f"{r.row_count:,}" if r.row_count else "N/A" for r in result.successful],
            }
            st.dataframe(success_data, use_container_width=True, hide_index=True)

    # Skipped files
    if result.skipped:
        with st.expander(f"⚠️ Skipped Files ({len(result.skipped)})", expanded=True):
            skip_data = {
                "Filename": [r.filename for r in result.skipped],
                "Reason": [r.error_reason or "Unknown error" for r in result.skipped],
                "Error Type": [r.error_type or "unknown" for r in result.skipped],
            }
            st.dataframe(skip_data, use_container_width=True, hide_index=True)

            # Group by error type
            error_types = {}
            for r in result.skipped:
                error_type = r.error_type or "unknown"
                if error_type not in error_types:
                    error_types[error_type] = []
                error_types[error_type].append(r.filename)

            if len(error_types) > 1:
                st.markdown("**Summary by Error Type:**")
                for error_type, filenames in error_types.items():
                    st.markdown(f"- **{error_type.replace('_', ' ').title()}**: {len(filenames)} file(s)")

    # Success message if all succeeded
    if result.successful and not result.skipped:
        st.success(
            f"🎉 All {len(result.successful)} file(s) uploaded successfully! "
            f"Added {result.total_rows_added:,} total rows to '{result.dataset_name}'."
        )
    elif result.successful:
        st.info(
            f"✅ {len(result.successful)} file(s) uploaded successfully. "
            f"{len(result.skipped)} file(s) were skipped. See details above."
        )
    elif result.skipped:
        st.warning(
            f"⚠️ No files were uploaded. All {len(result.skipped)} file(s) were skipped. "
            f"Please review the errors above and try again."
        )

