"""
Bulk Uploader page for CSV Wrangler.

Allows users to upload multiple CSV/Pickle files to a selected dataset
with validation, duplicate detection, and comprehensive error reporting.
"""
import os
from pathlib import Path

import streamlit as st

from src.database.connection import get_session
from src.database.repository import DatasetRepository
from src.services.bulk_upload_service import process_bulk_upload
from src.ui.components.bulk_uploader import (
    render_dataset_selector,
    render_multi_file_uploader,
    render_upload_results,
)
from src.ui.components.sidebar import render_sidebar

# Render uniform sidebar
render_sidebar()

st.title("📦 Bulk Uploader")
st.markdown("Upload multiple CSV or Pickle files to any dataset simultaneously")
st.markdown("---")

with get_session() as session:
    repo = DatasetRepository(session)
    datasets = repo.get_all()

    # Dataset selector
    selected_dataset_id = render_dataset_selector(session, datasets)

    if selected_dataset_id:
        selected_dataset = repo.get_by_id(selected_dataset_id)

        if selected_dataset:
            st.markdown("---")

            # File uploader
            uploaded_files = render_multi_file_uploader(selected_dataset.name)

            # Process uploads if files are provided
            results_key = "bulk_upload_results"
            process_key = "bulk_upload_process_trigger"
            pending_files_key = "bulk_upload_pending_files"

            if uploaded_files and st.session_state.get(process_key, False):
                # Clear process trigger and pending files
                st.session_state[process_key] = False
                if pending_files_key in st.session_state:
                    del st.session_state[pending_files_key]

                try:
                    # Process bulk upload
                    with st.spinner("🔄 Processing uploads..."):
                        result = process_bulk_upload(
                            session=session,
                            dataset_id=selected_dataset_id,
                            files=uploaded_files,
                            show_progress=True,
                        )

                        # Store results as dict (session state friendly)
                        st.session_state[results_key] = {
                            "dataset_id": result.dataset_id,
                            "dataset_name": result.dataset_name,
                            "total_files": result.total_files,
                            "successful": [
                                {
                                    "filename": r.filename,
                                    "success": r.success,
                                    "row_count": r.row_count,
                                }
                                for r in result.successful
                            ],
                            "skipped": [
                                {
                                    "filename": r.filename,
                                    "success": r.success,
                                    "error_reason": r.error_reason,
                                    "error_type": r.error_type,
                                }
                                for r in result.skipped
                            ],
                            "total_rows_added": result.total_rows_added,
                        }

                        # Cleanup temporary files
                        temp_files_key = "bulk_upload_temp_files"
                        if temp_files_key in st.session_state:
                            for file_path_str in st.session_state[temp_files_key]:
                                file_path = Path(file_path_str)
                                if file_path.exists():
                                    try:
                                        os.unlink(file_path)
                                    except Exception as e:
                                        st.warning(f"Could not delete temporary file: {e}")
                            del st.session_state[temp_files_key]

                        # Trigger rerun to show results
                        st.rerun()

                except Exception as e:
                    st.error(f"❌ Bulk upload failed: {e}")
                    # Cleanup on error
                    for file_path, _ in uploaded_files:
                        if file_path.exists():
                            try:
                                os.unlink(file_path)
                            except Exception:
                                pass

            # Display results if available
            if results_key in st.session_state:
                result_dict = st.session_state[results_key]
                # Only show if this result is for the currently selected dataset
                if result_dict["dataset_id"] == selected_dataset_id:
                    # Reconstruct result object for display
                    from src.services.bulk_upload_service import BulkUploadResult, FileUploadResult

                    result = BulkUploadResult(
                        dataset_id=result_dict["dataset_id"],
                        dataset_name=result_dict["dataset_name"],
                        total_files=result_dict["total_files"],
                        successful=[
                            FileUploadResult(
                                filename=r["filename"],
                                success=r["success"],
                                row_count=r.get("row_count"),
                            )
                            for r in result_dict["successful"]
                        ],
                        skipped=[
                            FileUploadResult(
                                filename=r["filename"],
                                success=r["success"],
                                error_reason=r.get("error_reason"),
                                error_type=r.get("error_type"),
                            )
                            for r in result_dict["skipped"]
                        ],
                        total_rows_added=result_dict["total_rows_added"],
                    )

                    render_upload_results(result)

                    # Clear results button
                    if st.button("🔄 Clear Results", key="clear_results"):
                        if results_key in st.session_state:
                            del st.session_state[results_key]
                        st.rerun()

    else:
        st.info(
            "👆 Please initialize at least one dataset before using the Bulk Uploader. "
            "Navigate to one of the Dataset pages (1-5) to create a dataset."
        )

