"""
Shared dataset page template for CSV Wrangler.

This template provides the common functionality for all dataset pages (1-5).
"""
import streamlit as st
from pathlib import Path

from src.database.connection import get_session
from src.database.repository import DatasetRepository
from src.services.dataset_service import (
    check_duplicate_filename,
    get_dataset_statistics,
    initialize_dataset,
    upload_csv_to_dataset,
)
from src.ui.components.csv_uploader import render_file_uploader
from src.ui.components.data_viewer import render_data_viewer
from src.ui.components.dataset_config import render_dataset_config_ui
from src.ui.components.export_panel import render_export_panel
from src.ui.components.error_display import handle_exception
from src.utils.error_handler import SafeOperation
from src.utils.errors import DuplicateFileError


def render_dataset_page(slot_number: int) -> None:
    """
    Render a dataset page for the given slot number.
    
    Args:
        slot_number: Dataset slot number (1-5)
    """
    st.title(f"📁 Dataset #{slot_number}")
    st.markdown("---")

    with get_session() as session:
        repo = DatasetRepository(session)
        dataset = repo.get_by_slot(slot_number)

        # Check if initialized
        if not dataset:
            # Show initialization UI
            config = render_dataset_config_ui(slot_number, None)

            if config:
                with SafeOperation(
                    "initialize_dataset",
                    error_code="DATASET_NOT_FOUND",
                    show_troubleshooting=True,
                ):
                    with st.spinner("Initializing dataset..."):
                        dataset = initialize_dataset(
                            session=session,
                            name=config["name"],
                            slot_number=slot_number,
                            columns_config=config["columns_config"],
                            image_columns=config["image_columns"],
                        )
                        st.success(f"✅ Dataset '{dataset.name}' initialized successfully!")
                        st.rerun()

        else:
            # Dataset is initialized - show main interface
            st.success(f"✅ **{dataset.name}**")

            # Statistics
            try:
                stats = get_dataset_statistics(session, dataset.id)

                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Total Rows", f"{stats['total_rows']:,}")
                col2.metric("Total Uploads", stats["total_uploads"])
                col3.metric("Columns", len(stats["column_names"]))
                if stats["last_upload"]:
                    col4.metric("Last Upload", stats["last_upload"][:10])
            except Exception as e:
                handle_exception(e, "DATABASE_ERROR", show_troubleshooting=True)

            st.markdown("---")

            # File uploader
            uploaded_file_path, filename = render_file_uploader(dataset.id, dataset.name)

            if uploaded_file_path and filename:
                # Check if user has already confirmed duplicate upload
                duplicate_confirmed = st.session_state.get(f"confirm_upload_{dataset.id}", False)
                
                # Only check for duplicate if user hasn't confirmed yet
                if not duplicate_confirmed:
                    try:
                        check_duplicate_filename(session, dataset.id, filename)
                        # No duplicate - proceed with upload
                    except DuplicateFileError:
                        # Duplicate detected - show warning and get user confirmation
                        st.warning(f"⚠️ File '{filename}' has been uploaded before. Continue anyway?")
                        if st.button("Yes, Upload Anyway", key=f"confirm_{dataset.id}"):
                            st.session_state[f"confirm_upload_{dataset.id}"] = True
                            st.rerun()
                        else:
                            # User cancelled - clean up and stop
                            if uploaded_file_path.exists():
                                try:
                                    uploaded_file_path.unlink()
                                except Exception:
                                    pass
                            st.stop()
                        return  # Exit early - waiting for user confirmation
                    except Exception as e:
                        # Any other exception during duplicate check - propagate to SafeOperation
                        raise
                # User has confirmed duplicate upload - proceed with upload (skip duplicate check)
                
                # Upload file (SafeOperation handles errors and displays them)
                with SafeOperation(
                    "upload_file",
                    error_code="UPLOAD_FAILED",
                    show_troubleshooting=True,
                ):
                    # Skip duplicate check if user confirmed duplicate upload
                    skip_duplicate = st.session_state.get(f"confirm_upload_{dataset.id}", False)
                    upload_log = upload_csv_to_dataset(
                        session=session,
                        dataset_id=dataset.id,
                        csv_file=uploaded_file_path,
                        filename=filename,
                        show_progress=True,
                        skip_duplicate_check=skip_duplicate,
                    )
                
                # Only show success and rerun if upload completed without exception
                # (SafeOperation would have re-raised if there was an error)
                st.success(f"✅ Uploaded {upload_log.row_count} rows from {filename}")
                st.session_state[f"confirm_upload_{dataset.id}"] = False
                
                # Cleanup temporary file before rerun
                if uploaded_file_path.exists():
                    try:
                        uploaded_file_path.unlink()
                    except Exception:
                        pass  # Ignore cleanup errors
                
                st.rerun()

            st.markdown("---")

            # Data viewer - lazy load only when requested
            try:
                stats = get_dataset_statistics(session, dataset.id)
                if stats["total_rows"] > 0:
                    # Check if user wants to load data
                    data_viewer_key = f"show_data_viewer_{dataset.id}"
                    if data_viewer_key not in st.session_state:
                        st.session_state[data_viewer_key] = False
                    
                    # Button to toggle data viewer
                    col1, col2 = st.columns([1, 4])
                    with col1:
                        if st.button(
                            "📊 Load Data Viewer" if not st.session_state[data_viewer_key] else "📊 Hide Data Viewer",
                            key=f"toggle_data_viewer_{dataset.id}",
                            type="secondary" if st.session_state[data_viewer_key] else "primary",
                        ):
                            st.session_state[data_viewer_key] = not st.session_state[data_viewer_key]
                            st.rerun()
                    
                    with col2:
                        if st.session_state[data_viewer_key]:
                            st.caption(f"Showing data viewer for {stats['total_rows']:,} rows")
                        else:
                            st.caption("Click 'Load Data Viewer' to view dataset data")
                    
                    # Load and display data only if requested
                    if st.session_state[data_viewer_key]:
                        # Load recent data (exclude image columns for performance)
                        from src.services.dataframe_service import load_dataset_dataframe
                        import pandas as pd

                        with st.spinner("Loading data..."):
                            df = load_dataset_dataframe(
                                session=session,
                                dataset_id=dataset.id,
                                limit=10000,
                                offset=0,
                                include_image_columns=False,  # Exclude images for performance
                                order_by_recent=True,
                            )

                        if not df.empty:
                            render_data_viewer(df)
                        else:
                            st.info("No data loaded. Try adjusting the limit or offset.")
            except Exception as e:
                handle_exception(e, "DATABASE_ERROR", show_troubleshooting=True)

            st.markdown("---")

            # Export panel
            try:
                render_export_panel(session, dataset.id, dataset.name)
            except Exception as e:
                handle_exception(e, "EXPORT_FAILED", show_troubleshooting=True)

