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
                try:
                    # Check for duplicate
                    try:
                        check_duplicate_filename(session, dataset.id, filename)
                    except Exception as dup_error:
                        if "duplicate" in str(dup_error).lower():
                            if not st.session_state.get(f"confirm_upload_{dataset.id}", False):
                                st.warning(f"⚠️ File '{filename}' has been uploaded before. Continue anyway?")
                                if st.button("Yes, Upload Anyway", key=f"confirm_{dataset.id}"):
                                    st.session_state[f"confirm_upload_{dataset.id}"] = True
                                    st.rerun()
                                else:
                                    st.stop()
                        else:
                            raise

                    # Upload file
                    with SafeOperation(
                        "upload_file",
                        error_code="UPLOAD_FAILED",
                        show_troubleshooting=True,
                    ):
                        upload_log = upload_csv_to_dataset(
                            session=session,
                            dataset_id=dataset.id,
                            csv_file=uploaded_file_path,
                            filename=filename,
                            show_progress=True,
                        )
                        st.success(f"✅ Uploaded {upload_log.row_count} rows from {filename}")
                        st.session_state[f"confirm_upload_{dataset.id}"] = False
                        st.rerun()

                except Exception as e:
                    handle_exception(e, "UPLOAD_FAILED", show_troubleshooting=True)
                finally:
                    if uploaded_file_path.exists():
                        try:
                            uploaded_file_path.unlink()
                        except Exception:
                            pass  # Ignore cleanup errors

            st.markdown("---")

            # Data viewer
            try:
                stats = get_dataset_statistics(session, dataset.id)
                if stats["total_rows"] > 0:
                    # Load recent data
                    from sqlalchemy import text
                    from src.utils.validation import quote_identifier

                    quoted_table = quote_identifier(dataset.table_name)
                    query = text(f"SELECT * FROM {quoted_table} ORDER BY rowid DESC LIMIT 10000")
                    result = session.execute(query)
                    rows = result.fetchall()

                    if rows:
                        import pandas as pd
                        df = pd.DataFrame(rows, columns=result.keys())
                        render_data_viewer(df)
            except Exception as e:
                handle_exception(e, "DATABASE_ERROR", show_troubleshooting=True)

            st.markdown("---")

            # Export panel
            try:
                render_export_panel(session, dataset.id, dataset.name)
            except Exception as e:
                handle_exception(e, "EXPORT_FAILED", show_troubleshooting=True)

