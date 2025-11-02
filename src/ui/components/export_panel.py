"""
Export panel component for CSV Wrangler.
"""
from datetime import datetime

import streamlit as st

from src.services.export_service import export_dataset_to_csv, export_dataset_to_pickle, generate_export_filename


def render_export_panel(session, dataset_id: int, dataset_name: str):
    """
    Render export panel component.
    
    Args:
        session: Database session
        dataset_id: Dataset ID
        dataset_name: Dataset name
    """
    st.subheader("💾 Export Dataset")

    # File format selection
    export_format = st.radio(
        "Export Format",
        ["CSV", "Pickle"],
        horizontal=True,
        key=f"export_format_{dataset_id}",
    )

    # Date range filter
    use_date_filter = st.checkbox("Filter by Date Range", key=f"use_date_{dataset_id}")

    start_date = None
    end_date = None

    if use_date_filter:
        col1, col2 = st.columns(2)

        with col1:
            start_date = st.date_input(
                "Start Date",
                key=f"start_date_{dataset_id}",
            )

        with col2:
            end_date = st.date_input(
                "End Date",
                key=f"end_date_{dataset_id}",
            )

        if start_date:
            start_date = datetime.combine(start_date, datetime.min.time())
        if end_date:
            end_date = datetime.combine(end_date, datetime.max.time())

    # Export button
    if st.button("Export Dataset", type="primary", key=f"export_btn_{dataset_id}"):
        try:
            import tempfile
            from pathlib import Path

            # Generate filename
            file_ext = "csv" if export_format == "CSV" else "pkl"
            filename = generate_export_filename(dataset_name, file_ext)

            # Create temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=f".{file_ext}") as tmp_file:
                tmp_path = Path(tmp_file.name)

            # Export
            if export_format == "CSV":
                export_path = export_dataset_to_csv(
                    session,
                    dataset_id,
                    tmp_path,
                    start_date,
                    end_date,
                )
            else:
                export_path = export_dataset_to_pickle(
                    session,
                    dataset_id,
                    tmp_path,
                    start_date,
                    end_date,
                )

            # Provide download
            with open(export_path, "rb") as f:
                st.download_button(
                    label=f"Download {export_format} File",
                    data=f.read(),
                    file_name=filename,
                    mime="text/csv" if export_format == "CSV" else "application/octet-stream",
                    key=f"download_{dataset_id}",
                )

            st.success(f"✅ Export ready! {filename}")

            # Cleanup
            export_path.unlink()

        except Exception as e:
            st.error(f"Export failed: {e}")

