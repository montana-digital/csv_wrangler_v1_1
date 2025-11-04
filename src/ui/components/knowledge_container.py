"""
Knowledge Table container UI component for CSV Wrangler.

Collapsible container for displaying and managing a Knowledge Table.
Includes stats, upload area, data view, and delete functionality.
"""
import streamlit as st
import pandas as pd
from sqlalchemy.orm import Session
from typing import Optional

from src.database.models import KnowledgeTable
from src.services.dataframe_service import load_dataset_dataframe
from src.services.knowledge_service import (
    delete_knowledge_table,
    upload_to_knowledge_table,
    get_knowledge_table_stats,
)
from src.services.table_service import get_table_row_count
from src.ui.components.data_viewer import render_data_viewer
from src.ui.components.knowledge_stats import render_knowledge_stats
from src.utils.error_handler import SafeOperation


def render_knowledge_container(
    session: Session,
    knowledge_table: KnowledgeTable,
) -> None:
    """
    Render collapsible container for a Knowledge Table.
    
    Args:
        session: Database session
        knowledge_table: KnowledgeTable instance to display
    """
    # Container header with table info
    row_count = get_table_row_count(session, knowledge_table.table_name)
    
    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        st.markdown(f"### {knowledge_table.name}")
    with col2:
        st.caption(f"**Type:** {knowledge_table.data_type}")
    with col3:
        st.caption(f"**Rows:** {row_count:,}")
    
    st.caption(f"Table: `{knowledge_table.table_name}` | Updated: {knowledge_table.updated_at.strftime('%Y-%m-%d %H:%M:%S') if knowledge_table.updated_at else 'N/A'}")
    st.markdown("---")
    
    # Collapsible sections
    with st.expander("📊 Statistics", expanded=False):
        render_knowledge_stats(session, knowledge_table)
    
    with st.expander("📤 Upload Data", expanded=False):
        render_upload_section(session, knowledge_table)
    
    with st.expander("📋 Data View", expanded=False):
        render_data_view_section(session, knowledge_table)
    
    with st.expander("🗑️ Delete Knowledge Table", expanded=False):
        render_delete_section(session, knowledge_table)


def render_upload_section(
    session: Session,
    knowledge_table: KnowledgeTable,
) -> None:
    """Render upload section for Knowledge Table."""
    st.markdown("Upload CSV or Pickle file to add data to this Knowledge Table.")
    
    uploaded_file = st.file_uploader(
        "Choose file",
        type=["csv", "pkl", "pickle"],
        key=f"knowledge_upload_{knowledge_table.id}",
    )
    
    if uploaded_file is not None:
        import tempfile
        import os
        from pathlib import Path
        from src.services.file_import_service import import_file
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(uploaded_file.name)[1]) as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_path = tmp_file.name
        
        try:
            df, file_type = import_file(Path(tmp_path))
            
            st.success(f"✅ File parsed: {len(df):,} rows")
            
            if st.button("📤 Upload to Knowledge Table", key=f"upload_btn_{knowledge_table.id}", type="primary"):
                with st.spinner("Uploading data... This may take a moment."):
                    try:
                        result = upload_to_knowledge_table(session, knowledge_table.id, df)
                        
                        st.success(
                            f"✅ Upload complete!\n\n"
                            f"- **Total rows:** {result['total_rows']:,}\n"
                            f"- **Added:** {result['added']:,}\n"
                            f"- **Skipped (invalid):** {result['skipped_invalid']:,}\n"
                            f"- **Skipped (duplicates):** {result['skipped_duplicates']:,}"
                        )
                        
                        # Show skipped list if any
                        if result['skipped_list']:
                            with st.expander("View Skipped Rows", expanded=False):
                                skipped_df = pd.DataFrame(result['skipped_list'])
                                st.dataframe(skipped_df, use_container_width=True)
                        
                        # Clear cached stats
                        cache_key = f"knowledge_stats_{knowledge_table.id}"
                        if cache_key in st.session_state:
                            del st.session_state[cache_key]
                        
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Error uploading data: {e}")
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)


def render_data_view_section(
    session: Session,
    knowledge_table: KnowledgeTable,
) -> None:
    """Render data view section for Knowledge Table."""
    try:
        # Load data from Knowledge Table
        # Note: We need to query the actual table, not use load_dataset_dataframe
        from src.utils.validation import quote_identifier
        from sqlalchemy import text
        quoted_table = quote_identifier(knowledge_table.table_name)
        query = text(f"SELECT * FROM {quoted_table} ORDER BY created_at DESC LIMIT 10000")
        
        result = session.execute(query)
        rows = result.fetchall()
        
        if rows:
            columns = list(result.keys())
            df = pd.DataFrame(rows, columns=columns)
            
            # Hide image columns by default for performance
            # Extract image columns from columns_config
            if not knowledge_table.columns_config:
                st.error(f"Knowledge Table '{knowledge_table.name}' has invalid columns_config")
                return
            image_cols = [
                col_name
                for col_name, col_config in knowledge_table.columns_config.items()
                if col_config.get("is_image", False)
            ]
            display_cols = [col for col in df.columns if col not in image_cols]
            display_df = df[display_cols].copy()
            
            render_data_viewer(display_df, default_rows=10000)
        else:
            st.info("No data in Knowledge Table yet. Upload data to see it here.")
            
    except Exception as e:
        st.error(f"Error loading data: {e}")


def render_delete_section(
    session: Session,
    knowledge_table: KnowledgeTable,
) -> None:
    """Render delete section with 2-level confirmation."""
    st.warning("⚠️ **Danger Zone** - This action cannot be undone!")
    
    st.markdown(
        "Deleting this Knowledge Table will:\n"
        "- Permanently delete the database table and all data\n"
        "- Remove the Knowledge Table configuration\n"
        "- This action cannot be undone"
    )
    
    # Level 1: Checkbox confirmation
    confirm_delete_key = f"confirm_delete_{knowledge_table.id}"
    confirm_delete = st.checkbox(
        "I understand this will permanently delete the Knowledge Table and all data",
        key=confirm_delete_key,
    )
    
    # Level 2: Type table name to confirm
    if confirm_delete:
        st.markdown(f"**Type the Knowledge Table name to confirm deletion:** `{knowledge_table.name}`")
        typed_name_key = f"delete_typed_name_{knowledge_table.id}"
        typed_name = st.text_input(
            "Knowledge Table Name",
            key=typed_name_key,
            label_visibility="collapsed",
            placeholder=f"Type '{knowledge_table.name}' to confirm",
        )
        
        delete_enabled = typed_name == knowledge_table.name
        
        if st.button(
            "🗑️ Delete Knowledge Table",
            key=f"delete_btn_{knowledge_table.id}",
            type="primary",
            disabled=not delete_enabled,
        ):
            with SafeOperation(
                operation_name="Delete Knowledge Table",
                error_code="DATABASE_ERROR",
                suppress_error=False,  # Let exceptions propagate so session rollback works
            ):
                with st.spinner("Deleting Knowledge Table..."):
                    delete_knowledge_table(session, knowledge_table.id)
                    
                    # Explicitly flush and commit to ensure deletion is persisted
                    # This is necessary because st.rerun() will restart the page
                    session.flush()
                    session.commit()
            
            # If we get here, deletion succeeded (no exception)
            st.success(f"✅ Knowledge Table '{knowledge_table.name}' deleted successfully")
            st.rerun()

