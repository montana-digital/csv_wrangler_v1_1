"""
DataFrame View page for CSV Wrangler v1.1.

Advanced data exploration with filtering and search capabilities.
"""
import streamlit as st
import pandas as pd

from src.database.connection import get_session
from src.database.repository import DatasetRepository
from src.services.dataframe_service import (
    get_dataset_columns,
    get_dataset_row_count,
    get_enriched_dataset_columns,
    get_enriched_dataset_row_count,
    load_dataset_dataframe,
    load_enriched_dataset_dataframe,
)
from datetime import datetime
from typing import Optional

from src.database.models import EnrichedDataset
from src.services.enrichment_service import get_enriched_datasets
from src.services.export_service import filter_by_date_range
from src.ui.components.dataframe_filter import render_dataframe_filter_ui
from src.ui.components.sidebar import render_sidebar

# Render uniform sidebar
render_sidebar()

st.title("📋 DataFrame View")
st.markdown("**Advanced data exploration and filtering**")
st.markdown("---")

with get_session() as session:
    repo = DatasetRepository(session)
    all_datasets = repo.get_all()
    all_enriched_datasets = get_enriched_datasets(session)
    
    if not all_datasets and not all_enriched_datasets:
        st.info("No datasets initialized yet. Please initialize a dataset first.")
        st.stop()
    
    # Dataset selector - combine regular and enriched datasets
    st.subheader("Select Dataset")
    
    dataset_options = {}
    # Add regular datasets
    for d in all_datasets:
        dataset_options[f"{d.name} (Slot {d.slot_number})"] = ("dataset", d.id)
    
    # Add enriched datasets
    for ed in all_enriched_datasets:
        source_dataset = repo.get_by_id(ed.source_dataset_id)
        source_name = source_dataset.name if source_dataset else f"Dataset {ed.source_dataset_id}"
        dataset_options[f"⭐ {ed.name} (from {source_name})"] = ("enriched", ed.id)
    
    if not dataset_options:
        st.info("No datasets available. Please initialize a dataset first.")
        st.stop()
    
    selected_dataset_name = st.selectbox(
        "Choose a dataset to explore",
        options=list(dataset_options.keys()),
        key="dataframe_dataset_selector",
    )
    
    if selected_dataset_name:
        dataset_type, selected_id = dataset_options[selected_dataset_name]
        is_enriched = dataset_type == "enriched"
        
        if is_enriched:
            selected_enriched_dataset = session.get(EnrichedDataset, selected_id)
            if not selected_enriched_dataset:
                st.error("Enriched dataset not found")
                st.stop()
            
            # Get source dataset for reference
            source_dataset = repo.get_by_id(selected_enriched_dataset.source_dataset_id)
            selected_dataset = source_dataset  # Use source dataset for some metadata
        else:
            selected_dataset = repo.get_by_id(selected_id)
            selected_enriched_dataset = None
        
        if selected_dataset or (is_enriched and selected_enriched_dataset):
            # Get dataset info
            session_key = f"{dataset_type}_{selected_id}"
            
            if is_enriched:
                total_rows = get_enriched_dataset_row_count(session, selected_id)
                has_image_columns = bool(selected_dataset.image_columns) if selected_dataset else False
                display_name = selected_enriched_dataset.name if selected_enriched_dataset else "Unknown"
            else:
                total_rows = get_dataset_row_count(session, selected_id)
                has_image_columns = bool(selected_dataset.image_columns)
                display_name = selected_dataset.name
            
            # Configuration options
            col1, col2, col3 = st.columns(3)
            
            with col1:
                row_limit = st.number_input(
                    "Rows to load",
                    min_value=100,
                    max_value=100000,
                    value=10000,
                    step=1000,
                    key="dataframe_row_limit",
                )
            
            with col2:
                include_images = st.checkbox(
                    "Include image columns",
                    value=False,
                    key="dataframe_include_images",
                    disabled=not has_image_columns,
                    help="Image columns contain large Base64 data and may slow down loading",
                )
                if has_image_columns and not include_images and selected_dataset:
                    st.caption(f"⚠️ {len(selected_dataset.image_columns)} image column(s) hidden")
            
            with col3:
                if st.button("🔄 Load Data", type="primary", key="dataframe_load_btn"):
                    st.session_state["dataframe_loaded"] = True
                    st.session_state["dataframe_dataset_key"] = session_key
                    # Note: Don't manually set widget session state - widgets manage their own state
                    # include_images and row_limit are already in session_state via their widget keys
            
            st.markdown("---")
            
            # Load and display DataFrame
            if st.session_state.get("dataframe_loaded") and st.session_state.get(
                "dataframe_dataset_key"
            ) == session_key:
                try:
                    with st.spinner(f"Loading {row_limit} rows..."):
                        if is_enriched:
                            df = load_enriched_dataset_dataframe(
                                session,
                                selected_id,
                                limit=row_limit,
                                include_image_columns=include_images,
                                order_by_recent=True,
                            )
                        else:
                            df = load_dataset_dataframe(
                                session,
                                selected_id,
                                limit=row_limit,
                                include_image_columns=include_images,
                                order_by_recent=True,
                            )
                    
                    if df.empty:
                        st.info("Dataset is empty. Upload some data first.")
                    else:
                        st.success(f"✅ Loaded {len(df)} rows (Total in dataset: {total_rows:,})")
                        
                        # Detect date/datetime columns
                        date_columns = []
                        for col in df.columns:
                            # Check if already datetime type
                            if pd.api.types.is_datetime64_any_dtype(df[col]):
                                date_columns.append(col)
                            else:
                                # Try to detect date columns by name and sample values
                                col_lower = col.lower()
                                if any(pattern in col_lower for pattern in ["date", "time", "created", "updated", "timestamp"]):
                                    # Try to parse a sample value
                                    sample = df[col].dropna().head(10)
                                    if len(sample) > 0:
                                        try:
                                            pd.to_datetime(sample.iloc[0], errors="raise")
                                            date_columns.append(col)
                                        except (ValueError, TypeError):
                                            pass
                        
                        # Date range filtering (only if date columns exist)
                        filtered_df = df.copy()
                        if date_columns:
                            st.markdown("---")
                            st.subheader("📅 Date Range Filter")
                            
                            date_col1, date_col2, date_col3 = st.columns([2, 1, 1])
                            
                            with date_col1:
                                # Select date column if multiple exist
                                if len(date_columns) > 1:
                                    selected_date_column = st.selectbox(
                                        "Select date column",
                                        options=date_columns,
                                        key=f"{session_key}_date_column",
                                        help="Choose which date column to filter by",
                                    )
                                else:
                                    selected_date_column = date_columns[0]
                                    st.caption(f"📅 Filtering by: **{selected_date_column}**")
                            
                            with date_col2:
                                start_date = st.date_input(
                                    "Start date",
                                    value=None,
                                    key=f"{session_key}_start_date",
                                    help="Filter rows from this date onwards (inclusive)",
                                )
                            
                            with date_col3:
                                end_date = st.date_input(
                                    "End date",
                                    value=None,
                                    key=f"{session_key}_end_date",
                                    help="Filter rows up to this date (inclusive)",
                                )
                            
                            # Apply date filter if dates are selected
                            if start_date is not None or end_date is not None:
                                try:
                                    start_datetime = datetime.combine(start_date, datetime.min.time()) if start_date else None
                                    end_datetime = datetime.combine(end_date, datetime.max.time()) if end_date else None
                                    
                                    filtered_df = filter_by_date_range(
                                        filtered_df,
                                        selected_date_column,
                                        start_datetime,
                                        end_datetime,
                                    )
                                    
                                    rows_filtered = len(df) - len(filtered_df)
                                    if rows_filtered > 0:
                                        st.info(f"📊 Date filter applied: {len(filtered_df):,} rows remain (filtered out {rows_filtered:,} rows)")
                                except Exception as e:
                                    st.warning(f"⚠️ Date filter error: {e}")
                                    filtered_df = df.copy()  # Fallback to original
                        
                        # Advanced filtering
                        filtered_df = render_dataframe_filter_ui(
                            filtered_df, key_prefix=f"dataframe_{session_key}"
                        )
                        
                        st.markdown("---")
                        
                        # Display DataFrame
                        st.subheader("Data Preview")
                        st.dataframe(
                            filtered_df,
                            use_container_width=True,
                            height=600,
                        )
                        
                        # Statistics
                        col_stat1, col_stat2, col_stat3 = st.columns(3)
                        with col_stat1:
                            st.metric("Rows Loaded", len(df))
                        with col_stat2:
                            st.metric("Rows After Filter", len(filtered_df))
                        with col_stat3:
                            st.metric("Columns", len(df.columns))
                        
                        # Download option
                        st.markdown("---")
                        csv_data = filtered_df.to_csv(index=False)
                        download_filename = f"{display_name}_filtered.csv"
                        st.download_button(
                            "📥 Download Filtered Data (CSV)",
                            data=csv_data,
                            file_name=download_filename,
                            mime="text/csv",
                        )
                        
                        # Show enriched dataset info if applicable
                        if is_enriched and selected_enriched_dataset:
                            with st.expander("ℹ️ Enriched Dataset Information"):
                                st.write(f"**Name:** {selected_enriched_dataset.name}")
                                st.write(f"**Source Dataset:** {source_dataset.name if source_dataset else 'Unknown'}")
                                st.write(f"**Enriched Columns:** {', '.join(selected_enriched_dataset.columns_added) if selected_enriched_dataset.columns_added else 'None'}")
                                if selected_enriched_dataset.last_sync_date:
                                    st.write(f"**Last Synced:** {selected_enriched_dataset.last_sync_date.strftime('%Y-%m-%d %H:%M:%S')}")
                
                except Exception as e:
                    st.error(f"Failed to load DataFrame: {e}")
            else:
                st.info("👆 Click 'Load Data' to explore the dataset")

