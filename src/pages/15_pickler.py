"""
Pickler page for CSV Wrangler.

Allows users to upload pickle files, select columns, filter by date range,
and export filtered data as a new pickle file.
"""
import os
import pickle
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from src.services.analysis_service import detect_date_columns
from src.services.pickler_service import (
    export_filtered_pickle,
    filter_pickle_dataframe,
    process_pickle_file,
)
from src.ui.components.sidebar import render_sidebar
from src.utils.error_handler import SafeOperation
from src.utils.errors import ValidationError

# Render uniform sidebar
render_sidebar()

st.title("🥒 Pickler")
st.markdown("**Upload, filter, and export Pickle files**")
st.markdown("---")

# Session state keys (all prefixed with "pickler_")
UPLOADED_FILE_KEY = "pickler_uploaded_file"
PARSED_DF_KEY = "pickler_parsed_df"
FILENAME_KEY = "pickler_filename"
DATE_COLUMNS_KEY = "pickler_date_columns"
SELECTED_COLUMNS_KEY = "pickler_selected_columns"
USE_DATE_FILTER_KEY = "pickler_use_date_filter"
DATE_COLUMN_KEY = "pickler_date_column"
START_DATE_KEY = "pickler_start_date"
END_DATE_KEY = "pickler_end_date"

# File Upload Section
st.subheader("📤 Upload Pickle File")

uploaded_file = st.file_uploader(
    "Upload Pickle File",
    type=["pkl", "pickle"],
    key="pickler_file_uploader",
    help="Select a .pkl or .pickle file to process",
)

# Check if file was uploaded or changed
if uploaded_file is not None:
    current_filename = uploaded_file.name
    stored_filename = st.session_state.get(FILENAME_KEY)
    
    # If new file uploaded, process it
    if stored_filename != current_filename:
        # Save to temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pkl") as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_path = Path(tmp_file.name)
        
        # Show file info
        file_size_mb = tmp_path.stat().st_size / (1024 * 1024)
        st.info(f"📄 **{current_filename}** ({file_size_mb:.2f} MB)")
        
        # Process file
        try:
            with SafeOperation(
                "process_pickle_file",
                error_code="FILE_PROCESSING_ERROR",
                show_troubleshooting=True,
            ):
                # Show spinner for large files
                if file_size_mb > 1.0:
                    with st.spinner(f"📊 Parsing {current_filename} ({file_size_mb:.1f} MB)..."):
                        df = process_pickle_file(tmp_path)
                else:
                    df = process_pickle_file(tmp_path)
                
                # Store in session state (serialize DataFrame as pickle bytes for session state)
                df_bytes = pickle.dumps(df)
                st.session_state[PARSED_DF_KEY] = df_bytes
                st.session_state[FILENAME_KEY] = current_filename
                st.session_state[UPLOADED_FILE_KEY] = str(tmp_path)
                
                # Detect date columns
                date_columns = detect_date_columns(df)
                st.session_state[DATE_COLUMNS_KEY] = date_columns
                
                # Reset column selection and date filter settings
                st.session_state[SELECTED_COLUMNS_KEY] = list(df.columns)
                st.session_state[USE_DATE_FILTER_KEY] = False
                if DATE_COLUMN_KEY in st.session_state:
                    del st.session_state[DATE_COLUMN_KEY]
                if START_DATE_KEY in st.session_state:
                    del st.session_state[START_DATE_KEY]
                if END_DATE_KEY in st.session_state:
                    del st.session_state[END_DATE_KEY]
                
                st.success(f"✅ File parsed successfully! {len(df)} rows, {len(df.columns)} columns")
                
                # Clean up temp file after parsing
                try:
                    if tmp_path.exists():
                        os.unlink(tmp_path)
                except Exception as e:
                    st.warning(f"Could not delete temporary file: {e}")
                
                st.rerun()
                
        except Exception as e:
            st.error(f"❌ Failed to process file: {e}")
            # Cleanup on error
            try:
                if tmp_path.exists():
                    os.unlink(tmp_path)
            except Exception:
                pass
            st.stop()

# Check if we have a parsed DataFrame in session state
if PARSED_DF_KEY in st.session_state:
    # Restore DataFrame from session state
    df_bytes = st.session_state[PARSED_DF_KEY]
    df = pickle.loads(df_bytes)
    filename = st.session_state.get(FILENAME_KEY, "unknown")
    date_columns = st.session_state.get(DATE_COLUMNS_KEY, [])
    
    st.markdown("---")
    
    # File Information Display
    with st.expander("📊 File Information", expanded=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Rows", f"{len(df):,}")
        with col2:
            st.metric("Total Columns", len(df.columns))
        with col3:
            file_size_kb = len(df_bytes) / 1024
            if file_size_kb > 1024:
                st.metric("File Size", f"{file_size_kb / 1024:.2f} MB")
            else:
                st.metric("File Size", f"{file_size_kb:.2f} KB")
        
        st.markdown("**Columns:**")
        st.write(", ".join(df.columns.tolist()))
        
        st.markdown("**Data Types:**")
        dtype_info = df.dtypes.to_dict()
        dtype_str = ", ".join([f"{col}: {dtype}" for col, dtype in dtype_info.items()])
        st.caption(dtype_str)
        
        st.markdown("**Preview (first 10 rows):**")
        st.dataframe(df.head(10), use_container_width=True)
    
    st.markdown("---")
    
    # Column Selection Section
    st.subheader("📋 Select Columns to Keep")
    
    # Get selected columns from session state (default to all)
    all_columns = list(df.columns)
    default_selected = st.session_state.get(SELECTED_COLUMNS_KEY, all_columns)
    
    selected_columns = st.multiselect(
        "Select columns to keep in the filtered file",
        options=all_columns,
        default=default_selected,
        key=SELECTED_COLUMNS_KEY,
        help="Select one or more columns to include in the exported pickle file",
    )
    
    # Show selected count
    st.caption(f"📊 {len(selected_columns)} of {len(all_columns)} columns selected")
    
    # Validation
    if not selected_columns:
        st.warning("⚠️ Please select at least one column to export")
    
    st.markdown("---")
    
    # Date Range Filter Section (Conditional)
    if date_columns:
        st.subheader("📅 Date Range Filter (Optional)")
        
        use_date_filter = st.checkbox(
            "Filter by Date Range",
            key=USE_DATE_FILTER_KEY,
            value=st.session_state.get(USE_DATE_FILTER_KEY, False),
            help="Enable date range filtering to filter rows by a date column",
        )
        
        if use_date_filter:
            date_col1, date_col2, date_col3 = st.columns([2, 1, 1])
            
            with date_col1:
                # Select date column if multiple exist
                if len(date_columns) > 1:
                    selected_date_column = st.selectbox(
                        "Date Column",
                        options=date_columns,
                        key=DATE_COLUMN_KEY,
                        help="Choose which date column to filter by",
                    )
                else:
                    selected_date_column = date_columns[0]
                    st.caption(f"📅 Filtering by: **{selected_date_column}**")
                    # Store in session state if not already set
                    if DATE_COLUMN_KEY not in st.session_state:
                        st.session_state[DATE_COLUMN_KEY] = selected_date_column
            
            with date_col2:
                start_date = st.date_input(
                    "Start Date",
                    value=st.session_state.get(START_DATE_KEY),
                    key=START_DATE_KEY,
                    help="Filter rows from this date onwards (inclusive)",
                )
            
            with date_col3:
                end_date = st.date_input(
                    "End Date",
                    value=st.session_state.get(END_DATE_KEY),
                    key=END_DATE_KEY,
                    help="Filter rows up to this date (inclusive)",
                )
        else:
            selected_date_column = None
            start_date = None
            end_date = None
    else:
        st.subheader("📅 Date Range Filter")
        st.info("ℹ️ No date columns detected in this file")
        use_date_filter = False
        selected_date_column = None
        start_date = None
        end_date = None
    
    st.markdown("---")
    
    # Export Section
    st.subheader("💾 Export Filtered Pickle File")
    
    if not selected_columns:
        st.info("👆 Please select at least one column above to enable export")
    else:
        if st.button("Generate Filtered Pickle File", type="primary", key="pickler_export_btn"):
            try:
                with SafeOperation(
                    "export_filtered_pickle",
                    error_code="EXPORT_ERROR",
                    show_troubleshooting=True,
                ):
                    # Get filter settings
                    date_column = st.session_state.get(DATE_COLUMN_KEY) if use_date_filter else None
                    start_date_obj = st.session_state.get(START_DATE_KEY) if use_date_filter else None
                    end_date_obj = st.session_state.get(END_DATE_KEY) if use_date_filter else None
                    
                    # Convert date inputs to datetime
                    start_datetime = None
                    end_datetime = None
                    if start_date_obj:
                        start_datetime = datetime.combine(start_date_obj, datetime.min.time())
                    if end_date_obj:
                        end_datetime = datetime.combine(end_date_obj, datetime.max.time())
                    
                    # Apply filtering
                    with st.spinner("🔄 Generating filtered pickle file..."):
                        filtered_df = filter_pickle_dataframe(
                            df=df,
                            columns=selected_columns,
                            date_column=date_column,
                            start_date=start_datetime,
                            end_date=end_datetime,
                        )
                    
                    # Generate filename
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"pickled_filtered_{timestamp}.pkl"
                    
                    # Create temporary file for export
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".pkl") as tmp_file:
                        tmp_path = Path(tmp_file.name)
                    
                    # Export
                    export_path = export_filtered_pickle(filtered_df, tmp_path)
                    
                    # Read file for download
                    with open(export_path, "rb") as f:
                        file_bytes = f.read()
                    
                    # Display download button
                    st.download_button(
                        label="📥 Download Filtered Pickle File",
                        data=file_bytes,
                        file_name=filename,
                        mime="application/octet-stream",
                        key="pickler_download",
                    )
                    
                    # Show success message
                    st.success(
                        f"✅ Filtered pickle file ready! "
                        f"**{len(filtered_df):,} rows**, **{len(selected_columns)} columns**"
                    )
                    
                    # Show filtered data preview
                    st.markdown("**Filtered Data Preview (first 10 rows):**")
                    st.dataframe(filtered_df.head(10), use_container_width=True)
                    
                    # Show statistics
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Original Rows", f"{len(df):,}")
                    with col2:
                        st.metric("Filtered Rows", f"{len(filtered_df):,}")
                    
                    # Cleanup temp file
                    try:
                        if export_path.exists():
                            os.unlink(export_path)
                    except Exception as e:
                        st.warning(f"Could not delete temporary file: {e}")
                        
            except ValidationError as e:
                st.error(f"❌ Validation Error: {e}")
            except Exception as e:
                st.error(f"❌ Export failed: {e}")
    
    st.markdown("---")
    
    # Reset Button
    if st.button("🔄 Reset / Upload New File", key="pickler_reset", type="secondary"):
        # Clean up temp file if exists (check before clearing session state)
        if UPLOADED_FILE_KEY in st.session_state:
            temp_file_path_str = st.session_state[UPLOADED_FILE_KEY]
            try:
                temp_file_path = Path(temp_file_path_str)
                if temp_file_path.exists():
                    os.unlink(temp_file_path)
            except Exception:
                pass  # Ignore cleanup errors
        
        # Clear all session state keys
        keys_to_clear = [
            UPLOADED_FILE_KEY,
            PARSED_DF_KEY,
            FILENAME_KEY,
            DATE_COLUMNS_KEY,
            SELECTED_COLUMNS_KEY,
            USE_DATE_FILTER_KEY,
            DATE_COLUMN_KEY,
            START_DATE_KEY,
            END_DATE_KEY,
        ]
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
        
        st.rerun()

else:
    # No file uploaded yet
    st.info("👆 Please upload a pickle file to get started")

