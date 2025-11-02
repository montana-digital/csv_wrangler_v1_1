"""
Knowledge Table configuration UI component for CSV Wrangler.

Handles multi-step Knowledge Table initialization form.
Similar to dataset_config.py but includes data_type and primary_key_column selection.
"""
import streamlit as st
import pandas as pd

from src.services.file_import_service import import_file
from src.services.csv_service import detect_base64_image_columns

# Valid data types for Knowledge Tables
VALID_DATA_TYPES = ["phone_numbers", "emails", "web_domains"]
DATA_TYPE_DISPLAY_NAMES = {
    "phone_numbers": "Phone Numbers",
    "emails": "Emails",
    "web_domains": "Web Domains",
}


def render_knowledge_table_config_ui(existing_table=None):
    """
    Render Knowledge Table configuration UI.
    
    Uses session state to persist parsed DataFrame across reruns.
    File upload is OUTSIDE form to prevent form reset issues.
    
    Args:
        existing_table: Existing KnowledgeTable if already initialized
        
    Returns:
        Dictionary with configuration or None if cancelled
    """
    if existing_table:
        st.success(f"✅ Knowledge Table '{existing_table.name}' is initialized")
        st.json(existing_table.to_dict())
        return None

    st.subheader("Initialize Knowledge Table")

    # Session state keys for persisting parsed data
    df_key = "knowledge_parsed_df"
    file_type_key = "knowledge_file_type"
    detected_images_key = "knowledge_detected_images"

    # Step 1: Upload file OUTSIDE form (prevents form reset on upload)
    st.markdown("#### Step 1: Upload File")
    
    uploaded_file = st.file_uploader(
        "Upload CSV or Pickle file",
        type=["csv", "pkl", "pickle"],
        key="knowledge_upload",
    )

    # Parse file when uploaded and store in session state
    if uploaded_file is not None:
        # Check if this is a new file (by comparing filename)
        current_filename = uploaded_file.name
        stored_filename = st.session_state.get("knowledge_uploaded_filename")
        
        if stored_filename != current_filename:
            # New file uploaded - parse it
            import tempfile
            import os
            from pathlib import Path

            with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(uploaded_file.name)[1]) as tmp_file:
                tmp_file.write(uploaded_file.getvalue())
                tmp_path = tmp_file.name

            try:
                # Show progress for file parsing
                file_size_mb = Path(tmp_path).stat().st_size / (1024 * 1024)
                if file_size_mb > 1.0:
                    with st.spinner(f"📊 Parsing {current_filename} ({file_size_mb:.1f} MB)..."):
                        df, file_type = import_file(Path(tmp_path))
                else:
                    df, file_type = import_file(Path(tmp_path))
                
                # Store in session state
                st.session_state[df_key] = df
                st.session_state[file_type_key] = file_type
                st.session_state["knowledge_uploaded_filename"] = current_filename
                
                # Detect Base64 images
                detected_images = detect_base64_image_columns(df)
                st.session_state[detected_images_key] = detected_images
                
                # Reset column configs when new file is uploaded
                for col in df.columns:
                    type_key = f"knowledge_type_{col}"
                    image_key = f"knowledge_image_{col}"
                    if type_key in st.session_state:
                        del st.session_state[type_key]
                    if image_key in st.session_state:
                        del st.session_state[image_key]
                
                st.success(f"✅ File parsed successfully ({file_type}, {len(df):,} rows)")
                
            except Exception as e:
                st.error(f"Error parsing file: {e}")
                # Clear session state on error
                if df_key in st.session_state:
                    del st.session_state[df_key]
            finally:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)

    # Get parsed DataFrame from session state
    df = st.session_state.get(df_key)
    file_type = st.session_state.get(file_type_key, "CSV")
    detected_images = st.session_state.get(detected_images_key, [])

    if df is None:
        st.info("👆 Please upload a file to begin")
        return None

    # Step 2: Enter Knowledge Table name and select data type
    st.markdown("#### Step 2: Name & Data Type")
    
    name_key = "knowledge_table_name"
    dataset_name = st.text_input(
        "Knowledge Table Name",
        placeholder="e.g., Phone White List",
        key=name_key,
        help="Unique name for this Knowledge Table (e.g., 'Phone White List', 'Carrier Info Source A')"
    )

    data_type_key = "knowledge_data_type"
    if data_type_key not in st.session_state:
        st.session_state[data_type_key] = "phone_numbers"
    
    data_type = st.selectbox(
        "Data Type",
        options=VALID_DATA_TYPES,
        format_func=lambda x: DATA_TYPE_DISPLAY_NAMES.get(x, x),
        key=data_type_key,
        help="Select the type of data this Knowledge Table will store. This determines which enrichment function will be used to standardize Key_IDs."
    )

    # Step 3: Select primary key column
    st.markdown("#### Step 3: Select Primary Key Column")
    
    primary_key_key = "knowledge_primary_key_column"
    if primary_key_key not in st.session_state:
        if len(df.columns) > 0:
            st.session_state[primary_key_key] = df.columns[0]
    
    primary_key_column = st.selectbox(
        "Primary Key Column",
        options=df.columns.tolist(),
        key=primary_key_key,
        help="Select the column that will be used to generate Key_ID values. This column will be standardized using the enrichment function matching the selected data type."
    )
    
    # Show preview of standardization
    if primary_key_column:
        st.info(f"**Preview:** Values from '{primary_key_column}' will be standardized for Key_ID generation")
        # Show sample values
        sample_values = df[primary_key_column].dropna().head(5)
        if not sample_values.empty:
            st.caption(f"Sample values: {', '.join(str(v) for v in sample_values)}")

    # Step 4: Configure columns (only if file is parsed)
    st.markdown("#### Step 4: Configure Column Data Types")
    
    columns_config = {}
    image_columns = []

    for col in df.columns:
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            st.text(f"**{col}**")
        
        with col2:
            type_key = f"knowledge_type_{col}"
            # Widget automatically manages session state via key
            if type_key not in st.session_state:
                st.session_state[type_key] = "TEXT"
            
            current_type = st.session_state[type_key]
            type_options = ["TEXT", "INTEGER", "REAL"]
            default_index = type_options.index(current_type) if current_type in type_options else 0
            
            data_type_col = st.selectbox(
                "Type",
                options=type_options,
                index=default_index,
                key=type_key,
                label_visibility="collapsed",
            )
        
        with col3:
            image_key = f"knowledge_image_{col}"
            if image_key not in st.session_state:
                # Auto-detect if column contains Base64 images
                st.session_state[image_key] = col in detected_images
            
            is_image = st.checkbox(
                "Image",
                value=st.session_state[image_key],
                key=image_key,
                label_visibility="collapsed",
            )
        
        columns_config[col] = {
            "type": data_type_col,
            "is_image": is_image,
        }
        
        if is_image:
            image_columns.append(col)

    # Preview DataFrame (limited rows)
    if len(df) > 0:
        st.markdown("#### Preview Data")
        preview_rows = min(10, len(df))
        st.dataframe(df.head(preview_rows), use_container_width=True)
        st.caption(f"Showing first {preview_rows} of {len(df):,} rows")

    # Initialize button
    st.markdown("---")
    
    config_ready = (
        dataset_name and
        data_type and
        primary_key_column and
        df is not None and
        len(columns_config) > 0
    )
    
    with st.form("knowledge_table_config_form", clear_on_submit=False):
        submitted = st.form_submit_button(
            "✅ Initialize Knowledge Table",
            type="primary",
            use_container_width=True,
            disabled=not config_ready,
        )
        
        # Process submission INSIDE form context
        if submitted:
            if not config_ready:
                st.error("⚠️ Please complete all steps above before initializing the Knowledge Table.")
                return None
            
            if not dataset_name:
                st.error("⚠️ Please enter a Knowledge Table name.")
                return None
            
            if df is None:
                st.error("⚠️ Please upload and parse a file first.")
                return None
            
            # Build final config from current widget values
            final_columns_config = {}
            final_image_columns = []
            
            for col in df.columns:
                type_key = f"knowledge_type_{col}"
                image_key = f"knowledge_image_{col}"
                
                data_type_col = st.session_state.get(type_key, "TEXT")
                is_image = st.session_state.get(image_key, False)
                
                final_columns_config[col] = {
                    "type": data_type_col,
                    "is_image": is_image,
                }
                
                if is_image:
                    final_image_columns.append(col)
            
            # Clear session state after successful submission
            for key in [df_key, file_type_key, detected_images_key, "knowledge_uploaded_filename", name_key]:
                if key in st.session_state:
                    del st.session_state[key]
            
            # Clear column-specific session state
            for col in df.columns:
                for key_suffix in [f"knowledge_type_{col}", f"knowledge_image_{col}"]:
                    if key_suffix in st.session_state:
                        del st.session_state[key_suffix]
            
            return {
                "name": dataset_name,
                "data_type": data_type,
                "primary_key_column": primary_key_column,
                "columns_config": final_columns_config,
                "image_columns": final_image_columns,
                "file_type": file_type,
                "initial_data_df": df,  # Pass DataFrame for initial upload
            }

    return None

