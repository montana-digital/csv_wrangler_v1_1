"""
Dataset configuration UI component for CSV Wrangler.

Handles multi-step dataset initialization form.
"""
import streamlit as st
import pandas as pd

from src.services.file_import_service import import_file
from src.services.csv_service import detect_base64_image_columns


def render_dataset_config_ui(slot_number: int, existing_dataset=None):
    """
    Render dataset configuration UI.
    
    Uses session state to persist parsed DataFrame across reruns.
    File upload is OUTSIDE form to prevent form reset issues.
    
    Args:
        slot_number: Dataset slot number (1-5)
        existing_dataset: Existing DatasetConfig if already initialized
        
    Returns:
        Dictionary with configuration or None if cancelled
    """
    if existing_dataset:
        st.success(f"✅ Dataset '{existing_dataset.name}' is initialized")
        st.json(existing_dataset.to_dict())
        return None

    st.subheader(f"Initialize Dataset #{slot_number}")

    # Session state keys for persisting parsed data
    df_key = f"parsed_df_{slot_number}"
    file_type_key = f"file_type_{slot_number}"
    detected_images_key = f"detected_images_{slot_number}"

    # Step 1: Upload file OUTSIDE form (prevents form reset on upload)
    st.markdown("#### Step 1: Upload File & Name Dataset")
    
    uploaded_file = st.file_uploader(
        "Upload CSV or Pickle file",
        type=["csv", "pkl", "pickle"],
        key=f"upload_{slot_number}",
    )

    # Parse file when uploaded and store in session state
    if uploaded_file is not None:
        # Check if this is a new file (by comparing filename)
        current_filename = uploaded_file.name
        stored_filename = st.session_state.get(f"uploaded_filename_{slot_number}")
        
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
                st.session_state[f"uploaded_filename_{slot_number}"] = current_filename
                
                # Detect Base64 images
                detected_images = detect_base64_image_columns(df)
                st.session_state[detected_images_key] = detected_images
                
                # Reset column configs when new file is uploaded
                for col in df.columns:
                    type_key = f"type_{slot_number}_{col}"
                    image_key = f"image_{slot_number}_{col}"
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
    file_type = st.session_state.get(file_type_key)
    detected_images = st.session_state.get(detected_images_key, [])

    # Step 2: Dataset name and column configuration (inside form)
    with st.form(f"init_dataset_{slot_number}"):
        # Dataset name input
        name_key = f"dataset_name_{slot_number}"
        dataset_name = st.text_input(
            "Dataset Name",
            placeholder=f"Dataset {slot_number}",
            key=name_key
        )

        # Step 2: Configure columns (only if file is parsed)
        if df is not None:
            st.markdown("#### Step 2: Configure Column Data Types")
            
            columns_config = {}
            image_columns = []

            for col in df.columns:
                col1, col2, col3 = st.columns([2, 1, 1])
                
                with col1:
                    st.text(f"**{col}**")
                
                with col2:
                    type_key = f"type_{slot_number}_{col}"
                    # Widget automatically manages session state via key
                    # Initialize default only if not exists (BEFORE widget)
                    if type_key not in st.session_state:
                        st.session_state[type_key] = "TEXT"
                    
                    current_type = st.session_state[type_key]
                    type_options = ["TEXT", "INTEGER", "REAL"]
                    default_index = type_options.index(current_type) if current_type in type_options else 0
                    
                    data_type = st.selectbox(
                        "Type",
                        type_options,
                        index=default_index,
                        key=type_key,
                        label_visibility="collapsed"
                    )
                
                with col3:
                    image_key = f"image_{slot_number}_{col}"
                    # Initialize default only if not exists (BEFORE widget)
                    if image_key not in st.session_state:
                        st.session_state[image_key] = col in detected_images
                    
                    current_image = st.session_state[image_key]
                    
                    is_image = st.checkbox(
                        "Base64",
                        value=current_image,
                        key=image_key,
                        label_visibility="collapsed"
                    )

                columns_config[col] = {
                    "type": data_type,
                    "is_image": is_image,
                }

                if is_image:
                    image_columns.append(col)

            config_ready = True
        else:
            columns_config = {}
            image_columns = []
            config_ready = False
            
            if uploaded_file is None:
                st.info("👆 Please upload a CSV or Pickle file to begin")
            else:
                st.info("⏳ Processing file...")

        # Submit button - MUST be inside form
        if not config_ready:
            st.info("💡 Please upload a file and enter a dataset name, then configure columns above")
        
        submitted = st.form_submit_button(
            "Initialize Dataset", 
            type="primary",
            use_container_width=True
        )
        
        # Process submission INSIDE form context
        if submitted:
            if not config_ready:
                st.error("⚠️ Please complete all steps above before initializing the dataset.")
                return None
            
            if not dataset_name:
                st.error("⚠️ Please enter a dataset name.")
                return None
            
            if df is None:
                st.error("⚠️ Please upload and parse a file first.")
                return None
            
            # Build final config from current widget values
            final_columns_config = {}
            final_image_columns = []
            
            for col in df.columns:
                type_key = f"type_{slot_number}_{col}"
                image_key = f"image_{slot_number}_{col}"
                
                data_type = st.session_state.get(type_key, "TEXT")
                is_image = st.session_state.get(image_key, False)
                
                final_columns_config[col] = {
                    "type": data_type,
                    "is_image": is_image,
                }
                
                if is_image:
                    final_image_columns.append(col)
            
            # Clear session state after successful submission
            for key in [df_key, file_type_key, detected_images_key, f"uploaded_filename_{slot_number}", name_key]:
                if key in st.session_state:
                    del st.session_state[key]
            
            # Clear column-specific session state
            for col in df.columns:
                for key_suffix in [f"type_{slot_number}_{col}", f"image_{slot_number}_{col}"]:
                    if key_suffix in st.session_state:
                        del st.session_state[key_suffix]
            
            return {
                "name": dataset_name,
                "columns_config": final_columns_config,
                "image_columns": final_image_columns,
                "file_type": file_type,
            }

    return None
