"""
CSV/Pickle uploader component for CSV Wrangler.
"""
import os
import tempfile
from pathlib import Path

import streamlit as st

from src.services.file_import_service import import_file
from src.utils.file_utils import copy_to_originals


def render_file_uploader(dataset_id: int, dataset_name: str):
    """
    Render file uploader component.
    
    Uses session state to track upload trigger and persist file across reruns.
    
    Args:
        dataset_id: Dataset ID
        dataset_name: Dataset name
        
    Returns:
        Tuple of (uploaded_file_path, filename) or (None, None)
    """
    st.subheader("📤 Upload New File")

    # Session state keys
    upload_trigger_key = f"upload_trigger_{dataset_id}"
    uploaded_file_key = f"uploaded_file_{dataset_id}"
    uploaded_filename_key = f"uploaded_filename_{dataset_id}"

    uploaded_file = st.file_uploader(
        "Drag and drop CSV or Pickle file here",
        type=["csv", "pkl", "pickle"],
        key=f"uploader_{dataset_id}",
    )

    # Reset upload trigger if file changes
    if uploaded_file is not None:
        current_filename = uploaded_file.name
        if st.session_state.get(uploaded_filename_key) != current_filename:
            st.session_state[upload_trigger_key] = False
            st.session_state[uploaded_filename_key] = current_filename

    if uploaded_file is not None:
        # Display file info
        file_size = len(uploaded_file.getvalue()) / 1024  # KB
        st.info(f"📄 **{uploaded_file.name}** ({file_size:.2f} KB)")

        col1, col2 = st.columns(2)

        with col1:
            # Preview button
            if st.button("👁️ Preview File", key=f"preview_{dataset_id}"):
                with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(uploaded_file.name)[1]) as tmp_file:
                    tmp_file.write(uploaded_file.getvalue())
                    tmp_path = Path(tmp_file.name)

                try:
                    df, file_type = import_file(tmp_path)
                    st.dataframe(df.head(10), use_container_width=True)
                    st.caption(f"Showing first 10 rows of {len(df)} total rows ({file_type})")
                except Exception as e:
                    st.error(f"Error previewing file: {e}")
                finally:
                    if tmp_path.exists():
                        os.unlink(tmp_path)

        with col2:
            # Upload button - triggers upload on next rerun
            if st.button("📤 Upload File", type="primary", key=f"upload_btn_{dataset_id}"):
                st.session_state[upload_trigger_key] = True

        # Process upload if triggered
        if st.session_state.get(upload_trigger_key, False):
            # Reset trigger immediately to prevent re-processing
            st.session_state[upload_trigger_key] = False

            with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(uploaded_file.name)[1]) as tmp_file:
                tmp_file.write(uploaded_file.getvalue())
                tmp_path = Path(tmp_file.name)

            try:
                # Copy to originals
                copy_to_originals(tmp_path, dataset_name)

                # Store in session state so it persists
                st.session_state[uploaded_file_key] = str(tmp_path)
                st.session_state[uploaded_filename_key] = uploaded_file.name

                return tmp_path, uploaded_file.name

            except Exception as e:
                st.error(f"Error preparing file: {e}")
                if tmp_path.exists():
                    os.unlink(tmp_path)
                return None, None

    # Check if we have a pending upload from previous rerun
    if st.session_state.get(uploaded_file_key):
        file_path = Path(st.session_state[uploaded_file_key])
        filename = st.session_state.get(uploaded_filename_key, "unknown")
        
        # Clear session state after returning
        del st.session_state[uploaded_file_key]
        del st.session_state[uploaded_filename_key]
        
        if file_path.exists():
            return file_path, filename

    return None, None

