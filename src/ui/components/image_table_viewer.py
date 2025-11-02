"""
Image table viewer component for CSV Wrangler.

Displays an interactive table with row selection capability using st.data_editor.
"""
from typing import Optional

import streamlit as st
import pandas as pd
from sqlalchemy.orm import Session

from src.services.dataframe_service import (
    load_dataset_dataframe,
    load_enriched_dataset_dataframe,
)
from src.utils.errors import DatabaseError, ValidationError
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def render_image_table_viewer(
    session: Session,
    table_info: dict,
) -> Optional[int]:
    """
    Render interactive table viewer with row selection for image search.
    
    Loads all data from the selected table and displays it in an interactive table.
    Uses st.data_editor with on_select callback for row selection.
    
    Args:
        session: Database session
        table_info: Dictionary with table metadata:
            - type: "dataset" or "enriched_dataset"
            - id: dataset_id or enriched_dataset_id
            - name: Display name
            - table_name: Database table name
            - image_columns: List of image column names
    
    Returns:
        Selected row index (0-based) or None if no selection
    """
    table_type = table_info["type"]
    table_id = table_info["id"]
    table_name = table_info["name"]
    
    # Session state keys
    cache_key = f"image_table_data_{table_type}_{table_id}"
    selection_key = f"image_selected_row_{table_type}_{table_id}"
    
    # Load data (cached in session state)
    if cache_key not in st.session_state:
        with st.spinner(f"Loading data from {table_name}..."):
            try:
                # Load all data with image columns
                if table_type == "dataset":
                    df = load_dataset_dataframe(
                        session=session,
                        dataset_id=table_id,
                        limit=1000000,  # Very large limit to get all data
                        offset=0,
                        include_image_columns=True,  # Include images
                        order_by_recent=True,
                    )
                else:  # enriched_dataset
                    df = load_enriched_dataset_dataframe(
                        session=session,
                        enriched_dataset_id=table_id,
                        limit=1000000,  # Very large limit to get all data
                        offset=0,
                        include_image_columns=True,  # Include images
                        order_by_recent=True,
                    )
                
                st.session_state[cache_key] = df
                logger.info(f"Loaded {len(df)} rows from {table_name}")
                
            except (ValidationError, DatabaseError) as e:
                st.error(f"Error loading data: {e}")
                return None
            except Exception as e:
                st.error(f"Unexpected error loading data: {e}")
                logger.error(f"Failed to load table data: {e}", exc_info=True)
                return None
    
    # Get cached DataFrame
    df = st.session_state[cache_key]
    
    if df.empty:
        st.info("No data available in this table.")
        return None
    
    # Separate image columns from display columns
    image_columns = table_info.get("image_columns", [])
    display_columns = [col for col in df.columns if col not in image_columns]
    
    if not display_columns:
        st.warning("No non-image columns to display.")
        return None
    
    # Create display DataFrame (without image columns)
    display_df = df[display_columns].copy()
    
    # Add Select checkbox column for row selection
    display_df_with_select = display_df.copy()
    display_df_with_select.insert(0, "Select", False)
    
    # Display table info
    st.markdown(f"**Table:** {table_name}")
    st.caption(f"Showing {len(df)} rows, {len(image_columns)} image column(s). Check a row to view its image and details.")
    
    # Use st.data_editor with checkbox column for selection
    editor_key = f"image_table_editor_{table_type}_{table_id}"
    
    # Check if we need to reset the editor state (e.g., when Close button was clicked)
    reset_key = f"reset_editor_{table_type}_{table_id}"
    if reset_key in st.session_state and st.session_state[reset_key]:
        # Reset all checkboxes by clearing editor state and recreating DataFrame
        if editor_key in st.session_state:
            del st.session_state[editor_key]
        display_df_with_select = display_df.copy()
        display_df_with_select.insert(0, "Select", False)
        # Clear the reset flag
        st.session_state[reset_key] = False
    else:
        # Initialize or maintain existing state
        if editor_key not in st.session_state:
            display_df_with_select = display_df.copy()
            display_df_with_select.insert(0, "Select", False)
        else:
            # Use existing state but ensure it matches current data structure
            existing_df = st.session_state[editor_key]
            if len(existing_df) == len(display_df) and "Select" in existing_df.columns:
                display_df_with_select = existing_df.copy()
            else:
                # Data structure changed, reset
                display_df_with_select = display_df.copy()
                display_df_with_select.insert(0, "Select", False)
    
    edited_df = st.data_editor(
        display_df_with_select,
        use_container_width=True,
        height=400,
        key=editor_key,
        num_rows="fixed",
        column_config={
            "Select": st.column_config.CheckboxColumn(
                "Select",
                help="Select this row to view image and details",
                default=False,
            )
        },
        disabled=[col for col in display_df.columns],  # Disable all columns except Select
        hide_index=True,
    )
    
    # Handle single-row selection logic
    selected_mask = edited_df["Select"]
    selected_indices = edited_df[selected_mask].index.tolist()
    
    # Handle multiple selections - show warning instead of error
    if len(selected_indices) > 1:
        # Show warning that only one row can be selected
        st.warning(
            "⚠️ Only one row can be selected at a time. "
            "Please deselect other rows to view details."
        )
        # Return None - don't show details until only one is selected
        return None
    elif len(selected_indices) == 1:
        # Valid single selection
        # The index in edited_df matches the position in display_df, which matches df
        selected_row_idx = selected_indices[0]
        # Ensure index is valid
        if 0 <= selected_row_idx < len(df):
            st.session_state[selection_key] = selected_row_idx
            return selected_row_idx
        else:
            # Invalid index, clear selection
            if selection_key in st.session_state:
                del st.session_state[selection_key]
            return None
    else:
        # No selection
        if selection_key in st.session_state:
            del st.session_state[selection_key]
        return None

