"""
Image Search page for CSV Wrangler.

Allows users to browse tables with image columns, select rows to view images,
and see associated Knowledge Table data for phone numbers and web domains.
"""
import streamlit as st
import pandas as pd
from sqlalchemy.orm import Session

from src.database.connection import get_session
from src.services.image_service import get_tables_with_image_columns
from src.ui.components.image_detail_viewer import render_image_detail_viewer
from src.ui.components.image_table_viewer import render_image_table_viewer

# Page configuration
st.set_page_config(
    page_title="Image Search",
    page_icon="🖼️",
    layout="wide",
)

st.title("🖼️ Image Search")
st.markdown(
    "Browse tables containing images, select rows to view images and details, "
    "and explore associated Knowledge Table data for phone numbers and web domains."
)

# Get database session
with get_session() as session:
    # Get all tables with image columns
    tables_with_images = get_tables_with_image_columns(session)
    
    if not tables_with_images:
        st.info(
            "No tables with image columns found. "
            "Upload data with image columns to use Image Search."
        )
        st.stop()
    
    # Table selector
    table_options = [f"{table['name']}" for table in tables_with_images]
    
    selected_table_idx = st.selectbox(
        "Select Table",
        options=range(len(table_options)),
        format_func=lambda x: table_options[x],
        key="image_search_table_select",
        help="Select a table containing image columns to browse",
    )
    
    if selected_table_idx is not None:
        selected_table = tables_with_images[selected_table_idx]
        
        # Track table changes to clear pinned details when switching tables
        prev_table_key = "image_search_prev_table_id"
        current_table_id = f"{selected_table['type']}_{selected_table['id']}"
        
        if prev_table_key in st.session_state:
            prev_table_id = st.session_state[prev_table_key]
            if prev_table_id != current_table_id:
                # Clear all pinned details for previous table
                prev_table_pinned_key = f"pinned_details_{prev_table_id}"
                if prev_table_pinned_key in st.session_state:
                    del st.session_state[prev_table_pinned_key]
        
        st.session_state[prev_table_key] = current_table_id
        
        # Display table viewer
        st.markdown("---")
        selected_row_idx = render_image_table_viewer(
            session=session,
            table_info=selected_table,
        )
        
        # Get cached DataFrame
        cache_key = f"image_table_data_{selected_table['type']}_{selected_table['id']}"
        
        if cache_key not in st.session_state:
            st.warning("Table data not available. Please select the table again.")
        else:
            df = st.session_state[cache_key]
            
            # Display pinned detail viewers first (persistent)
            pinned_set_key = f"pinned_details_{selected_table['type']}_{selected_table['id']}"
            if pinned_set_key in st.session_state:
                pinned_indices = st.session_state[pinned_set_key]
                # Convert to list if it's not already
                if isinstance(pinned_indices, set):
                    pinned_indices = list(pinned_indices)
                if pinned_indices:
                    st.markdown("---")
                    st.subheader("📌 Pinned Details")
                    for pinned_idx in sorted(pinned_indices):
                        if 0 <= pinned_idx < len(df):
                            # Get cached pinned row data or current row data
                            pinned_data_key = f"pinned_row_data_{selected_table['type']}_{selected_table['id']}_{pinned_idx}"
                            if pinned_data_key in st.session_state:
                                # Use cached data (preserves original selection state)
                                row_dict = st.session_state[pinned_data_key]
                                # Convert dict back to Series with proper index
                                row_data = pd.Series(row_dict)
                            else:
                                # Fallback to current DataFrame row (cache it for next time)
                                row_data = df.iloc[pinned_idx]
                                st.session_state[pinned_data_key] = row_data.to_dict()
                            
                            # Render pinned detail viewer
                            render_image_detail_viewer(
                                session=session,
                                row_data=row_data,
                                image_columns=selected_table["image_columns"],
                                table_info=selected_table,
                                row_index=pinned_idx,
                                is_pinned=True,
                            )
            
            # Display detail viewer for currently selected row (if not already pinned)
            if selected_row_idx is not None:
                # Check if this row is already pinned
                is_currently_pinned = False
                if pinned_set_key in st.session_state:
                    pinned_list = st.session_state[pinned_set_key]
                    # Convert to list if it's a set
                    if isinstance(pinned_list, set):
                        pinned_list = list(pinned_list)
                    is_currently_pinned = selected_row_idx in pinned_list
                
                if not is_currently_pinned:
                    if 0 <= selected_row_idx < len(df):
                        row_data = df.iloc[selected_row_idx]
                        
                        st.markdown("---")
                        # Render detail viewer for selected row (not pinned)
                        render_image_detail_viewer(
                            session=session,
                            row_data=row_data,
                            image_columns=selected_table["image_columns"],
                            table_info=selected_table,
                            row_index=selected_row_idx,
                            is_pinned=False,
                        )
                    else:
                        st.warning("Selected row index is invalid.")

