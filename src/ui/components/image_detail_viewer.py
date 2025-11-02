"""
Image detail viewer component for CSV Wrangler.

Displays selected row's images, details, and Knowledge Table associations.
"""
from typing import Any

import streamlit as st
import pandas as pd
from sqlalchemy.orm import Session

from src.services.image_service import get_knowledge_associations_for_row
from src.services.search_service import (
    get_enriched_dataset_data_for_key,
    get_knowledge_table_data_for_key,
)
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def render_image_detail_viewer(
    session: Session,
    row_data: pd.Series,
    image_columns: list[str],
    table_info: dict[str, Any],
    row_index: int,
    is_pinned: bool = False,
) -> None:
    """
    Render detailed view for selected row including images, details, and Knowledge Table associations.
    
    Args:
        session: Database session
        row_data: Pandas Series containing the selected row data (all columns)
        image_columns: List of image column names
        table_info: Dictionary with table metadata (type, id, name, etc.)
        row_index: Index of the row in the DataFrame
        is_pinned: Whether this detail viewer is pinned (persists when row deselected)
    """
    # Create unique key for this row detail viewer
    detail_key = f"image_detail_{table_info['type']}_{table_info['id']}_{row_index}"
    
    # Get row identifier for display
    row_identifier = "Selected Row"
    if "name" in row_data:
        row_identifier = f"Row: {row_data['name']}"
    elif "email" in row_data:
        row_identifier = f"Row: {row_data['email']}"
    else:
        row_identifier = f"Row #{row_index + 1}"
    
    # Create expander with pin indicator
    pin_indicator = " 📌" if is_pinned else ""
    expander_label = f"📸 {row_identifier}{pin_indicator}"
    
    # Use expander for collapsible detail viewer
    with st.expander(expander_label, expanded=True):
        # Pin/Unpin button
        pin_col1, pin_col2 = st.columns([1, 10])
        with pin_col1:
            if is_pinned:
                if st.button("📌 Unpin", key=f"unpin_{detail_key}", help="Unpin this detail viewer"):
                    # Remove from pinned list
                    pinned_set_key = f"pinned_details_{table_info['type']}_{table_info['id']}"
                    if pinned_set_key in st.session_state:
                        pinned_list = list(st.session_state[pinned_set_key])
                        if row_index in pinned_list:
                            pinned_list.remove(row_index)
                        if len(pinned_list) == 0:
                            del st.session_state[pinned_set_key]
                        else:
                            st.session_state[pinned_set_key] = pinned_list
                    # Clear pinned row data cache
                    pinned_data_key = f"pinned_row_data_{table_info['type']}_{table_info['id']}_{row_index}"
                    if pinned_data_key in st.session_state:
                        del st.session_state[pinned_data_key]
                    st.rerun()
            else:
                if st.button("📌 Pin", key=f"pin_{detail_key}", help="Pin this detail viewer to keep it visible"):
                    # Add to pinned set (use list for JSON serialization compatibility)
                    pinned_set_key = f"pinned_details_{table_info['type']}_{table_info['id']}"
                    if pinned_set_key not in st.session_state:
                        st.session_state[pinned_set_key] = []
                    pinned_list = list(st.session_state[pinned_set_key])
                    if row_index not in pinned_list:
                        pinned_list.append(row_index)
                        st.session_state[pinned_set_key] = pinned_list
                    # Cache the row data for pinned display
                    pinned_data_key = f"pinned_row_data_{table_info['type']}_{table_info['id']}_{row_index}"
                    st.session_state[pinned_data_key] = row_data.to_dict()
                    st.rerun()
        
        with pin_col2:
            # Close button (only if not pinned)
            if not is_pinned:
                if st.button("✖ Close", key=f"close_{detail_key}"):
                    # Clear selection in session state
                    selection_key = f"image_selected_row_{table_info['type']}_{table_info['id']}"
                    if selection_key in st.session_state:
                        del st.session_state[selection_key]
                    
                    # Set reset flag to clear checkboxes in table editor on next rerun
                    reset_key = f"reset_editor_{table_info['type']}_{table_info['id']}"
                    st.session_state[reset_key] = True
                    
                    st.rerun()
        
        # Image Section
        has_images = False
        for image_col in image_columns:
            if image_col in row_data and pd.notna(row_data[image_col]) and row_data[image_col]:
                has_images = True
                st.markdown(f"**{image_col}:**")
                try:
                    image_data = str(row_data[image_col])
                    # Handle Base64 image data
                    if image_data.startswith("data:image"):
                        st.image(image_data, use_container_width=True)
                    elif len(image_data) > 100:  # Likely Base64 without prefix
                        # Try to construct proper data URL
                        # Determine image type from common patterns
                        if image_data.startswith("/9j/"):
                            image_type = "jpeg"
                        elif image_data.startswith("iVBORw0KG"):
                            image_type = "png"
                        else:
                            image_type = "png"  # Default
                        st.image(f"data:image/{image_type};base64,{image_data}", use_container_width=True)
                    else:
                        st.warning(f"Image data in {image_col} appears to be invalid or too short.")
                except Exception as e:
                    st.error(f"Error displaying image from {image_col}: {e}")
                    logger.error(f"Failed to display image: {e}", exc_info=True)
        
        if not has_images:
            st.info("No images found in this row.")
        
        st.markdown("---")
        
        # Details Section
        st.subheader("📋 Row Details")
        
        # Separate image columns from other columns
        detail_columns = [col for col in row_data.index if col not in image_columns]
        
        if detail_columns:
            # Create DataFrame for display (single row)
            details_df = pd.DataFrame([row_data[detail_columns]])
            st.dataframe(details_df, use_container_width=True, hide_index=True)
        else:
            st.info("No additional details to display.")
        
        st.markdown("---")
        
        # Knowledge Table Associations Section
        st.subheader("🔗 Knowledge Table Associations")
        
        # Convert row_data Series to dict for service function
        row_dict = row_data.to_dict()
        
        # Get Knowledge Table associations
        try:
            associations = get_knowledge_associations_for_row(
                session=session,
                row_data=row_dict,
                table_type=table_info["type"],
                table_id=table_info["id"],
            )
            
            # Display phone number associations
            if associations.get("phone_numbers"):
                st.markdown("#### 📞 Phone Numbers")
                for phone_assoc in associations["phone_numbers"]:
                    phone_value = phone_assoc["value"]
                    search_results = phone_assoc["search_results"]
                    presence = search_results.get("presence", {})
                    
                    with st.expander(f"Phone: {phone_value}", expanded=False):
                        # Knowledge Tables
                        kt_results = presence.get("knowledge_tables", [])
                        if kt_results:
                            st.markdown("**Knowledge Tables:**")
                            for kt in kt_results:
                                if kt.get("has_data"):
                                    with st.expander(f"📊 {kt['name']} ({kt['row_count']} match(es))", expanded=False):
                                        try:
                                            # Load data from this Knowledge Table
                                            kt_data = get_knowledge_table_data_for_key(
                                                session=session,
                                                knowledge_table_id=kt["table_id"],
                                                key_id=phone_value,
                                                limit=1000,
                                            )
                                            if not kt_data.empty:
                                                # Exclude image columns for display
                                                display_cols = [col for col in kt_data.columns if not col.endswith("_image")]
                                                display_df = kt_data[display_cols] if display_cols else kt_data
                                                st.dataframe(display_df, use_container_width=True)
                                            else:
                                                st.info("No data found (this shouldn't happen if presence shows matches)")
                                        except Exception as e:
                                            st.error(f"Error loading data from {kt['name']}: {e}")
                                            logger.error(f"Failed to load Knowledge Table data: {e}", exc_info=True)
                                else:
                                    st.caption(f"📊 {kt['name']}: No matches")
                        else:
                            st.caption("No Knowledge Tables contain this phone number")
                        
                        # Enriched Datasets
                        ed_results = presence.get("enriched_datasets", [])
                        if ed_results:
                            st.markdown("**Enriched Datasets:**")
                            for ed in ed_results:
                                if ed.get("has_data"):
                                    st.caption(
                                        f"📁 {ed['name']} - Column: {ed['enriched_column']} "
                                        f"({ed['row_count']} match(es))"
                                    )
                                else:
                                    st.caption(f"📁 {ed['name']}: No matches")
                        else:
                            st.caption("No enriched datasets contain this phone number")
            
            # Display web domain associations
            if associations.get("web_domains"):
                st.markdown("#### 🌐 Web Domains")
                for domain_assoc in associations["web_domains"]:
                    domain_value = domain_assoc["value"]
                    search_results = domain_assoc["search_results"]
                    presence = search_results.get("presence", {})
                    
                    with st.expander(f"Domain: {domain_value}", expanded=False):
                        # Knowledge Tables
                        kt_results = presence.get("knowledge_tables", [])
                        if kt_results:
                            st.markdown("**Knowledge Tables:**")
                            for kt in kt_results:
                                if kt.get("has_data"):
                                    with st.expander(f"📊 {kt['name']} ({kt['row_count']} match(es))", expanded=False):
                                        try:
                                            # Load data from this Knowledge Table
                                            kt_data = get_knowledge_table_data_for_key(
                                                session=session,
                                                knowledge_table_id=kt["table_id"],
                                                key_id=domain_value,
                                                limit=1000,
                                            )
                                            if not kt_data.empty:
                                                # Exclude image columns for display
                                                display_cols = [col for col in kt_data.columns if not col.endswith("_image")]
                                                display_df = kt_data[display_cols] if display_cols else kt_data
                                                st.dataframe(display_df, use_container_width=True)
                                            else:
                                                st.info("No data found (this shouldn't happen if presence shows matches)")
                                        except Exception as e:
                                            st.error(f"Error loading data from {kt['name']}: {e}")
                                            logger.error(f"Failed to load Knowledge Table data: {e}", exc_info=True)
                                else:
                                    st.caption(f"📊 {kt['name']}: No matches")
                        else:
                            st.caption("No Knowledge Tables contain this domain")
                        
                        # Enriched Datasets
                        ed_results = presence.get("enriched_datasets", [])
                        if ed_results:
                            st.markdown("**Enriched Datasets:**")
                            for ed in ed_results:
                                if ed.get("has_data"):
                                    st.caption(
                                        f"📁 {ed['name']} - Column: {ed['enriched_column']} "
                                        f"({ed['row_count']} match(es))"
                                    )
                                else:
                                    st.caption(f"📁 {ed['name']}: No matches")
                        else:
                            st.caption("No enriched datasets contain this domain")
            
            # Show message if no associations found
            if not associations.get("phone_numbers") and not associations.get("web_domains"):
                st.info("No phone numbers or web domains found in this row to search in Knowledge Tables.")
        
        except Exception as e:
            st.error(f"Error retrieving Knowledge Table associations: {e}")
            logger.error(f"Failed to get Knowledge Table associations: {e}", exc_info=True)

