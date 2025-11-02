"""
Knowledge Base search UI component for CSV Wrangler.

Provides search interface for querying across Knowledge Tables and enriched datasets.
Two-phase approach: presence search first, detailed retrieval on drill-down.
"""
import time
import streamlit as st
import pandas as pd
from sqlalchemy.orm import Session
from typing import Any, Optional

from src.services.search_service import (
    get_enriched_dataset_data_for_key,
    get_knowledge_table_data_for_key,
    search_knowledge_base,
)
from src.ui.components.data_viewer import render_data_viewer

# Valid data types for search
VALID_DATA_TYPES = ["phone_numbers", "emails", "web_domains"]
DATA_TYPE_DISPLAY = {
    "phone_numbers": "Phone Numbers",
    "emails": "Emails",
    "web_domains": "Web Domains",
}


def render_search_input() -> tuple[Optional[str], Optional[str]]:
    """
    Render search input form.
    
    Returns:
        Tuple of (search_value, data_type) or (None, None) if not submitted
    """
    st.subheader("🔍 Search Knowledge Base")
    st.markdown(
        "Enter a value to search across all Knowledge Tables and enriched datasets. "
        "Results show presence flags first - click to drill down for detailed data."
    )
    
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        search_value = st.text_input(
            "Search Value",
            key="kb_search_value",
            placeholder="e.g., +1234567890 or user@example.com",
            help="Enter phone number, email, or web domain to search",
        )
    
    with col2:
        data_type = st.selectbox(
            "Data Type",
            options=VALID_DATA_TYPES,
            format_func=lambda x: DATA_TYPE_DISPLAY.get(x, x),
            key="kb_search_data_type",
        )
    
    with col3:
        st.write("")  # Spacing
        search_clicked = st.button("🔍 Search", type="primary", key="kb_search_button", use_container_width=True)
    
    if search_clicked:
        if not search_value or not search_value.strip():
            st.error("Please enter a search value")
            return None, None
        return search_value.strip(), data_type
    
    return None, None


def render_source_filters(
    presence_data: dict[str, list[dict[str, Any]]],
) -> dict[str, list[str]]:
    """
    Render source filtering checkboxes.
    
    Args:
        presence_data: Presence results from search
        
    Returns:
        Dictionary with selected filters: {"knowledge_tables": [...], "enriched_datasets": [...]}
    """
    st.markdown("#### Filter Sources")
    
    knowledge_tables = presence_data.get("knowledge_tables", [])
    enriched_datasets = presence_data.get("enriched_datasets", [])
    
    selected_kb_filters = []
    selected_ed_filters = []
    
    if knowledge_tables or enriched_datasets:
        col1, col2 = st.columns(2)
        
        with col1:
            if knowledge_tables:
                st.markdown("**Knowledge Tables:**")
                for kt in knowledge_tables:
                    checked = st.checkbox(
                        kt["name"],
                        value=True,
                        key=f"filter_kt_{kt['table_id']}",
                    )
                    if checked:
                        selected_kb_filters.append(kt["name"])
        
        with col2:
            if enriched_datasets:
                st.markdown("**Enriched Datasets:**")
                for ed in enriched_datasets:
                    checked = st.checkbox(
                        ed["name"],
                        value=True,
                        key=f"filter_ed_{ed['dataset_id']}_{ed['enriched_column']}",
                    )
                    if checked:
                        selected_ed_filters.append(str(ed["dataset_id"]))
    
    return {
        "knowledge_tables": selected_kb_filters,
        "enriched_datasets": selected_ed_filters,
    }


def render_presence_results(
    session: Session,
    presence_data: dict[str, list[dict[str, Any]]],
    standardized_key_id: Optional[str],
    search_stats: dict[str, Any],
) -> None:
    """
    Render presence search results with drill-down capability.
    
    Args:
        session: Database session
        presence_data: Presence results from search
        standardized_key_id: Standardized Key_ID value
        search_stats: Search statistics
    """
    st.markdown("---")
    st.subheader("📊 Search Results")
    
    if standardized_key_id is None:
        st.warning("⚠️ Search value could not be standardized. Please check the format and try again.")
        return
    
    # Display search stats
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Sources", search_stats.get("total_sources", 0))
    with col2:
        st.metric("Matched Sources", search_stats.get("matched_sources", 0))
    with col3:
        search_time = search_stats.get("search_time_ms", 0)
        st.metric("Search Time", f"{search_time:.1f} ms")
    
    st.caption(f"Standardized Key_ID: `{standardized_key_id}`")
    st.markdown("---")
    
    knowledge_tables = presence_data.get("knowledge_tables", [])
    enriched_datasets = presence_data.get("enriched_datasets", [])
    
    if not knowledge_tables and not enriched_datasets:
        st.info("No Knowledge Tables or enriched datasets found for this data type.")
        return
    
    # Render Knowledge Tables section
    if knowledge_tables:
        st.markdown("#### 📚 Knowledge Tables")
        
        for kt in knowledge_tables:
            has_data = kt.get("has_data", False)
            row_count = kt.get("row_count", 0)
            
            # Presence indicator
            if has_data:
                status_icon = "✅"
                status_text = f"Found {row_count} row(s)"
                status_color = "green"
            else:
                status_icon = "❌"
                status_text = "Not found"
                status_color = "gray"
            
            # Collapsible section for each Knowledge Table
            with st.expander(
                f"{status_icon} **{kt['name']}** - {status_text}",
                expanded=has_data,  # Expand if has data
            ):
                st.caption(f"Table: `{kt['table_name']}` | ID: {kt['table_id']}")
                
                if has_data:
                    # Drill-down: Load and display data using nested expander
                    with st.expander("📋 View Data", expanded=False):
                        data_cache_key = f"kb_data_kt_{kt['table_id']}_{standardized_key_id}"
                        
                        # Load data if not cached
                        if data_cache_key not in st.session_state:
                            with st.spinner(f"Loading data from {kt['name']}..."):
                                try:
                                    df = get_knowledge_table_data_for_key(
                                        session,
                                        kt["table_id"],
                                        standardized_key_id,
                                    )
                                    
                                    if not df.empty:
                                        # Hide image columns if present
                                        image_cols = []  # Could extract from KnowledgeTable.columns_config if needed
                                        display_cols = [col for col in df.columns if col not in image_cols]
                                        display_df = df[display_cols].copy()
                                        st.session_state[data_cache_key] = display_df
                                    else:
                                        st.session_state[data_cache_key] = pd.DataFrame()
                                except Exception as e:
                                    st.error(f"Error loading data: {e}")
                                    st.session_state[data_cache_key] = pd.DataFrame()
                        
                        # Display cached data
                        cached_df = st.session_state.get(data_cache_key, pd.DataFrame())
                        if cached_df is not None and not cached_df.empty:
                            st.markdown(f"**Data from {kt['name']}:**")
                            # Use unique keys for data viewer to avoid conflicts
                            viewer_key_prefix = f"kb_viewer_kt_{kt['table_id']}_{standardized_key_id}"
                            
                            # Simple display instead of render_data_viewer to avoid key conflicts
                            st.dataframe(cached_df, use_container_width=True, height=400)
                            st.caption(f"Showing {len(cached_df)} row(s)")
                        else:
                            st.info("No data found (this shouldn't happen if presence shows matches)")
                else:
                    st.info(f"`{standardized_key_id}` not found in this Knowledge Table.")
        
        st.markdown("---")
    
    # Render Enriched Datasets section
    if enriched_datasets:
        st.markdown("#### 🔗 Enriched Datasets")
        
        for ed in enriched_datasets:
            row_count = ed.get("row_count", 0)
            has_data = row_count > 0
            
            # Presence indicator
            if has_data:
                status_icon = "✅"
                status_text = f"Found {row_count} row(s)"
            else:
                status_icon = "❌"
                status_text = "Not found"
            
            # Collapsible section for each enriched dataset
            with st.expander(
                f"{status_icon} **{ed['name']}** - {status_text}",
                expanded=has_data,  # Expand if has data
            ):
                st.caption(
                    f"Table: `{ed['enriched_table_name']}` | "
                    f"Column: `{ed['enriched_column']}` | "
                    f"Dataset ID: {ed['dataset_id']}"
                )
                
                if has_data:
                    # Drill-down: Load and display data using nested expander
                    with st.expander("📋 View Data", expanded=False):
                        data_cache_key = f"kb_data_ed_{ed['dataset_id']}_{ed['enriched_column']}_{standardized_key_id}"
                        
                        # Load data if not cached
                        if data_cache_key not in st.session_state:
                            with st.spinner(f"Loading data from {ed['name']}..."):
                                try:
                                    df = get_enriched_dataset_data_for_key(
                                        session,
                                        ed["dataset_id"],
                                        ed["enriched_column"],
                                        standardized_key_id,
                                    )
                                    
                                    if not df.empty:
                                        # Hide image columns by default
                                        display_cols = [col for col in df.columns]
                                        display_df = df[display_cols].copy()
                                        st.session_state[data_cache_key] = display_df
                                    else:
                                        st.session_state[data_cache_key] = pd.DataFrame()
                                except Exception as e:
                                    st.error(f"Error loading data: {e}")
                                    st.session_state[data_cache_key] = pd.DataFrame()
                        
                        # Display cached data
                        cached_df = st.session_state.get(data_cache_key, pd.DataFrame())
                        if cached_df is not None and not cached_df.empty:
                            st.markdown(f"**Data from {ed['name']}:**")
                            # Use unique keys for data viewer to avoid conflicts
                            viewer_key_prefix = f"kb_viewer_ed_{ed['dataset_id']}_{ed['enriched_column']}_{standardized_key_id}"
                            
                            # Simple display instead of render_data_viewer to avoid key conflicts
                            st.dataframe(cached_df, use_container_width=True, height=400)
                            st.caption(f"Showing {len(cached_df)} row(s)")
                        else:
                            st.info("No data found (this shouldn't happen if presence shows matches)")
                else:
                    st.info(
                        f"`{standardized_key_id}` not found in {ed['enriched_column']} column."
                    )


def render_knowledge_search(session: Session) -> None:
    """
    Main search UI component.
    
    Handles search input, execution, and result display.
    
    Args:
        session: Database session
    """
    # Search input
    search_value, data_type = render_search_input()
    
    # Execute search if requested
    if search_value and data_type:
        # Check cache first (5-minute TTL)
        cache_key = f"kb_search_cache_{data_type}_{search_value}"
        cache_time_key = f"{cache_key}_time"
        
        use_cache = False
        if cache_key in st.session_state and cache_time_key in st.session_state:
            cache_age = time.time() - st.session_state[cache_time_key]
            if cache_age < 300:  # 5 minutes
                use_cache = True
        
        if use_cache:
            st.info("📋 Showing cached results (search again to refresh)")
            search_results = st.session_state[cache_key]
        else:
            # Perform search
            with st.spinner("Searching across Knowledge Tables and enriched datasets..."):
                try:
                    search_results = search_knowledge_base(
                        session,
                        search_value,
                        data_type,
                    )
                    
                    # Cache results
                    st.session_state[cache_key] = search_results
                    st.session_state[cache_time_key] = time.time()
                    
                except Exception as e:
                    st.error(f"Error performing search: {e}")
                    return
        
        # Render results
        presence_data = search_results.get("presence", {})
        standardized_key_id = search_results.get("standardized_key_id")
        search_stats = search_results.get("search_stats", {})
        
        # Source filtering (optional - can be added later)
        # filters = render_source_filters(presence_data)
        
        # Display results
        render_presence_results(
            session,
            presence_data,
            standardized_key_id,
            search_stats,
        )
    else:
        # Show help/info when no search
        st.info(
            "💡 **How to search:**\n\n"
            "1. Enter a phone number, email, or web domain\n"
            "2. Select the matching data type\n"
            "3. Click Search to see presence across all Knowledge Tables and enriched datasets\n"
            "4. Click 'View Data' on any result to see detailed information"
        )

