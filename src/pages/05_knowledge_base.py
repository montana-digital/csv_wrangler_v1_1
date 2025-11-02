"""
Knowledge Base page for CSV Wrangler.

Manages Knowledge Tables for linking enriched data across multiple tables.
Supports multiple Knowledge Tables per data_type for different purposes.
"""
import streamlit as st
from collections import defaultdict
from datetime import datetime

from src.database.connection import get_session
from src.database.repository import KnowledgeTableRepository
from src.services.knowledge_service import (
    get_all_knowledge_tables,
    get_knowledge_tables_by_type,
    initialize_knowledge_table,
)
from src.services.table_service import get_table_row_count
from src.ui.components.knowledge_container import render_knowledge_container
from src.ui.components.knowledge_table_config import render_knowledge_table_config_ui
from src.ui.components.sidebar import render_sidebar

# Render uniform sidebar
render_sidebar()

st.title("📚 Knowledge Base")
st.markdown(
    "**Manage Knowledge Tables for linking enriched data**\n\n"
    "Knowledge Tables store standardized Key_ID values that automatically link to enriched columns. "
    "Create multiple Knowledge Tables per data type (e.g., White Lists, Black Lists, Carrier Info sources)."
)
st.markdown("---")

with get_session() as session:
    repo = KnowledgeTableRepository(session)
    
    # Get all Knowledge Tables
    all_tables = get_all_knowledge_tables(session)
    
    # General Stats Section
    st.subheader("📊 Overview")
    
    if not all_tables:
        st.info("👆 No Knowledge Tables created yet. Create your first Knowledge Table below.")
    else:
        # Count per data type
        stats_by_type = defaultdict(lambda: {"count": 0, "rows": 0, "latest_updated": None})
        
        for table in all_tables:
            stats_by_type[table.data_type]["count"] += 1
            row_count = get_table_row_count(session, table.table_name)
            stats_by_type[table.data_type]["rows"] += row_count
            
            # Track latest updated timestamp
            if table.updated_at:
                current_latest = stats_by_type[table.data_type]["latest_updated"]
                if current_latest is None or table.updated_at > current_latest:
                    stats_by_type[table.data_type]["latest_updated"] = table.updated_at
        
        # Display stats
        col1, col2, col3, col4 = st.columns(4)
        
        phone_tables = stats_by_type.get("phone_numbers", {"count": 0, "rows": 0})
        email_tables = stats_by_type.get("emails", {"count": 0, "rows": 0})
        web_tables = stats_by_type.get("web_domains", {"count": 0, "rows": 0})
        
        with col1:
            st.metric("Phone Tables", phone_tables["count"], help="Knowledge Tables for phone numbers")
            if phone_tables["count"] > 0:
                st.caption(f"{phone_tables['rows']:,} total rows")
        
        with col2:
            st.metric("Email Tables", email_tables["count"], help="Knowledge Tables for emails")
            if email_tables["count"] > 0:
                st.caption(f"{email_tables['rows']:,} total rows")
        
        with col3:
            st.metric("Web Tables", web_tables["count"], help="Knowledge Tables for web domains")
            if web_tables["count"] > 0:
                st.caption(f"{web_tables['rows']:,} total rows")
        
        with col4:
            st.metric("Total Tables", len(all_tables))
        
        # Interactive list of all Knowledge Tables
        st.markdown("#### All Knowledge Tables")
        
        # Group by data_type
        tables_by_type = defaultdict(list)
        for table in all_tables:
            tables_by_type[table.data_type].append(table)
        
        # Display grouped by type
        for data_type in ["phone_numbers", "emails", "web_domains"]:
            type_tables = tables_by_type.get(data_type, [])
            if type_tables:
                type_display = {
                    "phone_numbers": "Phone Numbers",
                    "emails": "Emails",
                    "web_domains": "Web Domains",
                }[data_type]
                
                with st.expander(f"📋 {type_display} ({len(type_tables)} table{'s' if len(type_tables) != 1 else ''})", expanded=False):
                    for table in sorted(type_tables, key=lambda t: t.name):
                        row_count = get_table_row_count(session, table.table_name)
                        updated_str = table.updated_at.strftime("%Y-%m-%d") if table.updated_at else "N/A"
                        
                        # Make table name clickable to select
                        if st.button(
                            f"**{table.name}** - {row_count:,} rows - Updated {updated_str}",
                            key=f"select_table_{table.id}",
                            use_container_width=True,
                        ):
                            st.session_state["selected_knowledge_table_id"] = table.id
                            st.rerun()
    
    st.markdown("---")
    
    # Create New / Select Existing
    st.subheader("Create New / Select Existing")
    
    mode_key = "knowledge_base_mode"
    valid_options = ["Select Existing", "Create New"]
    
    # Handle mode switch flag (set after table creation)
    # This needs to happen before widget creation
    switch_flag = st.session_state.pop("_kb_switch_to_select", False)
    if switch_flag:
        # Force mode to "Select Existing" by clearing the key
        # The widget will use the default index (0 = "Select Existing")
        if mode_key in st.session_state:
            # Only delete if it exists - let widget recreate with correct value
            del st.session_state[mode_key]
    
    # Calculate initial index based on current state
    if mode_key not in st.session_state:
        initial_index = 0  # Default to "Select Existing"
    elif st.session_state[mode_key] not in valid_options:
        # Invalid value - reset to default
        initial_index = 0
    else:
        # Valid value exists - use it
        initial_index = 0 if st.session_state[mode_key] == "Select Existing" else 1
    
    mode = st.radio(
        "Mode",
        options=valid_options,
        index=initial_index,
        key=mode_key,
        horizontal=True,
    )
    
    selected_table = None
    
    if mode == "Select Existing":
        if not all_tables:
            st.info("No Knowledge Tables available. Create one to get started.")
        else:
            # Two-step selection: data_type first, then table
            data_types = ["phone_numbers", "emails", "web_domains"]
            data_type_display = {
                "phone_numbers": "Phone Numbers",
                "emails": "Emails",
                "web_domains": "Web Domains",
            }
            
            col1, col2 = st.columns(2)
            
            with col1:
                selected_data_type_key = "knowledge_selected_data_type"
                if selected_data_type_key not in st.session_state:
                    st.session_state[selected_data_type_key] = data_types[0]
                
                selected_data_type = st.selectbox(
                    "Select Data Type",
                    options=data_types,
                    format_func=lambda x: data_type_display.get(x, x),
                    key=selected_data_type_key,
                )
            
            # Filter tables by selected data_type
            filtered_tables = get_knowledge_tables_by_type(session, selected_data_type)
            
            with col2:
                if filtered_tables:
                    table_options = {
                        f"{t.name} ({get_table_row_count(session, t.table_name)} rows, updated {t.updated_at.strftime('%Y-%m-%d') if t.updated_at else 'N/A'})": t.id
                        for t in filtered_tables
                    }
                    
                    selected_table_name = st.selectbox(
                        "Select Knowledge Table",
                        options=list(table_options.keys()),
                        key=f"knowledge_table_selector_{selected_data_type}",
                    )
                    
                    if selected_table_name:
                        selected_table_id = table_options[selected_table_name]
                        selected_table = repo.get_by_id(selected_table_id)
                        
                        # Store selected table ID in session state
                        st.session_state["selected_knowledge_table_id"] = selected_table_id
                else:
                    st.info(f"No Knowledge Tables found for {data_type_display[selected_data_type]}")
    
    else:  # Create New
        config = render_knowledge_table_config_ui()
        
        if config:
            try:
                with st.spinner("Creating Knowledge Table and processing initial data..."):
                    knowledge_table = initialize_knowledge_table(
                        session=session,
                        name=config["name"],
                        data_type=config["data_type"],
                        primary_key_column=config["primary_key_column"],
                        columns_config=config["columns_config"],
                        image_columns=config.get("image_columns", []),
                        initial_data_df=config.get("initial_data_df"),
                    )
                    
                    st.success(f"✅ Knowledge Table '{knowledge_table.name}' created successfully!")
                    
                    # Switch to select mode and select the newly created table
                    # Set state for next render (rerun will pick this up)
                    st.session_state["selected_knowledge_table_id"] = knowledge_table.id
                    # Use a separate flag to switch mode on next render (avoid widget state conflict)
                    st.session_state["_kb_switch_to_select"] = True
                    st.rerun()
                    
            except Exception as e:
                st.error(f"Error creating Knowledge Table: {e}")
    
    # Display selected table container
    if selected_table is None:
        selected_table_id = st.session_state.get("selected_knowledge_table_id")
        if selected_table_id:
            selected_table = repo.get_by_id(selected_table_id)
    
    if selected_table:
        st.markdown("---")
        render_knowledge_container(session, selected_table)

