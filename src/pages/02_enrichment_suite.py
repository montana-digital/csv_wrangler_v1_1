"""
Enrichment Suite page for CSV Wrangler v1.1.

Allows users to create enriched datasets with validation and formatting functions.
"""
import streamlit as st

from sqlalchemy import inspect

from src.config.settings import UNIQUE_ID_COLUMN_NAME
from src.database.connection import get_session
from src.database.repository import DatasetRepository
from src.services.enrichment_service import (
    create_enriched_dataset,
    delete_enriched_dataset,
    get_enriched_datasets,
    sync_enriched_dataset,
)
from src.ui.components.enrichment_config import render_enrichment_config_ui
from src.ui.components.enriched_tracker import render_enriched_tracker
from src.ui.components.sidebar import render_sidebar

# Render uniform sidebar
render_sidebar()

st.title("🔧 Enrichment Suite")
st.markdown("**Enrich and validate data columns with intelligent parsing**")
st.markdown("---")

with get_session() as session:
    repo = DatasetRepository(session)
    all_datasets = repo.get_all()
    
    if not all_datasets:
        st.info("No datasets initialized yet. Please initialize a dataset first.")
        st.stop()
    
    # Dataset selector
    st.subheader("Select Source Dataset")
    dataset_options = {f"{d.name} (Slot {d.slot_number})": d.id for d in all_datasets}
    selected_dataset_name = st.selectbox(
        "Choose a dataset to enrich",
        options=list(dataset_options.keys()),
        key="enrichment_dataset_selector",
    )
    
    if selected_dataset_name:
        selected_dataset_id = dataset_options[selected_dataset_name]
        selected_dataset = repo.get_by_id(selected_dataset_id)
        
        if selected_dataset:
            st.markdown("---")
            
            # Get actual table columns from database (more reliable than columns_config)
            inspector = inspect(session.bind)
            try:
                table_columns = [col["name"] for col in inspector.get_columns(selected_dataset.table_name)]
                # Filter out unique_id - it's automatically added and shouldn't be enriched
                dataset_columns = [col for col in table_columns if col != UNIQUE_ID_COLUMN_NAME]
            except Exception as e:
                # Fallback to columns_config if inspection fails
                st.warning(f"Could not inspect table columns: {e}. Using columns_config.")
                dataset_columns = [
                    col for col in selected_dataset.columns_config.keys() 
                    if col != "unique_id" and col != UNIQUE_ID_COLUMN_NAME
                ]
            
            # Enrichment configuration section
            st.subheader("Configure Enrichment")
            
            with st.form("enrichment_config_form"):
                enrichment_config = render_enrichment_config_ui(dataset_columns)
                
                # Enriched dataset name
                enriched_name_key = "enriched_dataset_name"
                default_name = f"enriched-{selected_dataset.name}_v1"
                enriched_name = st.text_input(
                    "Enriched Dataset Name",
                    value=st.session_state.get(enriched_name_key, default_name),
                    key=enriched_name_key,
                )
                
                submitted = st.form_submit_button(
                    "Create Enriched Dataset",
                    type="primary",
                    use_container_width=True,
                )
                
                if submitted:
                    if not enrichment_config:
                        st.error("⚠️ Please select at least one column to enrich.")
                    elif not enriched_name or not enriched_name.strip():
                        st.error("⚠️ Please enter a name for the enriched dataset.")
                    else:
                        try:
                            with st.spinner("Creating enriched dataset..."):
                                enriched = create_enriched_dataset(
                                    session,
                                    selected_dataset_id,
                                    enriched_name.strip(),
                                    enrichment_config,
                                )
                                st.success(
                                    f"✅ Enriched dataset '{enriched.name}' created successfully!"
                                )
                                st.rerun()
                        except Exception as e:
                            st.error(f"Failed to create enriched dataset: {e}")
            
            st.markdown("---")
            
            # Enriched datasets tracker
            enriched_datasets = get_enriched_datasets(session, selected_dataset_id)
            
            def handle_sync(enriched_id: int):
                """Handle sync action."""
                try:
                    sync_enriched_dataset(session, enriched_id)
                except Exception as e:
                    st.error(f"Sync failed: {e}")
                    raise
            
            def handle_delete(enriched_id: int):
                """Handle delete action."""
                try:
                    delete_enriched_dataset(session, enriched_id)
                except Exception as e:
                    st.error(f"Delete failed: {e}")
                    raise
            
            render_enriched_tracker(
                enriched_datasets,
                on_sync=handle_sync,
                on_delete=handle_delete,
            )

