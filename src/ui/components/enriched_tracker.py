"""
Enriched datasets tracker component for CSV Wrangler v1.1.

Displays list of enriched datasets with sync status and management options.
"""
from datetime import datetime

import streamlit as st

from src.database.models import EnrichedDataset


def render_enriched_tracker(
    enriched_datasets: list[EnrichedDataset],
    on_sync=None,
    on_delete=None,
) -> None:
    """
    Render enriched datasets tracker.
    
    Args:
        enriched_datasets: List of EnrichedDataset instances
        on_sync: Callback function(enriched_dataset_id) for sync action
        on_delete: Callback function(enriched_dataset_id) for delete action
    """
    st.subheader("📊 Enriched Datasets Tracker")
    
    if not enriched_datasets:
        st.info("No enriched datasets created yet. Use the configuration above to create one.")
        return
    
    # Display enriched datasets in a table format
    for enriched in enriched_datasets:
        with st.expander(
            f"**{enriched.name}** (Source: Dataset {enriched.source_dataset_id})",
            expanded=False,
        ):
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown(f"**Enriched Table:** `{enriched.enriched_table_name}`")
                st.markdown(f"**Source Table:** `{enriched.source_table_name}`")
                
                # Columns added
                if enriched.columns_added:
                    st.markdown(f"**Columns Added:** {', '.join(enriched.columns_added)}")
                else:
                    st.markdown("**Columns Added:** None")
                
                # Last sync date
                if enriched.last_sync_date:
                    sync_date_str = enriched.last_sync_date.strftime("%Y-%m-%d %H:%M:%S")
                    st.markdown(f"**Last Sync:** {sync_date_str}")
                    
                    # Check if sync is needed (more than 1 hour old)
                    time_since_sync = datetime.now() - enriched.last_sync_date
                    if time_since_sync.total_seconds() > 3600:
                        st.warning("⚠️ Sync may be needed")
                    else:
                        st.success("✅ Up to date")
                else:
                    st.warning("⚠️ Never synced")
                
                # Enrichment config
                st.markdown("**Enrichment Config:**")
                for col, func in enriched.enrichment_config.items():
                    st.caption(f"  • {col} → {func}")
            
            with col2:
                # Sync button
                if on_sync:
                    if st.button("🔄 Sync Now", key=f"sync_{enriched.id}"):
                        with st.spinner("Syncing enriched dataset..."):
                            # Handler function handles errors and shows messages
                            on_sync(enriched.id)
                            # Rerun to refresh the UI
                            st.rerun()
                
                # Delete button
                if on_delete:
                    if st.button("🗑️ Delete", key=f"delete_{enriched.id}", type="secondary"):
                        with st.spinner("Deleting enriched dataset..."):
                            try:
                                on_delete(enriched.id)
                                st.success("✅ Deleted!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Delete failed: {e}")
        
        st.markdown("---")

