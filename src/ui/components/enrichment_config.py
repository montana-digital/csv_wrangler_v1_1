"""
Enrichment configuration UI component for CSV Wrangler v1.1.

Allows users to select columns and enrichment functions.
"""
import streamlit as st

from src.services.enrichment_functions import ENRICHMENT_FUNCTIONS

# Mapping of function names to display names
ENRICHMENT_DISPLAY_NAMES = {
    "phone_numbers": "Check & Format | Phone Numbers",
    "web_domains": "Check & Format | Web Domains",
    "emails": "Check & Format | Emails",
    "date_only": "Check & Format | Date Only Field",
    "datetime": "Check & Format | Date Time Field",
}


def render_enrichment_config_ui(
    dataset_columns: list[str],
    existing_config: dict[str, str] = None,
) -> dict[str, str]:
    """
    Render enrichment configuration UI.
    
    Args:
        dataset_columns: List of column names from dataset
        existing_config: Existing enrichment config to edit (optional)
        
    Returns:
        Dictionary mapping column names to enrichment function names,
        or None if cancelled
    """
    if not dataset_columns:
        st.warning("No columns available in this dataset.")
        return None
    
    enrichment_config = existing_config or {}
    
    # Initialize session state for each column if not exists
    for col in dataset_columns:
        key = f"enrichment_{col}"
        if key not in st.session_state:
            st.session_state[key] = enrichment_config.get(col, "None")
    
    st.markdown("Select columns to enrich and choose enrichment functions:")
    st.markdown("---")
    
    # Multi-column enrichment configuration
    for col in dataset_columns:
        col_key = f"enrichment_{col}"
        current_value = st.session_state.get(col_key, "None")
        
        # Get index for selectbox
        options = ["None"] + list(ENRICHMENT_FUNCTIONS.keys())
        default_index = 0
        if current_value in options:
            default_index = options.index(current_value)
        
        st.selectbox(
            f"**{col}**",
            options=options,
            index=default_index,
            key=col_key,
            format_func=lambda x: ENRICHMENT_DISPLAY_NAMES.get(x, x) if x != "None" else "No enrichment",
        )
        
        # Note: Don't manually set session_state here - the widget handles it automatically
    
    st.markdown("---")
    
    # Build final config (exclude "None" selections)
    final_config = {}
    for col in dataset_columns:
        col_key = f"enrichment_{col}"
        selected = st.session_state.get(col_key, "None")
        if selected != "None":
            final_config[col] = selected
    
    # Preview
    if final_config:
        st.info(f"**Preview:** {len(final_config)} column(s) will be enriched")
        for col, func in final_config.items():
            st.caption(f"  • {col} → {ENRICHMENT_DISPLAY_NAMES.get(func, func)}")
    else:
        st.warning("⚠️ Please select at least one column to enrich")
    
    return final_config if final_config else None

