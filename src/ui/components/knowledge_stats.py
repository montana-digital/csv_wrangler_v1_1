"""
Knowledge Table statistics UI component for CSV Wrangler.

Displays statistics for a Knowledge Table including:
- Top 20 Key_IDs by count (matches in enriched datasets)
- Recently added records
- Missing values (in enriched datasets but not in Knowledge Table)
"""
import streamlit as st
import pandas as pd
from datetime import datetime
from typing import Optional

from src.database.models import KnowledgeTable
from src.services.knowledge_service import get_knowledge_table_stats


def render_knowledge_stats(
    session,
    knowledge_table: KnowledgeTable,
    cache_key: Optional[str] = None,
) -> None:
    """
    Render statistics for a Knowledge Table.
    
    Args:
        session: Database session
        knowledge_table: KnowledgeTable instance
        cache_key: Optional cache key for session state
    """
    st.subheader(f"📊 Statistics: {knowledge_table.name}")
    
    # Cache key for this table's stats
    stats_cache_key = f"knowledge_stats_{knowledge_table.id}" if cache_key is None else cache_key
    last_calc_key = f"{stats_cache_key}_last_calc"
    
    # Check if stats need refresh
    stats_cached = stats_cache_key in st.session_state
    needs_refresh = False
    
    if stats_cached:
        last_calc = st.session_state.get(last_calc_key)
        if last_calc:
            # Check if Knowledge Table was updated after last calculation
            if knowledge_table.updated_at and knowledge_table.updated_at > last_calc:
                needs_refresh = True
                st.warning("⚠️ Statistics may be outdated - Knowledge Table has been updated")
    
    # Refresh button
    col1, col2 = st.columns([1, 4])
    with col1:
        refresh_clicked = st.button("🔄 Refresh Statistics", type="secondary", key=f"refresh_stats_{knowledge_table.id}")
    
    if refresh_clicked:
        needs_refresh = True
    
    # Calculate or load from cache
    if needs_refresh or not stats_cached:
        with st.spinner("Calculating statistics... This may take a moment for large datasets."):
            try:
                stats = get_knowledge_table_stats(session, knowledge_table.id)
                
                # Store in session state
                st.session_state[stats_cache_key] = stats
                st.session_state[last_calc_key] = datetime.now()
                
                st.success("✅ Statistics calculated successfully")
            except Exception as e:
                st.error(f"Error calculating statistics: {e}")
                return
    else:
        stats = st.session_state[stats_cache_key]
    
    # Display Top 20 by count
    st.markdown("#### Top 20 Key_IDs by Count")
    top_20 = stats.get("top_20", pd.DataFrame())
    
    if not top_20.empty:
        st.dataframe(
            top_20,
            use_container_width=True,
            hide_index=True,
        )
        st.caption(f"Showing top {len(top_20)} Key_IDs by number of matches in enriched datasets")
    else:
        st.info("No matches found in enriched datasets. Create enriched datasets with matching enrichment functions to see statistics.")
    
    st.markdown("---")
    
    # Display Recently Added
    st.markdown("#### Recently Added (Last 20)")
    recently_added = stats.get("recently_added", pd.DataFrame())
    
    if not recently_added.empty:
        # Hide image columns if present (for performance)
        display_cols = [col for col in recently_added.columns if col not in ["uuid_value"]]
        display_df = recently_added[display_cols].copy()
        
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
        )
        st.caption(f"Showing most recently added {len(display_df)} records")
    else:
        st.info("No data in Knowledge Table yet. Upload data to see recently added records.")
    
    st.markdown("---")
    
    # Display Missing Values
    st.markdown("#### Missing Values")
    st.caption("Values present in enriched datasets but not in this Knowledge Table (limited to top 1000)")
    
    missing_values = stats.get("missing_values", pd.DataFrame())
    
    if not missing_values.empty:
        st.dataframe(
            missing_values,
            use_container_width=True,
            hide_index=True,
        )
        st.caption(f"Showing {len(missing_values)} missing values (out of potentially more)")
        st.info("💡 Tip: These are Key_ID values that appear in your enriched datasets but haven't been added to this Knowledge Table yet.")
    else:
        st.success("✅ No missing values found - all enriched dataset values are present in this Knowledge Table!")
    
    # Show last calculation time
    if last_calc_key in st.session_state:
        last_calc = st.session_state[last_calc_key]
        if isinstance(last_calc, datetime):
            st.caption(f"Last calculated: {last_calc.strftime('%Y-%m-%d %H:%M:%S')}")

