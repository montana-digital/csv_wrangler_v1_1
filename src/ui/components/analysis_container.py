"""
Analysis container UI component for Data Geek page.

Displays analysis results in persistent containers with refresh and delete functionality.
"""
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

from src.database.models import DataAnalysis
from src.services.analysis_service import (
    check_refresh_needed,
    delete_analysis,
    load_analysis_result,
    refresh_analysis,
)
from src.services.visualization_service import (
    create_chart,
    is_visualization_available,
    PlotlyNotAvailableError,
)
from src.ui.components.error_display import display_error, display_warning
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def render_analysis_container(
    session,
    analysis: DataAnalysis,
    container_key: str,
) -> None:
    """
    Render analysis result container with data, visualization, and controls.
    
    Args:
        session: Database session
        analysis: DataAnalysis instance
        container_key: Unique key for this container
    """
    # Container styling
    with st.container():
        st.markdown("---")
        
        # Header with metadata
        col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
        
        with col1:
            st.subheader(f"📊 {analysis.name}")
            st.caption(
                f"Operation: {analysis.operation_type} | "
                f"Created: {analysis.created_at.strftime('%Y-%m-%d %H:%M:%S')}"
            )
        
        with col2:
            # Add visualization button (if not exists and plotly available)
            if not analysis.visualization_config:
                if is_visualization_available():
                    add_viz_key = f"add_viz_{analysis.id}"
                    if st.button("📊 Add Chart", key=f"add_viz_btn_{container_key}", use_container_width=True):
                        st.session_state[add_viz_key] = True
                        st.rerun()
                else:
                    # Show that visualizations aren't available
                    st.caption("📊 Charts unavailable")
        
        with col3:
            # Refresh indicator and button
            needs_refresh = check_refresh_needed(session, analysis)
            
            if needs_refresh:
                st.warning("🔄 Update")
            
            refresh_key = f"refresh_{container_key}"
            if st.button("🔄 Refresh", key=refresh_key, use_container_width=True):
                try:
                    with st.spinner("Refreshing analysis..."):
                        refresh_analysis(session, analysis.id)
                    st.success("Analysis refreshed!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to refresh: {e}")
        
        with col4:
            # Delete button with confirmation
            delete_key = f"delete_{container_key}"
            confirm_key = f"confirm_delete_{container_key}"
            
            if confirm_key not in st.session_state:
                if st.button("🗑️ Delete", key=delete_key, use_container_width=True, type="secondary"):
                    st.session_state[confirm_key] = True
                    st.rerun()
            else:
                st.warning("⚠️ Confirm?")
                col_yes, col_no = st.columns(2)
                with col_yes:
                    if st.button("✅ Yes", key=f"{delete_key}_yes", use_container_width=True):
                        try:
                            delete_analysis(session, analysis.id)
                            del st.session_state[confirm_key]
                            st.success("Analysis deleted!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed to delete: {e}")
                with col_no:
                    if st.button("❌ Cancel", key=f"{delete_key}_no", use_container_width=True):
                        del st.session_state[confirm_key]
                        st.rerun()
        
        # Load and display result
        try:
            result_df = load_analysis_result(analysis.result_file_path)
            
            # Statistics
            st.markdown("**Statistics**")
            col_stat1, col_stat2, col_stat3 = st.columns(3)
            with col_stat1:
                st.metric("Rows", len(result_df))
            with col_stat2:
                st.metric("Columns", len(result_df.columns))
            with col_stat3:
                # Show sum of first numeric column if available
                numeric_cols = result_df.select_dtypes(include=["number"]).columns
                if len(numeric_cols) > 0:
                    try:
                        sum_value = result_df[numeric_cols[0]].sum()
                        st.metric("Sum", f"{sum_value:,.0f}")
                    except Exception:
                        st.metric("Numeric Columns", len(numeric_cols))
                else:
                    st.metric("Text Columns", len(result_df.select_dtypes(include=["object"]).columns))
            
            # Visualization
            if analysis.visualization_config:
                st.markdown("**Visualization**")
                try:
                    if not is_visualization_available():
                        display_warning(
                            "Visualizations are not available because the Plotly package is not installed.",
                            troubleshooting=[
                                "Install Plotly to enable visualizations: pip install plotly",
                                "Visualizations are optional - other features work without it",
                                "See the Help page for installation instructions",
                            ],
                        )
                    else:
                        chart_config = analysis.visualization_config
                        chart_type = chart_config.get("chart_type")
                        
                        if chart_type:
                            fig = create_chart(
                                result_df,
                                chart_type,
                                x_column=chart_config.get("x_column"),
                                y_column=chart_config.get("y_column"),
                                title=analysis.name,
                            )
                            st.plotly_chart(fig, use_container_width=True)
                except PlotlyNotAvailableError:
                    display_error(
                        "VISUALIZATION_ERROR",
                        show_troubleshooting=True,
                    )
                except Exception as e:
                    display_error(
                        "VISUALIZATION_ERROR",
                        exception=e,
                        show_troubleshooting=True,
                    )
                    logger.error(f"Visualization error: {e}", exc_info=True)
            
            # Data preview
            st.markdown("**Data Preview**")
            st.dataframe(result_df.head(100), use_container_width=True)
            
            if len(result_df) > 100:
                st.caption(f"Showing first 100 of {len(result_df)} rows")
        
        except Exception as e:
            st.error(f"Failed to load analysis result: {e}")
            logger.error(f"Failed to load analysis {analysis.id}: {e}", exc_info=True)
        
        st.markdown("---")

