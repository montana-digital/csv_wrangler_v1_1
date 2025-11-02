"""
Visualization selector UI component for Data Geek page.

Provides UI for selecting and configuring chart types.
"""
from typing import Any, Optional

import streamlit as st

from src.services.visualization_service import (
    CHART_BAR,
    CHART_BOX,
    CHART_CORRELATION,
    CHART_HEATMAP,
    CHART_HISTOGRAM,
    CHART_LINE,
    CHART_SCATTER,
    is_visualization_available,
    suggest_chart_types,
)
from src.ui.components.error_display import display_warning


def render_chart_type_selector(
    df,
    key_prefix: str = "chart",
) -> Optional[dict[str, Any]]:
    """
    Render chart type selector with intelligent suggestions.
    
    Args:
        df: DataFrame to analyze for suggestions
        key_prefix: Prefix for widget keys
        
    Returns:
        Chart configuration dictionary if chart created, None otherwise
    """
    # Check if visualizations are available
    if not is_visualization_available():
        display_warning(
            "Visualizations are not available because the Plotly package is not installed.",
            troubleshooting=[
                "Install Plotly to enable visualizations: pip install plotly",
                "Visualizations are optional - other features work without it",
                "See the Help page for installation instructions",
            ],
        )
        return None
    
    # Get suggestions
    suggestions = suggest_chart_types(df)
    
    if not suggestions:
        st.info("No chart suggestions available for this data")
        return None
    
    # Recommended charts
    recommended = [s for s in suggestions if s.get("recommended", False)]
    other_charts = [s for s in suggestions if not s.get("recommended", False)]
    
    # Check if a suggestion was selected
    selected_config = None
    
    st.markdown("**Suggested Charts**")
    
    # Show recommended charts first
    if recommended:
        st.caption("Recommended for this data:")
        for i, suggestion in enumerate(recommended):
            chart_type = suggestion["chart_type"]
            description = suggestion["description"]
            
            col1, col2 = st.columns([3, 1])
            with col1:
                st.markdown(f"- **{description}**")
            with col2:
                button_key = f"{key_prefix}_rec_{i}"
                if st.button("Use", key=button_key, use_container_width=True):
                    selected_config = {
                        "chart_type": chart_type,
                        "x_column": suggestion.get("x_column"),
                        "y_column": suggestion.get("y_column"),
                        "columns": suggestion.get("columns"),
                    }
                    st.session_state[f"{key_prefix}_selected"] = selected_config
                    st.rerun()
    
    # Show other charts
    if other_charts and not selected_config:
        st.caption("Other options:")
        for i, suggestion in enumerate(other_charts):
            chart_type = suggestion["chart_type"]
            description = suggestion["description"]
            
            col1, col2 = st.columns([3, 1])
            with col1:
                st.markdown(f"- {description}")
            with col2:
                button_key = f"{key_prefix}_other_{i}"
                if st.button("Use", key=button_key, use_container_width=True):
                    selected_config = {
                        "chart_type": chart_type,
                        "x_column": suggestion.get("x_column"),
                        "y_column": suggestion.get("y_column"),
                        "columns": suggestion.get("columns"),
                    }
                    st.session_state[f"{key_prefix}_selected"] = selected_config
                    st.rerun()
    
    # Check session state for selected config
    if f"{key_prefix}_selected" in st.session_state:
        selected_config = st.session_state[f"{key_prefix}_selected"]
        del st.session_state[f"{key_prefix}_selected"]
        return selected_config
    
    # Manual chart selection
    st.markdown("---")
    st.markdown("**Or select manually:**")
    
    chart_type = st.selectbox(
        "Chart type",
        options=[
            CHART_BAR,
            CHART_LINE,
            CHART_SCATTER,
            CHART_HISTOGRAM,
            CHART_HEATMAP,
            CHART_BOX,
            CHART_CORRELATION,
        ],
        key=f"{key_prefix}_manual_type",
    )
    
    # Get column configuration based on chart type
    available_columns = list(df.columns)
    
    if chart_type == CHART_BAR:
        x_col = st.selectbox("X-axis (categorical)", options=available_columns, key=f"{key_prefix}_x")
        y_col = st.selectbox("Y-axis (numeric)", options=available_columns, key=f"{key_prefix}_y")
        if st.button("Create Chart", key=f"{key_prefix}_create"):
            return {"chart_type": chart_type, "x_column": x_col, "y_column": y_col}
    
    elif chart_type == CHART_LINE:
        x_col = st.selectbox("X-axis (datetime/numeric)", options=available_columns, key=f"{key_prefix}_x")
        y_col = st.selectbox("Y-axis (numeric)", options=available_columns, key=f"{key_prefix}_y")
        if st.button("Create Chart", key=f"{key_prefix}_create"):
            return {"chart_type": chart_type, "x_column": x_col, "y_column": y_col}
    
    elif chart_type == CHART_SCATTER:
        x_col = st.selectbox("X-axis (numeric)", options=available_columns, key=f"{key_prefix}_x")
        y_col = st.selectbox("Y-axis (numeric)", options=available_columns, key=f"{key_prefix}_y")
        if st.button("Create Chart", key=f"{key_prefix}_create"):
            return {"chart_type": chart_type, "x_column": x_col, "y_column": y_col}
    
    elif chart_type == CHART_HISTOGRAM:
        x_col = st.selectbox("Column (numeric)", options=available_columns, key=f"{key_prefix}_x")
        if st.button("Create Chart", key=f"{key_prefix}_create"):
            return {"chart_type": chart_type, "x_column": x_col}
    
    elif chart_type == CHART_BOX:
        x_col = st.selectbox("X-axis (categorical)", options=available_columns, key=f"{key_prefix}_x")
        y_col = st.selectbox("Y-axis (numeric)", options=available_columns, key=f"{key_prefix}_y")
        if st.button("Create Chart", key=f"{key_prefix}_create"):
            return {"chart_type": chart_type, "x_column": x_col, "y_column": y_col}
    
    elif chart_type in [CHART_HEATMAP, CHART_CORRELATION]:
        if st.button("Create Chart", key=f"{key_prefix}_create"):
            return {"chart_type": chart_type}
    
    return None

