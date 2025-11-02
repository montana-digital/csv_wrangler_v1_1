"""
Data viewer component for CSV Wrangler.

Displays DataFrame with pagination and filtering options.
"""
import streamlit as st
import pandas as pd


def render_data_viewer(
    df: pd.DataFrame,
    default_rows: int = 10000,
):
    """
    Render data viewer component.
    
    Args:
        df: DataFrame to display
        default_rows: Default number of rows to display
    """
    st.subheader("📊 Data Viewer")

    # Controls
    col1, col2 = st.columns(2)

    with col1:
        max_rows = st.number_input(
            "Max rows to display",
            min_value=100,
            max_value=100000,
            value=default_rows,
            step=1000,
            key="max_rows",
        )

    with col2:
        search_term = st.text_input("Search", placeholder="Search data...", key="search")

    # Filter data
    display_df = df.copy()

    # Apply search
    if search_term:
        # Search across all string columns
        mask = pd.Series([False] * len(display_df))
        for col in display_df.select_dtypes(include=["object"]).columns:
            mask |= display_df[col].astype(str).str.contains(search_term, case=False, na=False)
        display_df = display_df[mask]
        st.info(f"Found {len(display_df)} rows matching '{search_term}'")

    # Limit rows
    display_df = display_df.head(max_rows)

    # Display DataFrame
    if len(display_df) > 0:
        st.dataframe(display_df, use_container_width=True, height=600)
        st.caption(f"Showing {len(display_df)} of {len(df)} total rows")
    else:
        st.info("No data to display")

