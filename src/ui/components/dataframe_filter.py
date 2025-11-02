"""
Advanced DataFrame filtering component for CSV Wrangler v1.1.

Provides UI for filtering and searching DataFrames.
"""
import streamlit as st
import pandas as pd


def render_dataframe_filter_ui(
    df: pd.DataFrame,
    key_prefix: str = "filter",
) -> pd.DataFrame:
    """
    Render advanced filtering UI for DataFrame.
    
    Args:
        df: DataFrame to filter
        key_prefix: Prefix for session state keys
        
    Returns:
        Filtered DataFrame
    """
    if df.empty:
        return df
    
    st.subheader("🔍 Advanced Filtering")
    
    filtered_df = df.copy()
    
    # Global search
    search_key = f"{key_prefix}_search"
    search_term = st.text_input(
        "Search across all columns",
        placeholder="Enter search term...",
        key=search_key,
    )
    
    if search_term:
        # Search across all string columns
        mask = pd.Series([False] * len(filtered_df))
        for col in filtered_df.select_dtypes(include=["object"]).columns:
            mask |= filtered_df[col].astype(str).str.contains(
                search_term, case=False, na=False
            )
        filtered_df = filtered_df[mask]
        st.info(f"Found {len(filtered_df)} rows matching '{search_term}'")
    
    # Column-specific filters
    with st.expander("Column Filters", expanded=False):
        numeric_columns = list(filtered_df.select_dtypes(include=["number"]).columns)
        text_columns = list(filtered_df.select_dtypes(include=["object"]).columns)
        
        if numeric_columns or text_columns:
            filter_col1, filter_col2 = st.columns(2)
            
            with filter_col1:
                if numeric_columns:
                    st.markdown("**Numeric Columns**")
                    for col in numeric_columns[:5]:  # Limit to first 5
                        col_key = f"{key_prefix}_num_{col}"
                        min_val = st.number_input(
                            f"{col} (min)",
                            value=float(filtered_df[col].min()) if len(filtered_df) > 0 else 0.0,
                            key=f"{col_key}_min",
                        )
                        max_val = st.number_input(
                            f"{col} (max)",
                            value=float(filtered_df[col].max()) if len(filtered_df) > 0 else 0.0,
                            key=f"{col_key}_max",
                        )
                        
                        if min_val > filtered_df[col].min() or max_val < filtered_df[col].max():
                            filtered_df = filtered_df[
                                (filtered_df[col] >= min_val) & (filtered_df[col] <= max_val)
                            ]
            
            with filter_col2:
                if text_columns:
                    st.markdown("**Text Columns**")
                    for col in text_columns[:5]:  # Limit to first 5
                        col_key = f"{key_prefix}_text_{col}"
                        filter_text = st.text_input(
                            f"{col} contains",
                            key=col_key,
                        )
                        
                        if filter_text:
                            filtered_df = filtered_df[
                                filtered_df[col].astype(str).str.contains(
                                    filter_text, case=False, na=False
                                )
                            ]
    
    return filtered_df

