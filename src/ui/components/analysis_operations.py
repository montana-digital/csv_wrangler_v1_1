"""
Analysis operations UI components for Data Geek page.

Provides UI for configuring data analysis operations.
"""
from typing import Any, Optional

import streamlit as st


def render_groupby_config(
    available_columns: list[str],
    key_prefix: str = "groupby",
) -> Optional[dict[str, Any]]:
    """
    Render GroupBy operation configuration UI.
    
    Args:
        available_columns: List of available column names
        key_prefix: Prefix for widget keys
        
    Returns:
        Configuration dictionary if submitted, None otherwise
    """
    with st.expander("📊 GroupBy Operation", expanded=False):
        st.markdown("Group data by one or more columns and apply aggregations.")
        
        # Group columns selection
        group_columns = st.multiselect(
            "Select columns to group by",
            options=available_columns,
            key=f"{key_prefix}_group_columns",
            help="Select one or more columns to group the data by",
        )
        
        if not group_columns:
            st.info("Select at least one column to group by")
            return None
        
        # Aggregation configuration
        st.markdown("**Configure Aggregations**")
        aggregations = {}
        
        # Get numeric columns for aggregation
        numeric_hint_cols = [col for col in available_columns if col not in group_columns]
        
        if numeric_hint_cols:
            selected_agg_col = st.selectbox(
                "Select column to aggregate",
                options=numeric_hint_cols,
                key=f"{key_prefix}_agg_column",
            )
            
            agg_functions = st.multiselect(
                "Select aggregation functions",
                options=["sum", "mean", "count", "min", "max", "std", "median"],
                default=["sum"],
                key=f"{key_prefix}_agg_functions",
            )
            
            if selected_agg_col and agg_functions:
                aggregations[selected_agg_col] = agg_functions
        
        # Analysis name
        analysis_name = st.text_input(
            "Analysis name",
            value=f"GroupBy: {', '.join(group_columns)}",
            key=f"{key_prefix}_name",
        )
        
        # Submit button
        if st.button("Create GroupBy Analysis", key=f"{key_prefix}_submit", use_container_width=True):
            if not group_columns:
                st.error("Please select at least one column to group by")
                return None
            
            if not aggregations:
                st.error("Please configure at least one aggregation")
                return None
            
            return {
                "operation_type": "groupby",
                "name": analysis_name or f"GroupBy: {', '.join(group_columns)}",
                "operation_config": {
                    "group_columns": group_columns,
                    "aggregations": aggregations,
                },
            }
    
    return None


def render_pivot_config(
    available_columns: list[str],
    key_prefix: str = "pivot",
) -> Optional[dict[str, Any]]:
    """
    Render Pivot Table operation configuration UI.
    
    Args:
        available_columns: List of available column names
        key_prefix: Prefix for widget keys
        
    Returns:
        Configuration dictionary if submitted, None otherwise
    """
    with st.expander("📋 Pivot Table Operation", expanded=False):
        st.markdown("Create a pivot table to summarize data.")
        
        # Index column
        index_col = st.selectbox(
            "Index (rows)",
            options=available_columns,
            key=f"{key_prefix}_index",
            help="Column to use as row index",
        )
        
        # Columns
        columns_col = st.selectbox(
            "Columns",
            options=[col for col in available_columns if col != index_col],
            key=f"{key_prefix}_columns",
            help="Column to use as column headers",
        )
        
        # Values column (should be numeric)
        values_col = st.selectbox(
            "Values",
            options=[col for col in available_columns if col not in [index_col, columns_col]],
            key=f"{key_prefix}_values",
            help="Column to aggregate",
        )
        
        # Aggregation function
        aggfunc = st.selectbox(
            "Aggregation function",
            options=["sum", "mean", "count", "min", "max"],
            index=0,
            key=f"{key_prefix}_aggfunc",
        )
        
        # Analysis name
        analysis_name = st.text_input(
            "Analysis name",
            value=f"Pivot: {values_col} by {index_col} x {columns_col}",
            key=f"{key_prefix}_name",
        )
        
        # Submit button
        if st.button("Create Pivot Table", key=f"{key_prefix}_submit", use_container_width=True):
            if not all([index_col, columns_col, values_col]):
                st.error("Please select index, columns, and values")
                return None
            
            return {
                "operation_type": "pivot",
                "name": analysis_name or f"Pivot: {values_col}",
                "operation_config": {
                    "index": index_col,
                    "columns": columns_col,
                    "values": values_col,
                    "aggfunc": aggfunc,
                },
            }
    
    return None


def render_merge_config(
    available_columns: list[str],
    secondary_datasets: list[dict[str, Any]],
    key_prefix: str = "merge",
) -> Optional[dict[str, Any]]:
    """
    Render Merge operation configuration UI.
    
    Args:
        available_columns: List of available column names from source dataset
        secondary_datasets: List of available secondary datasets
        key_prefix: Prefix for widget keys
        
    Returns:
        Configuration dictionary if submitted, None otherwise
    """
    with st.expander("🔗 Merge Operation", expanded=False):
        st.markdown("Merge this dataset with another dataset.")
        
        if not secondary_datasets:
            st.warning("No other datasets available for merging")
            return None
        
        # Secondary dataset selection
        secondary_options = {
            f"{ds['name']} (Slot {ds['slot_number']})": ds["id"]
            for ds in secondary_datasets
        }
        
        selected_secondary = st.selectbox(
            "Select secondary dataset",
            options=list(secondary_options.keys()),
            key=f"{key_prefix}_secondary",
        )
        
        if not selected_secondary:
            return None
        
        secondary_dataset_id = secondary_options[selected_secondary]
        
        # Join type
        join_type = st.selectbox(
            "Join type",
            options=["inner", "left", "right", "outer"],
            index=0,
            key=f"{key_prefix}_join_type",
            help="inner: only matching rows, left: all left rows, right: all right rows, outer: all rows",
        )
        
        # Join keys
        st.markdown("**Configure Join Keys**")
        st.caption("Select matching columns from both datasets")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Source Dataset**")
            left_keys = st.multiselect(
                "Join columns",
                options=available_columns,
                key=f"{key_prefix}_left_keys",
            )
        
        with col2:
            st.markdown("**Secondary Dataset**")
            st.caption("Select same number of columns")
            right_keys_hint = st.text_input(
                "Enter column names (comma-separated)",
                key=f"{key_prefix}_right_keys_input",
                help="Enter column names from secondary dataset matching the order of left keys",
            )
        
        # Parse right keys
        right_keys = [key.strip() for key in right_keys_hint.split(",") if key.strip()] if right_keys_hint else []
        
        # Analysis name
        analysis_name = st.text_input(
            "Analysis name",
            value=f"Merge: {selected_secondary}",
            key=f"{key_prefix}_name",
        )
        
        # Submit button
        if st.button("Create Merge Analysis", key=f"{key_prefix}_submit", use_container_width=True):
            if not left_keys:
                st.error("Please select join columns from source dataset")
                return None
            
            if len(right_keys) != len(left_keys):
                st.error(f"Number of right keys ({len(right_keys)}) must match left keys ({len(left_keys)})")
                return None
            
            return {
                "operation_type": "merge",
                "name": analysis_name or f"Merge: {selected_secondary}",
                "secondary_dataset_id": secondary_dataset_id,
                "operation_config": {
                    "left_on": left_keys,
                    "right_on": right_keys,
                    "how": join_type,
                },
            }
    
    return None


def render_concat_config(
    secondary_datasets: list[dict[str, Any]],
    key_prefix: str = "concat",
) -> Optional[dict[str, Any]]:
    """
    Render Concat operation configuration UI.
    
    Args:
        secondary_datasets: List of available secondary datasets
        key_prefix: Prefix for widget keys
        
    Returns:
        Configuration dictionary if submitted, None otherwise
    """
    with st.expander("🔀 Concat Operation", expanded=False):
        st.markdown("Concatenate this dataset with another dataset.")
        
        if not secondary_datasets:
            st.warning("No other datasets available for concatenation")
            return None
        
        # Secondary dataset selection
        secondary_options = {
            f"{ds['name']} (Slot {ds['slot_number']})": ds["id"]
            for ds in secondary_datasets
        }
        
        selected_secondary = st.selectbox(
            "Select secondary dataset",
            options=list(secondary_options.keys()),
            key=f"{key_prefix}_secondary",
        )
        
        if not selected_secondary:
            return None
        
        secondary_dataset_id = secondary_options[selected_secondary]
        
        # Axis selection
        axis = st.radio(
            "Concatenation direction",
            options=["Vertical (rows)", "Horizontal (columns)"],
            index=0,
            key=f"{key_prefix}_axis",
        )
        axis_value = 0 if axis == "Vertical (rows)" else 1
        
        # Ignore index
        ignore_index = st.checkbox(
            "Ignore index",
            value=True,
            key=f"{key_prefix}_ignore_index",
            help="Reset index after concatenation",
        )
        
        # Analysis name
        analysis_name = st.text_input(
            "Analysis name",
            value=f"Concat: {selected_secondary}",
            key=f"{key_prefix}_name",
        )
        
        # Submit button
        if st.button("Create Concat Analysis", key=f"{key_prefix}_submit", use_container_width=True):
            return {
                "operation_type": "concat",
                "name": analysis_name or f"Concat: {selected_secondary}",
                "secondary_dataset_id": secondary_dataset_id,
                "operation_config": {
                    "axis": axis_value,
                    "ignore_index": ignore_index,
                },
            }
    
    return None

