"""
Data Geek page for CSV Wrangler.

Advanced data analysis operations with dynamic visualizations.
"""
from datetime import datetime
from typing import Optional

import streamlit as st

from src.database.connection import get_session
from src.database.repository import DatasetRepository
from src.services.analysis_service import (
    create_analysis,
    detect_date_columns,
    get_all_analyses,
    load_filtered_dataset,
)
from src.services.dataframe_service import get_dataset_columns
from src.ui.components.analysis_container import render_analysis_container
from src.ui.components.analysis_operations import (
    render_concat_config,
    render_groupby_config,
    render_merge_config,
    render_pivot_config,
)
from src.ui.components.sidebar import render_sidebar
from src.ui.components.visualization_selector import render_chart_type_selector
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# Render uniform sidebar
render_sidebar()

st.title("🧪 Data Geek")
st.markdown("**Advanced data analysis operations with dynamic visualizations**")
st.markdown("---")

with get_session() as session:
    repo = DatasetRepository(session)
    all_datasets = repo.get_all()
    
    if not all_datasets:
        st.info("No datasets initialized yet. Please initialize a dataset first.")
        st.stop()
    
    # Dataset Selection & Filtering Section
    st.subheader("📊 Dataset Selection & Filtering")
    
    # Dataset selector
    dataset_options = {f"{d.name} (Slot {d.slot_number})": d.id for d in all_datasets}
    selected_dataset_name = st.selectbox(
        "Select dataset",
        options=list(dataset_options.keys()),
        key="data_geek_dataset_selector",
    )
    
    if not selected_dataset_name:
        st.stop()
    
    selected_dataset_id = dataset_options[selected_dataset_name]
    selected_dataset = repo.get_by_id(selected_dataset_id)
    
    if not selected_dataset:
        st.error("Selected dataset not found")
        st.stop()
    
    # Date range filtering
    st.markdown("**Date Range Filter (Optional)**")
    
    col_date1, col_date2, col_date3 = st.columns([1, 1, 2])
    
    with col_date1:
        use_date_filter = st.checkbox("Filter by date", key="use_date_filter")
    
    date_range_start = None
    date_range_end = None
    date_column = None
    
    if use_date_filter:
        # Load dataset to detect date columns
        try:
            df_sample = load_filtered_dataset(session, selected_dataset_id)
            date_columns = detect_date_columns(df_sample)
            
            with col_date2:
                date_range_start = st.date_input(
                    "Start date",
                    key="date_start",
                )
            
            with col_date3:
                date_range_end = st.date_input(
                    "End date",
                    key="date_end",
                )
            
            if date_columns:
                date_column = st.selectbox(
                    "Date column",
                    options=date_columns,
                    key="date_column_selector",
                )
            else:
                st.warning("No date columns detected in dataset")
        
        except Exception as e:
            st.warning(f"Could not load dataset for date detection: {e}")
    
    # Convert date inputs to datetime
    if date_range_start:
        date_range_start = datetime.combine(date_range_start, datetime.min.time())
    if date_range_end:
        date_range_end = datetime.combine(date_range_end, datetime.max.time())
    
    st.markdown("---")
    
    # Operations Section
    st.subheader("🔧 Analysis Operations")
    st.markdown("Select an operation below to analyze your data:")
    
    # Get available columns for operations
    try:
        df_sample = load_filtered_dataset(session, selected_dataset_id)
        available_columns = [col for col in df_sample.columns if col != "uuid_value"]
    except Exception as e:
        st.error(f"Failed to load dataset: {e}")
        available_columns = []
    
    # Get secondary datasets (exclude current)
    secondary_datasets = [
        {"id": d.id, "name": d.name, "slot_number": d.slot_number}
        for d in all_datasets
        if d.id != selected_dataset_id
    ]
    
    # Operation configurations
    operation_configs = []
    
    # GroupBy operation
    groupby_config = render_groupby_config(available_columns, key_prefix="groupby")
    if groupby_config:
        operation_configs.append(groupby_config)
    
    # Pivot operation
    pivot_config = render_pivot_config(available_columns, key_prefix="pivot")
    if pivot_config:
        operation_configs.append(pivot_config)
    
    # Merge operation
    merge_config = render_merge_config(available_columns, secondary_datasets, key_prefix="merge")
    if merge_config:
        operation_configs.append(merge_config)
    
    # Concat operation
    concat_config = render_concat_config(secondary_datasets, key_prefix="concat")
    if concat_config:
        operation_configs.append(concat_config)
    
    # Process operation submissions
    for op_config in operation_configs:
        try:
            with st.spinner(f"Creating {op_config['operation_type']} analysis..."):
                # Create analysis
                analysis = create_analysis(
                    session=session,
                    name=op_config["name"],
                    operation_type=op_config["operation_type"],
                    source_dataset_id=selected_dataset_id,
                    operation_config=op_config["operation_config"],
                    secondary_dataset_id=op_config.get("secondary_dataset_id"),
                    date_range_start=date_range_start,
                    date_range_end=date_range_end,
                    date_column=date_column,
                )
                
                st.success(f"✅ Analysis '{analysis.name}' created!")
                st.rerun()
        
        except Exception as e:
            st.error(f"Failed to create analysis: {e}")
            logger.error(f"Analysis creation error: {e}", exc_info=True)
    
    st.markdown("---")
    
    # Results Section
    st.subheader("📈 Analysis Results")
    
    # Get all analyses
    all_analyses = get_all_analyses(session)
    
    if not all_analyses:
        st.info("No analyses created yet. Use the operations above to create your first analysis.")
    else:
        # Check for visualization creation requests
        for analysis in all_analyses:
            viz_key = f"add_viz_{analysis.id}"
            if viz_key in st.session_state and st.session_state[viz_key]:
                try:
                    from src.services.analysis_service import load_analysis_result
                    from src.database.repository import DataAnalysisRepository
                    
                    result_df = load_analysis_result(analysis.result_file_path)
                    
                    st.markdown(f"**Add Visualization to: {analysis.name}**")
                    viz_config = render_chart_type_selector(result_df, key_prefix=f"viz_{analysis.id}")
                    
                    if viz_config:
                        # Update analysis with visualization config
                        analysis.visualization_config = viz_config
                        analysis_repo = DataAnalysisRepository(session)
                        analysis_repo.update(analysis)
                        del st.session_state[viz_key]
                        st.success("Visualization added!")
                        st.rerun()
                except Exception as e:
                    st.error(f"Failed to add visualization: {e}")
                    logger.error(f"Visualization error: {e}", exc_info=True)
        
        # Display each analysis in a container
        for analysis in all_analyses:
            container_key = f"analysis_{analysis.id}"
            render_analysis_container(session, analysis, container_key)

