"""
App Home page for CSV Wrangler.

Displays overview dashboard with high-level information.
"""
import streamlit as st

from src.database.connection import get_session
from src.database.repository import DatasetRepository
from src.services.profile_service import get_current_profile
from src.ui.components.sidebar import render_sidebar

# Render uniform sidebar
render_sidebar()

# Page title
st.title("📊 CSV Wrangler")
st.markdown("**Manage large CSV and Pickle datasets with ease**")
st.markdown("---")

with get_session() as session:
    # Get user profile
    profile = get_current_profile(session)
    if profile:
        st.subheader(f"Welcome, {profile.name}!")
        st.markdown(f"**Profile created:** {profile.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
        st.markdown("---")

    # Get dataset statistics
    repo = DatasetRepository(session)
    all_datasets = repo.get_all()

    st.subheader("📊 Dataset Overview")

    if not all_datasets:
        st.info("No datasets initialized yet. Navigate to a Dataset page to get started!")
    else:
        # Display dataset slots status
        cols = st.columns(5)
        for i in range(5):
            slot_num = i + 1
            dataset = repo.get_by_slot(slot_num)
            
            with cols[i]:
                if dataset:
                    st.success(f"**Dataset #{slot_num}**\n\n✅ {dataset.name}")
                else:
                    st.info(f"**Dataset #{slot_num}**\n\n⏳ Empty")

        st.markdown("---")

        # Summary statistics
        total_datasets = len(all_datasets)
        st.metric("Total Datasets", total_datasets)

        # Recent uploads summary
        st.subheader("📥 Recent Activity")
        st.info("Recent uploads summary will be displayed here.")

    st.markdown("---")
    
    # Version History Section
    st.subheader("📋 Version History")
    from src.__version__ import get_version, get_version_history
    
    current_version = get_version()
    version_history = get_version_history()
    
    # Display current version
    st.markdown(f"**Current Version:** `{current_version}`")
    st.markdown("---")
    
    # Filter versions to only show 1.0.3 and below
    def version_compare(version_str):
        """Compare version strings numerically."""
        try:
            parts = version_str.split('.')
            return tuple(int(part) for part in parts)
        except (ValueError, AttributeError):
            return (0, 0, 0)
    
    current_version_tuple = version_compare(current_version)
    filtered_history = {
        v: info for v, info in version_history.items()
        if version_compare(v) <= current_version_tuple
    }
    
    # Display version history (most recent first)
    # Sort versions by date (descending)
    sorted_versions = sorted(
        filtered_history.items(),
        key=lambda x: x[1]["date"],
        reverse=True
    )
    
    for version, info in sorted_versions:
        with st.expander(
            f"**v{version}** - {info['description']} ({info['date']})",
            expanded=(version == current_version)
        ):
            st.markdown(f"**Release Date:** {info['date']}")
            st.markdown(f"**Status:** {info['status'].title()}")
            st.markdown("**Features:**")
            for feature in info["features"]:
                st.markdown(f"- {feature}")

