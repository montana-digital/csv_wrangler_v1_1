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

