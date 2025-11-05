"""
Main entry point for CSV Wrangler Streamlit application.

Displays general application information and core features.
"""
import sys
from pathlib import Path

# Ensure project root is in Python path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import streamlit as st

from src.database.connection import init_database
from src.config.settings import ensure_userdata_directories, LOGO_DIR
from src.utils.logging_config import setup_logging
from src.__version__ import __version__, VERSION_HISTORY

# Setup logging
logger = setup_logging()

# Ensure directories exist
ensure_userdata_directories()

# Initialize database
try:
    init_database()
except Exception as e:
    logger.error(f"Failed to initialize database: {e}", exc_info=True)
    # Can't use st.error() before st.set_page_config(), so just stop
    import sys
    print(f"ERROR: Failed to initialize database: {e}", file=sys.stderr)
    sys.exit(1)

# Check if app is initialized and apply user preferences
# This must happen before st.set_page_config() which must be the first Streamlit command
from src.database.connection import get_session
from src.services.profile_service import is_app_initialized

# Get user preferences and apply to page config
# MUST be first Streamlit command, so we need to check initialization first
try:
    with get_session() as session:
        if is_app_initialized(session):
            from src.utils.preference_manager import apply_user_preferences
            apply_user_preferences(session)
        else:
            # Default config for initialization screen
            st.set_page_config(
                page_title="CSV Wrangler",
                page_icon="📊",
                layout="wide",
                initial_sidebar_state="expanded",
            )
except Exception as e:
    # Fallback to default config if preference loading fails
    logger.warning(f"Failed to load user preferences: {e}")
    st.set_page_config(
        page_title="CSV Wrangler",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

from src.services.profile_service import get_current_profile

with get_session() as session:
    if not is_app_initialized(session):
        # Show initialization UI on main page
        st.title("📊 CSV Wrangler")
        st.markdown("**Manage large CSV and Pickle datasets with ease**")
        st.info("👋 Welcome! Please initialize the application.")
        st.markdown("---")
        
        # Initialize profile
        with st.form("initialize_profile"):
            st.subheader("Create Your Profile")
            user_name = st.text_input("Enter your name:", placeholder="Your Name")
            
            if st.form_submit_button("Initialize Application"):
                if user_name and user_name.strip():
                    try:
                        from src.services.profile_service import create_user_profile
                        profile = create_user_profile(session, user_name.strip())
                        st.success(f"Welcome, {profile.name}! Application initialized.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to create profile: {e}")
                else:
                    st.error("Please enter a valid name.")
        
        st.stop()
    else:
        # App is initialized - render uniform sidebar
        from src.ui.components.sidebar import render_sidebar
        render_sidebar()
        
        # Get profile for logo display
        profile = get_current_profile(session)
        
        # Main page content - App Info
        st.title("📊 CSV Wrangler")
        st.markdown("**Manage large CSV and Pickle datasets with ease**")
        st.markdown("---")
        
        # Logo display section
        col_logo, col_info = st.columns([1, 2])
        
        with col_logo:
            st.subheader("Logo")
            if profile and profile.logo_path:
                logo_path = Path(profile.logo_path)
                if logo_path.exists():
                    st.image(str(logo_path), use_container_width=True)
                else:
                    st.info("Logo file not found. Please upload a new logo in Settings.")
            else:
                st.info("No logo uploaded. Upload a logo in Settings to display it here.")
        
        with col_info:
            st.subheader("Application Information")
            st.markdown(f"**Version:** {__version__}")
            st.markdown("**Description:** A powerful tool for managing and exploring large CSV and Pickle datasets.")
            
            st.markdown("---")
            st.markdown("### Core Features")
            st.markdown("""
            - **Multi-Dataset Management**: Manage up to 5 datasets simultaneously
            - **File Support**: Upload and process CSV and Pickle files
            - **Data Exploration**: Advanced filtering and search capabilities
            - **Data Enrichment**: Validate and format data with intelligent parsing
            - **Export Functionality**: Export filtered data with date range support
            - **Image Handling**: Automatic detection and optional display of Base64 image columns
            """)
        
        st.markdown("---")
        
        # Version history
        st.subheader("Version History")
        for version, info in sorted(VERSION_HISTORY.items(), reverse=True):
            with st.expander(f"Version {version} - {info['description']}", expanded=False):
                st.markdown(f"**Date:** {info['date']}")
                st.markdown(f"**Status:** {info['status']}")
                st.markdown("**Features:**")
                for feature in info['features']:
                    st.markdown(f"  - {feature}")
        
        st.markdown("---")
        
        # Quick links
        st.subheader("Quick Links")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**Dataset Management**")
            st.markdown("- Navigate to Dataset pages (1-5) to initialize and manage datasets")
            st.markdown("- Upload CSV or Pickle files to your datasets")
        
        with col2:
            st.markdown("**Data Exploration**")
            st.markdown("- Use DataFrame View for advanced filtering")
            st.markdown("- Use Enrichment Suite to validate and format data")
        
        with col3:
            st.markdown("**Settings**")
            st.markdown("- Configure datasets and view statistics")
            st.markdown("- Upload a custom logo for your organization")

