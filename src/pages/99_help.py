"""
Help and Support page for CSV Wrangler.

Provides troubleshooting guides, error code lookup, and support information.
"""
import streamlit as st

from src.ui.components.sidebar import render_sidebar
from src.utils.diagnostics import (
    export_diagnostics,
    format_diagnostics_for_display,
    get_full_diagnostics,
)
from src.utils.error_reporter import format_error_report_for_display
from src.utils.package_check import get_missing_optional_packages, get_package_status_report
from src.utils.user_messages import ERROR_MESSAGES, get_error_message

# Render uniform sidebar
render_sidebar()

st.title("ℹ️ Help & Support")
st.markdown("---")

# Tabs for different help sections
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["Getting Started", "Troubleshooting", "Error Codes", "System Diagnostics", "Optional Packages"]
)

with tab1:
    st.header("Getting Started")

    st.markdown("### Welcome to CSV Wrangler!")
    st.markdown(
        """
        CSV Wrangler helps you manage, analyze, and enrich CSV data files with ease.
        
        **Key Features:**
        - 📁 **5 Dataset Slots**: Organize multiple CSV datasets
        - 📊 **Data Analysis**: Advanced operations (merge, group, pivot, etc.)
        - 🔍 **Search**: Full-text search across all datasets
        - 🎨 **Enrichment**: Clean and enrich your data automatically
        - 📈 **Visualizations**: Create charts and graphs (requires Plotly)
        - 💾 **Export**: Export your data in various formats
        
        **Quick Start:**
        1. Navigate to one of the Dataset pages (1-5)
        2. Initialize a dataset by providing a name and column configuration
        3. Upload your CSV files
        4. Use other pages to analyze, search, and enrich your data
        """
    )

    st.markdown("### Common Tasks")
    st.markdown(
        """
        **Uploading Files:**
        - Go to any Dataset page
        - Click "Choose Files" and select your CSV or Pickle file
        - The file will be validated and uploaded automatically
        
        **Analyzing Data:**
        - Use the Data Geek page for advanced operations
        - Create visualizations using the chart selector
        - Export results for further analysis
        
        **Searching Data:**
        - Use Knowledge Search for full-text search
        - Use Image Search to find images in your datasets
        """
    )

with tab2:
    st.header("Troubleshooting")

    st.markdown("### Common Issues and Solutions")

    st.markdown("#### Upload Issues")
    st.markdown(
        """
        **File not uploading:**
        - Check that the file format is CSV or Pickle
        - Ensure the file isn't too large (max 500MB)
        - Verify column names match the dataset configuration
        - Check that you have write permissions
        
        **Column mismatch error:**
        - Ensure all required columns are present
        - Column names must match exactly (including capitalization)
        - Check the dataset configuration in Settings
        """
    )

    st.markdown("#### Performance Issues")
    st.markdown(
        """
        **Slow file reading:**
        - Install PyArrow for faster CSV reading: `pip install pyarrow`
        - Large files are processed in chunks - be patient
        - Consider splitting very large files
        
        **Memory errors:**
        - Use smaller files
        - Close other applications
        - Export some data to reduce dataset size
        """
    )

    st.markdown("#### Feature Unavailable")
    st.markdown(
        """
        **Visualizations not working:**
        - Install Plotly: `pip install plotly`
        - Visualizations are optional - other features work without it
        
        **Advanced date parsing not working:**
        - Install python-dateutil: `pip install python-dateutil`
        - Basic date parsing still works without it
        """
    )

    st.markdown("### Still Having Issues?")
    st.markdown(
        """
        If you continue to experience problems:
        1. Check the Error Codes tab for specific error messages
        2. Export diagnostics (see System Diagnostics tab)
        3. Contact support with the diagnostics file
        """
    )

with tab3:
    st.header("Error Code Reference")

    st.markdown("Search for an error code or browse all error messages:")

    # Search box
    search_term = st.text_input("Search error codes", key="error_search")

    # Filter error messages
    filtered_errors = ERROR_MESSAGES
    if search_term:
        filtered_errors = {
            code: msg
            for code, msg in ERROR_MESSAGES.items()
            if search_term.lower() in code.lower()
            or search_term.lower() in msg.message.lower()
            or search_term.lower() in msg.title.lower()
        }

    # Display error messages
    if not filtered_errors:
        st.info("No error codes found matching your search.")
    else:
        for error_code, error_msg in sorted(filtered_errors.items()):
            with st.expander(f"**{error_code}**: {error_msg.title}", expanded=False):
                st.markdown(f"**Message:** {error_msg.message}")
                if error_msg.troubleshooting:
                    st.markdown("**Troubleshooting:**")
                    for tip in error_msg.troubleshooting:
                        st.markdown(f"- {tip}")
                if error_msg.recovery_action:
                    st.markdown(f"**Suggested Action:** {error_msg.recovery_action}")

with tab4:
    st.header("System Diagnostics")

    st.markdown(
        """
        System diagnostics help identify configuration issues and missing dependencies.
        Export this information to share with support.
        """
    )

    # Show diagnostics
    if st.button("Generate Diagnostics Report"):
        with st.spinner("Collecting system information..."):
            diagnostics = get_full_diagnostics()
            formatted = format_diagnostics_for_display(diagnostics)

            st.markdown("### Diagnostics Report")
            st.code(formatted, language="markdown")

            # Export button
            if st.button("Export Diagnostics"):
                try:
                    file_path = export_diagnostics()
                    st.success(f"✅ Diagnostics exported to: {file_path}")
                    st.info("You can share this file with support for troubleshooting.")
                except Exception as e:
                    st.error(f"Failed to export diagnostics: {e}")

    # Quick status check
    st.markdown("### Quick Status Check")
    package_status = get_package_status_report()

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Required Packages")
        for pkg, status in package_status.items():
            if status["category"] == "required":
                available = "✅" if status["available"] else "❌"
                version = status.get("version", "N/A")
                st.markdown(f"{available} **{pkg}**: {version}")

    with col2:
        st.markdown("#### Optional Packages")
        for pkg, status in package_status.items():
            if status["category"] == "optional":
                available = "✅" if status["available"] else "⚠️"
                version = status.get("version", "Not installed")
                st.markdown(f"{available} **{pkg}**: {version}")

with tab5:
    st.header("Optional Packages")

    st.markdown(
        """
        CSV Wrangler works with just the core packages, but optional packages
        add enhanced functionality.
        """
    )

    missing = get_missing_optional_packages()

    if missing:
        st.warning("**Missing Optional Packages**")
        st.markdown("The following optional packages are not installed:")
        for pkg in missing:
            st.markdown(f"- **{pkg}**")

        st.markdown("### Installation Instructions")
        st.code(f"pip install {' '.join(missing)}", language="bash")

        st.markdown("### What You're Missing")
        package_info = {
            "plotly": {
                "name": "Plotly",
                "description": "Interactive visualizations and charts",
                "install": "pip install plotly",
                "features": ["Chart creation", "Interactive graphs", "Data visualization"],
            },
            "pyarrow": {
                "name": "PyArrow",
                "description": "Faster CSV file reading",
                "install": "pip install pyarrow",
                "features": ["Faster file processing", "Better performance on large files"],
            },
            "dateutil": {
                "name": "python-dateutil",
                "description": "Advanced date and time parsing",
                "install": "pip install python-dateutil",
                "features": ["More date formats supported", "Better date parsing"],
            },
        }

        for pkg in missing:
            if pkg in package_info:
                info = package_info[pkg]
                with st.expander(f"📦 {info['name']}", expanded=False):
                    st.markdown(f"**Description:** {info['description']}")
                    st.markdown(f"**Install:** `{info['install']}`")
                    st.markdown("**Features:**")
                    for feature in info["features"]:
                        st.markdown(f"- {feature}")
    else:
        st.success("✅ All optional packages are installed!")
        st.markdown("You have access to all features.")

    st.markdown("---")
    st.markdown("### All Optional Packages")
    st.markdown(
        """
        - **plotly**: Interactive visualizations (Data Geek page)
        - **pyarrow**: Faster CSV reading performance
        - **python-dateutil**: Advanced date parsing in enrichment functions
        """
    )

