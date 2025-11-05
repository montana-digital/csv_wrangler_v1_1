"""
Directory Dave page for CSV Wrangler.

Allows users to explore directory structures with ASCII tree visualization.
"""
import streamlit as st
from pathlib import Path

from src.ui.components.sidebar import render_sidebar
from src.utils.directory_tree import generate_directory_tree, validate_directory_path
from src.utils.error_handler import SafeOperation
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# Render uniform sidebar
render_sidebar()

# Page title
st.title("📁 Directory Dave")
st.markdown("**Explore directory structures with ASCII tree visualization**")
st.markdown("---")

# Initialize session state defaults
if "dir_dave_path" not in st.session_state:
    st.session_state.dir_dave_path = str(Path.cwd())
if "dir_dave_include_files" not in st.session_state:
    st.session_state.dir_dave_include_files = False
if "dir_dave_max_depth" not in st.session_state:
    st.session_state.dir_dave_max_depth = 10
if "dir_dave_max_items" not in st.session_state:
    st.session_state.dir_dave_max_items = 1000

# Input section
st.subheader("Directory Path")
col1, col2 = st.columns([4, 1])

with col1:
    directory_path = st.text_input(
        "Enter directory path:",
        value=st.session_state.dir_dave_path,
        placeholder=str(Path.cwd()),
        help="Enter the root directory path to explore. Defaults to current working directory.",
        key="directory_path_input"
    )

with col2:
    st.write("")  # Spacing
    generate_button = st.button("Generate Tree", type="primary", use_container_width=True)

# Update session state
if directory_path:
    st.session_state.dir_dave_path = directory_path

# Options panel
with st.expander("⚙️ Options", expanded=False):
    include_files = st.checkbox(
        "Include files in tree",
        value=st.session_state.dir_dave_include_files,
        help="Show files in addition to directories in the tree visualization.",
        key="include_files_checkbox"
    )
    st.session_state.dir_dave_include_files = include_files
    
    col_depth, col_items = st.columns(2)
    
    with col_depth:
        max_depth = st.number_input(
            "Max depth",
            min_value=1,
            max_value=50,
            value=st.session_state.dir_dave_max_depth,
            help="Maximum directory depth to traverse. Prevents infinite loops.",
            key="max_depth_input"
        )
        st.session_state.dir_dave_max_depth = max_depth
    
    with col_items:
        max_items = st.number_input(
            "Max items per directory",
            min_value=10,
            max_value=10000,
            value=st.session_state.dir_dave_max_items,
            help="Maximum items to show per directory before truncation.",
            key="max_items_input"
        )
        st.session_state.dir_dave_max_items = max_items

st.markdown("---")

# Generate tree when button is clicked
if generate_button or st.session_state.get("dir_dave_auto_generate", False):
    # Clear auto-generate flag
    st.session_state.dir_dave_auto_generate = False
    
    if not directory_path or not directory_path.strip():
        st.warning("⚠️ Please enter a directory path.")
    else:
        with SafeOperation(
            "generate_directory_tree",
            error_code="DIRECTORY_TREE_ERROR",
            show_troubleshooting=True,
        ):
            # Validate path
            root_path = validate_directory_path(Path(directory_path.strip()))
            
            # Generate tree with progress indicator
            with st.spinner("Generating directory tree..."):
                tree_string, stats = generate_directory_tree(
                    root=root_path,
                    include_files=st.session_state.dir_dave_include_files,
                    max_depth=st.session_state.dir_dave_max_depth,
                    max_items=st.session_state.dir_dave_max_items,
                )
            
            # Display statistics
            st.subheader("Tree Statistics")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Items", f"{stats['total_items']:,}")
            
            with col2:
                st.metric("Directories", f"{stats['total_dirs']:,}")
            
            with col3:
                if st.session_state.dir_dave_include_files:
                    st.metric("Files", f"{stats['total_files']:,}")
                else:
                    st.metric("Max Depth", stats['max_depth_reached'])
            
            with col4:
                st.metric("Generation Time", f"{stats['generation_time']:.2f}s")
            
            # Show warnings if limits were hit
            warnings = []
            if stats.get("items_truncated", 0) > 0:
                warnings.append(
                    f"⚠️ {stats['items_truncated']} items were truncated due to max items limit."
                )
            if stats.get("directories_truncated", 0) > 0:
                warnings.append(
                    f"⚠️ {stats['directories_truncated']} directories were truncated."
                )
            if stats.get("max_depth_reached", 0) > st.session_state.dir_dave_max_depth:
                warnings.append(
                    f"⚠️ Maximum depth ({st.session_state.dir_dave_max_depth}) was reached."
                )
            if stats.get("permission_errors", 0) > 0:
                warnings.append(
                    f"⚠️ {stats['permission_errors']} permission errors encountered."
                )
            
            if warnings:
                for warning in warnings:
                    st.warning(warning)
            
            st.markdown("---")
            
            # Display tree
            st.subheader("Directory Tree")
            st.code(tree_string, language=None)
            
            # Optional: Add download button for large trees
            if len(tree_string) > 1000:
                st.download_button(
                    label="📥 Download Tree as Text",
                    data=tree_string,
                    file_name=f"directory_tree_{Path(root_path).name}.txt",
                    mime="text/plain",
                )

# Show initial instructions if no tree generated yet
if not generate_button and not st.session_state.get("dir_dave_tree_generated", False):
    st.info("""
    👋 **Welcome to Directory Dave!**
    
    Enter a directory path above and click "Generate Tree" to visualize the directory structure.
    
    **Features:**
    - 📁 ASCII tree visualization with Unicode box-drawing characters
    - ⚙️ Configurable depth and item limits
    - 🔒 Symlink cycle detection
    - 📊 Tree statistics and generation time
    - ⚡ Performance optimized for large directories
    
    **Tips:**
    - Start with your current directory (default) or project root
    - Use the options panel to customize depth and item limits
    - Enable "Include files" to see files in addition to directories
    """)
    
    # Show example tree for current directory
    try:
        current_dir = Path.cwd()
        st.markdown("---")
        st.subheader("Example: Current Directory Structure")
        with st.spinner("Generating example tree..."):
            example_tree, example_stats = generate_directory_tree(
                root=current_dir,
                include_files=False,
                max_depth=3,
                max_items=20,
            )
        st.code(example_tree, language=None)
        st.caption(f"Showing first 3 levels, max 20 items per directory. Total: {example_stats['total_items']} items.")
    except Exception as e:
        logger.warning(f"Failed to generate example tree: {e}")

# Mark tree as generated if we got here with a successful generation
if generate_button and directory_path and directory_path.strip():
    st.session_state.dir_dave_tree_generated = True

