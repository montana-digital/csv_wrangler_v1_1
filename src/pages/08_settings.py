"""
Settings page for CSV Wrangler.
"""
import streamlit as st
from pathlib import Path

from src.database.connection import get_session
from src.database.repository import DatasetRepository
from src.services.dataset_service import delete_dataset, get_dataset_statistics
from src.services.profile_service import (
    get_current_profile,
    update_profile_logo,
    update_profile_name,
)
from src.utils.file_utils import cleanup_dataset_originals
from src.utils.error_handler import SafeOperation
from src.config.settings import LOGO_DIR
from src.ui.components.sidebar import render_sidebar

# Render uniform sidebar
render_sidebar()

st.title("⚙️ Settings")
st.markdown("---")

with get_session() as session:
    # App Configuration Section
    st.subheader("🎨 App Configuration")
    
    profile = get_current_profile(session)
    if profile:
        # Username editing
        st.markdown("**Profile Name**")
        with st.form("update_username_form"):
            new_username = st.text_input(
                "Username",
                value=profile.name,
                key="username_input",
                help="Change your display name shown in the sidebar"
            )
            if st.form_submit_button("💾 Update Username", use_container_width=True):
                if new_username and new_username.strip() and new_username != profile.name:
                    try:
                        update_profile_name(session, new_username.strip())
                        st.success("✅ Username updated successfully!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to update username: {e}")
                elif new_username == profile.name:
                    st.info("Username unchanged")
        
        st.markdown("---")
        
        # Logo upload
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.markdown("**Current Logo**")
            if profile.logo_path:
                logo_path = Path(profile.logo_path)
                if logo_path.exists():
                    st.image(str(logo_path), use_container_width=True)
                else:
                    st.info("Logo file not found")
            else:
                st.info("No logo uploaded")
        
        with col2:
            st.markdown("**Upload Logo**")
            uploaded_logo = st.file_uploader(
                "Choose a logo image file",
                type=["png", "jpg", "jpeg", "gif", "svg", "webp"],
                key="logo_uploader",
                help="Upload a logo image to display on the App Info page. Supported formats: PNG, JPG, JPEG, GIF, SVG, WEBP"
            )
            
            if uploaded_logo is not None:
                try:
                    # Ensure logo directory exists
                    LOGO_DIR.mkdir(parents=True, exist_ok=True)
                    
                    # Save uploaded file
                    file_extension = Path(uploaded_logo.name).suffix.lower()
                    logo_filename = f"logo{file_extension}"
                    logo_file_path = LOGO_DIR / logo_filename
                    
                    # Write file
                    with open(logo_file_path, "wb") as f:
                        f.write(uploaded_logo.getbuffer())
                    
                    # Update profile with logo path
                    update_profile_logo(session, str(logo_file_path))
                    
                    st.success(f"✅ Logo uploaded successfully!")
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"Failed to upload logo: {e}")
    
    st.markdown("---")
    
    # Database Configuration Section
    repo = DatasetRepository(session)
    all_datasets = repo.get_all()

    st.subheader("📊 Database Configuration")

    if not all_datasets:
        st.info("No datasets configured yet.")
    else:
        # Dataset selector
        dataset_options = {f"{d.name} (Slot {d.slot_number})": d.id for d in all_datasets}
        selected_name = st.selectbox(
            "Select Dataset to View Details",
            options=list(dataset_options.keys()),
            key="dataset_selector",
        )

        if selected_name:
            dataset_id = dataset_options[selected_name]
            dataset = repo.get_by_id(dataset_id)

            if dataset:
                st.markdown("---")
                st.subheader(f"Dataset Details: {dataset.name}")

                # Get statistics
                stats = get_dataset_statistics(session, dataset.id)

                # Display details
                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("**Basic Information**")
                    st.json({
                        "Name": dataset.name,
                        "Slot Number": dataset.slot_number,
                        "Table Name": dataset.table_name,
                        "Total Rows": f"{stats['total_rows']:,}",
                        "Total Uploads": stats["total_uploads"],
                        "Columns": len(stats["column_names"]),
                    })

                with col2:
                    st.markdown("**Upload History**")
                    if stats["first_upload"]:
                        st.write(f"**First Upload:** {stats['first_upload']}")
                    if stats["last_upload"]:
                        st.write(f"**Last Upload:** {stats['last_upload']}")

                st.markdown("---")

                # Column information
                st.subheader("Column Information")
                col_info = []
                for col_name, col_type in stats["column_types"].items():
                    col_info.append({
                        "Column": col_name,
                        "Type": col_type,
                        "Is Image": col_name in stats["image_columns"],
                    })

                st.dataframe(col_info, use_container_width=True)

                st.markdown("---")

                # Delete dataset
                st.subheader("⚠️ Delete Dataset")
                st.warning(
                    "Deleting a dataset will permanently remove all data, "
                    "the database table, and all upload history. This action cannot be undone."
                )

                with st.expander("Delete Dataset", expanded=False):
                    confirm_text = st.text_input(
                        f"Type '{dataset.name}' to confirm deletion",
                        key=f"delete_confirm_{dataset_id}",
                    )

                    if st.button(
                        "Delete Dataset",
                        type="primary",
                        key=f"delete_btn_{dataset_id}",
                        disabled=confirm_text != dataset.name,
                    ):
                        with SafeOperation(
                            operation_name="Delete Dataset",
                            error_code="DATABASE_ERROR",
                            suppress_error=False,  # Let exceptions propagate so session rollback works
                        ):
                            with st.spinner("Deleting dataset..."):
                                # Cleanup originals
                                cleanup_dataset_originals(dataset.name)

                                # Delete dataset (this will cascade delete upload logs)
                                # DDL operations (DROP TABLE) auto-commit, but session.delete needs commit
                                delete_dataset(session, dataset.id)
                                
                                # Explicitly flush and commit to ensure deletion is persisted
                                # This is necessary because st.rerun() will restart the page
                                session.flush()
                                session.commit()
                        
                        # If we get here, deletion succeeded (no exception)
                        st.success(f"✅ Dataset '{dataset.name}' deleted successfully!")
                        st.rerun()

    st.markdown("---")

    # Database info
    st.subheader("📁 Database Information")
    from src.config.settings import DATABASE_PATH

    if DATABASE_PATH.exists():
        db_size = DATABASE_PATH.stat().st_size / (1024 * 1024)  # MB
        st.info(f"Database location: `{DATABASE_PATH}`\n\nDatabase size: {db_size:.2f} MB")
    else:
        st.info("Database not yet created.")


