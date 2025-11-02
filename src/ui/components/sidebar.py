"""
Sidebar component for CSV Wrangler.

Provides uniform sidebar across all pages with username, version, and notes.
"""
import streamlit as st

from src.database.connection import get_session
from src.services.profile_service import get_current_profile
from src.services.note_service import (
    create_note,
    delete_note,
    get_all_notes,
)
from src.__version__ import __version__


def render_sidebar() -> None:
    """
    Render uniform sidebar with username, version, and notes.
    
    This should be called at the start of each page.
    """
    with get_session() as session:
        # Get user profile
        profile = get_current_profile(session)
        
        # Username display
        if profile:
            st.sidebar.success(f"👤 {profile.name}")
        else:
            st.sidebar.info("👤 No profile")
        
        st.sidebar.markdown("---")
        
        # Notes section
        st.sidebar.subheader("📝 Notes")
        
        # Add new note
        with st.sidebar.form("add_note_form", clear_on_submit=True):
            note_text = st.text_area(
                "Add a note",
                placeholder="Enter your note here...",
                key="new_note_input",
                height=100,
            )
            if st.form_submit_button("➕ Add Note", use_container_width=True):
                if note_text and note_text.strip():
                    try:
                        create_note(session, note_text.strip())
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to add note: {e}")
                else:
                    st.warning("Please enter some text for your note.")
        
        st.sidebar.markdown("---")
        
        # Display existing notes
        notes = get_all_notes(session)
        
        if notes:
            st.sidebar.markdown("**Your Notes:**")
            for note in notes:
                # Note content in expandable
                with st.sidebar.expander(
                    f"📄 {note.content[:50]}{'...' if len(note.content) > 50 else ''}",
                    expanded=False
                ):
                    st.text(note.content)
                    st.caption(f"Created: {note.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    # Delete button with confirmation - subtle, inside expander
                    confirm_key = f"confirm_delete_{note.id}"
                    
                    st.markdown("---")
                    
                    if confirm_key not in st.session_state:
                        # Show small delete button
                        if st.button(
                            "🗑️ Delete",
                            key=f"delete_note_{note.id}",
                            use_container_width=False,
                            type="secondary",
                        ):
                            st.session_state[confirm_key] = True
                            st.rerun()
                    else:
                        # Show subtle confirmation
                        st.caption("⚠️ Delete this note?")
                        col1, col2 = st.columns(2)
                        with col1:
                            if st.button(
                                "Yes",
                                key=f"confirm_yes_{note.id}",
                                use_container_width=True,
                                type="primary",
                            ):
                                try:
                                    delete_note(session, note.id)
                                    del st.session_state[confirm_key]
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Failed to delete note: {e}")
                        with col2:
                            if st.button(
                                "Cancel",
                                key=f"confirm_no_{note.id}",
                                use_container_width=True,
                                type="secondary",
                            ):
                                del st.session_state[confirm_key]
                                st.rerun()
                
                st.sidebar.markdown("---")
        else:
            st.sidebar.info("No notes yet. Add one above!")
        
        # Version display at bottom
        st.sidebar.markdown("---")
        st.sidebar.caption(f"Version {__version__}")

