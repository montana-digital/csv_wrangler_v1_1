"""
Preference management utilities for CSV Wrangler.

Handles user preferences for theme mode and layout settings.
"""
from typing import Optional
import streamlit as st
from sqlalchemy.orm import Session

from src.database.models import UserProfile
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def get_user_preferences(session: Session, use_cache: bool = True) -> dict[str, any]:
    """
    Get user preferences from database (with session state caching).
    
    Args:
        session: Database session
        use_cache: If True, use session state cache if available
        
    Returns:
        Dictionary with preferences:
        - theme_mode: "dark" or "light" (default: "dark")
        - wide_mode: bool (default: True)
    """
    # Check session state cache first
    if use_cache and "_user_preferences" in st.session_state:
        cached_prefs = st.session_state["_user_preferences"]
        logger.debug("Using cached user preferences from session state")
        return cached_prefs
    
    # Load from database
    try:
        from src.services.profile_service import get_current_profile
        
        profile = get_current_profile(session)
        if profile:
            preferences = {
                "theme_mode": profile.theme_mode if profile.theme_mode else "dark",
                "wide_mode": profile.wide_mode if profile.wide_mode is not None else True,
            }
            # Cache in session state
            if use_cache:
                st.session_state["_user_preferences"] = preferences
            return preferences
    except Exception as e:
        logger.warning(f"Failed to get user preferences: {e}")
    
    # Defaults
    defaults = {
        "theme_mode": "dark",
        "wide_mode": True,
    }
    # Cache defaults in session state
    if use_cache:
        st.session_state["_user_preferences"] = defaults
    return defaults


def apply_user_preferences(session: Session) -> None:
    """
    Apply user preferences to Streamlit page config.
    
    This should be called early in the app, before any other Streamlit commands.
    Only the first call to st.set_page_config() will take effect.
    
    Args:
        session: Database session
    """
    preferences = get_user_preferences(session)
    
    # Apply theme and layout
    # Note: theme parameter requires Streamlit 1.16+
    # Try to use theme parameter if available
    try:
        # Check if theme parameter is supported by trying to inspect the function signature
        import inspect
        sig = inspect.signature(st.set_page_config)
        if "theme" in sig.parameters:
            # Theme parameter is supported
            theme_config = {
                "base": preferences["theme_mode"],
            }
            st.set_page_config(
                page_title="CSV Wrangler",
                page_icon="📊",
                layout="wide" if preferences["wide_mode"] else "centered",
                initial_sidebar_state="expanded",
                theme=theme_config,
            )
            logger.debug(f"Applied preferences: theme={preferences['theme_mode']}, wide_mode={preferences['wide_mode']}")
        else:
            # Theme parameter not supported, use layout only
            st.set_page_config(
                page_title="CSV Wrangler",
                page_icon="📊",
                layout="wide" if preferences["wide_mode"] else "centered",
                initial_sidebar_state="expanded",
            )
            logger.info(f"Theme preference: {preferences['theme_mode']} (theme parameter not supported, using config.toml)")
    except (TypeError, AttributeError, ValueError) as e:
        # Fallback if theme parameter not supported or invalid (older Streamlit versions)
        st.set_page_config(
            page_title="CSV Wrangler",
            page_icon="📊",
            layout="wide" if preferences["wide_mode"] else "centered",
            initial_sidebar_state="expanded",
        )
        logger.info(f"Theme preference: {preferences['theme_mode']} (fallback mode, using config.toml)")


def update_user_preferences(
    session: Session,
    theme_mode: Optional[str] = None,
    wide_mode: Optional[bool] = None,
) -> bool:
    """
    Update user preferences in database.
    
    Args:
        session: Database session
        theme_mode: "dark" or "light" (optional)
        wide_mode: bool (optional)
        
    Returns:
        True if updated successfully, False otherwise
    """
    try:
        from src.services.profile_service import get_current_profile
        
        profile = get_current_profile(session)
        if not profile:
            logger.warning("No user profile found, cannot update preferences")
            return False
        
        if theme_mode is not None:
            if theme_mode not in ["dark", "light"]:
                logger.warning(f"Invalid theme_mode: {theme_mode}, must be 'dark' or 'light'")
                return False
            profile.theme_mode = theme_mode
        
        if wide_mode is not None:
            profile.wide_mode = wide_mode
        
        session.flush()
        logger.info(f"Updated user preferences: theme_mode={profile.theme_mode}, wide_mode={profile.wide_mode}")
        
        # Update session state cache
        if "_user_preferences" in st.session_state:
            st.session_state["_user_preferences"] = {
                "theme_mode": profile.theme_mode if profile.theme_mode else "dark",
                "wide_mode": profile.wide_mode if profile.wide_mode is not None else True,
            }
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to update user preferences: {e}", exc_info=True)
        return False
