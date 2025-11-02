"""
Profile service for CSV Wrangler.

Handles user profile creation and management.
"""
from typing import Optional

from sqlalchemy.orm import Session

from src.database.models import UserProfile
from src.database.repository import UserProfileRepository
from src.utils.errors import ValidationError
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def create_user_profile(
    session: Session,
    name: str,
) -> UserProfile:
    """
    Create a new user profile.
    
    Args:
        session: Database session
        name: User name
        
    Returns:
        Created UserProfile instance
        
    Raises:
        ValidationError: If name is empty or invalid
    """
    # Validate name
    if not name or not name.strip():
        raise ValidationError(
            "User name cannot be empty",
            field="name",
            value=name,
        )

    # Check if profile already exists
    repo = UserProfileRepository(session)
    if repo.exists():
        existing = repo.get_first()
        logger.warning(f"Profile already exists: {existing.name}")
        return existing

    # Create new profile
    profile = UserProfile(name=name.strip())
    created = repo.create(profile)

    logger.info(f"Created user profile: {created.name}")

    return created


def is_app_initialized(session: Session) -> bool:
    """
    Check if the app has been initialized (profile exists).
    
    Args:
        session: Database session
        
    Returns:
        True if profile exists, False otherwise
    """
    repo = UserProfileRepository(session)
    return repo.exists()


def get_current_profile(session: Session) -> Optional[UserProfile]:
    """
    Get the current user profile.
    
    Args:
        session: Database session
        
    Returns:
        UserProfile instance or None if not initialized
    """
    repo = UserProfileRepository(session)
    return repo.get_first()


def update_profile_logo(
    session: Session,
    logo_path: str,
) -> UserProfile:
    """
    Update the logo path for the current user profile.
    
    Args:
        session: Database session
        logo_path: Path to the logo file
        
    Returns:
        Updated UserProfile instance
        
    Raises:
        ValidationError: If profile doesn't exist
    """
    repo = UserProfileRepository(session)
    profile = repo.get_first()
    
    if not profile:
        raise ValidationError(
            "No profile found. Please initialize the application first.",
            field="profile",
        )
    
    # Update logo path
    profile.logo_path = logo_path
    session.commit()
    session.refresh(profile)
    
    logger.info(f"Updated logo for profile: {profile.name}")
    
    return profile


def update_profile_name(
    session: Session,
    new_name: str,
) -> UserProfile:
    """
    Update the name for the current user profile.
    
    Args:
        session: Database session
        new_name: New name for the profile
        
    Returns:
        Updated UserProfile instance
        
    Raises:
        ValidationError: If profile doesn't exist or name is invalid
    """
    # Validate name
    if not new_name or not new_name.strip():
        raise ValidationError(
            "User name cannot be empty",
            field="name",
            value=new_name,
        )
    
    repo = UserProfileRepository(session)
    profile = repo.get_first()
    
    if not profile:
        raise ValidationError(
            "No profile found. Please initialize the application first.",
            field="profile",
        )
    
    # Update name
    profile.name = new_name.strip()
    updated = repo.update(profile)
    
    logger.info(f"Updated profile name to: {updated.name}")
    
    return updated