"""
Unit tests for Profile service.

Following TDD: Tests written first (RED phase).
"""
import pytest

from src.database.models import UserProfile
from src.services.profile_service import (
    create_user_profile,
    get_current_profile,
    is_app_initialized,
)
from src.utils.errors import ValidationError


class TestCreateUserProfile:
    """Test user profile creation."""

    def test_create_profile_success(self, test_session):
        """Test successful profile creation."""
        profile = create_user_profile(
            session=test_session,
            name="Test User"
        )

        assert profile is not None
        assert profile.name == "Test User"
        assert profile.id is not None
        assert profile.created_at is not None

    def test_create_profile_empty_name_raises_error(self, test_session):
        """Test that empty name raises validation error."""
        with pytest.raises(ValidationError):
            create_user_profile(
                session=test_session,
                name=""
            )

    def test_create_profile_whitespace_name_raises_error(self, test_session):
        """Test that whitespace-only name raises validation error."""
        with pytest.raises(ValidationError):
            create_user_profile(
                session=test_session,
                name="   "
            )


class TestIsAppInitialized:
    """Test app initialization check."""

    def test_not_initialized_when_no_profile(self, test_session):
        """Test that app is not initialized when no profile exists."""
        assert is_app_initialized(test_session) is False

    def test_initialized_when_profile_exists(self, test_session):
        """Test that app is initialized when profile exists."""
        create_user_profile(
            session=test_session,
            name="Test User"
        )

        assert is_app_initialized(test_session) is True


class TestGetCurrentProfile:
    """Test getting current profile."""

    def test_get_profile_when_exists(self, test_session):
        """Test getting profile when it exists."""
        created = create_user_profile(
            session=test_session,
            name="Test User"
        )

        profile = get_current_profile(test_session)

        assert profile is not None
        assert profile.id == created.id
        assert profile.name == "Test User"

    def test_get_profile_when_not_exists(self, test_session):
        """Test getting profile when none exists."""
        profile = get_current_profile(test_session)

        assert profile is None

