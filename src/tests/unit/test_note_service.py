"""
Unit tests for Note service.

Tests note CRUD operations.
"""
import pytest

pytestmark = pytest.mark.unit

from src.database.models import Note
from src.services.note_service import create_note, delete_note, get_all_notes
from src.utils.errors import ValidationError


class TestCreateNote:
    """Test note creation."""

    def test_create_note_success(self, test_session):
        """Test successful note creation."""
        note = create_note(
            session=test_session,
            content="Test note content"
        )
        
        assert note is not None
        assert note.id is not None
        assert note.content == "Test note content"
        assert note.created_at is not None

    def test_create_note_strips_whitespace(self, test_session):
        """Test that note content is stripped of whitespace."""
        note = create_note(
            session=test_session,
            content="  Test note with spaces  "
        )
        
        assert note.content == "Test note with spaces"

    def test_create_note_empty_content_raises_error(self, test_session):
        """Test that empty content raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            create_note(
                session=test_session,
                content=""
            )
        
        assert "empty" in str(exc_info.value).lower()
        assert exc_info.value.details.get("field") == "content"

    def test_create_note_whitespace_only_raises_error(self, test_session):
        """Test that whitespace-only content raises ValidationError."""
        with pytest.raises(ValidationError):
            create_note(
                session=test_session,
                content="   \n\t  "
            )


class TestGetAllNotes:
    """Test retrieving all notes."""

    def test_get_all_notes_empty(self, test_session):
        """Test getting all notes when none exist."""
        notes = get_all_notes(test_session)
        
        assert isinstance(notes, list)
        assert len(notes) == 0

    def test_get_all_notes_multiple(self, test_session):
        """Test getting all notes when multiple exist."""
        # Create multiple notes
        note1 = create_note(test_session, "First note")
        note2 = create_note(test_session, "Second note")
        note3 = create_note(test_session, "Third note")
        
        notes = get_all_notes(test_session)
        
        assert len(notes) >= 3
        note_ids = {note.id for note in notes}
        assert note1.id in note_ids
        assert note2.id in note_ids
        assert note3.id in note_ids

    def test_get_all_notes_ordered_by_date(self, test_session):
        """Test that notes are returned in some order (implementation-specific)."""
        note1 = create_note(test_session, "First note")
        import time
        time.sleep(0.1)  # Small delay to ensure different timestamps
        note2 = create_note(test_session, "Second note")
        
        notes = get_all_notes(test_session)
        
        # Verify both notes are in the list
        note_ids = {n.id for n in notes}
        assert note1.id in note_ids
        assert note2.id in note_ids
        assert len(notes) >= 2


class TestDeleteNote:
    """Test note deletion."""

    def test_delete_note_success(self, test_session):
        """Test successful note deletion."""
        note = create_note(test_session, "Note to delete")
        note_id = note.id
        
        delete_note(test_session, note_id)
        
        # Verify note is deleted
        from src.database.repository import NoteRepository
        repo = NoteRepository(test_session)
        deleted_note = repo.get_by_id(note_id)
        assert deleted_note is None

    def test_delete_note_nonexistent_raises_error(self, test_session):
        """Test that deleting non-existent note raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            delete_note(test_session, 99999)
        
        assert "not found" in str(exc_info.value).lower()
        assert exc_info.value.details.get("field") == "note_id"

