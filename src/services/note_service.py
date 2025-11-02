"""
Note service for CSV Wrangler.

Handles note creation, retrieval, and deletion for the sidebar notepad feature.
"""
from typing import Optional

from sqlalchemy.orm import Session

from src.database.models import Note
from src.database.repository import NoteRepository
from src.utils.errors import DatabaseError, ValidationError
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def create_note(
    session: Session,
    content: str,
) -> Note:
    """
    Create a new note.
    
    Args:
        session: Database session
        content: Note content text
        
    Returns:
        Created Note instance
        
    Raises:
        ValidationError: If content is empty
        DatabaseError: If creation fails
    """
    # Validate content
    if not content or not content.strip():
        raise ValidationError(
            "Note content cannot be empty",
            field="content",
            value=content,
        )
    
    # Create new note
    note = Note(content=content.strip())
    repo = NoteRepository(session)
    created = repo.create(note)
    
    logger.info(f"Created note: ID {created.id}")
    
    return created


def get_all_notes(session: Session) -> list[Note]:
    """
    Get all notes, ordered by creation date (newest first).
    
    Args:
        session: Database session
        
    Returns:
        List of Note instances
    """
    repo = NoteRepository(session)
    return repo.get_all()


def delete_note(
    session: Session,
    note_id: int,
) -> None:
    """
    Delete a note by ID.
    
    Args:
        session: Database session
        note_id: Note ID to delete
        
    Raises:
        ValidationError: If note not found
        DatabaseError: If deletion fails
    """
    repo = NoteRepository(session)
    note = repo.get_by_id(note_id)
    
    if not note:
        raise ValidationError(
            f"Note with ID {note_id} not found",
            field="note_id",
            value=note_id,
        )
    
    repo.delete(note_id)
    logger.info(f"Deleted note: ID {note_id}")

