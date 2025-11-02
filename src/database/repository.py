"""
Repository pattern implementation for CSV Wrangler.

Provides data access abstraction layer following Repository pattern.
"""
from typing import Any, Optional

from sqlalchemy.orm import Session

from src.database.models import (
    DataAnalysis,
    DatasetConfig,
    KnowledgeTable,
    Note,
    UploadLog,
    UserProfile,
)
from src.utils.errors import DatabaseError, ValidationError
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class DatasetRepository:
    """Repository for DatasetConfig operations."""

    def __init__(self, session: Session):
        """Initialize repository with database session."""
        self.session = session

    def create(self, dataset: DatasetConfig) -> DatasetConfig:
        """
        Create a new dataset configuration.
        
        Args:
            dataset: DatasetConfig instance to create
            
        Returns:
            Created DatasetConfig instance
            
        Raises:
            DatabaseError: If creation fails
        """
        try:
            self.session.add(dataset)
            self.session.commit()
            self.session.refresh(dataset)
            logger.debug(f"Created dataset: {dataset.name}")
            return dataset
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to create dataset: {e}", exc_info=True)
            raise DatabaseError(f"Failed to create dataset: {e}", operation="create_dataset") from e

    def get_by_id(self, dataset_id: int) -> Optional[DatasetConfig]:
        """
        Get dataset by ID.
        
        Args:
            dataset_id: Dataset ID
            
        Returns:
            DatasetConfig instance or None if not found
        """
        return self.session.get(DatasetConfig, dataset_id)

    def get_by_slot(self, slot_number: int) -> Optional[DatasetConfig]:
        """
        Get dataset by slot number.
        
        Args:
            slot_number: Slot number (1-5)
            
        Returns:
            DatasetConfig instance or None if not found
        """
        return (
            self.session.query(DatasetConfig)
            .filter_by(slot_number=slot_number)
            .first()
        )

    def get_by_name(self, name: str) -> Optional[DatasetConfig]:
        """
        Get dataset by name.
        
        Args:
            name: Dataset name
            
        Returns:
            DatasetConfig instance or None if not found
        """
        return (
            self.session.query(DatasetConfig)
            .filter_by(name=name)
            .first()
        )

    def get_all(self) -> list[DatasetConfig]:
        """
        Get all datasets.
        
        Returns:
            List of all DatasetConfig instances
        """
        return self.session.query(DatasetConfig).all()

    def update(self, dataset: DatasetConfig) -> DatasetConfig:
        """
        Update dataset configuration.
        
        Args:
            dataset: DatasetConfig instance to update
            
        Returns:
            Updated DatasetConfig instance
            
        Raises:
            DatabaseError: If update fails
        """
        try:
            self.session.commit()
            self.session.refresh(dataset)
            logger.debug(f"Updated dataset: {dataset.name}")
            return dataset
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to update dataset: {e}", exc_info=True)
            raise DatabaseError(f"Failed to update dataset: {e}", operation="update_dataset") from e

    def delete(self, dataset_id: int) -> None:
        """
        Delete dataset by ID.
        
        Args:
            dataset_id: Dataset ID
            
        Raises:
            ValidationError: If dataset not found
            DatabaseError: If deletion fails
        """
        dataset = self.get_by_id(dataset_id)
        if not dataset:
            raise ValidationError(f"Dataset with ID {dataset_id} not found")

        try:
            self.session.delete(dataset)
            self.session.commit()
            logger.debug(f"Deleted dataset: {dataset_id}")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to delete dataset: {e}", exc_info=True)
            raise DatabaseError(f"Failed to delete dataset: {e}", operation="delete_dataset") from e


class UploadLogRepository:
    """Repository for UploadLog operations."""

    def __init__(self, session: Session):
        """Initialize repository with database session."""
        self.session = session

    def create(self, upload_log: UploadLog) -> UploadLog:
        """
        Create a new upload log entry.
        
        Args:
            upload_log: UploadLog instance to create
            
        Returns:
            Created UploadLog instance
            
        Raises:
            DatabaseError: If creation fails
        """
        try:
            self.session.add(upload_log)
            self.session.commit()
            self.session.refresh(upload_log)
            logger.debug(f"Created upload log: {upload_log.filename}")
            return upload_log
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to create upload log: {e}", exc_info=True)
            raise DatabaseError(f"Failed to create upload log: {e}", operation="create_upload_log") from e

    def get_by_id(self, upload_log_id: int) -> Optional[UploadLog]:
        """
        Get upload log by ID.
        
        Args:
            upload_log_id: Upload log ID
            
        Returns:
            UploadLog instance or None if not found
        """
        return self.session.get(UploadLog, upload_log_id)

    def get_by_dataset_id(self, dataset_id: int) -> list[UploadLog]:
        """
        Get all upload logs for a dataset.
        
        Args:
            dataset_id: Dataset ID
            
        Returns:
            List of UploadLog instances
        """
        return (
            self.session.query(UploadLog)
            .filter_by(dataset_id=dataset_id)
            .order_by(UploadLog.upload_date.desc())
            .all()
        )

    def check_duplicate_filename(
        self, dataset_id: int, filename: str
    ) -> Optional[UploadLog]:
        """
        Check if filename exists for dataset.
        
        Args:
            dataset_id: Dataset ID
            filename: Filename to check
            
        Returns:
            UploadLog instance if found, None otherwise
        """
        return (
            self.session.query(UploadLog)
            .filter_by(dataset_id=dataset_id, filename=filename)
            .first()
        )


class UserProfileRepository:
    """Repository for UserProfile operations."""

    def __init__(self, session: Session):
        """Initialize repository with database session."""
        self.session = session

    def create(self, profile: UserProfile) -> UserProfile:
        """
        Create a new user profile.
        
        Args:
            profile: UserProfile instance to create
            
        Returns:
            Created UserProfile instance
            
        Raises:
            DatabaseError: If creation fails
        """
        try:
            self.session.add(profile)
            self.session.commit()
            self.session.refresh(profile)
            logger.debug(f"Created user profile: {profile.name}")
            return profile
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to create user profile: {e}", exc_info=True)
            raise DatabaseError(
                f"Failed to create user profile: {e}", operation="create_profile"
            ) from e

    def get_by_id(self, profile_id: int) -> Optional[UserProfile]:
        """
        Get user profile by ID.
        
        Args:
            profile_id: Profile ID
            
        Returns:
            UserProfile instance or None if not found
        """
        return self.session.get(UserProfile, profile_id)

    def get_first(self) -> Optional[UserProfile]:
        """
        Get the first (and typically only) user profile.
        
        Returns:
            UserProfile instance or None if not found
        """
        return self.session.query(UserProfile).first()

    def update(self, profile: UserProfile) -> UserProfile:
        """
        Update user profile.
        
        Args:
            profile: UserProfile instance to update
            
        Returns:
            Updated UserProfile instance
            
        Raises:
            DatabaseError: If update fails
        """
        try:
            self.session.commit()
            self.session.refresh(profile)
            logger.debug(f"Updated user profile: {profile.name}")
            return profile
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to update user profile: {e}", exc_info=True)
            raise DatabaseError(
                f"Failed to update user profile: {e}", operation="update_profile"
            ) from e

    def exists(self) -> bool:
        """
        Check if any user profile exists.
        
        Returns:
            True if profile exists, False otherwise
        """
        return self.session.query(UserProfile).first() is not None


class NoteRepository:
    """Repository for Note operations."""

    def __init__(self, session: Session):
        """Initialize repository with database session."""
        self.session = session

    def create(self, note: Note) -> Note:
        """
        Create a new note.
        
        Args:
            note: Note instance to create
            
        Returns:
            Created Note instance
            
        Raises:
            DatabaseError: If creation fails
        """
        try:
            self.session.add(note)
            self.session.commit()
            self.session.refresh(note)
            logger.debug(f"Created note: ID {note.id}")
            return note
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to create note: {e}", exc_info=True)
            raise DatabaseError(
                f"Failed to create note: {e}", operation="create_note"
            ) from e

    def get_by_id(self, note_id: int) -> Optional[Note]:
        """
        Get note by ID.
        
        Args:
            note_id: Note ID
            
        Returns:
            Note instance or None if not found
        """
        return self.session.get(Note, note_id)

    def get_all(self) -> list[Note]:
        """
        Get all notes, ordered by creation date (newest first).
        
        Returns:
            List of Note instances
        """
        return (
            self.session.query(Note)
            .order_by(Note.created_at.desc())
            .all()
        )

    def delete(self, note_id: int) -> None:
        """
        Delete a note by ID.
        
        Args:
            note_id: Note ID to delete
            
        Raises:
            DatabaseError: If deletion fails
        """
        try:
            note = self.session.get(Note, note_id)
            if note:
                self.session.delete(note)
                self.session.commit()
                logger.debug(f"Deleted note: ID {note_id}")
            else:
                raise ValidationError(
                    f"Note with ID {note_id} not found",
                    field="note_id",
                    value=note_id,
                )
        except ValidationError:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to delete note: {e}", exc_info=True)
            raise DatabaseError(
                f"Failed to delete note: {e}", operation="delete_note"
            ) from e


class DataAnalysisRepository:
    """Repository for DataAnalysis operations."""

    def __init__(self, session: Session):
        """Initialize repository with database session."""
        self.session = session

    def create(self, analysis: DataAnalysis) -> DataAnalysis:
        """
        Create a new data analysis.
        
        Args:
            analysis: DataAnalysis instance to create
            
        Returns:
            Created DataAnalysis instance
            
        Raises:
            DatabaseError: If creation fails
        """
        try:
            self.session.add(analysis)
            self.session.commit()
            self.session.refresh(analysis)
            logger.debug(f"Created data analysis: ID {analysis.id}")
            return analysis
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to create data analysis: {e}", exc_info=True)
            raise DatabaseError(
                f"Failed to create data analysis: {e}", operation="create_analysis"
            ) from e

    def get_by_id(self, analysis_id: int) -> Optional[DataAnalysis]:
        """
        Get data analysis by ID.
        
        Args:
            analysis_id: Analysis ID
            
        Returns:
            DataAnalysis instance or None if not found
        """
        return self.session.get(DataAnalysis, analysis_id)

    def get_all(self) -> list[DataAnalysis]:
        """
        Get all data analyses, ordered by creation date (newest first).
        
        Returns:
            List of DataAnalysis instances
        """
        return (
            self.session.query(DataAnalysis)
            .order_by(DataAnalysis.created_at.desc())
            .all()
        )

    def get_by_source_dataset(self, dataset_id: int) -> list[DataAnalysis]:
        """
        Get all analyses for a source dataset.
        
        Args:
            dataset_id: Source dataset ID
            
        Returns:
            List of DataAnalysis instances
        """
        return (
            self.session.query(DataAnalysis)
            .filter(DataAnalysis.source_dataset_id == dataset_id)
            .order_by(DataAnalysis.created_at.desc())
            .all()
        )

    def update(self, analysis: DataAnalysis) -> DataAnalysis:
        """
        Update data analysis.
        
        Args:
            analysis: DataAnalysis instance to update
            
        Returns:
            Updated DataAnalysis instance
            
        Raises:
            DatabaseError: If update fails
        """
        try:
            self.session.commit()
            self.session.refresh(analysis)
            logger.debug(f"Updated data analysis: ID {analysis.id}")
            return analysis
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to update data analysis: {e}", exc_info=True)
            raise DatabaseError(
                f"Failed to update data analysis: {e}", operation="update_analysis"
            ) from e

    def delete(self, analysis_id: int) -> None:
        """
        Delete a data analysis by ID.
        
        Args:
            analysis_id: Analysis ID to delete
            
        Raises:
            DatabaseError: If deletion fails
        """
        try:
            analysis = self.session.get(DataAnalysis, analysis_id)
            if analysis:
                self.session.delete(analysis)
                self.session.commit()
                logger.debug(f"Deleted data analysis: ID {analysis_id}")
            else:
                raise ValidationError(
                    f"Data analysis with ID {analysis_id} not found",
                    field="analysis_id",
                    value=analysis_id,
                )
        except ValidationError:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to delete data analysis: {e}", exc_info=True)
            raise DatabaseError(
                f"Failed to delete data analysis: {e}", operation="delete_analysis"
            ) from e


class KnowledgeTableRepository:
    """Repository for KnowledgeTable operations."""

    def __init__(self, session: Session):
        """Initialize repository with database session."""
        self.session = session

    def create(self, knowledge_table: KnowledgeTable) -> KnowledgeTable:
        """
        Create a new Knowledge Table.
        
        Args:
            knowledge_table: KnowledgeTable instance to create
            
        Returns:
            Created KnowledgeTable instance
            
        Raises:
            DatabaseError: If creation fails
        """
        try:
            self.session.add(knowledge_table)
            self.session.commit()
            self.session.refresh(knowledge_table)
            logger.debug(f"Created Knowledge Table: {knowledge_table.name}")
            return knowledge_table
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to create Knowledge Table: {e}", exc_info=True)
            raise DatabaseError(
                f"Failed to create Knowledge Table: {e}", operation="create_knowledge_table"
            ) from e

    def get_by_id(self, knowledge_table_id: int) -> Optional[KnowledgeTable]:
        """
        Get Knowledge Table by ID.
        
        Args:
            knowledge_table_id: Knowledge Table ID
            
        Returns:
            KnowledgeTable instance or None if not found
        """
        return self.session.get(KnowledgeTable, knowledge_table_id)

    def get_by_name(self, name: str) -> Optional[KnowledgeTable]:
        """
        Get Knowledge Table by name.
        
        Args:
            name: Knowledge Table name
            
        Returns:
            KnowledgeTable instance or None if not found
        """
        return (
            self.session.query(KnowledgeTable)
            .filter_by(name=name)
            .first()
        )

    def get_by_table_name(self, table_name: str) -> Optional[KnowledgeTable]:
        """
        Get Knowledge Table by database table name.
        
        Args:
            table_name: Database table name
            
        Returns:
            KnowledgeTable instance or None if not found
        """
        return (
            self.session.query(KnowledgeTable)
            .filter_by(table_name=table_name)
            .first()
        )

    def get_by_data_type(self, data_type: str) -> list[KnowledgeTable]:
        """
        Get all Knowledge Tables of matching data_type.
        
        Args:
            data_type: Data type (phone_numbers, emails, web_domains)
            
        Returns:
            List of KnowledgeTable instances
        """
        return (
            self.session.query(KnowledgeTable)
            .filter_by(data_type=data_type)
            .order_by(KnowledgeTable.created_at.desc())
            .all()
        )

    def get_all(self) -> list[KnowledgeTable]:
        """
        Get all Knowledge Tables.
        
        Returns:
            List of all KnowledgeTable instances
        """
        return (
            self.session.query(KnowledgeTable)
            .order_by(KnowledgeTable.data_type, KnowledgeTable.created_at.desc())
            .all()
        )

    def update(self, knowledge_table: KnowledgeTable) -> KnowledgeTable:
        """
        Update Knowledge Table configuration.
        
        Args:
            knowledge_table: KnowledgeTable instance to update
            
        Returns:
            Updated KnowledgeTable instance
            
        Raises:
            DatabaseError: If update fails
        """
        try:
            self.session.commit()
            self.session.refresh(knowledge_table)
            logger.debug(f"Updated Knowledge Table: {knowledge_table.name}")
            return knowledge_table
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to update Knowledge Table: {e}", exc_info=True)
            raise DatabaseError(
                f"Failed to update Knowledge Table: {e}", operation="update_knowledge_table"
            ) from e

    def delete(self, knowledge_table_id: int) -> None:
        """
        Delete Knowledge Table by ID.
        
        Args:
            knowledge_table_id: Knowledge Table ID
            
        Raises:
            ValidationError: If Knowledge Table not found
            DatabaseError: If deletion fails
        """
        knowledge_table = self.get_by_id(knowledge_table_id)
        if not knowledge_table:
            raise ValidationError(
                f"Knowledge Table with ID {knowledge_table_id} not found",
                field="knowledge_table_id",
                value=knowledge_table_id,
            )

        try:
            self.session.delete(knowledge_table)
            self.session.commit()
            logger.debug(f"Deleted Knowledge Table: {knowledge_table_id}")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to delete Knowledge Table: {e}", exc_info=True)
            raise DatabaseError(
                f"Failed to delete Knowledge Table: {e}", operation="delete_knowledge_table"
            ) from e

