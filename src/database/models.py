"""
SQLAlchemy models for CSV Wrangler.

Defines database schema for DatasetConfig and UploadLog tables.
"""
from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    create_engine,
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

Base = declarative_base()


class DatasetConfig(Base):
    """
    Dataset configuration and metadata.
    
    Stores information about each dataset slot (1-5) including:
    - Table name and structure
    - Column configurations (data types, image columns)
    - Creation and update timestamps
    """

    __tablename__ = "dataset_config"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, unique=True)
    slot_number = Column(Integer, nullable=False, unique=True)  # 1-5
    table_name = Column(String(255), nullable=False, unique=True)
    columns_config = Column(
        JSON, nullable=False
    )  # {"column_name": {"type": "TEXT", "is_image": False}}
    duplicate_filter_column = Column(String(255), nullable=True)
    image_columns = Column(JSON, nullable=False, default=list)  # List of column names
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now()
    )

    # Relationship to upload logs
    upload_logs = relationship("UploadLog", back_populates="dataset", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<DatasetConfig(id={self.id}, name='{self.name}', slot_number={self.slot_number})>"

    def to_dict(self) -> dict[str, Any]:
        """Convert model to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "slot_number": self.slot_number,
            "table_name": self.table_name,
            "columns_config": self.columns_config,
            "duplicate_filter_column": self.duplicate_filter_column,
            "image_columns": self.image_columns,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class UploadLog(Base):
    """
    Log of CSV file uploads to datasets.
    
    Tracks each file upload including filename, row count, and timestamp.
    """

    __tablename__ = "upload_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    dataset_id = Column(Integer, ForeignKey("dataset_config.id", ondelete="CASCADE"), nullable=False)
    filename = Column(String(255), nullable=False)
    file_type = Column(String(50), nullable=False)  # CSV or PICKLE
    row_count = Column(Integer, nullable=False)
    upload_date = Column(DateTime, nullable=False, server_default=func.now())

    # Relationship to dataset
    dataset = relationship("DatasetConfig", back_populates="upload_logs")

    def __repr__(self) -> str:
        return f"<UploadLog(id={self.id}, dataset_id={self.dataset_id}, filename='{self.filename}')>"

    def to_dict(self) -> dict[str, Any]:
        """Convert model to dictionary."""
        return {
            "id": self.id,
            "dataset_id": self.dataset_id,
            "filename": self.filename,
            "file_type": self.file_type,
            "row_count": self.row_count,
            "upload_date": self.upload_date.isoformat() if self.upload_date else None,
        }


class UserProfile(Base):
    """
    User profile information.
    
    Stores user name and profile creation timestamp.
    """

    __tablename__ = "user_profile"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    logo_path = Column(String(500), nullable=True)  # Path to uploaded logo file
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now()
    )

    def __repr__(self) -> str:
        return f"<UserProfile(id={self.id}, name='{self.name}')>"

    def to_dict(self) -> dict[str, Any]:
        """Convert model to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "logo_path": self.logo_path,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class EnrichedDataset(Base):
    """
    Tracks enriched datasets created from source datasets.
    
    Stores information about enriched tables including:
    - Source dataset reference
    - Enriched table name
    - Enrichment configuration (columns and functions applied)
    - Columns added by enrichment
    - Last sync date with source dataset
    """

    __tablename__ = "enriched_dataset"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)  # User-friendly name
    source_dataset_id = Column(
        Integer, ForeignKey("dataset_config.id", ondelete="CASCADE"), nullable=False
    )
    enriched_table_name = Column(String(255), nullable=False, unique=True)
    source_table_name = Column(String(255), nullable=False)
    enrichment_config = Column(
        JSON, nullable=False
    )  # {"column_name": "enrichment_function_name"}
    columns_added = Column(JSON, nullable=False, default=list)  # List of enriched column names
    last_sync_date = Column(DateTime, nullable=True)  # Last time enriched table was synced
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now()
    )

    # Relationship to source dataset
    source_dataset = relationship("DatasetConfig", foreign_keys=[source_dataset_id])

    def __repr__(self) -> str:
        return (
            f"<EnrichedDataset(id={self.id}, name='{self.name}', "
            f"source_dataset_id={self.source_dataset_id})>"
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert model to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "source_dataset_id": self.source_dataset_id,
            "enriched_table_name": self.enriched_table_name,
            "source_table_name": self.source_table_name,
            "enrichment_config": self.enrichment_config,
            "columns_added": self.columns_added,
            "last_sync_date": self.last_sync_date.isoformat() if self.last_sync_date else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class Note(Base):
    """
    User notes for the notepad feature.
    
    Stores notes that persist across sessions.
    """

    __tablename__ = "note"

    id = Column(Integer, primary_key=True, autoincrement=True)
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now()
    )

    def __repr__(self) -> str:
        return f"<Note(id={self.id}, content_length={len(self.content) if self.content else 0})>"

    def to_dict(self) -> dict[str, Any]:
        """Convert model to dictionary."""
        return {
            "id": self.id,
            "content": self.content,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class KnowledgeTable(Base):
    """
    Knowledge Table configuration and metadata.
    
    Stores information about Knowledge Tables used for linking enriched data.
    Knowledge Tables store standardized Key_ID values that match enriched column values,
    enabling automatic value-based linking across multiple tables per data_type.
    """

    __tablename__ = "knowledge_table"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, unique=True)  # User-friendly name, unique globally
    data_type = Column(String(50), nullable=False)  # phone_numbers, emails, web_domains
    table_name = Column(String(255), nullable=False, unique=True)  # Database table name
    primary_key_column = Column(String(255), nullable=False)  # Source column for Key_ID generation
    columns_config = Column(
        JSON, nullable=False
    )  # {"column_name": {"type": "TEXT", "is_image": False}}
    key_id_column = Column(String(255), nullable=False, default="Key_ID")  # Always "Key_ID"
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now()
    )

    def __repr__(self) -> str:
        return (
            f"<KnowledgeTable(id={self.id}, name='{self.name}', "
            f"data_type='{self.data_type}')>"
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert model to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "data_type": self.data_type,
            "table_name": self.table_name,
            "primary_key_column": self.primary_key_column,
            "columns_config": self.columns_config,
            "key_id_column": self.key_id_column,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class DataAnalysis(Base):
    """
    Tracks data analysis operations performed on datasets.
    
    Stores analysis configurations, results, and visualization settings
    for operations like GroupBy, Pivot, Merge, Join, Concat, Apply, and Map.
    """

    __tablename__ = "data_analysis"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)  # User-friendly name for the analysis
    operation_type = Column(
        String(50), nullable=False
    )  # groupby, pivot, merge, join, concat, apply, map
    source_dataset_id = Column(
        Integer, ForeignKey("dataset_config.id", ondelete="CASCADE"), nullable=False
    )
    secondary_dataset_id = Column(
        Integer, ForeignKey("dataset_config.id", ondelete="CASCADE"), nullable=True
    )  # For merge/join/concat operations
    operation_config = Column(
        JSON, nullable=False
    )  # {"columns": [...], "aggregations": {...}, "join_keys": [...], etc.}
    result_file_path = Column(String(500), nullable=False)  # Path to parquet file
    visualization_config = Column(
        JSON, nullable=True
    )  # {"chart_type": "bar", "x_column": "...", "y_column": "...", etc.}
    date_range_start = Column(DateTime, nullable=True)  # Start date for filtering
    date_range_end = Column(DateTime, nullable=True)  # End date for filtering
    date_column = Column(String(255), nullable=True)  # Column used for date filtering
    last_refreshed_at = Column(DateTime, nullable=False, server_default=func.now())
    source_updated_at = Column(
        DateTime, nullable=False, server_default=func.now()
    )  # Timestamp of source dataset when analysis was created/refreshed
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    source_dataset = relationship("DatasetConfig", foreign_keys=[source_dataset_id])
    secondary_dataset = relationship("DatasetConfig", foreign_keys=[secondary_dataset_id])

    def __repr__(self) -> str:
        return (
            f"<DataAnalysis(id={self.id}, name='{self.name}', "
            f"operation_type='{self.operation_type}', source_dataset_id={self.source_dataset_id})>"
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert model to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "operation_type": self.operation_type,
            "source_dataset_id": self.source_dataset_id,
            "secondary_dataset_id": self.secondary_dataset_id,
            "operation_config": self.operation_config,
            "result_file_path": self.result_file_path,
            "visualization_config": self.visualization_config,
            "date_range_start": self.date_range_start.isoformat()
            if self.date_range_start
            else None,
            "date_range_end": self.date_range_end.isoformat()
            if self.date_range_end
            else None,
            "date_column": self.date_column,
            "last_refreshed_at": self.last_refreshed_at.isoformat()
            if self.last_refreshed_at
            else None,
            "source_updated_at": self.source_updated_at.isoformat()
            if self.source_updated_at
            else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
