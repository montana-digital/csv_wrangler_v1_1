"""
Unit tests for Dataset service.

Following TDD: Tests written first (RED phase).
"""
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from src.database.models import DatasetConfig, UploadLog
from src.services.dataset_service import (
    check_duplicate_filename,
    delete_dataset,
    get_dataset_statistics,
    initialize_dataset,
    upload_csv_to_dataset,
)
from src.utils.errors import DuplicateFileError, SchemaMismatchError, ValidationError


class TestInitializeDataset:
    """Test dataset initialization."""

    def test_initialize_dataset_creates_table(self, test_session):
        """Test that initializing a dataset creates the correct table schema."""
        # Create sample CSV structure
        df = pd.DataFrame({
            "name": ["John", "Jane"],
            "age": ["30", "25"],
            "email": ["john@test.com", "jane@test.com"]
        })

        columns_config = {
            "name": {"type": "TEXT", "is_image": False},
            "age": {"type": "INTEGER", "is_image": False},
            "email": {"type": "TEXT", "is_image": False}
        }

        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            duplicate_filter_column="email",
            image_columns=[]
        )

        assert dataset is not None
        assert dataset.name == "Test Dataset"
        assert dataset.slot_number == 1
        assert dataset.table_name is not None
        assert dataset.duplicate_filter_column == "email"
        assert len(dataset.columns_config) == 3

    def test_initialize_dataset_with_image_columns(self, test_session):
        """Test initializing dataset with image columns."""
        columns_config = {
            "name": {"type": "TEXT", "is_image": False},
            "image": {"type": "TEXT", "is_image": True}
        }

        dataset = initialize_dataset(
            session=test_session,
            name="Image Dataset",
            slot_number=2,
            columns_config=columns_config,
            duplicate_filter_column="name",
            image_columns=["image"]
        )

        assert dataset.image_columns == ["image"]
        assert dataset.columns_config["image"]["is_image"] is True

    def test_initialize_dataset_validates_slot_number(self, test_session):
        """Test that slot number validation works."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}

        # Valid slot numbers (1-5)
        for slot in range(1, 6):
            dataset = initialize_dataset(
                session=test_session,
                name=f"Dataset {slot}",
                slot_number=slot,
                columns_config=columns_config,
                duplicate_filter_column="name",
                image_columns=[]
            )
            assert dataset.slot_number == slot

        # Invalid slot number should raise error
        with pytest.raises(ValidationError):
            initialize_dataset(
                session=test_session,
                name="Invalid",
                slot_number=6,  # Invalid
                columns_config=columns_config,
                duplicate_filter_column="name",
                image_columns=[]
            )


class TestUploadCSVToDataset:
    """Test uploading CSV to existing dataset."""

    def test_upload_csv_validates_column_matching(self, test_session, tmp_path):
        """Test that upload validates column matching."""
        # Initialize dataset
        columns_config = {
            "name": {"type": "TEXT", "is_image": False},
            "age": {"type": "INTEGER", "is_image": False}
        }
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            duplicate_filter_column="name",
            image_columns=[]
        )

        # Create CSV with mismatched columns
        csv_file = tmp_path / "test.csv"
        csv_content = "name,email\nJohn,john@test.com"
        csv_file.write_text(csv_content, encoding="utf-8")

        with pytest.raises(SchemaMismatchError):
            upload_csv_to_dataset(
                session=test_session,
                dataset_id=dataset.id,
                csv_file=csv_file,
                filename="test.csv"
            )

    def test_upload_csv_success(self, test_session, tmp_path):
        """Test successful CSV upload."""
        # Initialize dataset
        columns_config = {
            "name": {"type": "TEXT", "is_image": False},
            "age": {"type": "INTEGER", "is_image": False}
        }
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            duplicate_filter_column="name",
            image_columns=[]
        )

        # Create valid CSV
        csv_file = tmp_path / "test.csv"
        csv_content = "name,age\nJohn,30\nJane,25"
        csv_file.write_text(csv_content, encoding="utf-8")

        upload_log = upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="test.csv"
        )

        assert upload_log is not None
        assert upload_log.filename == "test.csv"
        assert upload_log.row_count == 2
        assert upload_log.file_type == "CSV"

    def test_upload_csv_adds_unique_ids(self, test_session, tmp_path):
        """Test that uploaded CSV gets unique IDs."""
        columns_config = {
            "name": {"type": "TEXT", "is_image": False}
        }
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            duplicate_filter_column="name",
            image_columns=[]
        )

        csv_file = tmp_path / "test.csv"
        csv_content = "name\nJohn\nJane"
        csv_file.write_text(csv_content, encoding="utf-8")

        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="test.csv"
        )

        # Check that uuid_value column exists in table
        from sqlalchemy import text
        from src.config.settings import UNIQUE_ID_COLUMN_NAME
        result = test_session.execute(
            text(f"SELECT {UNIQUE_ID_COLUMN_NAME} FROM {dataset.table_name}")
        )
        rows = result.fetchall()
        assert len(rows) == 2
        # All IDs should be unique UUIDs (strings)
        assert all(len(str(row[0])) > 30 for row in rows)  # UUIDs are long strings


class TestCheckDuplicateFilename:
    """Test duplicate filename detection."""

    def test_check_duplicate_filename_no_duplicate(self, test_session):
        """Test when no duplicate exists."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            duplicate_filter_column="name",
            image_columns=[]
        )

        # Should not raise
        check_duplicate_filename(
            session=test_session,
            dataset_id=dataset.id,
            filename="new_file.csv"
        )

    def test_check_duplicate_filename_raises_error(self, test_session, tmp_path):
        """Test that duplicate filename raises error."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            duplicate_filter_column="name",
            image_columns=[]
        )

        # Upload first file
        csv_file = tmp_path / "test.csv"
        csv_content = "name\nJohn"
        csv_file.write_text(csv_content, encoding="utf-8")

        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="test.csv"
        )

        # Try to upload same filename again
        with pytest.raises(DuplicateFileError):
            check_duplicate_filename(
                session=test_session,
                dataset_id=dataset.id,
                filename="test.csv"
            )


class TestGetDatasetStatistics:
    """Test getting dataset statistics."""

    def test_get_statistics_empty_dataset(self, test_session):
        """Test statistics for empty dataset."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            duplicate_filter_column="name",
            image_columns=[]
        )

        stats = get_dataset_statistics(
            session=test_session,
            dataset_id=dataset.id
        )

        assert stats["total_rows"] == 0
        assert stats["total_uploads"] == 0
        assert stats["column_names"] == ["name"]
        assert stats["first_upload"] is None
        assert stats["last_upload"] is None

    def test_get_statistics_with_data(self, test_session, tmp_path):
        """Test statistics for dataset with data."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            duplicate_filter_column="name",
            image_columns=[]
        )

        # Upload data
        csv_file = tmp_path / "test.csv"
        csv_content = "name\nJohn\nJane\nBob"
        csv_file.write_text(csv_content, encoding="utf-8")

        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="test.csv"
        )

        stats = get_dataset_statistics(
            session=test_session,
            dataset_id=dataset.id
        )

        assert stats["total_rows"] == 3
        assert stats["total_uploads"] == 1
        assert stats["first_upload"] is not None
        assert stats["last_upload"] is not None


class TestDeleteDataset:
    """Test dataset deletion."""

    def test_delete_dataset_removes_table_and_metadata(self, test_session, tmp_path):
        """Test that deleting dataset removes table and metadata."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            duplicate_filter_column="name",
            image_columns=[]
        )

        # Upload some data
        csv_file = tmp_path / "test.csv"
        csv_content = "name\nJohn"
        csv_file.write_text(csv_content, encoding="utf-8")

        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="test.csv"
        )

        table_name = dataset.table_name

        # Delete dataset
        delete_dataset(
            session=test_session,
            dataset_id=dataset.id
        )

        # Check table is dropped
        from sqlalchemy import inspect
        inspector = inspect(test_session.bind)
        tables = inspector.get_table_names()
        assert table_name not in tables

        # Check metadata is removed
        deleted_dataset = test_session.get(DatasetConfig, dataset.id)
        assert deleted_dataset is None

        # Check upload logs are removed (cascade delete)
        upload_logs = test_session.query(UploadLog).filter_by(dataset_id=dataset.id).all()
        assert len(upload_logs) == 0

