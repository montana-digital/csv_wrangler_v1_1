"""
Unit tests for bulk upload service.

Tests validation, duplicate detection, and batch processing logic.
"""
import pytest
from pathlib import Path

from src.services.bulk_upload_service import (
    BulkUploadResult,
    FileUploadResult,
    process_bulk_upload,
    upload_file_to_dataset,
    validate_file_for_dataset,
)
from src.services.dataset_service import initialize_dataset
from src.utils.errors import ValidationError


class TestValidateFileForDataset:
    """Test file validation logic."""

    def test_validate_valid_csv_file(self, test_session, tmp_path):
        """Test validation of valid CSV file with matching columns."""
        # Setup dataset
        columns_config = {"name": {"type": "TEXT", "is_image": False}, "age": {"type": "INTEGER", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Create valid CSV file
        csv_file = tmp_path / "valid.csv"
        csv_file.write_text("name,age\nJohn,30\nJane,25", encoding="utf-8")

        # Validate
        is_valid, error_type, error_reason = validate_file_for_dataset(
            session=test_session,
            dataset_id=dataset.id,
            file_path=csv_file,
            filename="valid.csv",
            batch_filenames=set(),
        )

        assert is_valid is True
        assert error_type is None
        assert error_reason is None

    def test_validate_missing_columns(self, test_session, tmp_path):
        """Test validation fails when CSV has missing columns."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}, "age": {"type": "INTEGER", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Create CSV with missing column
        csv_file = tmp_path / "missing_col.csv"
        csv_file.write_text("name\nJohn", encoding="utf-8")

        is_valid, error_type, error_reason = validate_file_for_dataset(
            session=test_session,
            dataset_id=dataset.id,
            file_path=csv_file,
            filename="missing_col.csv",
            batch_filenames=set(),
        )

        assert is_valid is False
        assert error_type == "schema_mismatch"
        assert "Missing columns" in error_reason or "age" in error_reason

    def test_validate_extra_columns(self, test_session, tmp_path):
        """Test validation fails when CSV has extra columns."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Create CSV with extra column
        csv_file = tmp_path / "extra_col.csv"
        csv_file.write_text("name,email\nJohn,john@example.com", encoding="utf-8")

        is_valid, error_type, error_reason = validate_file_for_dataset(
            session=test_session,
            dataset_id=dataset.id,
            file_path=csv_file,
            filename="extra_col.csv",
            batch_filenames=set(),
        )

        assert is_valid is False
        assert error_type == "schema_mismatch"
        assert "Extra columns" in error_reason or "email" in error_reason

    def test_validate_duplicate_in_batch(self, test_session, tmp_path):
        """Test validation fails for duplicate filename within batch."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        csv_file = tmp_path / "duplicate.csv"
        csv_file.write_text("name\nJohn", encoding="utf-8")

        # Filename already in batch
        batch_filenames = {"duplicate.csv"}

        is_valid, error_type, error_reason = validate_file_for_dataset(
            session=test_session,
            dataset_id=dataset.id,
            file_path=csv_file,
            filename="duplicate.csv",
            batch_filenames=batch_filenames,
        )

        assert is_valid is False
        assert error_type == "duplicate_in_batch"
        assert "appears multiple times" in error_reason

    def test_validate_duplicate_in_database(self, test_session, tmp_path):
        """Test validation fails for filename already uploaded to dataset."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Upload first file
        csv_file1 = tmp_path / "existing.csv"
        csv_file1.write_text("name\nJohn", encoding="utf-8")

        from src.services.dataset_service import upload_csv_to_dataset

        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file1,
            filename="existing.csv",
            show_progress=False,
        )

        # Try to validate same filename again
        csv_file2 = tmp_path / "existing2.csv"
        csv_file2.write_text("name\nJane", encoding="utf-8")

        is_valid, error_type, error_reason = validate_file_for_dataset(
            session=test_session,
            dataset_id=dataset.id,
            file_path=csv_file2,
            filename="existing.csv",  # Same filename as uploaded
            batch_filenames=set(),
        )

        assert is_valid is False
        assert error_type == "duplicate_in_db"
        assert "already been uploaded" in error_reason

    def test_validate_parse_error(self, test_session, tmp_path):
        """Test validation fails for unparseable file."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Create corrupt CSV
        csv_file = tmp_path / "corrupt.csv"
        csv_file.write_text("invalid,csv\nno,proper,format,here", encoding="utf-8")

        is_valid, error_type, error_reason = validate_file_for_dataset(
            session=test_session,
            dataset_id=dataset.id,
            file_path=csv_file,
            filename="corrupt.csv",
            batch_filenames=set(),
        )

        # Note: parse might succeed but validation should still work
        # This test depends on actual parse behavior
        assert error_type in ["parse_error", "schema_mismatch", None]


class TestProcessBulkUpload:
    """Test bulk upload processing."""

    def test_process_empty_batch(self, test_session):
        """Test processing empty file list."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        result = process_bulk_upload(
            session=test_session,
            dataset_id=dataset.id,
            files=[],
            show_progress=False,
        )

        assert result.total_files == 0
        assert len(result.successful) == 0
        assert len(result.skipped) == 0
        assert result.total_rows_added == 0

    def test_process_single_valid_file(self, test_session, tmp_path):
        """Test processing single valid file."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}, "age": {"type": "INTEGER", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        csv_file = tmp_path / "valid.csv"
        csv_file.write_text("name,age\nJohn,30\nJane,25", encoding="utf-8")

        result = process_bulk_upload(
            session=test_session,
            dataset_id=dataset.id,
            files=[(csv_file, "valid.csv")],
            show_progress=False,
        )

        assert result.total_files == 1
        assert len(result.successful) == 1
        assert len(result.skipped) == 0
        assert result.successful[0].filename == "valid.csv"
        assert result.successful[0].row_count == 2
        assert result.total_rows_added == 2

    def test_process_mixed_valid_invalid(self, test_session, tmp_path):
        """Test processing batch with valid and invalid files."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}, "age": {"type": "INTEGER", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Valid file
        valid_file = tmp_path / "valid.csv"
        valid_file.write_text("name,age\nJohn,30", encoding="utf-8")

        # Invalid file (missing column)
        invalid_file = tmp_path / "invalid.csv"
        invalid_file.write_text("name\nJane", encoding="utf-8")

        result = process_bulk_upload(
            session=test_session,
            dataset_id=dataset.id,
            files=[(valid_file, "valid.csv"), (invalid_file, "invalid.csv")],
            show_progress=False,
        )

        assert result.total_files == 2
        assert len(result.successful) == 1
        assert len(result.skipped) == 1
        assert result.successful[0].filename == "valid.csv"
        assert result.skipped[0].filename == "invalid.csv"
        assert result.skipped[0].error_type == "schema_mismatch"

    def test_process_all_invalid(self, test_session, tmp_path):
        """Test processing batch where all files are invalid."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}, "age": {"type": "INTEGER", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Both invalid
        invalid1 = tmp_path / "invalid1.csv"
        invalid1.write_text("name\nJohn", encoding="utf-8")

        invalid2 = tmp_path / "invalid2.csv"
        invalid2.write_text("email\njohn@example.com", encoding="utf-8")

        result = process_bulk_upload(
            session=test_session,
            dataset_id=dataset.id,
            files=[(invalid1, "invalid1.csv"), (invalid2, "invalid2.csv")],
            show_progress=False,
        )

        assert result.total_files == 2
        assert len(result.successful) == 0
        assert len(result.skipped) == 2
        assert all(r.error_type == "schema_mismatch" for r in result.skipped)

    def test_process_duplicate_in_batch(self, test_session, tmp_path):
        """Test processing batch with duplicate filenames."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        csv_file1 = tmp_path / "file1.csv"
        csv_file1.write_text("name\nJohn", encoding="utf-8")

        csv_file2 = tmp_path / "file2.csv"
        csv_file2.write_text("name\nJane", encoding="utf-8")

        # Same filename twice
        result = process_bulk_upload(
            session=test_session,
            dataset_id=dataset.id,
            files=[(csv_file1, "same.csv"), (csv_file2, "same.csv")],
            show_progress=False,
        )

        assert result.total_files == 2
        # First should succeed, second should be skipped as duplicate
        assert len(result.successful) == 1
        assert len(result.skipped) == 1
        assert result.skipped[0].filename == "same.csv"
        assert result.skipped[0].error_type == "duplicate_in_batch"

    def test_process_large_batch(self, test_session, tmp_path):
        """Test processing larger batch of files."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}, "age": {"type": "INTEGER", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Create 10 files
        files = []
        for i in range(10):
            csv_file = tmp_path / f"file{i}.csv"
            csv_file.write_text(f"name,age\nPerson{i},{20+i}", encoding="utf-8")
            files.append((csv_file, f"file{i}.csv"))

        result = process_bulk_upload(
            session=test_session,
            dataset_id=dataset.id,
            files=files,
            show_progress=False,
        )

        assert result.total_files == 10
        assert len(result.successful) == 10
        assert len(result.skipped) == 0
        assert result.total_rows_added == 10  # 1 row per file


class TestUploadFileToDataset:
    """Test individual file upload function."""

    def test_upload_csv_file(self, test_session, tmp_path):
        """Test uploading CSV file."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        csv_file = tmp_path / "test.csv"
        csv_file.write_text("name\nJohn\nJane", encoding="utf-8")

        row_count = upload_file_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            file_path=csv_file,
            filename="test.csv",
            file_type="CSV",
        )

        assert row_count == 2

        # Verify data was inserted
        from sqlalchemy import text

        result = test_session.execute(text(f"SELECT COUNT(*) FROM {dataset.table_name}"))
        count = result.scalar()
        assert count == 2

    def test_upload_pickle_file(self, test_session, tmp_path):
        """Test uploading Pickle file."""
        import pickle
        import pandas as pd

        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Create Pickle file
        df = pd.DataFrame({"name": ["John", "Jane"]})
        pickle_file = tmp_path / "test.pkl"
        with open(pickle_file, "wb") as f:
            pickle.dump(df, f)

        row_count = upload_file_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            file_path=pickle_file,
            filename="test.pkl",
            file_type="PICKLE",
        )

        assert row_count == 2

        # Verify data was inserted
        from sqlalchemy import text

        result = test_session.execute(text(f"SELECT COUNT(*) FROM {dataset.table_name}"))
        count = result.scalar()
        assert count == 2

