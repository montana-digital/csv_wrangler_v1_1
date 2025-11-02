"""
Integration tests for bulk upload service.

Tests database interactions and end-to-end upload flows.
"""
import pytest
from pathlib import Path

from src.services.bulk_upload_service import process_bulk_upload
from src.services.dataset_service import initialize_dataset
from src.database.models import UploadLog


class TestBulkUploadIntegration:
    """Integration tests for bulk upload with database."""

    def test_bulk_upload_creates_upload_logs(self, test_session, tmp_path):
        """Test that bulk upload creates UploadLog entries."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}, "age": {"type": "INTEGER", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Create multiple files
        files = []
        for i in range(3):
            csv_file = tmp_path / f"file{i}.csv"
            csv_file.write_text(f"name,age\nPerson{i},{20+i}", encoding="utf-8")
            files.append((csv_file, f"file{i}.csv"))

        result = process_bulk_upload(
            session=test_session,
            dataset_id=dataset.id,
            files=files,
            show_progress=False,
        )

        assert len(result.successful) == 3

        # Verify UploadLog entries
        upload_logs = test_session.query(UploadLog).filter_by(dataset_id=dataset.id).all()
        assert len(upload_logs) == 3

        # Verify filenames match
        uploaded_filenames = {log.filename for log in upload_logs}
        assert uploaded_filenames == {"file0.csv", "file1.csv", "file2.csv"}

    def test_bulk_upload_verifies_row_counts(self, test_session, tmp_path):
        """Test that bulk upload correctly counts rows."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Files with different row counts
        file1 = tmp_path / "file1.csv"
        file1.write_text("name\nA\nB\nC", encoding="utf-8")  # 3 rows

        file2 = tmp_path / "file2.csv"
        file2.write_text("name\nD\nE", encoding="utf-8")  # 2 rows

        result = process_bulk_upload(
            session=test_session,
            dataset_id=dataset.id,
            files=[(file1, "file1.csv"), (file2, "file2.csv")],
            show_progress=False,
        )

        assert result.total_rows_added == 5
        assert result.successful[0].row_count == 3
        assert result.successful[1].row_count == 2

        # Verify database
        from sqlalchemy import text

        result_db = test_session.execute(text(f"SELECT COUNT(*) FROM {dataset.table_name}"))
        total_rows = result_db.scalar()
        assert total_rows == 5

    def test_bulk_upload_handles_duplicate_in_database(self, test_session, tmp_path):
        """Test that bulk upload skips files already in database."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Upload a file first
        from src.services.dataset_service import upload_csv_to_dataset

        existing_file = tmp_path / "existing.csv"
        existing_file.write_text("name\nExisting", encoding="utf-8")

        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=existing_file,
            filename="existing.csv",
            show_progress=False,
        )

        # Try to bulk upload same filename
        new_file = tmp_path / "new_file.csv"
        new_file.write_text("name\nNew", encoding="utf-8")

        result = process_bulk_upload(
            session=test_session,
            dataset_id=dataset.id,
            files=[(new_file, "existing.csv")],  # Same filename
            show_progress=False,
        )

        assert len(result.successful) == 0
        assert len(result.skipped) == 1
        assert result.skipped[0].error_type == "duplicate_in_db"

        # Verify only original file exists
        upload_logs = test_session.query(UploadLog).filter_by(dataset_id=dataset.id).all()
        assert len(upload_logs) == 1
        assert upload_logs[0].filename == "existing.csv"

    def test_bulk_upload_mixed_csv_pickle(self, test_session, tmp_path):
        """Test bulk upload with both CSV and Pickle files."""
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

        # CSV file
        csv_file = tmp_path / "file.csv"
        csv_file.write_text("name\nJohn", encoding="utf-8")

        # Pickle file
        df = pd.DataFrame({"name": ["Jane"]})
        pickle_file = tmp_path / "file.pkl"
        with open(pickle_file, "wb") as f:
            pickle.dump(df, f)

        result = process_bulk_upload(
            session=test_session,
            dataset_id=dataset.id,
            files=[(csv_file, "file.csv"), (pickle_file, "file.pkl")],
            show_progress=False,
        )

        assert len(result.successful) == 2
        assert result.total_rows_added == 2

        # Verify both file types in UploadLog
        upload_logs = test_session.query(UploadLog).filter_by(dataset_id=dataset.id).all()
        assert len(upload_logs) == 2

        file_types = {log.file_type for log in upload_logs}
        assert file_types == {"CSV", "PICKLE"}

    def test_bulk_upload_preserves_data_integrity(self, test_session, tmp_path):
        """Test that bulk upload correctly inserts data."""
        columns_config = {
            "name": {"type": "TEXT", "is_image": False},
            "age": {"type": "INTEGER", "is_image": False},
        }
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Create files with specific data
        file1 = tmp_path / "file1.csv"
        file1.write_text("name,age\nAlice,30\nBob,25", encoding="utf-8")

        file2 = tmp_path / "file2.csv"
        file2.write_text("name,age\nCharlie,35", encoding="utf-8")

        result = process_bulk_upload(
            session=test_session,
            dataset_id=dataset.id,
            files=[(file1, "file1.csv"), (file2, "file2.csv")],
            show_progress=False,
        )

        assert len(result.successful) == 2

        # Verify data integrity
        from sqlalchemy import text

        result_db = test_session.execute(
            text(f"SELECT name, age FROM {dataset.table_name} ORDER BY name")
        )
        rows = result_db.fetchall()

        assert len(rows) == 3
        names = [row[0] for row in rows]
        assert "Alice" in names
        assert "Bob" in names
        assert "Charlie" in names

    def test_bulk_upload_continues_after_validation_error(self, test_session, tmp_path):
        """Test that bulk upload continues processing after encountering errors."""
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

        # Invalid file (wrong columns)
        invalid_file = tmp_path / "invalid.csv"
        invalid_file.write_text("name,email\nJane,jane@example.com", encoding="utf-8")

        # Another valid file
        valid_file2 = tmp_path / "valid2.csv"
        valid_file2.write_text("name,age\nBob,25", encoding="utf-8")

        result = process_bulk_upload(
            session=test_session,
            dataset_id=dataset.id,
            files=[(valid_file, "valid.csv"), (invalid_file, "invalid.csv"), (valid_file2, "valid2.csv")],
            show_progress=False,
        )

        assert len(result.successful) == 2
        assert len(result.skipped) == 1
        assert result.total_rows_added == 2

        # Verify both valid files were uploaded
        from sqlalchemy import text

        result_db = test_session.execute(text(f"SELECT COUNT(*) FROM {dataset.table_name}"))
        total_rows = result_db.scalar()
        assert total_rows == 2

        upload_logs = test_session.query(UploadLog).filter_by(dataset_id=dataset.id).all()
        assert len(upload_logs) == 2

