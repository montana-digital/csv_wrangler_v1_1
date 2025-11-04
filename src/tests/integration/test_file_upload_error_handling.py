"""
Integration tests for file upload error handling and sync fixes.

Tests cover:
1. Enriched dataset sync error handling (no error propagation)
2. Duplicate file handling with skip_duplicate_check
3. Error suppression during upload (sync errors don't affect upload)
"""
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
import logging

pytestmark = pytest.mark.integration

from src.database.models import DatasetConfig, EnrichedDataset, UploadLog
from src.services.dataset_service import (
    initialize_dataset,
    upload_csv_to_dataset,
    check_duplicate_filename,
)
from src.services.enrichment_service import (
    create_enriched_dataset,
    sync_all_enriched_datasets_for_source,
)
from src.utils.errors import (
    DuplicateFileError,
    ValidationError,
    DatabaseError,
)
from src.utils.validation import table_exists
from sqlalchemy import text


class TestEnrichedDatasetSyncErrorHandling:
    """Test that enriched dataset sync errors don't cause upload failures."""

    def test_upload_with_valid_enriched_datasets(self, test_session, tmp_path):
        """Upload succeeds, sync succeeds, no errors displayed."""
        # Setup: Create dataset with enriched dataset
        columns_config = {"email": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Create enriched dataset
        enrichment_config = {"email": "emails"}
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched",
            enrichment_config=enrichment_config,
        )
        test_session.flush()

        # Upload file - should succeed and sync should work
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("email\njohn@test.com\njane@test.com", encoding="utf-8")

        # Upload should not raise any exceptions
        upload_log = upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="test.csv",
            show_progress=False,
        )

        assert upload_log is not None
        assert upload_log.row_count == 2
        assert upload_log.filename == "test.csv"

        # Verify enriched dataset was synced
        result = test_session.execute(
            text(f"SELECT COUNT(*) FROM {enriched.enriched_table_name}")
        )
        total_rows = result.scalar()
        assert total_rows == 2  # Initial data + new data

    def test_upload_with_missing_enriched_table(self, test_session, tmp_path):
        """Upload succeeds, sync fails silently with ValidationError."""
        # Setup: Create dataset with enriched dataset record but drop the table
        columns_config = {"email": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Create enriched dataset
        enrichment_config = {"email": "emails"}
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched",
            enrichment_config=enrichment_config,
        )
        test_session.flush()

        # Drop the enriched table to simulate missing table
        from src.utils.validation import quote_identifier
        quoted_table = quote_identifier(enriched.enriched_table_name)
        test_session.execute(text(f"DROP TABLE IF EXISTS {quoted_table}"))
        test_session.commit()

        # Upload file - should succeed despite missing table
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("email\njohn@test.com", encoding="utf-8")

        # Upload should not raise any exceptions even though sync will fail
        upload_log = upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="test.csv",
            show_progress=False,
        )

        assert upload_log is not None
        assert upload_log.row_count == 1
        # Sync failure should be caught and logged, but not propagated

    def test_upload_with_orphaned_enriched_dataset(self, test_session, tmp_path):
        """Upload succeeds, sync fails silently with ValidationError."""
        # Setup: Create dataset with enriched dataset, then manually delete the enriched table
        # to simulate orphaned state
        columns_config = {"email": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Create enriched dataset
        enrichment_config = {"email": "emails"}
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched",
            enrichment_config=enrichment_config,
        )
        test_session.flush()

        # Drop the enriched table to simulate orphaned state (table missing but record exists)
        from src.utils.validation import quote_identifier
        quoted_table = quote_identifier(enriched.enriched_table_name)
        test_session.execute(text(f"DROP TABLE IF EXISTS {quoted_table}"))
        test_session.commit()

        # Upload file - should succeed even though enriched table is missing
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("email\njohn@test.com", encoding="utf-8")

        # Upload should not raise any exceptions even though sync will fail
        upload_log = upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="test.csv",
            show_progress=False,
        )

        assert upload_log is not None
        assert upload_log.row_count == 1

    def test_upload_with_database_error_during_sync(self, test_session, tmp_path):
        """Upload succeeds, sync fails silently with DatabaseError."""
        # Setup: Create dataset with enriched dataset
        columns_config = {"email": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Create enriched dataset
        enrichment_config = {"email": "emails"}
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched",
            enrichment_config=enrichment_config,
        )
        test_session.flush()

        # Mock sync to raise DatabaseError
        with patch(
            "src.services.enrichment_service.sync_all_enriched_datasets_for_source"
        ) as mock_sync:
            mock_sync.side_effect = DatabaseError(
                "Simulated database error", operation="sync"
            )

            # Upload file - should succeed despite sync error
            csv_file = tmp_path / "test.csv"
            csv_file.write_text("email\njohn@test.com", encoding="utf-8")

            # Upload should not raise any exceptions
            upload_log = upload_csv_to_dataset(
                session=test_session,
                dataset_id=dataset.id,
                csv_file=csv_file,
                filename="test.csv",
                show_progress=False,
            )

            assert upload_log is not None
            assert upload_log.row_count == 1
            # Sync should have been called but error caught
            mock_sync.assert_called_once()

    def test_upload_with_no_enriched_datasets(self, test_session, tmp_path):
        """Upload succeeds, no sync attempted."""
        # Setup: Create dataset without enriched datasets
        columns_config = {"email": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Upload file - should succeed without attempting sync
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("email\njohn@test.com", encoding="utf-8")

        # Upload should not raise any exceptions
        upload_log = upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="test.csv",
            show_progress=False,
        )

        assert upload_log is not None
        assert upload_log.row_count == 1

    def test_sync_errors_dont_propagate(self, test_session, tmp_path):
        """Verify sync exceptions are caught and don't reach caller."""
        # Setup: Create dataset with enriched dataset
        columns_config = {"email": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Create enriched dataset
        enrichment_config = {"email": "emails"}
        create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched",
            enrichment_config=enrichment_config,
        )
        test_session.flush()

        # Mock sync to raise various exceptions
        test_exceptions = [
            ValidationError("Test validation error"),
            DatabaseError("Test database error", operation="test"),
            RuntimeError("Test runtime error"),
        ]

        for exc in test_exceptions:
            with patch(
                "src.services.enrichment_service.sync_all_enriched_datasets_for_source"
            ) as mock_sync:
                mock_sync.side_effect = exc

                # Upload file - should succeed despite any exception
                csv_file = tmp_path / f"test_{exc.__class__.__name__}.csv"
                csv_file.write_text("email\njohn@test.com", encoding="utf-8")

                # Upload should not raise any exceptions
                upload_log = upload_csv_to_dataset(
                    session=test_session,
                    dataset_id=dataset.id,
                    csv_file=csv_file,
                    filename=csv_file.name,
                    show_progress=False,
                )

                assert upload_log is not None
                assert upload_log.row_count == 1


class TestDuplicateFileHandling:
    """Test duplicate file detection and skip functionality."""

    def test_duplicate_detection_raises_error(self, test_session, tmp_path):
        """Verify duplicate check raises DuplicateFileError."""
        # Setup: Create dataset and upload a file
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Upload first file
        csv_file1 = tmp_path / "test.csv"
        csv_file1.write_text("name\nJohn", encoding="utf-8")

        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file1,
            filename="test.csv",
            show_progress=False,
        )

        # Try to check for duplicate - should raise DuplicateFileError
        with pytest.raises(DuplicateFileError) as exc_info:
            check_duplicate_filename(test_session, dataset.id, "test.csv")

        assert "test.csv" in str(exc_info.value)

    def test_skip_duplicate_check_allows_upload(self, test_session, tmp_path):
        """Verify skip_duplicate_check=True allows duplicate upload."""
        # Setup: Create dataset and upload a file
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Upload first file
        csv_file1 = tmp_path / "test1.csv"
        csv_file1.write_text("name\nJohn", encoding="utf-8")

        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file1,
            filename="duplicate.csv",
            show_progress=False,
        )

        # Upload same filename with skip_duplicate_check=True - should succeed
        csv_file2 = tmp_path / "test2.csv"
        csv_file2.write_text("name\nJane", encoding="utf-8")

        upload_log = upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file2,
            filename="duplicate.csv",
            show_progress=False,
            skip_duplicate_check=True,  # Skip duplicate check
        )

        assert upload_log is not None
        assert upload_log.row_count == 1
        assert upload_log.filename == "duplicate.csv"

        # Verify both uploads exist
        upload_logs = (
            test_session.query(UploadLog)
            .filter_by(dataset_id=dataset.id, filename="duplicate.csv")
            .all()
        )
        assert len(upload_logs) == 2  # Both uploads should exist

    def test_upload_anyway_flow(self, test_session, tmp_path):
        """Simulate full Upload Anyway flow with session state."""
        # Setup: Create dataset and upload a file
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Upload first file
        csv_file1 = tmp_path / "test1.csv"
        csv_file1.write_text("name\nJohn", encoding="utf-8")

        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file1,
            filename="duplicate.csv",
            show_progress=False,
        )

        # Simulate "Upload Anyway" flow:
        # 1. Check duplicate - should raise error
        # 2. User confirms - set flag (simulated)
        # 3. Upload with skip_duplicate_check=True

        # Step 1: Verify duplicate check raises error
        with pytest.raises(DuplicateFileError):
            check_duplicate_filename(test_session, dataset.id, "duplicate.csv")

        # Step 2 & 3: Upload with skip_duplicate_check=True (simulating user confirmation)
        csv_file2 = tmp_path / "test2.csv"
        csv_file2.write_text("name\nJane", encoding="utf-8")

        upload_log = upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file2,
            filename="duplicate.csv",
            show_progress=False,
            skip_duplicate_check=True,  # Simulates user clicking "Upload Anyway"
        )

        assert upload_log is not None
        assert upload_log.row_count == 1

    def test_duplicate_check_skipped_when_flag_set(self, test_session, tmp_path):
        """Verify duplicate check is skipped when user confirmed."""
        # Setup: Create dataset and upload a file
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Upload first file
        csv_file1 = tmp_path / "test1.csv"
        csv_file1.write_text("name\nJohn", encoding="utf-8")

        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file1,
            filename="duplicate.csv",
            show_progress=False,
        )

        # Verify that skip_duplicate_check=True bypasses the duplicate check
        # by checking that upload_csv_to_dataset doesn't call check_duplicate_filename
        csv_file2 = tmp_path / "test2.csv"
        csv_file2.write_text("name\nJane", encoding="utf-8")

        with patch(
            "src.services.dataset_service.check_duplicate_filename"
        ) as mock_check:
            upload_log = upload_csv_to_dataset(
                session=test_session,
                dataset_id=dataset.id,
                csv_file=csv_file2,
                filename="duplicate.csv",
                show_progress=False,
                skip_duplicate_check=True,
            )

            # Verify duplicate check was NOT called when skip_duplicate_check=True
            mock_check.assert_not_called()

            assert upload_log is not None
            assert upload_log.row_count == 1


class TestUploadErrorSuppression:
    """Test that errors during upload are properly suppressed."""

    def test_sync_errors_logged_but_not_displayed(self, test_session, tmp_path, caplog):
        """Verify sync errors are logged but don't appear in UI."""
        # Setup: Create dataset with enriched dataset
        columns_config = {"email": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Create enriched dataset
        enrichment_config = {"email": "emails"}
        create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched",
            enrichment_config=enrichment_config,
        )
        test_session.flush()

        # Mock sync to raise ValidationError (expected error)
        with patch(
            "src.services.enrichment_service.sync_all_enriched_datasets_for_source"
        ) as mock_sync:
            mock_sync.side_effect = ValidationError("Test validation error")

            # Upload file
            csv_file = tmp_path / "test.csv"
            csv_file.write_text("email\njohn@test.com", encoding="utf-8")

            with caplog.at_level(logging.WARNING):
                upload_log = upload_csv_to_dataset(
                    session=test_session,
                    dataset_id=dataset.id,
                    csv_file=csv_file,
                    filename="test.csv",
                    show_progress=False,
                )

            # Verify upload succeeded
            assert upload_log is not None
            assert upload_log.row_count == 1

            # Verify error was logged (but not displayed to user)
            assert any("validation error" in record.message.lower() for record in caplog.records)

    def test_upload_succeeds_despite_sync_failure(self, test_session, tmp_path):
        """Verify upload completes successfully even if sync fails."""
        # Setup: Create dataset with enriched dataset that will fail to sync
        columns_config = {"email": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Create enriched dataset
        enrichment_config = {"email": "emails"}
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched",
            enrichment_config=enrichment_config,
        )
        test_session.flush()

        # Drop the enriched table to cause sync failure
        from src.utils.validation import quote_identifier
        quoted_table = quote_identifier(enriched.enriched_table_name)
        test_session.execute(text(f"DROP TABLE IF EXISTS {quoted_table}"))
        test_session.commit()

        # Upload file - should succeed even though sync will fail
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("email\njohn@test.com\njane@test.com", encoding="utf-8")

        upload_log = upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="test.csv",
            show_progress=False,
        )

        # Verify upload succeeded
        assert upload_log is not None
        assert upload_log.row_count == 2

        # Verify data was inserted into source table
        result = test_session.execute(
            text(f"SELECT COUNT(*) FROM {dataset.table_name}")
        )
        total_rows = result.scalar()
        assert total_rows == 2

    def test_no_error_propagation_to_safe_operation(self, test_session, tmp_path):
        """Verify no exceptions from sync reach SafeOperation context."""
        # Setup: Create dataset with enriched dataset
        columns_config = {"email": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # Create enriched dataset
        enrichment_config = {"email": "emails"}
        create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched",
            enrichment_config=enrichment_config,
        )
        test_session.flush()

        # Test that various exceptions don't propagate
        test_exceptions = [
            ValidationError("Test validation"),
            DatabaseError("Test database error", operation="test"),
            RuntimeError("Test runtime"),
            ValueError("Test value error"),
        ]

        for exc in test_exceptions:
            with patch(
                "src.services.enrichment_service.sync_all_enriched_datasets_for_source"
            ) as mock_sync:
                mock_sync.side_effect = exc

                # Upload should not raise any exception
                csv_file = tmp_path / f"test_{exc.__class__.__name__}.csv"
                csv_file.write_text("email\njohn@test.com", encoding="utf-8")

                # This should not raise - exceptions are caught internally
                upload_log = upload_csv_to_dataset(
                    session=test_session,
                    dataset_id=dataset.id,
                    csv_file=csv_file,
                    filename=csv_file.name,
                    show_progress=False,
                )

                assert upload_log is not None
                assert upload_log.row_count == 1

