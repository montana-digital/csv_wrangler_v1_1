"""
End-to-end tests for CSV Wrangler.

Tests critical user journeys through the entire application.
"""
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from src.database.connection import get_session, init_database
from src.database.repository import DatasetRepository, UserProfileRepository
from src.services.dataset_service import (
    delete_dataset,
    get_dataset_statistics,
    initialize_dataset,
    upload_csv_to_dataset,
)
from src.services.export_service import export_dataset_to_csv, export_dataset_to_pickle
from src.services.file_import_service import import_file
from src.services.profile_service import (
    create_user_profile,
    get_current_profile,
    is_app_initialized,
)


@pytest.fixture
def fresh_app_db(temp_db_path: Path):
    """Create a fresh database for E2E testing."""
    from src.database.connection import get_engine
    from sqlalchemy import create_engine

    database_url = f"sqlite:///{temp_db_path}"
    engine = create_engine(
        database_url,
        connect_args={"check_same_thread": False},
        echo=False,
    )

    # Enable foreign keys
    from sqlalchemy import event

    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    # Create tables
    from src.database.models import Base

    Base.metadata.create_all(bind=engine)

    # Create session factory
    from sqlalchemy.orm import sessionmaker

    SessionLocal = sessionmaker(bind=engine)

    yield SessionLocal()

    # Cleanup
    Base.metadata.drop_all(bind=engine)
    engine.dispose()


class TestFirstLaunchFlow:
    """Test first launch user journey."""

    def test_first_launch_profile_creation(self, fresh_app_db):
        """Test: First launch → profile creation → dataset initialization."""
        session = fresh_app_db

        # Step 1: Check app is not initialized
        assert is_app_initialized(session) is False

        # Step 2: Create user profile
        profile = create_user_profile(session, "Test User")
        assert profile.name == "Test User"
        assert profile.id is not None

        # Step 3: Verify app is now initialized
        assert is_app_initialized(session) is True

        # Step 4: Get current profile
        current_profile = get_current_profile(session)
        assert current_profile.id == profile.id

    def test_first_launch_complete_dataset_initialization(self, fresh_app_db, tmp_path: Path):
        """Test: Complete flow from profile creation to dataset initialization."""
        session = fresh_app_db

        # Create profile
        profile = create_user_profile(session, "E2E Test User")
        assert profile is not None

        # Create sample CSV
        csv_file = tmp_path / "test_data.csv"
        csv_content = "name,age,email\nJohn Doe,30,john@test.com\nJane Smith,25,jane@test.com"
        csv_file.write_text(csv_content, encoding="utf-8")

        # Import file
        df, file_type = import_file(csv_file)
        assert file_type == "CSV"
        assert len(df) == 2

        # Initialize dataset
        columns_config = {
            "name": {"type": "TEXT", "is_image": False},
            "age": {"type": "INTEGER", "is_image": False},
            "email": {"type": "TEXT", "is_image": False},
        }

        dataset = initialize_dataset(
            session=session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            duplicate_filter_column="email",
            image_columns=[],
        )

        assert dataset.name == "Test Dataset"
        assert dataset.slot_number == 1

        # Verify dataset exists
        repo = DatasetRepository(session)
        retrieved = repo.get_by_slot(1)
        assert retrieved.id == dataset.id


class TestDatasetUploadFlow:
    """Test dataset upload user journey."""

    def test_upload_csv_view_data_export(self, fresh_app_db, tmp_path: Path):
        """Test: Upload CSV → view data → export."""
        session = fresh_app_db

        # Setup: Create profile and initialize dataset
        create_user_profile(session, "Test User")

        columns_config = {
            "name": {"type": "TEXT", "is_image": False},
            "age": {"type": "INTEGER", "is_image": False},
        }

        dataset = initialize_dataset(
            session=session,
            name="Upload Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            duplicate_filter_column="name",
            image_columns=[],
        )

        # Step 1: Create and upload CSV
        csv_file = tmp_path / "upload_test.csv"
        csv_content = "name,age\nAlice,28\nBob,35\nCharlie,22"
        csv_file.write_text(csv_content, encoding="utf-8")

        upload_log = upload_csv_to_dataset(
            session=session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="upload_test.csv",
        )

        assert upload_log.row_count == 3
        assert upload_log.filename == "upload_test.csv"

        # Step 2: Get statistics
        stats = get_dataset_statistics(session, dataset.id)
        assert stats["total_rows"] == 3
        assert stats["total_uploads"] == 1

        # Step 3: Export to CSV
        export_file = tmp_path / "export.csv"
        export_path = export_dataset_to_csv(
            session=session,
            dataset_id=dataset.id,
            output_path=export_file,
        )

        assert export_path.exists()

        # Verify exported data
        exported_df = pd.read_csv(export_path)
        assert len(exported_df) == 3
        assert "name" in exported_df.columns
        assert "age" in exported_df.columns

    def test_upload_pickle_file(self, fresh_app_db, tmp_path: Path):
        """Test: Upload Pickle file → verify data."""
        import pickle

        session = fresh_app_db

        # Setup
        create_user_profile(session, "Test User")

        columns_config = {
            "product": {"type": "TEXT", "is_image": False},
            "price": {"type": "REAL", "is_image": False},
        }

        dataset = initialize_dataset(
            session=session,
            name="Pickle Test Dataset",
            slot_number=2,
            columns_config=columns_config,
            duplicate_filter_column="product",
            image_columns=[],
        )

        # Create Pickle file
        pickle_data = pd.DataFrame({
            "product": ["Widget A", "Widget B"],
            "price": [19.99, 29.99]
        })

        pickle_file = tmp_path / "test.pkl"
        with open(pickle_file, "wb") as f:
            pickle.dump(pickle_data, f)

        # Import and convert to CSV-like format for upload
        # Note: For E2E, we'd need to convert pickle to CSV or extend upload function
        # For now, test that pickle can be imported
        df, file_type = import_file(pickle_file)
        assert file_type == "PICKLE"
        assert len(df) == 2


class TestMultipleDatasetsFlow:
    """Test managing multiple datasets."""

    def test_initialize_multiple_datasets(self, fresh_app_db, tmp_path: Path):
        """Test: Initialize multiple datasets in different slots."""
        session = fresh_app_db

        create_user_profile(session, "Test User")

        # Initialize 3 datasets
        for slot in range(1, 4):
            columns_config = {
                "data": {"type": "TEXT", "is_image": False}
            }

            dataset = initialize_dataset(
                session=session,
                name=f"Dataset {slot}",
                slot_number=slot,
                columns_config=columns_config,
                duplicate_filter_column="data",
                image_columns=[],
            )

            assert dataset.slot_number == slot

        # Verify all exist
        repo = DatasetRepository(session)
        all_datasets = repo.get_all()
        assert len(all_datasets) == 3

        # Verify slots are correct
        for slot in range(1, 4):
            dataset = repo.get_by_slot(slot)
            assert dataset is not None
            assert dataset.slot_number == slot


class TestDuplicateFileDetectionFlow:
    """Test duplicate file detection and handling."""

    def test_duplicate_filename_detection(self, fresh_app_db, tmp_path: Path):
        """Test: Upload same filename twice → detect duplicate."""
        session = fresh_app_db

        create_user_profile(session, "Test User")

        columns_config = {
            "value": {"type": "TEXT", "is_image": False}
        }

        dataset = initialize_dataset(
            session=session,
            name="Duplicate Test",
            slot_number=1,
            columns_config=columns_config,
            duplicate_filter_column="value",
            image_columns=[],
        )

        # First upload
        csv_file = tmp_path / "duplicate.csv"
        csv_content = "value\nFirst"
        csv_file.write_text(csv_content, encoding="utf-8")

        upload_csv_to_dataset(
            session=session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="duplicate.csv",
        )

        # Try to upload same filename again
        csv_file2 = tmp_path / "duplicate2.csv"
        csv_content2 = "value\nSecond"
        csv_file2.write_text(csv_content2, encoding="utf-8")

        from src.services.dataset_service import check_duplicate_filename
        from src.utils.errors import DuplicateFileError

        with pytest.raises(DuplicateFileError):
            check_duplicate_filename(session, dataset.id, "duplicate.csv")


class TestDatasetDeletionFlow:
    """Test dataset deletion user journey."""

    def test_delete_dataset_resets_slot(self, fresh_app_db, tmp_path: Path):
        """Test: Delete dataset → slot becomes available → can reinitialize."""
        session = fresh_app_db

        create_user_profile(session, "Test User")

        # Initialize dataset
        columns_config = {"data": {"type": "TEXT", "is_image": False}}

        dataset = initialize_dataset(
            session=session,
            name="To Delete",
            slot_number=1,
            columns_config=columns_config,
            duplicate_filter_column="data",
            image_columns=[],
        )

        # Upload some data
        csv_file = tmp_path / "data.csv"
        csv_content = "data\nTest"
        csv_file.write_text(csv_content, encoding="utf-8")

        upload_csv_to_dataset(
            session=session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="data.csv",
        )

        dataset_id = dataset.id
        table_name = dataset.table_name

        # Delete dataset
        delete_dataset(session, dataset.id)

        # Verify dataset is gone
        repo = DatasetRepository(session)
        deleted = repo.get_by_id(dataset_id)
        assert deleted is None

        # Verify slot is available
        slot_dataset = repo.get_by_slot(1)
        assert slot_dataset is None

        # Can initialize new dataset in same slot
        new_dataset = initialize_dataset(
            session=session,
            name="New Dataset",
            slot_number=1,
            columns_config=columns_config,
            duplicate_filter_column="data",
            image_columns=[],
        )

        assert new_dataset.slot_number == 1
        assert new_dataset.name == "New Dataset"


class TestExportWithDateFilteringFlow:
    """Test export with date filtering."""

    def test_export_with_date_range(self, fresh_app_db, tmp_path: Path):
        """Test: Upload files at different times → export with date filter."""
        from datetime import datetime, timedelta

        session = fresh_app_db

        create_user_profile(session, "Test User")

        columns_config = {
            "name": {"type": "TEXT", "is_image": False},
            "upload_date": {"type": "TEXT", "is_image": False},
        }

        dataset = initialize_dataset(
            session=session,
            name="Date Filter Test",
            slot_number=1,
            columns_config=columns_config,
            duplicate_filter_column="name",
            image_columns=[],
        )

        # Upload files (simulate different upload dates)
        base_date = datetime.now()

        for i in range(3):
            csv_file = tmp_path / f"file_{i}.csv"
            date_str = (base_date - timedelta(days=i * 5)).isoformat()
            csv_content = f"name,upload_date\nItem {i},{date_str}"
            csv_file.write_text(csv_content, encoding="utf-8")

            upload_csv_to_dataset(
                session=session,
                dataset_id=dataset.id,
                csv_file=csv_file,
                filename=f"file_{i}.csv",
            )

        # Export with date range (last 7 days)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)

        export_file = tmp_path / "filtered_export.csv"
        export_path = export_dataset_to_csv(
            session=session,
            dataset_id=dataset.id,
            output_path=export_file,
            start_date=start_date,
            end_date=end_date,
        )

        assert export_path.exists()

        # Verify export contains data
        exported_df = pd.read_csv(export_path)
        assert len(exported_df) > 0


class TestPickleExportFlow:
    """Test Pickle export functionality."""

    def test_export_to_pickle(self, fresh_app_db, tmp_path: Path):
        """Test: Upload data → export to Pickle."""
        import pickle

        session = fresh_app_db

        create_user_profile(session, "Test User")

        columns_config = {
            "item": {"type": "TEXT", "is_image": False},
            "quantity": {"type": "INTEGER", "is_image": False},
        }

        dataset = initialize_dataset(
            session=session,
            name="Pickle Export Test",
            slot_number=1,
            columns_config=columns_config,
            duplicate_filter_column="item",
            image_columns=[],
        )

        # Upload data
        csv_file = tmp_path / "items.csv"
        csv_content = "item,quantity\nWidget,10\nGadget,5"
        csv_file.write_text(csv_content, encoding="utf-8")

        upload_csv_to_dataset(
            session=session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="items.csv",
        )

        # Export to Pickle
        export_file = tmp_path / "export.pkl"
        export_path = export_dataset_to_pickle(
            session=session,
            dataset_id=dataset.id,
            output_path=export_file,
        )

        assert export_path.exists()

        # Verify Pickle file can be loaded
        with open(export_path, "rb") as f:
            exported_df = pickle.load(f)

        assert isinstance(exported_df, pd.DataFrame)
        assert len(exported_df) == 2
        assert "item" in exported_df.columns

