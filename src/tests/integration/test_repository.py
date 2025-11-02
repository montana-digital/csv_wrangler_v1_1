"""
Integration tests for Repository pattern.

Tests data access layer with real database operations.
"""
import pytest

from src.database.models import DatasetConfig, UploadLog, UserProfile
from src.database.repository import (
    DatasetRepository,
    UploadLogRepository,
    UserProfileRepository,
)


class TestDatasetRepository:
    """Test DatasetRepository."""

    def test_create_and_get_dataset(self, test_session):
        """Test creating and retrieving a dataset."""
        repo = DatasetRepository(test_session)

        dataset = DatasetConfig(
            name="Test Dataset",
            slot_number=1,
            table_name="test_table",
            columns_config={"name": {"type": "TEXT"}},
            duplicate_filter_column="name",
            image_columns=[],
        )

        created = repo.create(dataset)
        assert created.id is not None

        retrieved = repo.get_by_id(created.id)
        assert retrieved is not None
        assert retrieved.name == "Test Dataset"

    def test_get_by_slot(self, test_session):
        """Test getting dataset by slot number."""
        repo = DatasetRepository(test_session)

        dataset = DatasetConfig(
            name="Slot 2 Dataset",
            slot_number=2,
            table_name="slot2_table",
            columns_config={"name": {"type": "TEXT"}},
            duplicate_filter_column="name",
            image_columns=[],
        )

        repo.create(dataset)

        retrieved = repo.get_by_slot(2)
        assert retrieved is not None
        assert retrieved.slot_number == 2

    def test_get_all_datasets(self, test_session):
        """Test getting all datasets."""
        repo = DatasetRepository(test_session)

        # Create multiple datasets
        for i in range(1, 4):
            dataset = DatasetConfig(
                name=f"Dataset {i}",
                slot_number=i,
                table_name=f"table_{i}",
                columns_config={"name": {"type": "TEXT"}},
                duplicate_filter_column="name",
                image_columns=[],
            )
            repo.create(dataset)

        all_datasets = repo.get_all()
        assert len(all_datasets) == 3

    def test_update_dataset(self, test_session):
        """Test updating a dataset."""
        repo = DatasetRepository(test_session)

        dataset = DatasetConfig(
            name="Original Name",
            slot_number=1,
            table_name="test_table",
            columns_config={"name": {"type": "TEXT"}},
            duplicate_filter_column="name",
            image_columns=[],
        )

        created = repo.create(dataset)
        created.name = "Updated Name"
        updated = repo.update(created)

        assert updated.name == "Updated Name"

    def test_delete_dataset(self, test_session):
        """Test deleting a dataset."""
        repo = DatasetRepository(test_session)

        dataset = DatasetConfig(
            name="To Delete",
            slot_number=1,
            table_name="delete_table",
            columns_config={"name": {"type": "TEXT"}},
            duplicate_filter_column="name",
            image_columns=[],
        )

        created = repo.create(dataset)
        dataset_id = created.id

        repo.delete(dataset_id)

        deleted = repo.get_by_id(dataset_id)
        assert deleted is None


class TestUploadLogRepository:
    """Test UploadLogRepository."""

    def test_create_and_get_upload_log(self, test_session):
        """Test creating and retrieving upload log."""
        # Create dataset first
        dataset = DatasetConfig(
            name="Test Dataset",
            slot_number=1,
            table_name="test_table",
            columns_config={"name": {"type": "TEXT"}},
            duplicate_filter_column="name",
            image_columns=[],
        )
        test_session.add(dataset)
        test_session.commit()

        repo = UploadLogRepository(test_session)

        upload_log = UploadLog(
            dataset_id=dataset.id,
            filename="test.csv",
            file_type="CSV",
            row_count=10,
        )

        created = repo.create(upload_log)
        assert created.id is not None

        retrieved = repo.get_by_id(created.id)
        assert retrieved is not None
        assert retrieved.filename == "test.csv"

    def test_get_by_dataset_id(self, test_session):
        """Test getting upload logs by dataset ID."""
        # Create dataset
        dataset = DatasetConfig(
            name="Test Dataset",
            slot_number=1,
            table_name="test_table",
            columns_config={"name": {"type": "TEXT"}},
            duplicate_filter_column="name",
            image_columns=[],
        )
        test_session.add(dataset)
        test_session.commit()

        repo = UploadLogRepository(test_session)

        # Create multiple upload logs
        for i in range(3):
            upload_log = UploadLog(
                dataset_id=dataset.id,
                filename=f"file_{i}.csv",
                file_type="CSV",
                row_count=i * 10,
            )
            repo.create(upload_log)

        logs = repo.get_by_dataset_id(dataset.id)
        assert len(logs) == 3

    def test_check_duplicate_filename(self, test_session):
        """Test checking for duplicate filename."""
        # Create dataset
        dataset = DatasetConfig(
            name="Test Dataset",
            slot_number=1,
            table_name="test_table",
            columns_config={"name": {"type": "TEXT"}},
            duplicate_filter_column="name",
            image_columns=[],
        )
        test_session.add(dataset)
        test_session.commit()

        repo = UploadLogRepository(test_session)

        upload_log = UploadLog(
            dataset_id=dataset.id,
            filename="duplicate.csv",
            file_type="CSV",
            row_count=10,
        )
        repo.create(upload_log)

        # Check duplicate
        duplicate = repo.check_duplicate_filename(dataset.id, "duplicate.csv")
        assert duplicate is not None
        assert duplicate.filename == "duplicate.csv"

        # Check non-duplicate
        not_duplicate = repo.check_duplicate_filename(dataset.id, "new_file.csv")
        assert not_duplicate is None


class TestUserProfileRepository:
    """Test UserProfileRepository."""

    def test_create_and_get_profile(self, test_session):
        """Test creating and retrieving user profile."""
        repo = UserProfileRepository(test_session)

        profile = UserProfile(name="Test User")
        created = repo.create(profile)

        assert created.id is not None

        retrieved = repo.get_by_id(created.id)
        assert retrieved is not None
        assert retrieved.name == "Test User"

    def test_get_first_profile(self, test_session):
        """Test getting first profile."""
        repo = UserProfileRepository(test_session)

        profile = UserProfile(name="First User")
        repo.create(profile)

        first = repo.get_first()
        assert first is not None
        assert first.name == "First User"

    def test_profile_exists(self, test_session):
        """Test checking if profile exists."""
        repo = UserProfileRepository(test_session)

        assert repo.exists() is False

        profile = UserProfile(name="Test User")
        repo.create(profile)

        assert repo.exists() is True

    def test_update_profile(self, test_session):
        """Test updating user profile."""
        repo = UserProfileRepository(test_session)

        profile = UserProfile(name="Original Name")
        created = repo.create(profile)

        created.name = "Updated Name"
        updated = repo.update(created)

        assert updated.name == "Updated Name"

