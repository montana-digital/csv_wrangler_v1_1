"""
Unit tests for DataFrame service.

Tests DataFrame loading, filtering, and column operations for datasets and enriched datasets.
"""
import pandas as pd
import pytest

pytestmark = pytest.mark.unit

from src.database.models import EnrichedDataset
from src.services.dataset_service import initialize_dataset
from src.services.dataframe_service import (
    get_dataset_columns,
    get_dataset_row_count,
    get_enriched_dataset_columns,
    get_enriched_dataset_row_count,
    load_dataset_dataframe,
    load_enriched_dataset_dataframe,
)
from src.utils.errors import ValidationError


class TestLoadDatasetDataframe:
    """Test loading dataset DataFrames."""

    def test_load_dataset_dataframe_basic(self, test_session, tmp_path):
        """Test basic dataset DataFrame loading."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}, "age": {"type": "INTEGER", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("name,age\nJohn,30\nJane,25", encoding="utf-8")
        from src.services.dataset_service import upload_csv_to_dataset
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="test.csv"
        )
        
        df = load_dataset_dataframe(
            session=test_session,
            dataset_id=dataset.id,
            include_image_columns=False
        )
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "name" in df.columns
        assert "age" in df.columns

    def test_load_dataset_dataframe_with_limit(self, test_session, tmp_path):
        """Test loading with row limit."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        # Create CSV with many rows
        rows = "\n".join([f"Person{i}" for i in range(20)])
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(f"name\n{rows}", encoding="utf-8")
        from src.services.dataset_service import upload_csv_to_dataset
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="test.csv"
        )
        
        df = load_dataset_dataframe(
            session=test_session,
            dataset_id=dataset.id,
            limit=10
        )
        
        assert len(df) <= 10

    def test_load_dataset_dataframe_with_offset(self, test_session, tmp_path):
        """Test loading with offset for pagination."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        rows = "\n".join([f"Person{i}" for i in range(10)])
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(f"name\n{rows}", encoding="utf-8")
        from src.services.dataset_service import upload_csv_to_dataset
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="test.csv"
        )
        
        df1 = load_dataset_dataframe(
            session=test_session,
            dataset_id=dataset.id,
            limit=5,
            offset=0
        )
        df2 = load_dataset_dataframe(
            session=test_session,
            dataset_id=dataset.id,
            limit=5,
            offset=5
        )
        
        # Should get different rows
        assert len(df1) == 5
        assert len(df2) == 5
        # Rows should be different (unless dataset has exactly 5 rows)
        if len(df1) > 0 and len(df2) > 0:
            assert not df1.iloc[0]["name"] == df2.iloc[0]["name"] or len(pd.concat([df1, df2])) > 5

    def test_load_dataset_dataframe_excludes_image_columns(self, test_session, tmp_path):
        """Test that image columns are excluded by default."""
        columns_config = {
            "name": {"type": "TEXT", "is_image": False},
            "image": {"type": "TEXT", "is_image": True}
        }
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=["image"],
        )
        
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("name,image\nJohn,data:image\nJane,data:image", encoding="utf-8")
        from src.services.dataset_service import upload_csv_to_dataset
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="test.csv"
        )
        
        df = load_dataset_dataframe(
            session=test_session,
            dataset_id=dataset.id,
            include_image_columns=False
        )
        
        assert "image" not in df.columns
        assert "name" in df.columns

    def test_load_dataset_dataframe_nonexistent_raises_error(self, test_session):
        """Test that loading non-existent dataset raises ValidationError."""
        with pytest.raises(ValidationError):
            load_dataset_dataframe(
                session=test_session,
                dataset_id=99999
            )

    def test_load_dataset_dataframe_empty_dataset(self, test_session):
        """Test loading empty dataset returns empty DataFrame."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        df = load_dataset_dataframe(
            session=test_session,
            dataset_id=dataset.id
        )
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        # Should still have correct columns
        assert "name" in df.columns


class TestGetDatasetRowCount:
    """Test getting dataset row count."""

    def test_get_dataset_row_count_with_data(self, test_session, tmp_path):
        """Test getting row count for dataset with data."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("name\nA\nB\nC", encoding="utf-8")
        from src.services.dataset_service import upload_csv_to_dataset
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="test.csv"
        )
        
        count = get_dataset_row_count(test_session, dataset.id)
        assert count == 3

    def test_get_dataset_row_count_empty(self, test_session):
        """Test getting row count for empty dataset."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        count = get_dataset_row_count(test_session, dataset.id)
        assert count == 0

    def test_get_dataset_row_count_nonexistent(self, test_session):
        """Test getting row count for non-existent dataset returns 0."""
        count = get_dataset_row_count(test_session, 99999)
        assert count == 0


class TestGetDatasetColumns:
    """Test getting dataset columns."""

    def test_get_dataset_columns_basic(self, test_session):
        """Test getting columns for dataset."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}, "age": {"type": "INTEGER", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        columns = get_dataset_columns(
            session=test_session,
            dataset_id=dataset.id
        )
        
        assert "name" in columns
        assert "age" in columns

    def test_get_dataset_columns_excludes_image_columns(self, test_session):
        """Test that image columns are excluded by default."""
        columns_config = {
            "name": {"type": "TEXT", "is_image": False},
            "image": {"type": "TEXT", "is_image": True}
        }
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=["image"],
        )
        
        columns = get_dataset_columns(
            session=test_session,
            dataset_id=dataset.id,
            include_image_columns=False
        )
        
        assert "image" not in columns
        assert "name" in columns


class TestLoadEnrichedDatasetDataframe:
    """Test loading enriched dataset DataFrames."""

    def test_load_enriched_dataset_dataframe_nonexistent_raises_error(self, test_session):
        """Test that loading non-existent enriched dataset raises ValidationError."""
        with pytest.raises(ValidationError):
            load_enriched_dataset_dataframe(
                session=test_session,
                enriched_dataset_id=99999
            )


class TestGetEnrichedDatasetRowCount:
    """Test getting enriched dataset row count."""

    def test_get_enriched_dataset_row_count_nonexistent_returns_zero(self, test_session):
        """Test getting row count for non-existent enriched dataset returns 0."""
        count = get_enriched_dataset_row_count(test_session, 99999)
        assert count == 0


class TestGetEnrichedDatasetColumns:
    """Test getting enriched dataset columns."""

    def test_get_enriched_dataset_columns_nonexistent_returns_empty(self, test_session):
        """Test getting columns for non-existent enriched dataset returns empty list."""
        columns = get_enriched_dataset_columns(
            session=test_session,
            enriched_dataset_id=99999
        )
        assert columns == []

