"""
Integration tests for DataFrame service.

Tests DataFrame loading with filters, large datasets, and performance.
"""
import pytest
import pandas as pd

pytestmark = pytest.mark.integration

from src.services.dataset_service import initialize_dataset, upload_csv_to_dataset
from src.services.dataframe_service import (
    load_dataset_dataframe,
    load_enriched_dataset_dataframe,
    get_dataset_row_count,
)
from src.services.enrichment_service import create_enriched_dataset


class TestDataFrameLoadingIntegration:
    """Test DataFrame loading with real database."""

    def test_load_large_dataset_with_pagination(self, test_session, tmp_path):
        """Test loading large dataset with pagination."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Large Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        # Create CSV with many rows
        rows = "\n".join([f"Person{i}" for i in range(50)])
        csv_file = tmp_path / "large.csv"
        csv_file.write_text(f"name\n{rows}", encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="large.csv"
        )
        
        # Load with limit
        df = load_dataset_dataframe(
            session=test_session,
            dataset_id=dataset.id,
            limit=20
        )
        
        assert len(df) == 20
        
        # Load next page
        df2 = load_dataset_dataframe(
            session=test_session,
            dataset_id=dataset.id,
            limit=20,
            offset=20
        )
        
        assert len(df2) == 20
        # Should have different data
        assert df.iloc[0]["name"] != df2.iloc[0]["name"]

    def test_load_enriched_dataset_with_enriched_columns(self, test_session, tmp_path):
        """Test loading enriched dataset includes enriched columns."""
        columns_config = {
            "phone": {"type": "TEXT", "is_image": False},
            "email": {"type": "TEXT", "is_image": False}
        }
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        csv_file = tmp_path / "source.csv"
        csv_file.write_text("phone,email\n555-1234,john@test.com", encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="source.csv"
        )
        
        enrichment_config = {"phone": "phone_numbers", "email": "emails"}
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched",
            enrichment_config=enrichment_config
        )
        
        # Load enriched dataset
        df = load_enriched_dataset_dataframe(
            session=test_session,
            enriched_dataset_id=enriched.id,
            include_image_columns=False
        )
        
        assert len(df) == 1
        # Should have original columns
        assert "phone" in df.columns
        assert "email" in df.columns
        # Should have enriched columns
        assert "phone_enriched_phone_numbers" in df.columns
        assert "email_enriched_emails" in df.columns

    def test_row_count_accuracy(self, test_session, tmp_path):
        """Test that row count is accurate."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        # Upload multiple files
        for i in range(3):
            csv_file = tmp_path / f"file{i}.csv"
            csv_file.write_text(f"name\nRow{i}-1\nRow{i}-2", encoding="utf-8")
            upload_csv_to_dataset(
                session=test_session,
                dataset_id=dataset.id,
                csv_file=csv_file,
                filename=f"file{i}.csv"
            )
        
        # Total should be 6 rows (2 per file)
        row_count = get_dataset_row_count(test_session, dataset.id)
        assert row_count == 6

    def test_load_dataset_excludes_image_columns(self, test_session, tmp_path):
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
        csv_file.write_text("name,image\nJohn,data:image/png;base64,xxx", encoding="utf-8")
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

    def test_load_dataset_orders_by_recent(self, test_session, tmp_path):
        """Test that datasets are ordered by most recent first."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        # Upload files in sequence
        csv_file1 = tmp_path / "first.csv"
        csv_file1.write_text("name\nFirst", encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file1,
            filename="first.csv"
        )
        
        csv_file2 = tmp_path / "second.csv"
        csv_file2.write_text("name\nSecond", encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file2,
            filename="second.csv"
        )
        
        # With order_by_recent=True (default), most recent should be first
        df = load_dataset_dataframe(
            session=test_session,
            dataset_id=dataset.id,
            order_by_recent=True
        )
        
        # Most recent upload should appear first
        assert df.iloc[0]["name"] == "Second"

