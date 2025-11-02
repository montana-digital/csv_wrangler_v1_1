"""
Integration tests for Enrichment service.

Tests enrichment workflows with database interactions and data integrity.
"""
import pytest

pytestmark = pytest.mark.integration

from src.database.models import EnrichedDataset
from src.services.enrichment_service import (
    create_enriched_dataset,
    delete_enriched_dataset,
    sync_all_enriched_datasets_for_source,
    sync_enriched_dataset,
)
from src.services.dataset_service import initialize_dataset, upload_csv_to_dataset
from src.services.dataframe_service import load_enriched_dataset_dataframe
from sqlalchemy import text


class TestEnrichmentWorkflowIntegration:
    """Test complete enrichment workflows."""

    def test_create_enriched_dataset_copies_data(self, test_session, tmp_path):
        """Test that creating enriched dataset copies all source data."""
        columns_config = {
            "name": {"type": "TEXT", "is_image": False},
            "phone": {"type": "TEXT", "is_image": False}
        }
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        csv_file = tmp_path / "source.csv"
        csv_file.write_text("name,phone\nJohn,555-1111\nJane,555-2222\nBob,555-3333", encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="source.csv"
        )
        
        enrichment_config = {"phone": "phone_numbers"}
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched Dataset",
            enrichment_config=enrichment_config
        )
        
        # Verify data was copied
        result = test_session.execute(
            text(f"SELECT COUNT(*) FROM {enriched.enriched_table_name}")
        )
        row_count = result.scalar()
        assert row_count == 3

    def test_enriched_dataset_has_enriched_columns(self, test_session, tmp_path):
        """Test that enriched dataset has enriched columns populated."""
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        csv_file = tmp_path / "source.csv"
        csv_file.write_text("phone\n555-123-4567\n(555) 987-6543", encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="source.csv"
        )
        
        enrichment_config = {"phone": "phone_numbers"}
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched",
            enrichment_config=enrichment_config
        )
        
        # Check enriched column exists and has values
        result = test_session.execute(
            text(f"SELECT phone_enriched_phone_numbers FROM {enriched.enriched_table_name}")
        )
        enriched_values = [row[0] for row in result.fetchall()]
        assert len(enriched_values) == 2
        # At least some should be enriched (not None)
        assert any(val is not None for val in enriched_values)

    def test_sync_enriched_dataset_updates_new_rows(self, test_session, tmp_path):
        """Test that sync updates enriched dataset when source is updated."""
        columns_config = {"email": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        # Initial data
        csv_file1 = tmp_path / "source1.csv"
        csv_file1.write_text("email\njohn@test.com", encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file1,
            filename="source1.csv"
        )
        
        enrichment_config = {"email": "emails"}
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched",
            enrichment_config=enrichment_config
        )
        
        # Add new data
        # Note: upload_csv_to_dataset automatically syncs enriched datasets (v1.1 feature)
        csv_file2 = tmp_path / "source2.csv"
        csv_file2.write_text("email\njane@test.com\nbob@test.com", encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file2,
            filename="source2.csv"
        )
        
        # Verify automatic sync happened during upload
        # The enriched dataset should already have all 3 rows (1 initial + 2 auto-synced)
        result = test_session.execute(
            text(f"SELECT COUNT(*) FROM {enriched.enriched_table_name}")
        )
        total_rows = result.scalar()
        assert total_rows == 3, f"Expected 3 rows after auto-sync, got {total_rows}"
        
        # Manual sync should find 0 new rows (already synced automatically)
        rows_synced = sync_enriched_dataset(test_session, enriched.id)
        assert rows_synced == 0, "No new rows should be found after automatic sync"
        
        # Verify total rows unchanged
        result = test_session.execute(
            text(f"SELECT COUNT(*) FROM {enriched.enriched_table_name}")
        )
        total_rows_after = result.scalar()
        assert total_rows_after == 3  # Still 3 rows

    def test_multiple_enrichments_on_same_dataset(self, test_session, tmp_path):
        """Test creating multiple enriched datasets from same source."""
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
        
        # Create first enriched dataset (phone only)
        enriched1 = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Phone Enriched",
            enrichment_config={"phone": "phone_numbers"}
        )
        
        # Create second enriched dataset (email only)
        enriched2 = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Email Enriched",
            enrichment_config={"email": "emails"}
        )
        
        assert enriched1.id != enriched2.id
        assert enriched1.enriched_table_name != enriched2.enriched_table_name
        
        # Verify both have data
        result1 = test_session.execute(
            text(f"SELECT COUNT(*) FROM {enriched1.enriched_table_name}")
        )
        result2 = test_session.execute(
            text(f"SELECT COUNT(*) FROM {enriched2.enriched_table_name}")
        )
        assert result1.scalar() == 1
        assert result2.scalar() == 1

    def test_sync_all_enriched_datasets_auto_updates(self, test_session, tmp_path):
        """Test that sync_all updates all enriched datasets when source changes."""
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        csv_file1 = tmp_path / "source1.csv"
        csv_file1.write_text("phone\n555-1111", encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file1,
            filename="source1.csv"
        )
        
        # Create two enriched datasets
        enriched1 = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched 1",
            enrichment_config={"phone": "phone_numbers"}
        )
        enriched2 = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched 2",
            enrichment_config={"phone": "phone_numbers"}
        )
        
        # Add new data
        csv_file2 = tmp_path / "source2.csv"
        csv_file2.write_text("phone\n555-2222", encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file2,
            filename="source2.csv"
        )
        
        # Verify automatic sync happened - both enriched datasets should have 2 rows now
        result1 = test_session.execute(
            text(f"SELECT COUNT(*) FROM {enriched1.enriched_table_name}")
        )
        result2 = test_session.execute(
            text(f"SELECT COUNT(*) FROM {enriched2.enriched_table_name}")
        )
        assert result1.scalar() == 2, "Enriched dataset 1 should have 2 rows after auto-sync"
        assert result2.scalar() == 2, "Enriched dataset 2 should have 2 rows after auto-sync"
        
        # Manual sync should find 0 new rows (already synced automatically)
        from src.services.enrichment_service import sync_all_enriched_datasets_for_source
        results = sync_all_enriched_datasets_for_source(test_session, dataset.id)
        
        # Both enriched datasets should be in results but with 0 rows synced
        assert len(results) == 2
        assert all(rows_synced == 0 for rows_synced in results.values()), "No new rows should be found after automatic sync"

    def test_delete_enriched_dataset_removes_table(self, test_session, tmp_path):
        """Test that deleting enriched dataset removes the table."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        csv_file = tmp_path / "source.csv"
        csv_file.write_text("name\nJohn", encoding="utf-8")
        upload_csv_to_dataset(test_session, dataset.id, csv_file, "source.csv")
        
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched",
            enrichment_config={"name": "phone_numbers"}
        )
        
        table_name = enriched.enriched_table_name
        enriched_id = enriched.id
        
        # Verify table exists
        from sqlalchemy import inspect
        inspector = inspect(test_session.bind)
        tables_before = inspector.get_table_names()
        assert table_name in tables_before
        
        # Delete
        delete_enriched_dataset(test_session, enriched_id)
        
        # Verify table removed
        inspector = inspect(test_session.bind)
        tables_after = inspector.get_table_names()
        assert table_name not in tables_after

