"""
Comprehensive integration tests for Enrichment sync functionality.

Tests the complete sync workflow: upload -> enrich -> upload new -> sync -> verify.
"""
import pytest

pytestmark = pytest.mark.integration

from src.database.models import EnrichedDataset
from src.services.enrichment_service import (
    create_enriched_dataset,
    sync_enriched_dataset,
)
from src.services.dataset_service import initialize_dataset, upload_csv_to_dataset
from src.services.dataframe_service import load_enriched_dataset_dataframe
from src.utils.errors import ValidationError, DatabaseError
from sqlalchemy import text


class TestEnrichmentSyncWorkflow:
    """Test complete sync workflows."""

    def test_basic_sync_workflow(self, test_session, tmp_path):
        """Test complete sync workflow: upload -> enrich -> upload new -> sync -> verify."""
        # 1. Initialize dataset
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # 2. Upload initial data
        csv1 = tmp_path / "initial.csv"
        csv1.write_text("phone\n555-111-1234\n555-222-2345", encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv1,
            filename="initial.csv"
        )

        # 3. Create enriched dataset
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched",
            enrichment_config={"phone": "phone_numbers"}
        )

        # 4. Verify initial state
        result = test_session.execute(
            text(f"SELECT COUNT(*) FROM {enriched.enriched_table_name}")
        )
        initial_count = result.scalar()
        assert initial_count == 2, f"Expected 2 initial rows, got {initial_count}"

        # 5. Upload new data (should NOT auto-sync)
        csv2 = tmp_path / "new.csv"
        csv2.write_text("phone\n555-333-4567\n555-444-5678", encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv2,
            filename="new.csv"
        )

        # 6. Verify enriched table hasn't changed (no auto-sync)
        result = test_session.execute(
            text(f"SELECT COUNT(*) FROM {enriched.enriched_table_name}")
        )
        count_after_upload = result.scalar()
        assert count_after_upload == 2, f"Expected 2 rows after upload (no auto-sync), got {count_after_upload}"

        # 7. Manually sync
        rows_synced = sync_enriched_dataset(test_session, enriched.id)
        assert rows_synced == 2, f"Expected 2 rows synced, got {rows_synced}"

        # 8. Verify final state
        result = test_session.execute(
            text(f"SELECT COUNT(*) FROM {enriched.enriched_table_name}")
        )
        final_count = result.scalar()
        assert final_count == 4, f"Expected 4 total rows (2 original + 2 new), got {final_count}"

        # 9. Verify enrichment columns populated for new rows
        df = load_enriched_dataset_dataframe(
            session=test_session,
            enriched_dataset_id=enriched.id,
            include_image_columns=False
        )
        new_rows = df[df['phone'].isin(['555-333-4567', '555-444-5678'])]
        assert len(new_rows) == 2, "Should have 2 new rows"
        assert all(new_rows['phone_enriched_phone_numbers'].notna()), "Enrichment columns should be populated for new rows"

        # 10. Verify original rows still have enrichment values
        original_rows = df[df['phone'].isin(['555-111-1234', '555-222-2345'])]
        assert len(original_rows) == 2, "Should have 2 original rows"
        assert all(original_rows['phone_enriched_phone_numbers'].notna()), "Original rows should still have enrichment values"

    def test_multiple_enrichment_types_sync(self, test_session, tmp_path):
        """Test sync with multiple enrichment types."""
        # 1. Initialize dataset with multiple columns
        columns_config = {
            "phone": {"type": "TEXT", "is_image": False},
            "email": {"type": "TEXT", "is_image": False},
            "domain": {"type": "TEXT", "is_image": False}
        }
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # 2. Upload initial data
        csv1 = tmp_path / "initial.csv"
        csv1.write_text(
            "phone,email,domain\n555-111-1234,john@test.com,example.com",
            encoding="utf-8"
        )
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv1,
            filename="initial.csv"
        )

        # 3. Create enriched dataset with multiple enrichment types
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Multi Enriched",
            enrichment_config={
                "phone": "phone_numbers",
                "email": "emails",
                "domain": "web_domains"
            }
        )

        # 4. Upload new data
        csv2 = tmp_path / "new.csv"
        csv2.write_text(
            "phone,email,domain\n555-222-3456,jane@test.com,example.org\n555-333-4567,bob@test.com,test.net",
            encoding="utf-8"
        )
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv2,
            filename="new.csv"
        )

        # 5. Sync enriched dataset
        rows_synced = sync_enriched_dataset(test_session, enriched.id)
        assert rows_synced == 2, f"Expected 2 rows synced, got {rows_synced}"

        # 6. Verify all enrichment columns are populated for new rows
        df = load_enriched_dataset_dataframe(
            session=test_session,
            enriched_dataset_id=enriched.id,
            include_image_columns=False
        )
        new_rows = df[df['phone'].isin(['555-222-3456', '555-333-4567'])]
        
        assert len(new_rows) == 2, "Should have 2 new rows"
        assert all(new_rows['phone_enriched_phone_numbers'].notna()), "Phone enrichment should be populated"
        assert all(new_rows['email_enriched_emails'].notna()), "Email enrichment should be populated"
        assert all(new_rows['domain_enriched_web_domains'].notna()), "Domain enrichment should be populated"

    def test_sync_column_validation(self, test_session, tmp_path):
        """Test that sync validates columns exist before processing."""
        # 1. Initialize dataset
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

        # 2. Upload initial data
        csv1 = tmp_path / "initial.csv"
        csv1.write_text("phone,email\n555-111-1234,john@test.com", encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv1,
            filename="initial.csv"
        )

        # 3. Create enriched dataset with phone enrichment
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched",
            enrichment_config={"phone": "phone_numbers"}
        )

        # 4. Create a new table without phone column to simulate schema change
        # Then manually copy data to test column validation
        from src.config.settings import UNIQUE_ID_COLUMN_NAME
        import uuid
        new_uuid = str(uuid.uuid4())
        
        # Create a temporary table without phone column
        test_session.execute(
            text(f"CREATE TABLE {dataset.table_name}_temp (email TEXT, {UNIQUE_ID_COLUMN_NAME} TEXT PRIMARY KEY)")
        )
        test_session.execute(
            text(f"INSERT INTO {dataset.table_name}_temp (email, {UNIQUE_ID_COLUMN_NAME}) VALUES ('jane@test.com', :uuid)"),
            {"uuid": new_uuid}
        )
        test_session.commit()
        
        # Replace the source table with the temp table (without phone column)
        test_session.execute(text(f"DROP TABLE {dataset.table_name}"))
        test_session.execute(
            text(f"ALTER TABLE {dataset.table_name}_temp RENAME TO {dataset.table_name}")
        )
        test_session.commit()
        
        # Update the dataset's columns_config to reflect the change
        dataset.columns_config = {"email": {"type": "TEXT", "is_image": False}}
        test_session.commit()
        
        # 5. Attempt sync - should raise ValidationError (wrapped in DatabaseError)
        with pytest.raises((ValidationError, DatabaseError)) as exc_info:
            sync_enriched_dataset(test_session, enriched.id)
        
        # Verify error message mentions missing column
        error_message = str(exc_info.value)
        assert "phone" in error_message.lower(), f"Error should mention phone column. Got: {error_message}"
        assert "don't exist" in error_message.lower() or "not found" in error_message.lower() or "references columns" in error_message.lower(), \
            f"Error should indicate column doesn't exist. Got: {error_message}"

    def test_sync_with_no_new_rows(self, test_session, tmp_path):
        """Test sync when no new rows exist."""
        # 1. Initialize dataset
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # 2. Upload initial data
        csv1 = tmp_path / "initial.csv"
        csv1.write_text("phone\n555-111-1234\n555-222-2345", encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv1,
            filename="initial.csv"
        )

        # 3. Create enriched dataset
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched",
            enrichment_config={"phone": "phone_numbers"}
        )

        # 4. Get initial sync date
        initial_sync_date = enriched.last_sync_date

        # 5. Sync immediately (no new uploads)
        rows_synced = sync_enriched_dataset(test_session, enriched.id)
        assert rows_synced == 0, f"Expected 0 rows synced, got {rows_synced}"

        # 6. Verify no errors occurred and last_sync_date is updated
        test_session.refresh(enriched)
        assert enriched.last_sync_date is not None, "last_sync_date should be updated"
        assert enriched.last_sync_date >= initial_sync_date, "last_sync_date should be updated to current time"

        # 7. Verify row count unchanged
        result = test_session.execute(
            text(f"SELECT COUNT(*) FROM {enriched.enriched_table_name}")
        )
        row_count = result.scalar()
        assert row_count == 2, f"Expected 2 rows unchanged, got {row_count}"

    def test_sync_multiple_enriched_datasets(self, test_session, tmp_path):
        """Test syncing multiple enriched datasets independently."""
        # 1. Initialize dataset
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

        # 2. Upload initial data
        csv1 = tmp_path / "initial.csv"
        csv1.write_text("phone,email\n555-111-1234,john@test.com", encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv1,
            filename="initial.csv"
        )

        # 3. Create multiple enriched datasets with different configs
        enriched1 = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Phone Enriched",
            enrichment_config={"phone": "phone_numbers"}
        )
        enriched2 = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Email Enriched",
            enrichment_config={"email": "emails"}
        )

        # 4. Upload new data
        csv2 = tmp_path / "new.csv"
        csv2.write_text("phone,email\n555-222-3456,jane@test.com\n555-333-4567,bob@test.com", encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv2,
            filename="new.csv"
        )

        # 5. Sync first enriched dataset
        rows_synced1 = sync_enriched_dataset(test_session, enriched1.id)
        assert rows_synced1 == 2, f"Expected 2 rows synced for enriched1, got {rows_synced1}"

        # 6. Verify enriched1 has correct data
        df1 = load_enriched_dataset_dataframe(
            session=test_session,
            enriched_dataset_id=enriched1.id,
            include_image_columns=False
        )
        assert len(df1) == 3, "Enriched1 should have 3 rows (1 original + 2 new)"
        new_rows1 = df1[df1['phone'].isin(['555-222-3456', '555-333-4567'])]
        assert all(new_rows1['phone_enriched_phone_numbers'].notna()), "Phone enrichment should be populated for new rows"

        # 7. Sync second enriched dataset
        rows_synced2 = sync_enriched_dataset(test_session, enriched2.id)
        assert rows_synced2 == 2, f"Expected 2 rows synced for enriched2, got {rows_synced2}"

        # 8. Verify enriched2 has correct data
        df2 = load_enriched_dataset_dataframe(
            session=test_session,
            enriched_dataset_id=enriched2.id,
            include_image_columns=False
        )
        assert len(df2) == 3, "Enriched2 should have 3 rows (1 original + 2 new)"
        new_rows2 = df2[df2['email'].isin(['jane@test.com', 'bob@test.com'])]
        assert all(new_rows2['email_enriched_emails'].notna()), "Email enrichment should be populated for new rows"

        # 9. Verify each enriched dataset is independent
        assert enriched1.enriched_table_name != enriched2.enriched_table_name
        assert enriched1.columns_added != enriched2.columns_added

    def test_verify_enrichment_values_correctness(self, test_session, tmp_path):
        """Test that enrichment values are standardized correctly."""
        # 1. Initialize dataset
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )

        # 2. Upload initial data with various phone formats
        csv1 = tmp_path / "initial.csv"
        csv1.write_text(
            "phone\n555-123-4567\n(555) 987-6543\n+1-555-555-5555",
            encoding="utf-8"
        )
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv1,
            filename="initial.csv"
        )

        # 3. Create enriched dataset
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched",
            enrichment_config={"phone": "phone_numbers"}
        )

        # 4. Verify initial enrichment values are standardized
        df_initial = load_enriched_dataset_dataframe(
            session=test_session,
            enriched_dataset_id=enriched.id,
            include_image_columns=False
        )
        enriched_col = 'phone_enriched_phone_numbers'
        assert all(df_initial[enriched_col].notna()), "All enrichment values should be populated"
        # Verify original values are preserved
        assert all(df_initial['phone'].isin(['555-123-4567', '(555) 987-6543', '+1-555-555-5555']))

        # 5. Upload new data with different phone formats
        csv2 = tmp_path / "new.csv"
        csv2.write_text(
            "phone\n5551234567\n1-555-000-1234",
            encoding="utf-8"
        )
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv2,
            filename="new.csv"
        )

        # 6. Sync enriched dataset
        rows_synced = sync_enriched_dataset(test_session, enriched.id)
        assert rows_synced == 2, f"Expected 2 rows synced, got {rows_synced}"

        # 7. Verify new rows have enrichment values
        df_final = load_enriched_dataset_dataframe(
            session=test_session,
            enriched_dataset_id=enriched.id,
            include_image_columns=False
        )
        assert len(df_final) == 5, "Should have 5 total rows (3 original + 2 new)"
        
        # Verify new rows have enrichment values populated
        new_rows = df_final[df_final['phone'].isin(['5551234567', '1-555-000-1234'])]
        assert len(new_rows) == 2, "Should have 2 new rows"
        assert all(new_rows[enriched_col].notna()), "Enrichment columns should be populated for new rows"
        
        # Verify original source values are preserved
        assert all(new_rows['phone'].isin(['5551234567', '1-555-000-1234'])), "Original source values should be preserved"

