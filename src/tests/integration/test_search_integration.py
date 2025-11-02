"""
Integration tests for Knowledge Base search.

Tests end-to-end search workflows across Knowledge Tables and enriched datasets.
"""
import pandas as pd
import pytest
import time

from src.services.dataset_service import initialize_dataset
from src.services.enrichment_service import create_enriched_dataset
from src.services.csv_service import generate_unique_ids
from src.services.knowledge_service import initialize_knowledge_table
from src.services.search_service import (
    get_enriched_dataset_data_for_key,
    get_knowledge_table_data_for_key,
    search_knowledge_base,
)
from src.services.table_service import insert_dataframe_to_table


class TestEndToEndSearchWorkflow:
    """Test complete search workflow from start to finish."""

    def test_end_to_end_search_workflow(self, test_session):
        """Test full search flow: standardize → presence → retrieve."""
        # Setup: Create Knowledge Table
        columns_config = {
            "phone": {"type": "TEXT", "is_image": False},
            "carrier": {"type": "TEXT", "is_image": False},
        }
        initial_data = pd.DataFrame({
            "phone": ["+1234567890", "+0987654321"],
            "carrier": ["Verizon", "AT&T"],
        })
        
        knowledge_table = initialize_knowledge_table(
            session=test_session,
            name="Carrier Info",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
            initial_data_df=initial_data,
        )
        
        # Step 1: Search for presence
        results = search_knowledge_base(
            test_session,
            "+1234567890",
            "phone_numbers",
        )
        
        assert results["standardized_key_id"] is not None
        assert len(results["presence"]["knowledge_tables"]) > 0
        
        # Step 2: Get detailed data
        df = get_knowledge_table_data_for_key(
            test_session,
            knowledge_table.id,
            results["standardized_key_id"],
        )
        
        assert not df.empty
        assert len(df) >= 1
        assert "phone" in df.columns
        assert "carrier" in df.columns

    def test_search_across_mixed_sources(self, test_session):
        """Test search across Knowledge Tables + enriched datasets simultaneously."""
        # Create Knowledge Table
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        knowledge_table = initialize_knowledge_table(
            session=test_session,
            name="White List",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
            initial_data_df=pd.DataFrame({"phone": ["+1234567890"]}),
        )
        
        # Create enriched dataset
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config={"phone": {"type": "TEXT", "is_image": False}},
            image_columns=[],
        )
        
        df = pd.DataFrame({"phone": ["+1234567890"]})
        df = generate_unique_ids(df)
        insert_dataframe_to_table(test_session, dataset.table_name, df)
        
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched Dataset",
            enrichment_config={"phone": "phone_numbers"},
        )
        
        # Search should find both
        results = search_knowledge_base(
            test_session,
            "+1234567890",
            "phone_numbers",
        )
        
        assert len(results["presence"]["knowledge_tables"]) >= 1
        assert len(results["presence"]["enriched_datasets"]) >= 1
        assert results["search_stats"]["matched_sources"] >= 2

    def test_search_with_indexed_enriched_columns(self, test_session):
        """Verify indexes improve query performance."""
        # Create enriched dataset (indexes created automatically)
        dataset = initialize_dataset(
            session=test_session,
            name="Large Dataset",
            slot_number=1,
            columns_config={"phone": {"type": "TEXT", "is_image": False}},
            image_columns=[],
        )
        
        # Add multiple rows
        phones = [f"+123456789{i}" for i in range(100)]
        df = pd.DataFrame({"phone": phones})
        df = generate_unique_ids(df)
        insert_dataframe_to_table(test_session, dataset.table_name, df)
        
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched Large",
            enrichment_config={"phone": "phone_numbers"},
        )
        
        # Search should be fast even with many rows (indexed)
        start_time = time.time()
        results = search_knowledge_base(
            test_session,
            "+1234567890",
            "phone_numbers",
        )
        elapsed_ms = (time.time() - start_time) * 1000
        
        # Should complete quickly (<200ms) with indexes
        assert elapsed_ms < 200
        assert results["search_stats"]["search_time_ms"] < 200

    def test_search_after_enrichment_sync(self, test_session):
        """Test search works after new data added via sync."""
        # Create dataset and enriched dataset
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config={"phone": {"type": "TEXT", "is_image": False}},
            image_columns=[],
        )
        
        df1 = pd.DataFrame({"phone": ["+1111111111"]})
        df1 = generate_unique_ids(df1)
        insert_dataframe_to_table(test_session, dataset.table_name, df1)
        
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched Test",
            enrichment_config={"phone": "phone_numbers"},
        )
        
        # Search before sync - should find existing
        results1 = search_knowledge_base(
            test_session,
            "+1111111111",
            "phone_numbers",
        )
        assert results1["search_stats"]["matched_sources"] >= 1
        
        # Add new row to source dataset
        df2 = pd.DataFrame({"phone": ["+2222222222"]})
        df2 = generate_unique_ids(df2)
        insert_dataframe_to_table(test_session, dataset.table_name, df2)
        
        # Sync enriched dataset
        from src.services.enrichment_service import sync_enriched_dataset
        sync_enriched_dataset(test_session, enriched.id)
        
        # Search for new value - should find it
        results2 = search_knowledge_base(
            test_session,
            "+2222222222",
            "phone_numbers",
        )
        assert results2["search_stats"]["matched_sources"] >= 1


class TestSearchWithLargeDatasets:
    """Test search performance and correctness with larger datasets."""

    def test_search_with_large_knowledge_table(self, test_session):
        """Test with Knowledge Table >10K rows."""
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        
        # Create table with many rows
        phones = [f"+123456789{i:04d}" for i in range(10000)]
        initial_data = pd.DataFrame({"phone": phones})
        
        knowledge_table = initialize_knowledge_table(
            session=test_session,
            name="Large Knowledge Table",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
            initial_data_df=initial_data,
        )
        
        # Search should still be fast (indexed Key_ID)
        start_time = time.time()
        results = search_knowledge_base(
            test_session,
            "+1234567890000",
            "phone_numbers",
        )
        elapsed_ms = (time.time() - start_time) * 1000
        
        # Should complete quickly (<100ms) even with 10K rows
        assert elapsed_ms < 100
        assert results["standardized_key_id"] is not None

    def test_search_with_large_enriched_dataset(self, test_session):
        """Test with enriched dataset >10K rows."""
        dataset = initialize_dataset(
            session=test_session,
            name="Large Dataset",
            slot_number=1,
            columns_config={"phone": {"type": "TEXT", "is_image": False}},
            image_columns=[],
        )
        
        # Add 10K rows
        phones = [f"+123456789{i:04d}" for i in range(10000)]
        df = pd.DataFrame({"phone": phones})
        df = generate_unique_ids(df)
        insert_dataframe_to_table(test_session, dataset.table_name, df)
        
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Large Enriched",
            enrichment_config={"phone": "phone_numbers"},
        )
        
        # Search should be fast (indexed enriched column)
        start_time = time.time()
        results = search_knowledge_base(
            test_session,
            "+1234567890000",
            "phone_numbers",
        )
        elapsed_ms = (time.time() - start_time) * 1000
        
        # Should complete quickly (<100ms)
        assert elapsed_ms < 100


class TestSearchFilters:
    """Test source filtering functionality."""

    def test_search_filters_correctly(self, test_session):
        """Test source filters exclude correct sources."""
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        
        table1 = initialize_knowledge_table(
            session=test_session,
            name="White List",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
            initial_data_df=pd.DataFrame({"phone": ["+1234567890"]}),
        )
        
        table2 = initialize_knowledge_table(
            session=test_session,
            name="Black List",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
        )
        
        # Search with filter - only White List
        results = search_knowledge_base(
            test_session,
            "+1234567890",
            "phone_numbers",
            source_filters=["White List"],
        )
        
        table_names = {kt["name"] for kt in results["presence"]["knowledge_tables"]}
        assert "White List" in table_names
        assert "Black List" not in table_names


class TestSearchEdgeCases:
    """Test edge cases and special scenarios."""

    def test_search_handles_special_characters(self, test_session):
        """Test phone numbers and emails with special chars."""
        # Test phone with various formats
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        
        knowledge_table = initialize_knowledge_table(
            session=test_session,
            name="Test Table",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
            initial_data_df=pd.DataFrame({"phone": ["+1 (234) 567-8900"]}),
        )
        
        # Search with different formats - should still find
        results1 = search_knowledge_base(
            test_session,
            "+12345678900",
            "phone_numbers",
        )
        assert results1["search_stats"]["matched_sources"] >= 1

    def test_search_case_sensitivity(self, test_session):
        """Test email/domain case handling."""
        # Emails should be case-insensitive (handled by standardization)
        columns_config = {"email": {"type": "TEXT", "is_image": False}}
        
        knowledge_table = initialize_knowledge_table(
            session=test_session,
            name="Email Table",
            data_type="emails",
            primary_key_column="email",
            columns_config=columns_config,
            image_columns=[],
            initial_data_df=pd.DataFrame({"email": ["Test@Example.com"]}),
        )
        
        # Search with different case - should find
        results = search_knowledge_base(
            test_session,
            "test@example.com",
            "emails",
        )
        assert results["search_stats"]["matched_sources"] >= 1

    def test_search_with_duplicate_key_ids(self, test_session):
        """Test multiple rows with same Key_ID in different tables."""
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        
        table1 = initialize_knowledge_table(
            session=test_session,
            name="Table 1",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
            initial_data_df=pd.DataFrame({"phone": ["+1234567890"]}),
        )
        
        table2 = initialize_knowledge_table(
            session=test_session,
            name="Table 2",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
            initial_data_df=pd.DataFrame({"phone": ["+1234567890"]}),
        )
        
        # Search should find both tables
        results = search_knowledge_base(
            test_session,
            "+1234567890",
            "phone_numbers",
        )
        
        table_ids = {kt["table_id"] for kt in results["presence"]["knowledge_tables"]}
        assert table1.id in table_ids
        assert table2.id in table_ids
        assert results["search_stats"]["matched_sources"] >= 2

