"""
Unit tests for Search service.

Tests Knowledge Base search functionality following TDD principles.
"""
import pandas as pd
import pytest
import time

from src.services.knowledge_service import initialize_knowledge_table
from src.services.search_service import (
    get_enriched_dataset_data_for_key,
    get_knowledge_table_data_for_key,
    search_knowledge_base,
)
from src.utils.errors import ValidationError


class TestStandardizeSearchValue:
    """Test search value standardization (using knowledge_service function)."""

    def test_standardize_valid_phone_number(self, test_session):
        """Test that valid phone numbers are standardized correctly."""
        from src.services.search_service import standardize_key_value
        
        # Note: standardize_key_value is in knowledge_service, but we import for testing
        from src.services.knowledge_service import standardize_key_value
        
        result = standardize_key_value("+1234567890", "phone_numbers")
        assert result is not None
        
        result = standardize_key_value("(123) 456-7890", "phone_numbers")
        assert result is not None

    def test_standardize_invalid_value_returns_none(self, test_session):
        """Test that invalid values return None."""
        from src.services.knowledge_service import standardize_key_value
        
        result = standardize_key_value("invalid", "phone_numbers")
        assert result is None


class TestSearchKnowledgeBasePresence:
    """Test Phase 1: Presence search."""

    def test_search_knowledge_base_presence_only(self, test_session):
        """Test presence search returns flags without data retrieval."""
        # Create Knowledge Table
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        initial_data = pd.DataFrame({"phone": ["+1234567890"]})
        
        knowledge_table = initialize_knowledge_table(
            session=test_session,
            name="Test Phone Table",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
            initial_data_df=initial_data,
        )
        
        # Search for the phone number
        results = search_knowledge_base(
            test_session,
            "+1234567890",
            "phone_numbers",
        )
        
        assert "presence" in results
        assert "knowledge_tables" in results["presence"]
        assert "enriched_datasets" in results["presence"]
        assert "standardized_key_id" in results
        assert "search_stats" in results
        
        # Should find the Knowledge Table
        assert len(results["presence"]["knowledge_tables"]) >= 1
        found_table = None
        for kt in results["presence"]["knowledge_tables"]:
            if kt["table_id"] == knowledge_table.id:
                found_table = kt
                break
        
        assert found_table is not None
        assert found_table["has_data"] is True
        assert found_table["row_count"] == 1

    def test_search_no_matches(self, test_session):
        """Test search returns empty results when no matches found."""
        results = search_knowledge_base(
            test_session,
            "+9999999999",  # Non-existent phone
            "phone_numbers",
        )
        
        assert results["standardized_key_id"] is not None  # Standardization succeeded
        # May have tables but no matches
        assert "presence" in results
        assert "search_stats" in results
        assert results["search_stats"]["matched_sources"] == 0

    def test_search_returns_correct_structure(self, test_session):
        """Test that search returns expected dictionary structure."""
        results = search_knowledge_base(
            test_session,
            "+1234567890",
            "phone_numbers",
        )
        
        # Verify structure
        assert isinstance(results, dict)
        assert "presence" in results
        assert isinstance(results["presence"], dict)
        assert "knowledge_tables" in results["presence"]
        assert "enriched_datasets" in results["presence"]
        assert isinstance(results["presence"]["knowledge_tables"], list)
        assert isinstance(results["presence"]["enriched_datasets"], list)
        
        # Verify knowledge_table entries structure
        for kt in results["presence"]["knowledge_tables"]:
            assert "table_name" in kt
            assert "table_id" in kt
            assert "name" in kt
            assert "row_count" in kt
            assert "has_data" in kt
        
        # Verify enriched_dataset entries structure
        for ed in results["presence"]["enriched_datasets"]:
            assert "dataset_id" in ed
            assert "name" in ed
            assert "enriched_table_name" in ed
            assert "source_column" in ed
            assert "enriched_column" in ed
            assert "row_count" in ed
        
        # Verify stats
        assert "total_sources" in results["search_stats"]
        assert "matched_sources" in results["search_stats"]
        assert "search_time_ms" in results["search_stats"]

    def test_search_invalid_data_type(self, test_session):
        """Test that invalid data_type raises ValidationError."""
        with pytest.raises(ValidationError):
            search_knowledge_base(
                test_session,
                "+1234567890",
                "invalid_type",
            )

    def test_search_with_source_filters(self, test_session):
        """Test filtering by Knowledge Table names."""
        # Create two Knowledge Tables
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
        
        # Search with filter
        results = search_knowledge_base(
            test_session,
            "+1234567890",
            "phone_numbers",
            source_filters=["White List"],
        )
        
        # Should only return White List
        table_names = [kt["name"] for kt in results["presence"]["knowledge_tables"]]
        assert "White List" in table_names
        assert "Black List" not in table_names

    def test_search_handles_multiple_tables_same_type(self, test_session):
        """Test search across multiple Knowledge Tables of same data_type."""
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
        
        results = search_knowledge_base(
            test_session,
            "+1234567890",
            "phone_numbers",
        )
        
        # Should find both tables
        table_ids = {kt["table_id"] for kt in results["presence"]["knowledge_tables"]}
        assert table1.id in table_ids
        assert table2.id in table_ids


class TestGetKnowledgeTableDataForKey:
    """Test Phase 2: Detailed retrieval from Knowledge Tables."""

    def test_get_knowledge_table_data_for_key(self, test_session):
        """Test retrieving data for valid Key_ID."""
        columns_config = {
            "phone": {"type": "TEXT", "is_image": False},
            "carrier": {"type": "TEXT", "is_image": False},
        }
        initial_data = pd.DataFrame({
            "phone": ["+1234567890"],
            "carrier": ["Verizon"],
        })
        
        knowledge_table = initialize_knowledge_table(
            session=test_session,
            name="Test Table",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
            initial_data_df=initial_data,
        )
        
        # Get standardized Key_ID
        from src.services.knowledge_service import standardize_key_value
        key_id = standardize_key_value("+1234567890", "phone_numbers")
        
        # Retrieve data
        df = get_knowledge_table_data_for_key(
            test_session,
            knowledge_table.id,
            key_id,
        )
        
        assert not df.empty
        assert len(df) == 1
        assert "phone" in df.columns
        assert "carrier" in df.columns
        assert "Key_ID" in df.columns

    def test_get_knowledge_table_data_invalid_key(self, test_session):
        """Test handling non-existent Key_ID gracefully."""
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        
        knowledge_table = initialize_knowledge_table(
            session=test_session,
            name="Test Table",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
        )
        
        # Get data for non-existent Key_ID
        df = get_knowledge_table_data_for_key(
            test_session,
            knowledge_table.id,
            "+9999999999",
        )
        
        # Should return empty DataFrame, not raise error
        assert df.empty

    def test_get_knowledge_table_data_invalid_table_id(self, test_session):
        """Test that invalid Knowledge Table ID raises ValidationError."""
        with pytest.raises(ValidationError):
            get_knowledge_table_data_for_key(
                test_session,
                99999,  # Non-existent ID
                "+1234567890",
            )


class TestGetEnrichedDatasetDataForKey:
    """Test Phase 2: Detailed retrieval from enriched datasets."""

    def test_get_enriched_dataset_data_for_key(self, test_session):
        """Test retrieving enriched dataset data."""
        # Create dataset and enriched dataset
        from src.services.dataset_service import initialize_dataset
        from src.services.enrichment_service import create_enriched_dataset
        from src.services.csv_service import generate_unique_ids
        from src.services.table_service import insert_dataframe_to_table
        
        # Create dataset
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config={"phone": {"type": "TEXT", "is_image": False}},
            image_columns=[],
        )
        
        # Add data
        df = pd.DataFrame({"phone": ["+1234567890"]})
        df = generate_unique_ids(df)
        insert_dataframe_to_table(test_session, dataset.table_name, df)
        
        # Create enriched dataset
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched Test",
            enrichment_config={"phone": "phone_numbers"},
        )
        
        # Get standardized Key_ID
        from src.services.knowledge_service import standardize_key_value
        key_id = standardize_key_value("+1234567890", "phone_numbers")
        
        # Retrieve data
        df_result = get_enriched_dataset_data_for_key(
            test_session,
            enriched.id,
            "phone_enriched_phone_numbers",
            key_id,
        )
        
        assert not df_result.empty
        assert len(df_result) == 1

    def test_get_enriched_dataset_data_invalid_key(self, test_session):
        """Test handling non-existent Key_ID in enriched dataset."""
        from src.services.dataset_service import initialize_dataset
        from src.services.enrichment_service import create_enriched_dataset
        
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config={"phone": {"type": "TEXT", "is_image": False}},
            image_columns=[],
        )
        
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched Test",
            enrichment_config={"phone": "phone_numbers"},
        )
        
        # Get data for non-existent Key_ID
        df = get_enriched_dataset_data_for_key(
            test_session,
            enriched.id,
            "phone_enriched_phone_numbers",
            "+9999999999",
        )
        
        # Should return empty DataFrame
        assert df.empty

    def test_get_enriched_dataset_data_invalid_dataset_id(self, test_session):
        """Test that invalid enriched dataset ID raises ValidationError."""
        with pytest.raises(ValidationError):
            get_enriched_dataset_data_for_key(
                test_session,
                99999,  # Non-existent ID
                "phone_enriched_phone_numbers",
                "+1234567890",
            )

    def test_get_enriched_dataset_data_invalid_column(self, test_session):
        """Test that invalid enriched column raises ValidationError."""
        from src.services.dataset_service import initialize_dataset
        from src.services.enrichment_service import create_enriched_dataset
        
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config={"phone": {"type": "TEXT", "is_image": False}},
            image_columns=[],
        )
        
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched Test",
            enrichment_config={"phone": "phone_numbers"},
        )
        
        with pytest.raises(ValidationError):
            get_enriched_dataset_data_for_key(
                test_session,
                enriched.id,
                "nonexistent_column",  # Invalid column
                "+1234567890",
            )


class TestSearchPerformance:
    """Test search performance targets."""

    def test_search_performance_small_scale(self, test_session):
        """Test that search completes in reasonable time for small datasets."""
        # Create a few Knowledge Tables
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        
        for i in range(3):
            initialize_knowledge_table(
                session=test_session,
                name=f"Table {i}",
                data_type="phone_numbers",
                primary_key_column="phone",
                columns_config=columns_config,
                image_columns=[],
            )
        
        start_time = time.time()
        results = search_knowledge_base(
            test_session,
            "+1234567890",
            "phone_numbers",
        )
        elapsed_ms = (time.time() - start_time) * 1000
        
        # Should complete in reasonable time (<500ms for small scale)
        assert elapsed_ms < 500
        assert results["search_stats"]["search_time_ms"] < 500

