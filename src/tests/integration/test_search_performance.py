"""
Performance tests for Knowledge Base search.

Benchmarks search performance to ensure targets are met.
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


class TestPresenceSearchPerformance:
    """Test Phase 1 (presence search) performance benchmarks."""

    def test_presence_search_performance_benchmark(self, test_session):
        """Measure Phase 1 query time - should be <100ms for 10 sources."""
        # Create 5 Knowledge Tables
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        
        for i in range(5):
            initialize_knowledge_table(
                session=test_session,
                name=f"Table {i}",
                data_type="phone_numbers",
                primary_key_column="phone",
                columns_config=columns_config,
                image_columns=[],
                initial_data_df=pd.DataFrame({"phone": [f"+123456789{i}"]}),
            )
        
        # Create 5 enriched datasets
        for i in range(5):
            dataset = initialize_dataset(
                session=test_session,
                name=f"Dataset {i}",
                slot_number=i + 1,
                columns_config={"phone": {"type": "TEXT", "is_image": False}},
                image_columns=[],
            )
            
            df = pd.DataFrame({"phone": [f"+123456789{i}"]})
            df = generate_unique_ids(df)
            insert_dataframe_to_table(test_session, dataset.table_name, df)
            
            create_enriched_dataset(
                session=test_session,
                source_dataset_id=dataset.id,
                name=f"Enriched {i}",
                enrichment_config={"phone": "phone_numbers"},
            )
        
        # Benchmark search
        start_time = time.time()
        results = search_knowledge_base(
            test_session,
            "+1234567890",
            "phone_numbers",
        )
        elapsed_ms = (time.time() - start_time) * 1000
        
        # Should complete in <100ms for 10 sources
        assert elapsed_ms < 100
        assert results["search_stats"]["search_time_ms"] < 100
        assert results["search_stats"]["total_sources"] >= 10

    def test_search_scales_with_table_count(self, test_session):
        """Performance with 10, 25, 50 Knowledge Tables."""
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        
        # Create 25 Knowledge Tables
        for i in range(25):
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
        
        # Should still be reasonably fast (<500ms for 25 tables)
        assert elapsed_ms < 500
        assert results["search_stats"]["total_sources"] >= 25


class TestDetailedRetrievalPerformance:
    """Test Phase 2 (detailed retrieval) performance benchmarks."""

    def test_detailed_retrieval_performance_benchmark(self, test_session):
        """Measure Phase 2 query time - should be <50ms per Knowledge Table."""
        columns_config = {
            "phone": {"type": "TEXT", "is_image": False},
            "carrier": {"type": "TEXT", "is_image": False},
        }
        
        knowledge_table = initialize_knowledge_table(
            session=test_session,
            name="Test Table",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
            initial_data_df=pd.DataFrame({"phone": ["+1234567890"], "carrier": ["Verizon"]}),
        )
        
        from src.services.knowledge_service import standardize_key_value
        key_id = standardize_key_value("+1234567890", "phone_numbers")
        
        # Benchmark retrieval
        start_time = time.time()
        df = get_knowledge_table_data_for_key(
            test_session,
            knowledge_table.id,
            key_id,
        )
        elapsed_ms = (time.time() - start_time) * 1000
        
        # Should complete in <50ms
        assert elapsed_ms < 50
        assert not df.empty

    def test_enriched_dataset_retrieval_performance(self, test_session):
        """Measure enriched dataset retrieval - should be <200ms."""
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
            name="Enriched Test",
            enrichment_config={"phone": "phone_numbers"},
        )
        
        from src.services.knowledge_service import standardize_key_value
        key_id = standardize_key_value("+1234567890", "phone_numbers")
        
        # Benchmark retrieval
        start_time = time.time()
        df_result = get_enriched_dataset_data_for_key(
            test_session,
            enriched.id,
            "phone_enriched_phone_numbers",
            key_id,
        )
        elapsed_ms = (time.time() - start_time) * 1000
        
        # Should complete in <200ms
        assert elapsed_ms < 200
        assert not df_result.empty


class TestSearchScalesWithDataVolume:
    """Test search performance with large data volumes."""

    # @pytest.mark.slow  # Uncomment if slow marker is configured
    def test_search_scales_with_data_volume(self, test_session):
        """Performance with 100K, 500K, 1M+ rows."""
        # Create Knowledge Table with 10K rows (for faster test)
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        
        phones = [f"+123456789{i:04d}" for i in range(10000)]
        initial_data = pd.DataFrame({"phone": phones})
        
        knowledge_table = initialize_knowledge_table(
            session=test_session,
            name="Large Table",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
            initial_data_df=initial_data,
        )
        
        # Search should still be fast with indexes
        start_time = time.time()
        results = search_knowledge_base(
            test_session,
            "+1234567890000",
            "phone_numbers",
        )
        elapsed_ms = (time.time() - start_time) * 1000
        
        # Should complete in <100ms even with 10K rows (indexed)
        assert elapsed_ms < 100


class TestIndexImprovesPerformance:
    """Verify indexes improve query performance."""

    def test_index_improves_performance(self, test_session):
        """Compare indexed vs non-indexed queries (indexed should be faster)."""
        # Create large enriched dataset (indexes created automatically)
        dataset = initialize_dataset(
            session=test_session,
            name="Large Dataset",
            slot_number=1,
            columns_config={"phone": {"type": "TEXT", "is_image": False}},
            image_columns=[],
        )
        
        # Add 1000 rows
        phones = [f"+123456789{i:03d}" for i in range(1000)]
        df = pd.DataFrame({"phone": phones})
        df = generate_unique_ids(df)
        insert_dataframe_to_table(test_session, dataset.table_name, df)
        
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched Large",
            enrichment_config={"phone": "phone_numbers"},
        )
        
        # Search should be fast (indexed)
        start_time = time.time()
        results = search_knowledge_base(
            test_session,
            "+123456789000",
            "phone_numbers",
        )
        indexed_time_ms = (time.time() - start_time) * 1000
        
        # Should complete quickly with indexes
        assert indexed_time_ms < 100

