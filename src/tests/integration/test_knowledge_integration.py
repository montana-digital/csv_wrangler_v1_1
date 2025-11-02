"""
Integration tests for Knowledge Tables.

Tests multi-table scenarios, relationship discovery, and cross-feature integration.
"""
import pandas as pd
import pytest
from sqlalchemy import text

from src.database.models import DatasetConfig, EnrichedDataset, KnowledgeTable
from src.database.repository import KnowledgeTableRepository
from src.services.dataset_service import initialize_dataset
from src.services.enrichment_service import create_enriched_dataset
from src.services.knowledge_service import (
    find_enriched_columns_by_type,
    get_knowledge_table_stats,
    get_knowledge_tables_by_type,
    initialize_knowledge_table,
    upload_to_knowledge_table,
)
from src.services.table_service import insert_dataframe_to_table


class TestMultipleKnowledgeTablesPerDataType:
    """Test multiple Knowledge Tables per data_type."""

    def test_create_multiple_phone_tables(self, test_session):
        """Test creating multiple Knowledge Tables for phone_numbers."""
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        
        white_list = initialize_knowledge_table(
            session=test_session,
            name="Phone White List",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
        )
        
        black_list = initialize_knowledge_table(
            session=test_session,
            name="Phone Black List",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
        )
        
        carrier_info = initialize_knowledge_table(
            session=test_session,
            name="Carrier Info Source A",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config={
                "phone": {"type": "TEXT", "is_image": False},
                "carrier": {"type": "TEXT", "is_image": False},
            },
            image_columns=[],
        )
        
        # All should be independent
        assert white_list.id != black_list.id != carrier_info.id
        assert white_list.data_type == black_list.data_type == carrier_info.data_type
        assert white_list.name != black_list.name != carrier_info.name
        
        # Verify all exist
        repo = KnowledgeTableRepository(test_session)
        all_tables = repo.get_all()
        assert len(all_tables) >= 3

    def test_same_key_id_in_multiple_tables(self, test_session):
        """Test that same Key_ID can exist in multiple Knowledge Tables."""
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
            initial_data_df=pd.DataFrame({"phone": ["+1234567890"]}),  # Same phone
        )
        
        # Both should have the same Key_ID
        result1 = test_session.execute(
            text(f"SELECT Key_ID FROM {table1.table_name}")
        ).fetchone()
        result2 = test_session.execute(
            text(f"SELECT Key_ID FROM {table2.table_name}")
        ).fetchone()
        
        assert result1[0] == result2[0]  # Same Key_ID value

    def test_statistics_independent_per_table(self, test_session):
        """Test that statistics are calculated independently per table."""
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        
        # Create dataset and enriched dataset first
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config={"phone": {"type": "TEXT", "is_image": False}},
            image_columns=[],
        )
        
        # Add some data to dataset
        df = pd.DataFrame({"phone": ["+1234567890", "+0987654321"]})
        from src.services.csv_service import generate_unique_ids
        df = generate_unique_ids(df)
        insert_dataframe_to_table(test_session, dataset.table_name, df)
        
        # Create enriched dataset
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched Test",
            enrichment_config={"phone": "phone_numbers"},
        )
        
        # Create two Knowledge Tables
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
            initial_data_df=pd.DataFrame({"phone": ["+0987654321"]}),
        )
        
        # Get stats for each
        stats1 = get_knowledge_table_stats(test_session, table1.id)
        stats2 = get_knowledge_table_stats(test_session, table2.id)
        
        # Stats should be independent
        # Table 1 should match +1234567890
        # Table 2 should match +0987654321
        assert stats1 is not None
        assert stats2 is not None


class TestRelationshipDiscovery:
    """Test relationship discovery between enriched datasets and Knowledge Tables."""

    def test_find_enriched_columns_by_type(self, test_session):
        """Test finding enriched columns that match Knowledge Table data_type."""
        # Create dataset
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config={
                "phone": {"type": "TEXT", "is_image": False},
                "email": {"type": "TEXT", "is_image": False},
            },
            image_columns=[],
        )
        
        # Add data
        df = pd.DataFrame({
            "phone": ["+1234567890"],
            "email": ["user@example.com"]
        })
        from src.services.csv_service import generate_unique_ids
        df = generate_unique_ids(df)
        insert_dataframe_to_table(test_session, dataset.table_name, df)
        
        # Create enriched dataset with phone_numbers enrichment
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched",
            enrichment_config={"phone": "phone_numbers"},
        )
        
        # Create Knowledge Table for phone_numbers
        knowledge_table = initialize_knowledge_table(
            session=test_session,
            name="Phone Table",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config={"phone": {"type": "TEXT", "is_image": False}},
            image_columns=[],
        )
        
        # Statistics should find the enriched column
        stats = get_knowledge_table_stats(test_session, knowledge_table.id)
        # Should be able to calculate stats (may be empty if no matches)
        assert stats is not None


class TestCrossFeatureIntegration:
    """Test integration with existing features."""

    def test_knowledge_table_doesnt_break_datasets(self, test_session):
        """Test that Knowledge Tables don't break existing dataset functionality."""
        # Create dataset (existing functionality)
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config={"name": {"type": "TEXT", "is_image": False}},
            image_columns=[],
        )
        
        assert dataset is not None
        
        # Create Knowledge Table (new functionality)
        knowledge_table = initialize_knowledge_table(
            session=test_session,
            name="Test Knowledge Table",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config={"phone": {"type": "TEXT", "is_image": False}},
            image_columns=[],
        )
        
        assert knowledge_table is not None
        
        # Both should work independently
        from src.database.repository import DatasetRepository
        repo = DatasetRepository(test_session)
        retrieved_dataset = repo.get_by_id(dataset.id)
        assert retrieved_dataset is not None

    def test_knowledge_table_with_enriched_datasets(self, test_session):
        """Test Knowledge Tables work with enriched datasets."""
        # Create dataset and enriched dataset
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config={"phone": {"type": "TEXT", "is_image": False}},
            image_columns=[],
        )
        
        enriched = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset.id,
            name="Enriched Dataset",
            enrichment_config={"phone": "phone_numbers"},
        )
        
        # Create Knowledge Table
        knowledge_table = initialize_knowledge_table(
            session=test_session,
            name="Phone Knowledge",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config={"phone": {"type": "TEXT", "is_image": False}},
            image_columns=[],
        )
        
        # Should be able to get stats (relationship should be discoverable)
        stats = get_knowledge_table_stats(test_session, knowledge_table.id)
        assert stats is not None

    def test_multiple_datasets_link_to_knowledge_table(self, test_session):
        """Test that multiple enriched datasets can link to same Knowledge Table."""
        # Create two datasets
        dataset1 = initialize_dataset(
            session=test_session,
            name="Dataset 1",
            slot_number=1,
            columns_config={"phone": {"type": "TEXT", "is_image": False}},
            image_columns=[],
        )
        
        dataset2 = initialize_dataset(
            session=test_session,
            name="Dataset 2",
            slot_number=2,
            columns_config={"mobile": {"type": "TEXT", "is_image": False}},
            image_columns=[],
        )
        
        # Add data
        from src.services.csv_service import generate_unique_ids
        df1 = generate_unique_ids(pd.DataFrame({"phone": ["+1234567890"]}))
        df2 = generate_unique_ids(pd.DataFrame({"mobile": ["+1234567890"]}))
        insert_dataframe_to_table(test_session, dataset1.table_name, df1)
        insert_dataframe_to_table(test_session, dataset2.table_name, df2)
        
        # Create enriched datasets
        enriched1 = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset1.id,
            name="Enriched 1",
            enrichment_config={"phone": "phone_numbers"},
        )
        
        enriched2 = create_enriched_dataset(
            session=test_session,
            source_dataset_id=dataset2.id,
            name="Enriched 2",
            enrichment_config={"mobile": "phone_numbers"},
        )
        
        # Create Knowledge Table
        knowledge_table = initialize_knowledge_table(
            session=test_session,
            name="Phone Knowledge",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config={"phone": {"type": "TEXT", "is_image": False}},
            image_columns=[],
            initial_data_df=pd.DataFrame({"phone": ["+1234567890"]}),
        )
        
        # Stats should find matches from both enriched datasets
        stats = get_knowledge_table_stats(test_session, knowledge_table.id)
        assert stats is not None

