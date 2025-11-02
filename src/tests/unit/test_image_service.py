"""
Unit tests for Image service.

Tests image table discovery and Knowledge Table association functions.
"""
import pandas as pd
import pytest
from sqlalchemy import text

from src.database.models import DatasetConfig, EnrichedDataset, KnowledgeTable
from src.services.image_service import (
    extract_enriched_columns_from_row,
    get_knowledge_associations_for_row,
    get_tables_with_image_columns,
)
from src.utils.errors import ValidationError


@pytest.fixture
def sample_dataset_config(test_session):
    """Create a sample dataset config for testing."""
    dataset = DatasetConfig(
        name="Test Dataset",
        slot_number=1,
        table_name="test_dataset_1",
        columns_config={"name": {"type": "TEXT", "is_image": False}},
        image_columns=[],
    )
    test_session.add(dataset)
    test_session.commit()
    test_session.refresh(dataset)
    
    # Create actual table
    test_session.execute(text(
        f"CREATE TABLE IF NOT EXISTS {dataset.table_name} ("
        "uuid_value TEXT PRIMARY KEY, "
        "name TEXT"
        ")"
    ))
    test_session.commit()
    
    return dataset


@pytest.fixture
def sample_enriched_dataset(test_session, sample_dataset_config):
    """Create a sample enriched dataset for testing."""
    enriched = EnrichedDataset(
        name="Test Enriched",
        source_dataset_id=sample_dataset_config.id,
        enriched_table_name="test_enriched_1",
        source_table_name=sample_dataset_config.table_name,
        enrichment_config={"phone": "phone_numbers"},
        columns_added=["phone_enriched_phone_numbers"],
    )
    test_session.add(enriched)
    test_session.commit()
    test_session.refresh(enriched)
    
    # Create actual table
    test_session.execute(text(
        f"CREATE TABLE IF NOT EXISTS {enriched.enriched_table_name} ("
        "uuid_value TEXT PRIMARY KEY, "
        "phone_enriched_phone_numbers TEXT"
        ")"
    ))
    test_session.commit()
    
    return enriched


@pytest.fixture
def sample_knowledge_table(test_session):
    """Create a sample knowledge table for testing."""
    from src.services.knowledge_service import initialize_knowledge_table
    
    columns_config = {
        "phone": {"type": "TEXT", "is_image": False},
        "carrier": {"type": "TEXT", "is_image": False},
    }
    
    initial_data = pd.DataFrame({
        "phone": ["+1234567890"],
        "carrier": ["Verizon"],
    })
    
    kt = initialize_knowledge_table(
        session=test_session,
        name="Test Knowledge Table",
        data_type="phone_numbers",
        primary_key_column="phone",
        columns_config=columns_config,
        image_columns=[],
        initial_data_df=initial_data,
    )
    
    return kt


class TestGetTablesWithImageColumns:
    """Test finding tables with image columns."""

    def test_get_datasets_with_images(self, test_session, sample_dataset_config):
        """Test finding datasets with image columns."""
        # Create dataset with image columns
        dataset = sample_dataset_config
        dataset.image_columns = ["image1", "image2"]
        test_session.add(dataset)
        test_session.commit()

        # Get tables
        tables = get_tables_with_image_columns(test_session)

        # Should find the dataset
        assert len(tables) >= 1
        dataset_tables = [t for t in tables if t["type"] == "dataset" and t["id"] == dataset.id]
        assert len(dataset_tables) == 1
        assert dataset_tables[0]["image_columns"] == ["image1", "image2"]
        assert dataset_tables[0]["table_name"] == dataset.table_name

    def test_get_enriched_datasets_with_images(
        self, test_session, sample_dataset_config, sample_enriched_dataset
    ):
        """Test finding enriched datasets with images from source dataset."""
        # Set image columns on source dataset
        source_dataset = sample_dataset_config
        source_dataset.image_columns = ["photo", "thumbnail"]
        test_session.add(source_dataset)
        
        # Enriched dataset already has source_dataset_id set from fixture
        enriched = sample_enriched_dataset
        test_session.commit()

        # Get tables
        tables = get_tables_with_image_columns(test_session)

        # Should find enriched dataset
        enriched_tables = [
            t for t in tables
            if t["type"] == "enriched_dataset" and t["id"] == enriched.id
        ]
        assert len(enriched_tables) == 1
        assert enriched_tables[0]["image_columns"] == ["photo", "thumbnail"]
        assert enriched_tables[0]["table_name"] == enriched.enriched_table_name

    def test_exclude_tables_without_images(self, test_session, sample_dataset_config):
        """Test that tables without image columns are excluded."""
        # Create dataset without image columns
        dataset = sample_dataset_config
        dataset.image_columns = []
        test_session.add(dataset)
        test_session.commit()

        # Get tables
        tables = get_tables_with_image_columns(test_session)

        # Should not find this dataset
        dataset_tables = [t for t in tables if t["id"] == dataset.id]
        assert len(dataset_tables) == 0

    def test_empty_result_when_no_images(self, test_session):
        """Test empty result when no tables have images."""
        tables = get_tables_with_image_columns(test_session)
        assert isinstance(tables, list)
        # May have other tables, but none should have images
        for table in tables:
            assert len(table["image_columns"]) > 0


class TestExtractEnrichedColumnsFromRow:
    """Test extracting enriched columns from row data."""

    def test_extract_phone_columns(self):
        """Test extracting phone number enriched columns."""
        row_data = {
            "name": "John",
            "phone_enriched_phone_numbers": "+1234567890",
            "mobile_enriched_phone_numbers": "+0987654321",
            "email": "test@example.com",
        }

        result = extract_enriched_columns_from_row(row_data)

        assert len(result["phone_numbers"]) == 2
        assert "+1234567890" in result["phone_numbers"]
        assert "+0987654321" in result["phone_numbers"]
        assert len(result["web_domains"]) == 0

    def test_extract_domain_columns(self):
        """Test extracting web domain enriched columns."""
        row_data = {
            "name": "Company",
            "website_enriched_web_domains": "https://example.com",
            "domain_enriched_web_domains": "https://test.org",
            "phone": "1234567890",
        }

        result = extract_enriched_columns_from_row(row_data)

        assert len(result["web_domains"]) == 2
        assert "https://example.com" in result["web_domains"]
        assert "https://test.org" in result["web_domains"]
        assert len(result["phone_numbers"]) == 0

    def test_extract_both_phone_and_domain(self):
        """Test extracting both phone and domain columns."""
        row_data = {
            "name": "Contact",
            "phone_enriched_phone_numbers": "+1234567890",
            "website_enriched_web_domains": "https://example.com",
            "other_column": "value",
        }

        result = extract_enriched_columns_from_row(row_data)

        assert len(result["phone_numbers"]) == 1
        assert len(result["web_domains"]) == 1
        assert "+1234567890" in result["phone_numbers"]
        assert "https://example.com" in result["web_domains"]

    def test_skip_null_values(self):
        """Test that null/empty values are skipped."""
        row_data = {
            "phone_enriched_phone_numbers": None,
            "website_enriched_web_domains": "",
            "email_enriched_phone_numbers": pd.NA,
        }

        result = extract_enriched_columns_from_row(row_data)

        assert len(result["phone_numbers"]) == 0
        assert len(result["web_domains"]) == 0

    def test_deduplicate_values(self):
        """Test that duplicate values are removed."""
        row_data = {
            "phone1_enriched_phone_numbers": "+1234567890",
            "phone2_enriched_phone_numbers": "+1234567890",  # Duplicate
            "website1_enriched_web_domains": "https://example.com",
            "website2_enriched_web_domains": "https://example.com",  # Duplicate
        }

        result = extract_enriched_columns_from_row(row_data)

        assert len(result["phone_numbers"]) == 1
        assert len(result["web_domains"]) == 1
        assert result["phone_numbers"][0] == "+1234567890"
        assert result["web_domains"][0] == "https://example.com"

    def test_empty_row_returns_empty(self):
        """Test that empty row returns empty results."""
        row_data = {
            "name": "Test",
            "email": "test@example.com",
        }

        result = extract_enriched_columns_from_row(row_data)

        assert len(result["phone_numbers"]) == 0
        assert len(result["web_domains"]) == 0


class TestGetKnowledgeAssociationsForRow:
    """Test getting Knowledge Table associations for a row."""

    def test_get_associations_for_phone_number(
        self, test_session, sample_dataset_config, sample_knowledge_table
    ):
        """Test getting associations for phone number in row."""
        # Knowledge Table already has data from fixture
        kt = sample_knowledge_table

        # Row data with phone number
        row_data = {
            "name": "John",
            "phone_enriched_phone_numbers": "+1234567890",
        }

        # Get associations
        associations = get_knowledge_associations_for_row(
            session=test_session,
            row_data=row_data,
            table_type="dataset",
            table_id=sample_dataset_config.id,
        )

        # Should find phone associations
        assert "phone_numbers" in associations
        assert len(associations["phone_numbers"]) == 1
        assert associations["phone_numbers"][0]["value"] == "+1234567890"
        assert "search_results" in associations["phone_numbers"][0]

    def test_get_associations_for_domain(
        self, test_session, sample_dataset_config
    ):
        """Test getting associations for web domain in row."""
        # Row data with domain
        row_data = {
            "name": "Company",
            "website_enriched_web_domains": "https://example.com",
        }

        # Get associations
        associations = get_knowledge_associations_for_row(
            session=test_session,
            row_data=row_data,
            table_type="dataset",
            table_id=sample_dataset_config.id,
        )

        # Should find domain associations (may be empty if no Knowledge Tables)
        assert "web_domains" in associations
        assert len(associations["web_domains"]) == 1
        assert associations["web_domains"][0]["value"] == "https://example.com"
        assert "search_results" in associations["web_domains"][0]

    def test_get_associations_multiple_values(self, test_session, sample_dataset_config):
        """Test getting associations for multiple phone/domain values."""
        row_data = {
            "name": "Contact",
            "phone1_enriched_phone_numbers": "+1111111111",
            "phone2_enriched_phone_numbers": "+2222222222",
            "website1_enriched_web_domains": "https://example.com",
            "website2_enriched_web_domains": "https://test.org",
        }

        # Get associations
        associations = get_knowledge_associations_for_row(
            session=test_session,
            row_data=row_data,
            table_type="dataset",
            table_id=sample_dataset_config.id,
        )

        # Should find all values
        assert len(associations["phone_numbers"]) == 2
        assert len(associations["web_domains"]) == 2

    def test_no_associations_for_empty_row(self, test_session, sample_dataset_config):
        """Test that empty row returns empty associations."""
        row_data = {
            "name": "Test",
            "email": "test@example.com",
        }

        # Get associations
        associations = get_knowledge_associations_for_row(
            session=test_session,
            row_data=row_data,
            table_type="dataset",
            table_id=sample_dataset_config.id,
        )

        # Should have empty lists
        assert len(associations["phone_numbers"]) == 0
        assert len(associations["web_domains"]) == 0

    def test_handles_search_errors_gracefully(self, test_session, sample_dataset_config):
        """Test that search errors don't crash the function."""
        # Row with invalid phone format
        row_data = {
            "phone_enriched_phone_numbers": "invalid-phone",
        }

        # Should not raise exception
        associations = get_knowledge_associations_for_row(
            session=test_session,
            row_data=row_data,
            table_type="dataset",
            table_id=sample_dataset_config.id,
        )

        # Should still return structure with empty results
        assert "phone_numbers" in associations
        assert len(associations["phone_numbers"]) == 1
        assert "search_results" in associations["phone_numbers"][0]

