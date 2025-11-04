"""
Unit tests for column name handling with spaces and special characters.

Tests identifier quoting, sanitization, and SQL query construction
for columns with spaces, hyphens, and special characters.
"""
import pandas as pd
import pytest
from sqlalchemy import inspect, text

pytestmark = pytest.mark.unit

from src.config.settings import UNIQUE_ID_COLUMN_NAME
from src.database.models import DatasetConfig
from src.services.dataset_service import initialize_dataset
from src.services.enrichment_service import create_enriched_dataset, sync_enriched_dataset
from src.services.table_service import (
    add_column_to_table,
    create_index_on_column,
    get_table_row_count,
    update_enriched_column_values,
)
from src.services.dataframe_service import load_dataset_dataframe
from src.utils.errors import DatabaseError, ValidationError
from src.utils.validation import quote_identifier, sanitize_column_name


class TestColumnNameSanitization:
    """Test column name sanitization functions."""

    def test_sanitize_column_name_with_spaces(self):
        """Test sanitizing column names with spaces."""
        assert sanitize_column_name("admin contact email") == "admin_contact_email"
        assert sanitize_column_name("first name") == "first_name"
        assert sanitize_column_name("phone  number") == "phone_number"  # Multiple spaces

    def test_sanitize_column_name_with_hyphens(self):
        """Test sanitizing column names with hyphens."""
        assert sanitize_column_name("user-name") == "user_name"
        assert sanitize_column_name("email-address") == "email_address"

    def test_sanitize_column_name_with_special_chars(self):
        """Test sanitizing column names with special characters."""
        assert sanitize_column_name("column@name") == "columnname"
        assert sanitize_column_name("column#123") == "column123"
        assert sanitize_column_name("column.name") == "column_name"

    def test_sanitize_column_name_unicode(self):
        """Test sanitizing column names with unicode characters."""
        result = sanitize_column_name("café_email")
        assert "caf" in result.lower()  # Should handle unicode

    def test_sanitize_column_name_empty(self):
        """Test sanitizing empty column name."""
        result = sanitize_column_name("")
        assert result == "column"  # Default fallback

    def test_sanitize_column_name_preserves_valid(self):
        """Test that valid column names are preserved."""
        assert sanitize_column_name("valid_column_123") == "valid_column_123"
        assert sanitize_column_name("columnName") == "columnName"


class TestIdentifierQuoting:
    """Test identifier quoting functions."""

    def test_quote_identifier_with_spaces(self):
        """Test quoting identifiers with spaces."""
        assert quote_identifier("column name") == '"column name"'
        assert quote_identifier("admin contact email") == '"admin contact email"'

    def test_quote_identifier_with_quotes(self):
        """Test quoting identifiers that contain double quotes."""
        assert quote_identifier('column"name') == '"column""name"'
        assert quote_identifier('table""name') == '"table""""name"'

    def test_quote_identifier_valid_name(self):
        """Test quoting valid identifiers (still quotes for consistency)."""
        assert quote_identifier("valid_column") == '"valid_column"'
        assert quote_identifier("column123") == '"column123"'


class TestDatasetWithSpacesInColumnNames:
    """Test creating and working with datasets that have spaces in column names."""

    def test_create_dataset_with_spaces_in_column_names(self, test_session):
        """Test creating a dataset with column names containing spaces."""
        columns_config = {
            "admin contact email": {"type": "TEXT", "is_image": False},
            "phone number": {"type": "TEXT", "is_image": False},
            "user-name": {"type": "TEXT", "is_image": False},
        }
        
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset with Spaces",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        assert dataset is not None
        assert dataset.name == "Test Dataset with Spaces"
        
        # Verify table was created
        inspector = inspect(test_session.bind)
        assert dataset.table_name in inspector.get_table_names()
        
        # Verify columns exist (SQLAlchemy handles quoting automatically)
        columns = [col["name"] for col in inspector.get_columns(dataset.table_name)]
        assert "admin contact email" in columns
        assert "phone number" in columns
        assert "user-name" in columns

    def test_load_dataframe_with_spaces_in_column_names(self, test_session):
        """Test loading DataFrame from dataset with spaces in column names."""
        columns_config = {
            "admin contact email": {"type": "TEXT", "is_image": False},
            "phone number": {"type": "TEXT", "is_image": False},
        }
        
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        # Insert some test data
        test_data = pd.DataFrame({
            "admin contact email": ["admin@example.com", "admin2@example.com"],
            "phone number": ["123-456-7890", "098-765-4321"],
            UNIQUE_ID_COLUMN_NAME: ["uuid1", "uuid2"],
        })
        
        from src.services.table_service import insert_dataframe_to_table
        insert_dataframe_to_table(test_session, dataset.table_name, test_data)
        
        # Load DataFrame
        df = load_dataset_dataframe(test_session, dataset.id)
        
        assert len(df) == 2
        assert "admin contact email" in df.columns
        assert "phone number" in df.columns
        assert df["admin contact email"].iloc[0] == "admin@example.com"


class TestEnrichedDatasetWithSpacesInColumnNames:
    """Test creating enriched datasets from datasets with spaces in column names."""

    def test_create_enriched_dataset_with_spaces_in_source_columns(self, test_session):
        """Test creating enriched dataset from dataset with spaces in column names."""
        # Create source dataset with spaces in column names
        columns_config = {
            "admin contact email": {"type": "TEXT", "is_image": False},
            "phone number": {"type": "TEXT", "is_image": False},
        }
        
        source_dataset = initialize_dataset(
            session=test_session,
            name="Source with Spaces",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        # Add some test data
        test_data = pd.DataFrame({
            "admin contact email": ["admin@example.com", "test@example.com"],
            "phone number": ["123-456-7890", "098-765-4321"],
            UNIQUE_ID_COLUMN_NAME: ["uuid1", "uuid2"],
        })
        
        from src.services.table_service import insert_dataframe_to_table
        insert_dataframe_to_table(test_session, source_dataset.table_name, test_data)
        
        # Create enriched dataset
        enrichment_config = {
            "admin contact email": "emails",
            "phone number": "phone_numbers",
        }
        
        enriched_dataset = create_enriched_dataset(
            session=test_session,
            source_dataset_id=source_dataset.id,
            name="Enriched Dataset",
            enrichment_config=enrichment_config,
        )
        
        assert enriched_dataset is not None
        assert len(enriched_dataset.columns_added) == 2
        
        # Verify enriched column names are sanitized
        inspector = inspect(test_session.bind)
        enriched_columns = [col["name"] for col in inspector.get_columns(enriched_dataset.enriched_table_name)]
        
        # Enriched columns should be sanitized (no spaces)
        enriched_col1 = "admin_contact_email_enriched_emails"
        enriched_col2 = "phone_number_enriched_phone_numbers"
        
        assert enriched_col1 in enriched_columns
        assert enriched_col2 in enriched_columns

    def test_enriched_column_names_sanitized(self, test_session):
        """Test that enriched column names are properly sanitized."""
        columns_config = {
            "column with spaces": {"type": "TEXT", "is_image": False},
            "column-with-hyphens": {"type": "TEXT", "is_image": False},
        }
        
        source_dataset = initialize_dataset(
            session=test_session,
            name="Source",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        enrichment_config = {
            "column with spaces": "emails",
            "column-with-hyphens": "phone_numbers",
        }
        
        enriched_dataset = create_enriched_dataset(
            session=test_session,
            source_dataset_id=source_dataset.id,
            name="Enriched",
            enrichment_config=enrichment_config,
        )
        
        # Check that enriched column names are sanitized
        assert "column_with_spaces_enriched_emails" in enriched_dataset.columns_added
        assert "column_with_hyphens_enriched_phone_numbers" in enriched_dataset.columns_added


class TestTableServiceWithSpaces:
    """Test table service functions with spaces in column names."""

    def test_add_column_with_spaces(self, test_session):
        """Test adding a column with spaces in the name."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        # Add column with spaces
        add_column_to_table(
            session=test_session,
            table_name=dataset.table_name,
            column_name="new column with spaces",
            column_type="TEXT",
        )
        
        # Verify column was added
        inspector = inspect(test_session.bind)
        columns = [col["name"] for col in inspector.get_columns(dataset.table_name)]
        assert "new column with spaces" in columns

    def test_create_index_on_column_with_spaces(self, test_session):
        """Test creating an index on a column with spaces."""
        columns_config = {"column with spaces": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        # Create index on column with spaces
        create_index_on_column(
            session=test_session,
            table_name=dataset.table_name,
            column_name="column with spaces",
        )
        
        # Verify index was created (check via sqlite_master)
        result = test_session.execute(
            text(f"SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='{dataset.table_name}'")
        )
        indexes = [row[0] for row in result.fetchall()]
        assert len(indexes) > 0

    def test_update_enriched_column_values_with_spaces(self, test_session):
        """Test updating enriched column values with spaces in column names."""
        columns_config = {"source column": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        # Add enriched column with spaces
        add_column_to_table(
            session=test_session,
            table_name=dataset.table_name,
            column_name="enriched column with spaces",
            column_type="TEXT",
        )
        
        # Insert initial row
        test_data = pd.DataFrame({
            "source column": ["value1"],
            UNIQUE_ID_COLUMN_NAME: ["uuid1"],
        })
        from src.services.table_service import insert_dataframe_to_table
        insert_dataframe_to_table(test_session, dataset.table_name, test_data)
        
        # Update enriched column
        update_df = pd.DataFrame({
            UNIQUE_ID_COLUMN_NAME: ["uuid1"],
            "enriched column with spaces": ["enriched_value1"],
        })
        
        updated_count = update_enriched_column_values(
            session=test_session,
            table_name=dataset.table_name,
            column_name="enriched column with spaces",
            df=update_df,
        )
        
        assert updated_count == 1
        
        # Verify update
        df = load_dataset_dataframe(test_session, dataset.id)
        assert df["enriched column with spaces"].iloc[0] == "enriched_value1"


class TestSyncOperationsWithSpaces:
    """Test sync operations with columns containing spaces."""

    def test_sync_enriched_dataset_with_spaces(self, test_session):
        """Test syncing enriched dataset when source has spaces in column names."""
        # Create source dataset
        columns_config = {
            "admin contact email": {"type": "TEXT", "is_image": False},
        }
        
        source_dataset = initialize_dataset(
            session=test_session,
            name="Source",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        # Add initial data
        initial_data = pd.DataFrame({
            "admin contact email": ["admin@example.com"],
            UNIQUE_ID_COLUMN_NAME: ["uuid1"],
        })
        from src.services.table_service import insert_dataframe_to_table
        insert_dataframe_to_table(test_session, source_dataset.table_name, initial_data)
        
        # Create enriched dataset
        enriched_dataset = create_enriched_dataset(
            session=test_session,
            source_dataset_id=source_dataset.id,
            name="Enriched",
            enrichment_config={"admin contact email": "emails"},
        )
        
        # Add new row to source
        new_data = pd.DataFrame({
            "admin contact email": ["new@example.com"],
            UNIQUE_ID_COLUMN_NAME: ["uuid2"],
        })
        insert_dataframe_to_table(test_session, source_dataset.table_name, new_data)
        
        # Sync enriched dataset
        rows_synced = sync_enriched_dataset(test_session, enriched_dataset.id)
        
        assert rows_synced == 1
        
        # Verify enriched data was added
        from src.services.dataframe_service import load_enriched_dataset_dataframe
        df = load_enriched_dataset_dataframe(test_session, enriched_dataset.id)
        assert len(df) == 2


class TestEdgeCases:
    """Test edge cases for column name handling."""

    def test_very_long_column_name(self, test_session):
        """Test handling of very long column names."""
        long_name = "a" * 100  # 100 character column name
        columns_config = {long_name: {"type": "TEXT", "is_image": False}}
        
        dataset = initialize_dataset(
            session=test_session,
            name="Test",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        assert dataset is not None
        inspector = inspect(test_session.bind)
        columns = [col["name"] for col in inspector.get_columns(dataset.table_name)]
        assert long_name in columns

    def test_column_name_with_unicode(self, test_session):
        """Test handling column names with unicode characters."""
        columns_config = {
            "café_email": {"type": "TEXT", "is_image": False},
            "用户名称": {"type": "TEXT", "is_image": False},  # Chinese characters
        }
        
        dataset = initialize_dataset(
            session=test_session,
            name="Test Unicode",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        assert dataset is not None
        inspector = inspect(test_session.bind)
        columns = [col["name"] for col in inspector.get_columns(dataset.table_name)]
        assert "café_email" in columns or "caf_email" in columns  # May be sanitized
        # Unicode column may be sanitized or preserved depending on SQLAlchemy version

    def test_multiple_spaces_in_column_name(self, test_session):
        """Test handling column names with multiple consecutive spaces."""
        columns_config = {
            "column   with   spaces": {"type": "TEXT", "is_image": False},
        }
        
        dataset = initialize_dataset(
            session=test_session,
            name="Test",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        assert dataset is not None
        inspector = inspect(test_session.bind)
        columns = [col["name"] for col in inspector.get_columns(dataset.table_name)]
        # Column name should be preserved (SQLAlchemy handles it)
        assert any("column" in col.lower() and "spaces" in col.lower() for col in columns)

