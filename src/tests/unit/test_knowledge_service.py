"""
Unit tests for Knowledge service.

Tests Knowledge Table operations following TDD principles.
"""
import pandas as pd
import pytest
from sqlalchemy import text

from src.database.models import KnowledgeTable
from src.services.knowledge_service import (
    delete_knowledge_table,
    generate_key_ids_for_dataframe,
    get_all_knowledge_tables,
    get_knowledge_table_stats,
    get_knowledge_tables_by_type,
    initialize_knowledge_table,
    standardize_key_value,
    upload_to_knowledge_table,
)
from src.utils.errors import DatabaseError, ValidationError


class TestStandardizeKeyValue:
    """Test Key_ID standardization functions."""

    def test_standardize_phone_number(self):
        """Test phone number standardization."""
        # Valid phone numbers
        assert standardize_key_value("+1234567890", "phone_numbers") == "+1234567890"
        assert standardize_key_value("(123) 456-7890", "phone_numbers") is not None
        assert standardize_key_value("123-456-7890", "phone_numbers") is not None
        
        # Invalid phone numbers
        assert standardize_key_value("123", "phone_numbers") is None  # Too short
        assert standardize_key_value("", "phone_numbers") is None
        assert standardize_key_value(None, "phone_numbers") is None

    def test_standardize_email(self):
        """Test email standardization."""
        # Valid emails
        assert standardize_key_value("user@example.com", "emails") == "user@example.com"
        assert standardize_key_value("User@Example.COM", "emails") == "user@example.com"  # Lowercase
        
        # Invalid emails
        assert standardize_key_value("invalid", "emails") is None
        assert standardize_key_value("@example.com", "emails") is None
        assert standardize_key_value("", "emails") is None

    def test_standardize_web_domain(self):
        """Test web domain standardization."""
        # Valid domains/URLs
        result = standardize_key_value("https://example.com", "web_domains")
        assert result is not None
        
        result = standardize_key_value("example.com", "web_domains")
        assert result is not None
        
        # Invalid
        assert standardize_key_value("", "web_domains") is None

    def test_standardize_invalid_data_type(self):
        """Test that invalid data_type returns None."""
        # Invalid data_type logs warning and returns None (doesn't raise)
        result = standardize_key_value("test", "invalid_type")
        assert result is None


class TestGenerateKeyIdsForDataframe:
    """Test Key_ID generation for DataFrames."""

    def test_generate_key_ids_phone_numbers(self):
        """Test Key_ID generation for phone numbers."""
        df = pd.DataFrame({
            "phone": ["+1234567890", "(123) 456-7890", "invalid", None]
        })
        
        result_df, skipped = generate_key_ids_for_dataframe(df, "phone", "phone_numbers")
        
        assert "Key_ID" in result_df.columns
        assert result_df.loc[0, "Key_ID"] is not None
        assert result_df.loc[1, "Key_ID"] is not None
        assert pd.isna(result_df.loc[2, "Key_ID"])  # Invalid
        assert pd.isna(result_df.loc[3, "Key_ID"])  # None
        assert len(skipped) >= 2  # At least invalid and None

    def test_generate_key_ids_emails(self):
        """Test Key_ID generation for emails."""
        df = pd.DataFrame({
            "email": ["user@example.com", "User@Example.COM", "invalid", None]
        })
        
        result_df, skipped = generate_key_ids_for_dataframe(df, "email", "emails")
        
        assert "Key_ID" in result_df.columns
        assert result_df.loc[0, "Key_ID"] == "user@example.com"
        assert result_df.loc[1, "Key_ID"] == "user@example.com"  # Lowercase
        assert len(skipped) >= 2

    def test_generate_key_ids_missing_column(self):
        """Test that missing column raises error."""
        df = pd.DataFrame({"name": ["test"]})
        
        with pytest.raises(ValidationError):
            generate_key_ids_for_dataframe(df, "missing_column", "phone_numbers")


class TestInitializeKnowledgeTable:
    """Test Knowledge Table initialization."""

    def test_initialize_knowledge_table_creates_table(self, test_session):
        """Test that initialization creates table and processes data."""
        columns_config = {
            "phone": {"type": "TEXT", "is_image": False},
            "carrier": {"type": "TEXT", "is_image": False},
        }
        
        initial_data = pd.DataFrame({
            "phone": ["+1234567890", "+0987654321"],
            "carrier": ["Verizon", "AT&T"]
        })
        
        knowledge_table = initialize_knowledge_table(
            session=test_session,
            name="Phone Carriers",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
            initial_data_df=initial_data,
        )
        
        assert knowledge_table is not None
        assert knowledge_table.name == "Phone Carriers"
        assert knowledge_table.data_type == "phone_numbers"
        assert knowledge_table.primary_key_column == "phone"
        assert knowledge_table.key_id_column == "Key_ID"
        
        # Verify table exists and has data
        result = test_session.execute(
            text(f"SELECT COUNT(*) FROM {knowledge_table.table_name}")
        )
        count = result.scalar()
        assert count == 2  # Both rows should be inserted

    def test_initialize_knowledge_table_validates_name_uniqueness(self, test_session):
        """Test that duplicate names are rejected."""
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        
        initialize_knowledge_table(
            session=test_session,
            name="Test Table",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
        )
        
        # Try to create another with same name
        with pytest.raises(ValidationError):
            initialize_knowledge_table(
                session=test_session,
                name="Test Table",  # Duplicate
                data_type="phone_numbers",
                primary_key_column="phone",
                columns_config=columns_config,
                image_columns=[],
            )

    def test_initialize_knowledge_table_validates_data_type(self, test_session):
        """Test that invalid data_type is rejected."""
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        
        with pytest.raises(ValidationError):
            initialize_knowledge_table(
                session=test_session,
                name="Test",
                data_type="invalid_type",  # Invalid
                primary_key_column="phone",
                columns_config=columns_config,
                image_columns=[],
            )

    def test_initialize_knowledge_table_validates_primary_key_column(self, test_session):
        """Test that primary_key_column must exist in columns_config."""
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        
        with pytest.raises(ValidationError):
            initialize_knowledge_table(
                session=test_session,
                name="Test",
                data_type="phone_numbers",
                primary_key_column="missing_column",  # Not in columns_config
                columns_config=columns_config,
                image_columns=[],
            )

    def test_initialize_knowledge_table_with_table_name_collision(self, test_session):
        """Test that table name collisions are handled."""
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        
        # Create first table
        table1 = initialize_knowledge_table(
            session=test_session,
            name="Test Table",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
        )
        
        # Manually create a conflicting table name (simulate collision)
        from sqlalchemy import MetaData, Table, Column, String, Text
        metadata = MetaData()
        conflict_table = Table(
            f"{table1.table_name.rsplit('_v', 1)[0]}_v1",
            metadata,
            Column("uuid_value", String(36), primary_key=True),
            Column("Key_ID", Text),
        )
        metadata.create_all(bind=test_session.bind)
        
        # Try to create another with similar name (should increment version)
        table2 = initialize_knowledge_table(
            session=test_session,
            name="Test Table 2",  # Different name, but might conflict on sanitization
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
        )
        
        # Should have different table_name
        assert table2.table_name != table1.table_name

    def test_initialize_multiple_tables_same_data_type(self, test_session):
        """Test that multiple tables can be created for same data_type."""
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        
        table1 = initialize_knowledge_table(
            session=test_session,
            name="Phone White List",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
        )
        
        table2 = initialize_knowledge_table(
            session=test_session,
            name="Phone Black List",
            data_type="phone_numbers",  # Same data_type
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
        )
        
        assert table1.id != table2.id
        assert table1.data_type == table2.data_type
        assert table1.name != table2.name


class TestUploadToKnowledgeTable:
    """Test uploading data to Knowledge Tables."""

    def test_upload_validates_required_columns(self, test_session):
        """Test that upload validates required columns."""
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
        )
        
        # Upload with missing column
        df = pd.DataFrame({"phone": ["+1234567890"]})  # Missing "carrier"
        
        with pytest.raises(ValidationError):
            upload_to_knowledge_table(test_session, knowledge_table.id, df)

    def test_upload_skips_invalid_rows(self, test_session):
        """Test that invalid rows are skipped."""
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        
        knowledge_table = initialize_knowledge_table(
            session=test_session,
            name="Test Table",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
        )
        
        df = pd.DataFrame({
            "phone": ["+1234567890", "invalid", "+0987654321", "too-short"]
        })
        
        result = upload_to_knowledge_table(test_session, knowledge_table.id, df)
        
        assert result["total_rows"] == 4
        assert result["added"] == 2  # Only valid ones
        assert result["skipped_invalid"] == 2
        assert result["skipped_duplicates"] == 0

    def test_upload_skips_duplicates(self, test_session):
        """Test that duplicate Key_IDs are skipped."""
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        
        initial_data = pd.DataFrame({"phone": ["+1234567890"]})
        
        knowledge_table = initialize_knowledge_table(
            session=test_session,
            name="Test Table",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
            initial_data_df=initial_data,
        )
        
        # Upload same phone again (should be duplicate)
        df = pd.DataFrame({"phone": ["+1234567890"]})
        
        result = upload_to_knowledge_table(test_session, knowledge_table.id, df)
        
        assert result["added"] == 0
        assert result["skipped_duplicates"] == 1

    def test_upload_returns_correct_stats(self, test_session):
        """Test that upload returns correct statistics."""
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        
        knowledge_table = initialize_knowledge_table(
            session=test_session,
            name="Test Table",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
        )
        
        df = pd.DataFrame({
            "phone": ["+1234567890", "+0987654321"]
        })
        
        result = upload_to_knowledge_table(test_session, knowledge_table.id, df)
        
        assert "total_rows" in result
        assert "processed" in result
        assert "added" in result
        assert "skipped_duplicates" in result
        assert "skipped_invalid" in result
        assert "skipped_list" in result
        assert isinstance(result["skipped_list"], list)


class TestGetKnowledgeTableStats:
    """Test statistics calculation."""

    def test_get_stats_for_empty_table(self, test_session):
        """Test statistics for empty Knowledge Table."""
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        
        knowledge_table = initialize_knowledge_table(
            session=test_session,
            name="Empty Table",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
        )
        
        stats = get_knowledge_table_stats(test_session, knowledge_table.id)
        
        assert "top_20" in stats
        assert "recently_added" in stats
        assert "missing_values" in stats
        assert stats["top_20"].empty  # No matches
        assert stats["recently_added"].empty  # No data

    def test_get_stats_with_data(self, test_session):
        """Test statistics with data in table."""
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        
        initial_data = pd.DataFrame({
            "phone": ["+1234567890", "+0987654321"]
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
        
        stats = get_knowledge_table_stats(test_session, knowledge_table.id)
        
        assert "top_20" in stats
        assert "recently_added" in stats
        assert "missing_values" in stats
        # Should have 2 recently added records
        assert len(stats["recently_added"]) == 2


class TestDeleteKnowledgeTable:
    """Test Knowledge Table deletion."""

    def test_delete_knowledge_table(self, test_session):
        """Test deleting a Knowledge Table."""
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        
        knowledge_table = initialize_knowledge_table(
            session=test_session,
            name="To Delete",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
        )
        
        table_id = knowledge_table.id
        table_name = knowledge_table.table_name
        
        # Delete
        delete_knowledge_table(test_session, table_id)
        
        # Verify table is deleted
        result = test_session.execute(
            text(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        )
        assert result.fetchone() is None
        
        # Verify record is deleted
        from src.database.repository import KnowledgeTableRepository
        repo = KnowledgeTableRepository(test_session)
        assert repo.get_by_id(table_id) is None

    def test_delete_nonexistent_table(self, test_session):
        """Test that deleting non-existent table raises error."""
        with pytest.raises(ValidationError):
            delete_knowledge_table(test_session, 99999)


class TestGetAllKnowledgeTables:
    """Test getting all Knowledge Tables."""

    def test_get_all_tables(self, test_session):
        """Test getting all Knowledge Tables."""
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        
        table1 = initialize_knowledge_table(
            session=test_session,
            name="Table 1",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
        )
        
        table2 = initialize_knowledge_table(
            session=test_session,
            name="Table 2",
            data_type="emails",
            primary_key_column="email",
            columns_config={"email": {"type": "TEXT", "is_image": False}},
            image_columns=[],
        )
        
        all_tables = get_all_knowledge_tables(test_session)
        
        assert len(all_tables) >= 2
        table_ids = {t.id for t in all_tables}
        assert table1.id in table_ids
        assert table2.id in table_ids


class TestGetKnowledgeTablesByType:
    """Test filtering Knowledge Tables by data_type."""

    def test_get_tables_by_type(self, test_session):
        """Test getting tables filtered by data_type."""
        columns_config = {"phone": {"type": "TEXT", "is_image": False}}
        
        phone_table1 = initialize_knowledge_table(
            session=test_session,
            name="Phone Table 1",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
        )
        
        phone_table2 = initialize_knowledge_table(
            session=test_session,
            name="Phone Table 2",
            data_type="phone_numbers",
            primary_key_column="phone",
            columns_config=columns_config,
            image_columns=[],
        )
        
        email_table = initialize_knowledge_table(
            session=test_session,
            name="Email Table",
            data_type="emails",
            primary_key_column="email",
            columns_config={"email": {"type": "TEXT", "is_image": False}},
            image_columns=[],
        )
        
        phone_tables = get_knowledge_tables_by_type(test_session, "phone_numbers")
        
        assert len(phone_tables) == 2
        table_ids = {t.id for t in phone_tables}
        assert phone_table1.id in table_ids
        assert phone_table2.id in table_ids
        assert email_table.id not in table_ids

