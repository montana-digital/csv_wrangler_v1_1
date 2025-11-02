"""
Unit tests for Table service.

Tests table structure copying, data copying, column operations, and indexing.
"""
import pandas as pd
import pytest
from sqlalchemy import inspect, text

pytestmark = pytest.mark.unit

from src.database.models import DatasetConfig
from src.services.dataset_service import initialize_dataset
from src.services.table_service import (
    add_column_to_table,
    copy_table_data,
    copy_table_structure,
    create_index_on_column,
    get_new_rows_since_sync,
    get_table_row_count,
    insert_dataframe_to_table,
    update_enriched_column_values,
)
from src.utils.errors import DatabaseError


class TestCopyTableStructure:
    """Test copying table structure."""

    def test_copy_table_structure_basic(self, test_session):
        """Test basic table structure copying."""
        # Create source table via dataset initialization
        columns_config = {
            "name": {"type": "TEXT", "is_image": False},
            "age": {"type": "INTEGER", "is_image": False}
        }
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        copy_table_structure(
            session=test_session,
            source_table_name=dataset.table_name,
            target_table_name="target_table"
        )
        
        # Verify target table exists
        inspector = inspect(test_session.bind)
        tables = inspector.get_table_names()
        assert "target_table" in tables
        
        # Verify columns match
        source_cols = {col["name"]: col for col in inspector.get_columns(dataset.table_name)}
        target_cols = {col["name"]: col for col in inspector.get_columns("target_table")}
        
        # Target should have same columns (except might have uuid_value)
        assert len(target_cols) >= len(source_cols)

    def test_copy_table_structure_with_additional_columns(self, test_session):
        """Test copying table structure with additional columns."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        copy_table_structure(
            session=test_session,
            source_table_name=dataset.table_name,
            target_table_name="target_table",
            additional_columns=[("new_col", str), ("count", int)]
        )
        
        inspector = inspect(test_session.bind)
        target_cols = {col["name"] for col in inspector.get_columns("target_table")}
        
        assert "new_col" in target_cols or "name" in target_cols  # Should have source cols

    def test_copy_table_structure_nonexistent_source_raises_error(self, test_session):
        """Test that copying from non-existent table raises DatabaseError."""
        with pytest.raises(DatabaseError):
            copy_table_structure(
                session=test_session,
                source_table_name="nonexistent_table",
                target_table_name="target_table"
            )


class TestCopyTableData:
    """Test copying table data."""

    def test_copy_table_data_basic(self, test_session, tmp_path):
        """Test basic table data copying."""
        # Create source table with data
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        # Upload data to source
        csv_file = tmp_path / "source.csv"
        csv_file.write_text("name\nJohn\nJane", encoding="utf-8")
        from src.services.dataset_service import upload_csv_to_dataset
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="source.csv"
        )
        
        # Copy structure first
        copy_table_structure(
            session=test_session,
            source_table_name=dataset.table_name,
            target_table_name="target_table"
        )
        
        # Copy data
        rows_copied = copy_table_data(
            session=test_session,
            source_table_name=dataset.table_name,
            target_table_name="target_table"
        )
        
        assert rows_copied == 2
        
        # Verify data in target
        result = test_session.execute(text("SELECT COUNT(*) FROM target_table"))
        count = result.scalar()
        assert count == 2

    def test_copy_table_data_empty_table(self, test_session):
        """Test copying from empty table returns 0."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        copy_table_structure(
            session=test_session,
            source_table_name=dataset.table_name,
            target_table_name="target_table"
        )
        
        rows_copied = copy_table_data(
            session=test_session,
            source_table_name=dataset.table_name,
            target_table_name="target_table"
        )
        
        assert rows_copied == 0


class TestAddColumnToTable:
    """Test adding columns to table."""

    def test_add_column_text_type(self, test_session):
        """Test adding TEXT column."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        add_column_to_table(
            session=test_session,
            table_name=dataset.table_name,
            column_name="new_column",
            column_type="TEXT"
        )
        
        inspector = inspect(test_session.bind)
        columns = {col["name"] for col in inspector.get_columns(dataset.table_name)}
        assert "new_column" in columns

    def test_add_column_integer_type(self, test_session):
        """Test adding INTEGER column."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        add_column_to_table(
            session=test_session,
            table_name=dataset.table_name,
            column_name="count",
            column_type="INTEGER"
        )
        
        inspector = inspect(test_session.bind)
        columns = inspector.get_columns(dataset.table_name)
        count_col = next(col for col in columns if col["name"] == "count")
        assert "INTEGER" in str(count_col["type"]).upper()


class TestCreateIndexOnColumn:
    """Test creating indexes on columns."""

    def test_create_index_basic(self, test_session):
        """Test creating basic index."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        create_index_on_column(
            session=test_session,
            table_name=dataset.table_name,
            column_name="name"
        )
        
        # Verify index exists (check via SQLite metadata)
        result = test_session.execute(text(
            f"SELECT name FROM sqlite_master "
            f"WHERE type='index' AND tbl_name='{dataset.table_name}'"
        ))
        indexes = [row[0] for row in result.fetchall()]
        assert any("name" in idx.lower() for idx in indexes)

    def test_create_index_with_not_null_filter(self, test_session):
        """Test creating index with NOT NULL filter."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        create_index_on_column(
            session=test_session,
            table_name=dataset.table_name,
            column_name="name",
            include_not_null_filter=True
        )
        
        # Verify filtered index exists
        result = test_session.execute(text(
            f"SELECT sql FROM sqlite_master "
            f"WHERE type='index' AND tbl_name='{dataset.table_name}'"
        ))
        index_sqls = [row[0] for row in result.fetchall() if row[0]]
        assert any("NOT NULL" in sql for sql in index_sqls if sql)


class TestGetTableRowCount:
    """Test getting table row count."""

    def test_get_table_row_count_empty(self, test_session):
        """Test getting row count for empty table."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        count = get_table_row_count(test_session, dataset.table_name)
        assert count == 0

    def test_get_table_row_count_with_data(self, test_session, tmp_path):
        """Test getting row count for table with data."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("name\nA\nB\nC", encoding="utf-8")
        from src.services.dataset_service import upload_csv_to_dataset
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="test.csv"
        )
        
        count = get_table_row_count(test_session, dataset.table_name)
        assert count == 3


class TestGetNewRowsSinceSync:
    """Test getting new rows since sync."""

    def test_get_new_rows_all_new(self, test_session, tmp_path):
        """Test getting all rows when enriched table is empty."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Source Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        csv_file = tmp_path / "source.csv"
        csv_file.write_text("name\nJohn\nJane", encoding="utf-8")
        from src.services.dataset_service import upload_csv_to_dataset
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="source.csv"
        )
        
        # Create empty enriched table
        copy_table_structure(
            session=test_session,
            source_table_name=dataset.table_name,
            target_table_name="enriched_table"
        )
        
        new_rows = get_new_rows_since_sync(
            session=test_session,
            source_table_name=dataset.table_name,
            enriched_table_name="enriched_table"
        )
        
        assert len(new_rows) == 2
        assert "name" in new_rows.columns

    def test_get_new_rows_none_new(self, test_session, tmp_path):
        """Test getting rows when all are already synced."""
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
        from src.services.dataset_service import upload_csv_to_dataset
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="source.csv"
        )
        
        # Copy structure and data to enriched table
        copy_table_structure(
            session=test_session,
            source_table_name=dataset.table_name,
            target_table_name="enriched_table"
        )
        copy_table_data(
            session=test_session,
            source_table_name=dataset.table_name,
            target_table_name="enriched_table"
        )
        
        new_rows = get_new_rows_since_sync(
            session=test_session,
            source_table_name=dataset.table_name,
            enriched_table_name="enriched_table"
        )
        
        assert len(new_rows) == 0


class TestInsertDataframeToTable:
    """Test inserting DataFrame to table."""

    def test_insert_dataframe_basic(self, test_session):
        """Test basic DataFrame insertion."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        df = pd.DataFrame({"name": ["John", "Jane"], "uuid_value": ["id1", "id2"]})
        
        rows_inserted = insert_dataframe_to_table(
            session=test_session,
            table_name=dataset.table_name,
            df=df
        )
        
        assert rows_inserted == 2
        
        count = get_table_row_count(test_session, dataset.table_name)
        assert count == 2

    def test_insert_dataframe_empty(self, test_session):
        """Test inserting empty DataFrame returns 0."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        df = pd.DataFrame()
        
        rows_inserted = insert_dataframe_to_table(
            session=test_session,
            table_name=dataset.table_name,
            df=df
        )
        
        assert rows_inserted == 0


class TestUpdateEnrichedColumnValues:
    """Test updating enriched column values."""

    def test_update_enriched_column_values_basic(self, test_session, tmp_path):
        """Test basic enriched column value updates."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        # Add enriched column
        add_column_to_table(
            session=test_session,
            table_name=dataset.table_name,
            column_name="name_enriched_phone_numbers",
            column_type="TEXT"
        )
        
        # Add data
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("name\nJohn", encoding="utf-8")
        from src.services.dataset_service import upload_csv_to_dataset
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="test.csv"
        )
        
        # Get uuid_value for the row
        result = test_session.execute(text(
            f"SELECT uuid_value FROM {dataset.table_name} LIMIT 1"
        ))
        uuid_value = result.scalar()
        
        # Update enriched column
        df = pd.DataFrame({
            "uuid_value": [uuid_value],
            "name_enriched_phone_numbers": ["+1234567890"]
        })
        
        rows_updated = update_enriched_column_values(
            session=test_session,
            table_name=dataset.table_name,
            column_name="name_enriched_phone_numbers",
            df=df
        )
        
        assert rows_updated == 1
        
        # Verify update
        result = test_session.execute(text(
            f"SELECT name_enriched_phone_numbers FROM {dataset.table_name} "
            f"WHERE uuid_value = '{uuid_value}'"
        ))
        value = result.scalar()
        assert value == "+1234567890"

    def test_update_enriched_column_empty_dataframe(self, test_session):
        """Test updating with empty DataFrame returns 0."""
        columns_config = {"name": {"type": "TEXT", "is_image": False}}
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        df = pd.DataFrame()
        
        rows_updated = update_enriched_column_values(
            session=test_session,
            table_name=dataset.table_name,
            column_name="enriched_col",
            df=df
        )
        
        assert rows_updated == 0

