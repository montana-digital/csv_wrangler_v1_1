"""
Unit tests for Pickler service.

Tests for pickle file processing, filtering, and export operations.
"""
import pickle
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.services.pickler_service import (
    export_filtered_pickle,
    filter_pickle_dataframe,
    process_pickle_file,
)
from src.utils.errors import FileProcessingError, ValidationError


class TestProcessPickleFile:
    """Test processing pickle files."""

    def test_process_valid_pickle_dataframe(self, tmp_path: Path):
        """Test processing valid pickle file containing DataFrame."""
        df = pd.DataFrame({
            "name": ["John", "Jane"],
            "age": [30, 25],
            "email": ["john@test.com", "jane@test.com"]
        })

        pickle_file = tmp_path / "test.pkl"
        with open(pickle_file, "wb") as f:
            pickle.dump(df, f)

        result_df = process_pickle_file(pickle_file)

        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == 2
        assert list(result_df.columns) == ["name", "age", "email"]
        # Should return a copy
        assert result_df is not df

    def test_process_pickle_dict(self, tmp_path: Path):
        """Test processing pickle file containing dictionary."""
        data = {"name": "John", "age": 30}

        pickle_file = tmp_path / "test.pkl"
        with open(pickle_file, "wb") as f:
            pickle.dump(data, f)

        result_df = process_pickle_file(pickle_file)

        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == 1
        assert list(result_df.columns) == ["name", "age"]

    def test_process_pickle_list_of_dicts(self, tmp_path: Path):
        """Test processing pickle file containing list of dictionaries."""
        data = [
            {"name": "John", "age": 30},
            {"name": "Jane", "age": 25}
        ]

        pickle_file = tmp_path / "test.pkl"
        with open(pickle_file, "wb") as f:
            pickle.dump(data, f)

        result_df = process_pickle_file(pickle_file)

        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == 2

    def test_process_empty_pickle_raises_error(self, tmp_path: Path):
        """Test that empty pickle file raises error."""
        pickle_file = tmp_path / "empty.pkl"
        pickle_file.write_bytes(b"")

        with pytest.raises(FileProcessingError):
            process_pickle_file(pickle_file)

    def test_process_invalid_pickle_raises_error(self, tmp_path: Path):
        """Test that invalid pickle file raises error."""
        pickle_file = tmp_path / "invalid.pkl"
        pickle_file.write_bytes(b"not a pickle file")

        with pytest.raises(FileProcessingError):
            process_pickle_file(pickle_file)

    def test_process_unsupported_type_raises_error(self, tmp_path: Path):
        """Test processing unsupported pickle types."""
        # Pickle with unsupported type (set)
        data = {1, 2, 3}

        pickle_file = tmp_path / "test.pkl"
        with open(pickle_file, "wb") as f:
            pickle.dump(data, f)

        with pytest.raises(FileProcessingError):
            process_pickle_file(pickle_file)

    def test_process_nonexistent_file_raises_error(self, tmp_path: Path):
        """Test that nonexistent file raises error."""
        pickle_file = tmp_path / "nonexistent.pkl"

        with pytest.raises(FileProcessingError):
            process_pickle_file(pickle_file)


class TestFilterPickleDataframe:
    """Test filtering pickle DataFrames."""

    def test_filter_by_columns_only(self):
        """Test filtering by columns only."""
        df = pd.DataFrame({
            "name": ["John", "Jane", "Bob"],
            "age": [30, 25, 35],
            "email": ["john@test.com", "jane@test.com", "bob@test.com"],
            "city": ["NYC", "LA", "Chicago"]
        })

        filtered = filter_pickle_dataframe(
            df=df,
            columns=["name", "email"]
        )

        assert len(filtered) == 3
        assert list(filtered.columns) == ["name", "email"]
        assert "age" not in filtered.columns
        assert "city" not in filtered.columns

    def test_filter_by_columns_and_date_range(self):
        """Test filtering by both columns and date range."""
        dates = [
            datetime.now() - timedelta(days=10),
            datetime.now() - timedelta(days=5),
            datetime.now() - timedelta(days=1),
        ]

        df = pd.DataFrame({
            "name": ["John", "Jane", "Bob"],
            "age": [30, 25, 35],
            "upload_date": dates,
            "email": ["john@test.com", "jane@test.com", "bob@test.com"]
        })

        start_date = datetime.now() - timedelta(days=7)
        end_date = datetime.now()

        filtered = filter_pickle_dataframe(
            df=df,
            columns=["name", "upload_date"],
            date_column="upload_date",
            start_date=start_date,
            end_date=end_date
        )

        assert len(filtered) == 2  # Only Jane and Bob
        assert list(filtered.columns) == ["name", "upload_date"]
        assert "Jane" in filtered["name"].values
        assert "Bob" in filtered["name"].values
        assert "John" not in filtered["name"].values

    def test_filter_no_columns_selected_raises_error(self):
        """Test that no columns selected raises error."""
        df = pd.DataFrame({
            "name": ["John"],
            "age": [30]
        })

        with pytest.raises(ValidationError) as exc_info:
            filter_pickle_dataframe(df=df, columns=[])

        assert "at least one column" in str(exc_info.value).lower()

    def test_filter_invalid_column_raises_error(self):
        """Test that invalid column name raises error."""
        df = pd.DataFrame({
            "name": ["John"],
            "age": [30]
        })

        with pytest.raises(ValidationError) as exc_info:
            filter_pickle_dataframe(df=df, columns=["name", "invalid_column"])

        assert "not found" in str(exc_info.value).lower()

    def test_filter_date_range_start_only(self):
        """Test filtering with only start date."""
        dates = [
            datetime.now() - timedelta(days=10),
            datetime.now() - timedelta(days=5),
            datetime.now() - timedelta(days=1),
        ]

        df = pd.DataFrame({
            "name": ["John", "Jane", "Bob"],
            "date_col": dates
        })

        start_date = datetime.now() - timedelta(days=7)

        filtered = filter_pickle_dataframe(
            df=df,
            columns=["name", "date_col"],
            date_column="date_col",
            start_date=start_date,
            end_date=None
        )

        assert len(filtered) == 2  # Jane and Bob
        assert "Jane" in filtered["name"].values
        assert "Bob" in filtered["name"].values

    def test_filter_date_range_end_only(self):
        """Test filtering with only end date."""
        dates = [
            datetime.now() - timedelta(days=10),
            datetime.now() - timedelta(days=5),
            datetime.now() - timedelta(days=1),
        ]

        df = pd.DataFrame({
            "name": ["John", "Jane", "Bob"],
            "date_col": dates
        })

        end_date = datetime.now() - timedelta(days=7)

        filtered = filter_pickle_dataframe(
            df=df,
            columns=["name", "date_col"],
            date_column="date_col",
            start_date=None,
            end_date=end_date
        )

        assert len(filtered) == 1  # Only John
        assert "John" in filtered["name"].values

    def test_filter_date_range_both(self):
        """Test filtering with both start and end dates."""
        dates = [
            datetime.now() - timedelta(days=10),
            datetime.now() - timedelta(days=5),
            datetime.now() - timedelta(days=1),
        ]

        df = pd.DataFrame({
            "name": ["John", "Jane", "Bob"],
            "date_col": dates
        })

        start_date = datetime.now() - timedelta(days=7)
        end_date = datetime.now() - timedelta(days=2)

        filtered = filter_pickle_dataframe(
            df=df,
            columns=["name", "date_col"],
            date_column="date_col",
            start_date=start_date,
            end_date=end_date
        )

        assert len(filtered) == 1  # Only Jane
        assert "Jane" in filtered["name"].values

    def test_filter_date_range_none(self):
        """Test filtering with no date range (only column filtering)."""
        dates = [
            datetime.now() - timedelta(days=10),
            datetime.now() - timedelta(days=5),
        ]

        df = pd.DataFrame({
            "name": ["John", "Jane"],
            "date_col": dates
        })

        filtered = filter_pickle_dataframe(
            df=df,
            columns=["name"],
            date_column=None,
            start_date=None,
            end_date=None
        )

        assert len(filtered) == 2
        assert list(filtered.columns) == ["name"]

    def test_filter_date_range_inclusive(self):
        """Test that date range filtering is inclusive on both ends."""
        # Create dates exactly at boundaries
        base_date = datetime.now().replace(hour=12, minute=0, second=0, microsecond=0)
        dates = [
            base_date - timedelta(days=2),  # Exactly at start
            base_date - timedelta(days=1),  # In range
            base_date,  # Exactly at end
            base_date + timedelta(days=1),  # After end
        ]

        df = pd.DataFrame({
            "name": ["Start", "Middle", "End", "After"],
            "date_col": dates
        })

        start_date = base_date - timedelta(days=2)
        end_date = base_date

        filtered = filter_pickle_dataframe(
            df=df,
            columns=["name", "date_col"],
            date_column="date_col",
            start_date=start_date,
            end_date=end_date
        )

        assert len(filtered) == 3  # Start, Middle, End (all inclusive)
        assert "Start" in filtered["name"].values
        assert "Middle" in filtered["name"].values
        assert "End" in filtered["name"].values
        assert "After" not in filtered["name"].values

    def test_filter_preserves_row_order(self):
        """Test that filtering preserves row order."""
        dates = [
            datetime.now() - timedelta(days=5),
            datetime.now() - timedelta(days=3),
            datetime.now() - timedelta(days=1),
        ]

        df = pd.DataFrame({
            "name": ["First", "Second", "Third"],
            "date_col": dates
        })

        filtered = filter_pickle_dataframe(
            df=df,
            columns=["name", "date_col"],
            date_column="date_col",
            start_date=datetime.now() - timedelta(days=10),
            end_date=datetime.now()
        )

        # Should preserve order
        assert list(filtered["name"].values) == ["First", "Second", "Third"]

    def test_filter_date_column_not_in_selected_columns_raises_error(self):
        """Test that date column must be in selected columns."""
        dates = [datetime.now()] * 3

        df = pd.DataFrame({
            "name": ["John", "Jane", "Bob"],
            "date_col": dates
        })

        with pytest.raises(ValidationError) as exc_info:
            filter_pickle_dataframe(
                df=df,
                columns=["name"],  # date_col not selected
                date_column="date_col",
                start_date=None,
                end_date=None
            )

        assert "not found in selected columns" in str(exc_info.value).lower()


class TestExportFilteredPickle:
    """Test exporting filtered pickle files."""

    def test_export_valid_dataframe(self, tmp_path: Path):
        """Test exporting valid DataFrame."""
        df = pd.DataFrame({
            "name": ["John", "Jane"],
            "age": [30, 25]
        })

        output_path = tmp_path / "exported.pkl"
        result_path = export_filtered_pickle(df, output_path)

        assert result_path == output_path
        assert output_path.exists()

        # Verify file can be read back
        with open(output_path, "rb") as f:
            loaded_df = pickle.load(f)

        assert isinstance(loaded_df, pd.DataFrame)
        assert len(loaded_df) == 2
        assert list(loaded_df.columns) == ["name", "age"]

    def test_export_empty_dataframe_raises_error(self, tmp_path: Path):
        """Test that empty DataFrame raises error."""
        df = pd.DataFrame()

        output_path = tmp_path / "exported.pkl"

        with pytest.raises(ValidationError) as exc_info:
            export_filtered_pickle(df, output_path)

        assert "empty" in str(exc_info.value).lower()

    def test_export_file_can_be_read_back(self, tmp_path: Path):
        """Test that exported file can be read back correctly."""
        df = pd.DataFrame({
            "name": ["John", "Jane", "Bob"],
            "age": [30, 25, 35],
            "email": ["john@test.com", "jane@test.com", "bob@test.com"]
        })

        output_path = tmp_path / "exported.pkl"
        export_filtered_pickle(df, output_path)

        # Read back
        with open(output_path, "rb") as f:
            loaded_df = pickle.load(f)

        assert len(loaded_df) == len(df)
        assert list(loaded_df.columns) == list(df.columns)
        assert loaded_df.equals(df)

    def test_export_preserves_data_types(self, tmp_path: Path):
        """Test that export preserves data types."""
        df = pd.DataFrame({
            "name": ["John", "Jane"],
            "age": [30, 25],
            "score": [95.5, 87.3],
            "active": [True, False],
            "date_col": [datetime.now(), datetime.now()]
        })

        output_path = tmp_path / "exported.pkl"
        export_filtered_pickle(df, output_path)

        # Read back
        with open(output_path, "rb") as f:
            loaded_df = pickle.load(f)

        assert loaded_df["name"].dtype == df["name"].dtype
        assert loaded_df["age"].dtype == df["age"].dtype
        assert loaded_df["score"].dtype == df["score"].dtype
        assert loaded_df["active"].dtype == df["active"].dtype

    def test_export_creates_directory_if_needed(self, tmp_path: Path):
        """Test that export creates directory if it doesn't exist."""
        df = pd.DataFrame({
            "name": ["John"],
            "age": [30]
        })

        # Create path with non-existent directory
        output_path = tmp_path / "subdir" / "exported.pkl"

        export_filtered_pickle(df, output_path)

        assert output_path.exists()
        assert output_path.parent.exists()

    def test_export_with_large_dataframe(self, tmp_path: Path):
        """Test exporting large DataFrame."""
        # Create DataFrame with 1000 rows
        df = pd.DataFrame({
            "id": range(1000),
            "name": [f"Name_{i}" for i in range(1000)],
            "value": [i * 1.5 for i in range(1000)]
        })

        output_path = tmp_path / "large_exported.pkl"
        export_filtered_pickle(df, output_path)

        # Verify
        with open(output_path, "rb") as f:
            loaded_df = pickle.load(f)

        assert len(loaded_df) == 1000

