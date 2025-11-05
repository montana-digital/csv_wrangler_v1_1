"""
Integration tests for Pickler service.

Tests end-to-end pickle processing workflows including filtering and export.
"""
import pickle
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

pytestmark = pytest.mark.integration

from src.services.analysis_service import detect_date_columns
from src.services.pickler_service import (
    export_filtered_pickle,
    filter_pickle_dataframe,
    process_pickle_file,
)


class TestPicklerIntegration:
    """Test complete pickler workflows."""

    def test_full_workflow_with_date_columns(self, tmp_path: Path):
        """Test complete workflow: upload → select columns → filter by date → export."""
        # Create test data with date column
        dates = [
            datetime.now() - timedelta(days=10),
            datetime.now() - timedelta(days=5),
            datetime.now() - timedelta(days=1),
            datetime.now(),
        ]
        df = pd.DataFrame({
            "name": ["John", "Jane", "Bob", "Alice"],
            "age": [30, 25, 35, 28],
            "email": ["john@test.com", "jane@test.com", "bob@test.com", "alice@test.com"],
            "upload_date": dates,
            "score": [95, 87, 92, 88]
        })

        # Step 1: Create pickle file
        input_file = tmp_path / "input.pkl"
        with open(input_file, "wb") as f:
            pickle.dump(df, f)

        # Step 2: Process pickle file
        processed_df = process_pickle_file(input_file)
        assert len(processed_df) == 4
        assert len(processed_df.columns) == 5

        # Step 3: Detect date columns
        date_cols = detect_date_columns(processed_df)
        assert "upload_date" in date_cols

        # Step 4: Filter by columns and date range
        start_date = datetime.now() - timedelta(days=7)
        end_date = datetime.now()
        filtered_df = filter_pickle_dataframe(
            df=processed_df,
            columns=["name", "email", "upload_date"],
            date_column="upload_date",
            start_date=start_date,
            end_date=end_date
        )

        assert len(filtered_df) == 3  # Jane, Bob, Alice
        assert list(filtered_df.columns) == ["name", "email", "upload_date"]
        assert "Jane" in filtered_df["name"].values
        assert "Bob" in filtered_df["name"].values
        assert "Alice" in filtered_df["name"].values
        assert "John" not in filtered_df["name"].values

        # Step 5: Export filtered pickle
        output_file = tmp_path / "output.pkl"
        export_filtered_pickle(filtered_df, output_file)

        # Step 6: Verify exported file can be read back
        with open(output_file, "rb") as f:
            exported_df = pickle.load(f)

        assert len(exported_df) == 3
        assert list(exported_df.columns) == ["name", "email", "upload_date"]

    def test_full_workflow_without_date_columns(self, tmp_path: Path):
        """Test complete workflow without date columns."""
        df = pd.DataFrame({
            "name": ["John", "Jane", "Bob"],
            "age": [30, 25, 35],
            "email": ["john@test.com", "jane@test.com", "bob@test.com"],
            "score": [95, 87, 92]
        })

        # Step 1: Create pickle file
        input_file = tmp_path / "input.pkl"
        with open(input_file, "wb") as f:
            pickle.dump(df, f)

        # Step 2: Process pickle file
        processed_df = process_pickle_file(input_file)
        assert len(processed_df) == 3

        # Step 3: Detect date columns (should be empty)
        date_cols = detect_date_columns(processed_df)
        assert len(date_cols) == 0

        # Step 4: Filter by columns only (no date filtering)
        filtered_df = filter_pickle_dataframe(
            df=processed_df,
            columns=["name", "email"]
        )

        assert len(filtered_df) == 3
        assert list(filtered_df.columns) == ["name", "email"]

        # Step 5: Export
        output_file = tmp_path / "output.pkl"
        export_filtered_pickle(filtered_df, output_file)

        # Step 6: Verify
        with open(output_file, "rb") as f:
            exported_df = pickle.load(f)

        assert len(exported_df) == 3
        assert list(exported_df.columns) == ["name", "email"]

    def test_column_filtering_only(self, tmp_path: Path):
        """Test filtering by columns only (no date filtering)."""
        df = pd.DataFrame({
            "col1": ["a", "b", "c"],
            "col2": [1, 2, 3],
            "col3": [10.5, 20.5, 30.5],
            "col4": [True, False, True]
        })

        input_file = tmp_path / "input.pkl"
        with open(input_file, "wb") as f:
            pickle.dump(df, f)

        processed_df = process_pickle_file(input_file)
        filtered_df = filter_pickle_dataframe(
            df=processed_df,
            columns=["col1", "col3"]
        )

        assert len(filtered_df) == 3
        assert list(filtered_df.columns) == ["col1", "col3"]
        assert "col2" not in filtered_df.columns
        assert "col4" not in filtered_df.columns

    def test_date_filtering_only(self, tmp_path: Path):
        """Test filtering by date range only (all columns kept)."""
        dates = [
            datetime.now() - timedelta(days=10),
            datetime.now() - timedelta(days=5),
            datetime.now() - timedelta(days=1),
        ]
        df = pd.DataFrame({
            "name": ["John", "Jane", "Bob"],
            "date_col": dates,
            "value": [100, 200, 300]
        })

        input_file = tmp_path / "input.pkl"
        with open(input_file, "wb") as f:
            pickle.dump(df, f)

        processed_df = process_pickle_file(input_file)
        filtered_df = filter_pickle_dataframe(
            df=processed_df,
            columns=["name", "date_col", "value"],  # All columns
            date_column="date_col",
            start_date=datetime.now() - timedelta(days=7),
            end_date=datetime.now()
        )

        assert len(filtered_df) == 2  # Only Jane and Bob
        assert list(filtered_df.columns) == ["name", "date_col", "value"]

    def test_both_filters_combined(self, tmp_path: Path):
        """Test both column and date filtering combined."""
        dates = [
            datetime.now() - timedelta(days=10),
            datetime.now() - timedelta(days=5),
            datetime.now() - timedelta(days=1),
        ]
        df = pd.DataFrame({
            "name": ["John", "Jane", "Bob"],
            "age": [30, 25, 35],
            "date_col": dates,
            "email": ["john@test.com", "jane@test.com", "bob@test.com"],
            "score": [95, 87, 92]
        })

        input_file = tmp_path / "input.pkl"
        with open(input_file, "wb") as f:
            pickle.dump(df, f)

        processed_df = process_pickle_file(input_file)
        filtered_df = filter_pickle_dataframe(
            df=processed_df,
            columns=["name", "date_col"],  # Only 2 columns
            date_column="date_col",
            start_date=datetime.now() - timedelta(days=7),
            end_date=datetime.now()
        )

        assert len(filtered_df) == 2  # Jane and Bob
        assert list(filtered_df.columns) == ["name", "date_col"]
        assert "age" not in filtered_df.columns
        assert "email" not in filtered_df.columns
        assert "score" not in filtered_df.columns

    def test_large_file_processing(self, tmp_path: Path):
        """Test processing large pickle file."""
        # Create DataFrame with 1000 rows
        df = pd.DataFrame({
            "id": range(1000),
            "name": [f"Name_{i}" for i in range(1000)],
            "value": [i * 1.5 for i in range(1000)]
        })

        input_file = tmp_path / "large_input.pkl"
        with open(input_file, "wb") as f:
            pickle.dump(df, f)

        processed_df = process_pickle_file(input_file)
        assert len(processed_df) == 1000

        # Filter and export
        filtered_df = filter_pickle_dataframe(
            df=processed_df,
            columns=["id", "name"]
        )

        output_file = tmp_path / "large_output.pkl"
        export_filtered_pickle(filtered_df, output_file)

        # Verify
        with open(output_file, "rb") as f:
            exported_df = pickle.load(f)

        assert len(exported_df) == 1000

    def test_multiple_date_columns_selection(self, tmp_path: Path):
        """Test workflow with multiple date columns."""
        dates1 = [datetime.now()] * 3
        dates2 = [datetime.now() - timedelta(days=1)] * 3
        df = pd.DataFrame({
            "name": ["John", "Jane", "Bob"],
            "created_date": dates1,
            "updated_date": dates2,
            "value": [100, 200, 300]
        })

        input_file = tmp_path / "input.pkl"
        with open(input_file, "wb") as f:
            pickle.dump(df, f)

        processed_df = process_pickle_file(input_file)
        date_cols = detect_date_columns(processed_df)
        assert len(date_cols) >= 2  # Should detect both date columns

        # Filter using one date column
        filtered_df = filter_pickle_dataframe(
            df=processed_df,
            columns=["name", "created_date", "updated_date"],
            date_column="created_date",
            start_date=datetime.now() - timedelta(days=1),
            end_date=datetime.now() + timedelta(days=1)
        )

        assert len(filtered_df) == 3
        assert "created_date" in filtered_df.columns
        assert "updated_date" in filtered_df.columns

    def test_date_range_edge_cases(self, tmp_path: Path):
        """Test date range filtering edge cases."""
        # Create dates at boundaries
        base_date = datetime.now().replace(hour=12, minute=0, second=0, microsecond=0)
        dates = [
            base_date - timedelta(days=2),
            base_date - timedelta(days=1),
            base_date,
            base_date + timedelta(days=1),
        ]
        df = pd.DataFrame({
            "name": ["Before", "Start", "End", "After"],
            "date_col": dates
        })

        input_file = tmp_path / "input.pkl"
        with open(input_file, "wb") as f:
            pickle.dump(df, f)

        processed_df = process_pickle_file(input_file)

        # Test: start only
        filtered1 = filter_pickle_dataframe(
            df=processed_df,
            columns=["name", "date_col"],
            date_column="date_col",
            start_date=base_date - timedelta(days=1),
            end_date=None
        )
        assert len(filtered1) == 3  # Start, End, After

        # Test: end only
        filtered2 = filter_pickle_dataframe(
            df=processed_df,
            columns=["name", "date_col"],
            date_column="date_col",
            start_date=None,
            end_date=base_date
        )
        assert len(filtered2) == 3  # Before, Start, End

        # Test: both
        filtered3 = filter_pickle_dataframe(
            df=processed_df,
            columns=["name", "date_col"],
            date_column="date_col",
            start_date=base_date - timedelta(days=1),
            end_date=base_date
        )
        assert len(filtered3) == 2  # Start, End

    def test_exported_file_format(self, tmp_path: Path):
        """Test that exported file format is correct."""
        df = pd.DataFrame({
            "name": ["John", "Jane"],
            "age": [30, 25]
        })

        input_file = tmp_path / "input.pkl"
        with open(input_file, "wb") as f:
            pickle.dump(df, f)

        processed_df = process_pickle_file(input_file)
        filtered_df = filter_pickle_dataframe(
            df=processed_df,
            columns=["name", "age"]
        )

        output_file = tmp_path / "output.pkl"
        export_filtered_pickle(filtered_df, output_file)

        # Verify file format
        assert output_file.exists()
        assert output_file.suffix == ".pkl"

        # Verify it's a valid pickle file
        with open(output_file, "rb") as f:
            loaded_df = pickle.load(f)

        assert isinstance(loaded_df, pd.DataFrame)
        assert len(loaded_df) == 2

    def test_round_trip_verification(self, tmp_path: Path):
        """Test complete round trip: upload → filter → export → re-upload."""
        # Original data
        original_df = pd.DataFrame({
            "name": ["John", "Jane", "Bob"],
            "age": [30, 25, 35],
            "email": ["john@test.com", "jane@test.com", "bob@test.com"]
        })

        # Step 1: Create initial pickle
        input_file = tmp_path / "input.pkl"
        with open(input_file, "wb") as f:
            pickle.dump(original_df, f)

        # Step 2: Process and filter
        processed_df = process_pickle_file(input_file)
        filtered_df = filter_pickle_dataframe(
            df=processed_df,
            columns=["name", "email"]  # Only name and email
        )

        # Step 3: Export filtered pickle
        output_file = tmp_path / "output.pkl"
        export_filtered_pickle(filtered_df, output_file)

        # Step 4: Re-process exported file
        re_processed_df = process_pickle_file(output_file)

        # Verify round trip
        assert len(re_processed_df) == 3
        assert list(re_processed_df.columns) == ["name", "email"]
        assert "age" not in re_processed_df.columns
        assert re_processed_df["name"].tolist() == ["John", "Jane", "Bob"]
        assert re_processed_df["email"].tolist() == ["john@test.com", "jane@test.com", "bob@test.com"]

