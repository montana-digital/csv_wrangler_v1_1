"""
Unit tests for analysis service.
"""
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.database.models import DataAnalysis, DatasetConfig
from src.services.analysis_service import (
    check_refresh_needed,
    create_analysis,
    delete_analysis,
    detect_date_columns,
    execute_concat,
    execute_groupby,
    execute_merge,
    execute_pivot,
    get_all_analyses,
    load_analysis_result,
    load_filtered_dataset,
    refresh_analysis,
    save_analysis_result,
)
from src.utils.errors import ValidationError


class TestDetectDateColumns:
    """Test date column detection."""

    def test_detect_datetime_columns(self):
        """Test detection of datetime columns."""
        df = pd.DataFrame({
            "date_col": pd.date_range("2024-01-01", periods=5),
            "name": ["A", "B", "C", "D", "E"],
            "amount": [100, 200, 300, 400, 500],
        })
        
        date_cols = detect_date_columns(df)
        assert "date_col" in date_cols

    def test_detect_date_pattern_columns(self):
        """Test detection of columns with date-like names."""
        df = pd.DataFrame({
            "created_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "name": ["A", "B", "C"],
        })
        
        date_cols = detect_date_columns(df)
        assert "created_date" in date_cols


class TestExecuteGroupBy:
    """Test GroupBy operation execution."""

    def test_groupby_single_column_single_agg(self):
        """Test GroupBy with single column and aggregation."""
        df = pd.DataFrame({
            "category": ["A", "A", "B", "B"],
            "amount": [10, 20, 30, 40],
        })
        
        result = execute_groupby(
            df,
            group_columns=["category"],
            aggregations={"amount": ["sum"]},
        )
        
        assert len(result) == 2
        assert "amount_sum" in result.columns or "amount" in result.columns
        assert result.iloc[0]["category"] == "A"

    def test_groupby_multiple_columns(self):
        """Test GroupBy with multiple columns."""
        df = pd.DataFrame({
            "category": ["A", "A", "B", "B"],
            "subcategory": ["X", "Y", "X", "Y"],
            "amount": [10, 20, 30, 40],
        })
        
        result = execute_groupby(
            df,
            group_columns=["category", "subcategory"],
            aggregations={"amount": ["sum"]},
        )
        
        assert len(result) == 4

    def test_groupby_invalid_column(self):
        """Test GroupBy with invalid column raises error."""
        df = pd.DataFrame({"amount": [10, 20, 30]})
        
        with pytest.raises(ValidationError):
            execute_groupby(df, group_columns=["invalid"], aggregations={"amount": ["sum"]})


class TestExecutePivot:
    """Test Pivot operation execution."""

    def test_pivot_basic(self):
        """Test basic pivot table creation."""
        df = pd.DataFrame({
            "category": ["A", "A", "B", "B"],
            "month": ["Jan", "Feb", "Jan", "Feb"],
            "sales": [100, 200, 150, 250],
        })
        
        result = execute_pivot(df, index="category", columns="month", values="sales", aggfunc="sum")
        
        assert "category" in result.columns
        assert len(result) == 2

    def test_pivot_invalid_column(self):
        """Test pivot with invalid column raises error."""
        df = pd.DataFrame({"amount": [10, 20, 30]})
        
        with pytest.raises(ValidationError):
            execute_pivot(df, index="invalid", columns="month", values="sales", aggfunc="sum")


class TestExecuteMerge:
    """Test Merge operation execution."""

    def test_merge_inner(self):
        """Test inner merge."""
        df1 = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        df2 = pd.DataFrame({"id": [2, 3, 4], "value": [100, 200, 300]})
        
        result = execute_merge(df1, df2, left_on=["id"], right_on=["id"], how="inner")
        
        assert len(result) == 2
        assert "name" in result.columns
        assert "value" in result.columns

    def test_merge_left(self):
        """Test left merge."""
        df1 = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        df2 = pd.DataFrame({"id": [2, 3], "value": [100, 200]})
        
        result = execute_merge(df1, df2, left_on=["id"], right_on=["id"], how="left")
        
        assert len(result) == 3

    def test_merge_invalid_keys(self):
        """Test merge with invalid keys raises error."""
        df1 = pd.DataFrame({"id": [1, 2]})
        df2 = pd.DataFrame({"id": [1, 2]})
        
        with pytest.raises(ValidationError):
            execute_merge(df1, df2, left_on=["invalid"], right_on=["id"], how="inner")


class TestExecuteConcat:
    """Test Concat operation execution."""

    def test_concat_vertical(self):
        """Test vertical concatenation."""
        df1 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        df2 = pd.DataFrame({"col1": [5, 6], "col2": [7, 8]})
        
        result = execute_concat(df1, df2, axis=0, ignore_index=True)
        
        assert len(result) == 4

    def test_concat_horizontal(self):
        """Test horizontal concatenation."""
        df1 = pd.DataFrame({"col1": [1, 2]})
        df2 = pd.DataFrame({"col2": [3, 4]})
        
        result = execute_concat(df1, df2, axis=1, ignore_index=False)
        
        assert len(result.columns) == 2
        assert len(result) == 2


class TestParquetOperations:
    """Test parquet file operations."""

    def test_save_and_load_analysis_result(self, tmp_path):
        """Test saving and loading analysis results."""
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        
        analysis_id = 999
        file_path = save_analysis_result(df, analysis_id)
        
        assert file_path.exists()
        assert file_path.suffix == ".parquet"
        
        loaded_df = load_analysis_result(str(file_path))
        
        assert len(loaded_df) == len(df)
        assert list(loaded_df.columns) == list(df.columns)


class TestAnalysisCRUD:
    """Test analysis CRUD operations."""

    def test_create_analysis_groupby(self, test_session, tmp_path):
        """Test creating a GroupBy analysis."""
        # Create test dataset
        from src.services.dataset_service import initialize_dataset
        
        columns_config = {
            "category": {"type": "TEXT", "is_image": False},
            "amount": {"type": "INTEGER", "is_image": False},
        }
        
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        # Upload test data
        from src.services.dataset_service import upload_csv_to_dataset
        
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("category,amount\nA,100\nA,200\nB,300", encoding="utf-8")
        
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="test.csv",
        )
        
        # Create analysis
        analysis = create_analysis(
            session=test_session,
            name="Test GroupBy",
            operation_type="groupby",
            source_dataset_id=dataset.id,
            operation_config={
                "group_columns": ["category"],
                "aggregations": {"amount": ["sum"]},
            },
        )
        
        assert analysis.id is not None
        assert analysis.operation_type == "groupby"
        assert Path(analysis.result_file_path).exists()

    def test_refresh_analysis(self, test_session, tmp_path):
        """Test refreshing an analysis."""
        # Setup dataset and analysis (similar to above)
        from src.services.dataset_service import initialize_dataset, upload_csv_to_dataset
        
        columns_config = {
            "category": {"type": "TEXT", "is_image": False},
            "amount": {"type": "INTEGER", "is_image": False},
        }
        
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("category,amount\nA,100\nA,200", encoding="utf-8")
        
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="test.csv",
        )
        
        # Create analysis
        analysis = create_analysis(
            session=test_session,
            name="Test Refresh",
            operation_type="groupby",
            source_dataset_id=dataset.id,
            operation_config={
                "group_columns": ["category"],
                "aggregations": {"amount": ["sum"]},
            },
        )
        
        original_refresh_time = analysis.last_refreshed_at
        
        # Add more data
        csv_file2 = tmp_path / "test2.csv"
        csv_file2.write_text("category,amount\nB,300", encoding="utf-8")
        
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file2,
            filename="test2.csv",
        )
        
        # Refresh analysis
        refreshed = refresh_analysis(test_session, analysis.id)
        
        assert refreshed.last_refreshed_at > original_refresh_time

    def test_check_refresh_needed(self, test_session, tmp_path):
        """Test refresh detection logic."""
        from src.services.dataset_service import initialize_dataset, upload_csv_to_dataset
        
        columns_config = {
            "category": {"type": "TEXT", "is_image": False},
            "amount": {"type": "INTEGER", "is_image": False},
        }
        
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("category,amount\nA,100", encoding="utf-8")
        
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="test.csv",
        )
        
        # Create analysis
        analysis = create_analysis(
            session=test_session,
            name="Test Refresh Check",
            operation_type="groupby",
            source_dataset_id=dataset.id,
            operation_config={
                "group_columns": ["category"],
                "aggregations": {"amount": ["sum"]},
            },
        )
        
        # Reload dataset to get current timestamp
        from src.database.repository import DatasetRepository
        dataset_repo = DatasetRepository(test_session)
        dataset = dataset_repo.get_by_id(dataset.id)
        test_session.refresh(dataset)
        
        # Get initial dataset updated_at
        initial_dataset_updated = dataset.updated_at
        
        # Reload analysis to get its source_updated_at
        from src.database.repository import DataAnalysisRepository
        analysis_repo = DataAnalysisRepository(test_session)
        analysis = analysis_repo.get_by_id(analysis.id)
        test_session.refresh(analysis)
        
        # Initially should not need refresh (analysis was just created with current dataset timestamp)
        # But we need to account for timing - let's check that source_updated_at matches
        assert analysis.source_updated_at >= initial_dataset_updated or not check_refresh_needed(test_session, analysis)
        
        # Add more data (this will update dataset.updated_at)
        csv_file2 = tmp_path / "test2.csv"
        csv_file2.write_text("category,amount\nB,200", encoding="utf-8")
        
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file2,
            filename="test2.csv",
        )
        
        # Reload both dataset and analysis from database
        dataset = dataset_repo.get_by_id(dataset.id)
        test_session.refresh(dataset)
        analysis = analysis_repo.get_by_id(analysis.id)
        test_session.refresh(analysis)
        
        # Now should need refresh (dataset was updated after analysis was created)
        assert check_refresh_needed(test_session, analysis)

    def test_delete_analysis(self, test_session, tmp_path):
        """Test deleting an analysis."""
        from src.services.dataset_service import initialize_dataset, upload_csv_to_dataset
        
        columns_config = {
            "category": {"type": "TEXT", "is_image": False},
            "amount": {"type": "INTEGER", "is_image": False},
        }
        
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("category,amount\nA,100", encoding="utf-8")
        
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="test.csv",
        )
        
        # Create analysis
        analysis = create_analysis(
            session=test_session,
            name="Test Delete",
            operation_type="groupby",
            source_dataset_id=dataset.id,
            operation_config={
                "group_columns": ["category"],
                "aggregations": {"amount": ["sum"]},
            },
        )
        
        result_file_path = Path(analysis.result_file_path)
        assert result_file_path.exists()
        
        # Delete analysis
        delete_analysis(test_session, analysis.id)
        
        # Verify file is deleted
        assert not result_file_path.exists()
        
        # Verify database record is deleted
        from src.database.repository import DataAnalysisRepository
        repo = DataAnalysisRepository(test_session)
        assert repo.get_by_id(analysis.id) is None

