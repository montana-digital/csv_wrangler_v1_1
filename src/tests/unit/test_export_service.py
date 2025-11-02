"""
Unit tests for Export service.

Following TDD: Tests written first (RED phase).
"""
from datetime import datetime, timedelta

import pandas as pd
import pytest

from src.services.export_service import (
    export_dataset_to_csv,
    export_dataset_to_pickle,
    filter_by_date_range,
)
from src.utils.errors import ValidationError


class TestFilterByDateRange:
    """Test date range filtering."""

    def test_filter_with_date_range(self, test_session):
        """Test filtering data by date range."""
        # Create sample data with dates
        dates = [
            datetime.now() - timedelta(days=10),
            datetime.now() - timedelta(days=5),
            datetime.now() - timedelta(days=1),
        ]

        df = pd.DataFrame({
            "name": ["John", "Jane", "Bob"],
            "upload_date": dates
        })

        # Filter last 7 days
        start_date = datetime.now() - timedelta(days=7)
        end_date = datetime.now()

        filtered = filter_by_date_range(
            df=df,
            date_column="upload_date",
            start_date=start_date,
            end_date=end_date
        )

        assert len(filtered) == 2  # Last 5 days and 1 day ago
        assert "Jane" in filtered["name"].values
        assert "Bob" in filtered["name"].values

    def test_filter_no_start_date(self, test_session):
        """Test filtering with no start date (all before end_date)."""
        dates = [
            datetime.now() - timedelta(days=10),
            datetime.now() - timedelta(days=5),
        ]

        df = pd.DataFrame({
            "name": ["John", "Jane"],
            "upload_date": dates
        })

        end_date = datetime.now() - timedelta(days=7)

        filtered = filter_by_date_range(
            df=df,
            date_column="upload_date",
            start_date=None,
            end_date=end_date
        )

        assert len(filtered) == 1  # Only the 10 days ago record
        assert "John" in filtered["name"].values

    def test_filter_no_end_date(self, test_session):
        """Test filtering with no end date (all after start_date)."""
        dates = [
            datetime.now() - timedelta(days=10),
            datetime.now() - timedelta(days=5),
        ]

        df = pd.DataFrame({
            "name": ["John", "Jane"],
            "upload_date": dates
        })

        start_date = datetime.now() - timedelta(days=7)

        filtered = filter_by_date_range(
            df=df,
            date_column="upload_date",
            start_date=start_date,
            end_date=None
        )

        assert len(filtered) == 1  # Only the 5 days ago record
        assert "Jane" in filtered["name"].values

    def test_filter_no_dates_returns_all(self, test_session):
        """Test that filtering with no dates returns all data."""
        df = pd.DataFrame({
            "name": ["John", "Jane"],
            "upload_date": [datetime.now(), datetime.now()]
        })

        filtered = filter_by_date_range(
            df=df,
            date_column="upload_date",
            start_date=None,
            end_date=None
        )

        assert len(filtered) == len(df)


class TestExportDatasetToCSV:
    """Test CSV export functionality."""

    def test_export_all_data(self, test_session, tmp_path):
        """Test exporting all data without date filter."""
        # This would require a real dataset setup
        # For now, test the function signature and basic logic
        pass

    def test_export_with_date_range(self, test_session, tmp_path):
        """Test exporting with date range filter."""
        # This would require a real dataset setup
        pass


class TestExportDatasetToPickle:
    """Test Pickle export functionality."""

    def test_export_to_pickle(self, test_session, tmp_path):
        """Test exporting dataset to Pickle file."""
        # This would require a real dataset setup
        pass

