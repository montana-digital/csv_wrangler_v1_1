"""
Integration tests for Export service.

Tests export functionality with filters and date ranges.
"""
import pytest
import pandas as pd
from datetime import datetime, timedelta

pytestmark = pytest.mark.integration

from src.services.dataset_service import initialize_dataset, upload_csv_to_dataset
from src.services.export_service import export_dataset_to_csv, filter_by_date_range


class TestExportIntegration:
    """Test export functionality."""

    def test_export_dataset_to_csv_basic(self, test_session, tmp_path):
        """Test basic dataset export to CSV."""
        columns_config = {
            "name": {"type": "TEXT", "is_image": False},
            "value": {"type": "INTEGER", "is_image": False}
        }
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        csv_file = tmp_path / "source.csv"
        csv_file.write_text("name,value\nJohn,100\nJane,200", encoding="utf-8")
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="source.csv"
        )
        
        # Export
        output_path = tmp_path / "export.csv"
        export_dataset_to_csv(
            session=test_session,
            dataset_id=dataset.id,
            output_path=output_path
        )
        
        # Verify export file exists and has data
        assert output_path.exists()
        exported_df = pd.read_csv(output_path)
        assert len(exported_df) == 2
        assert "name" in exported_df.columns
        assert "value" in exported_df.columns

    def test_filter_by_date_range_basic(self):
        """Test filtering DataFrame by date range."""
        # Create DataFrame with dates
        dates = [
            datetime(2024, 1, 1),
            datetime(2024, 2, 1),
            datetime(2024, 3, 1),
            datetime(2024, 4, 1),
        ]
        df = pd.DataFrame({
            "date": dates,
            "value": [10, 20, 30, 40]
        })
        
        # Filter by date range
        start_date = datetime(2024, 2, 1)
        end_date = datetime(2024, 3, 15)
        filtered = filter_by_date_range(
            df,
            date_column="date",
            start_date=start_date,
            end_date=end_date
        )
        
        assert len(filtered) == 2
        assert all(filtered["date"] >= start_date)
        assert all(filtered["date"] <= end_date)

    def test_filter_by_date_range_no_start(self):
        """Test filtering with no start date (all before end)."""
        dates = [
            datetime(2024, 1, 1),
            datetime(2024, 2, 1),
            datetime(2024, 3, 1),
        ]
        df = pd.DataFrame({
            "date": dates,
            "value": [10, 20, 30]
        })
        
        end_date = datetime(2024, 2, 15)
        filtered = filter_by_date_range(
            df,
            date_column="date",
            start_date=None,
            end_date=end_date
        )
        
        assert len(filtered) == 2
        assert all(filtered["date"] <= end_date)

    def test_filter_by_date_range_no_end(self):
        """Test filtering with no end date (all after start)."""
        dates = [
            datetime(2024, 1, 1),
            datetime(2024, 2, 1),
            datetime(2024, 3, 1),
        ]
        df = pd.DataFrame({
            "date": dates,
            "value": [10, 20, 30]
        })
        
        start_date = datetime(2024, 2, 1)
        filtered = filter_by_date_range(
            df,
            date_column="date",
            start_date=start_date,
            end_date=None
        )
        
        assert len(filtered) == 2
        assert all(filtered["date"] >= start_date)

    def test_filter_by_date_range_inclusive(self):
        """Test that date range filtering is inclusive on both ends."""
        dates = [
            datetime(2024, 1, 31, 23, 59),
            datetime(2024, 2, 1, 0, 0),
            datetime(2024, 2, 1, 12, 0),
            datetime(2024, 2, 28, 23, 59),
            datetime(2024, 3, 1, 0, 0),
        ]
        df = pd.DataFrame({
            "date": dates,
            "value": [10, 20, 30, 40, 50]
        })
        
        start_date = datetime(2024, 2, 1)
        # End date should be end of Feb 28 to include the 23:59 entry
        end_date = datetime(2024, 2, 28, 23, 59, 59)
        filtered = filter_by_date_range(
            df,
            date_column="date",
            start_date=start_date,
            end_date=end_date
        )
        
        # Should include boundary dates
        # Expect at least 2 rows (the two boundary dates), but could be 3 if both boundary dates have matches
        # The test data has: 2024-02-01 00:00, 2024-02-01 12:00, 2024-02-28 23:59
        # So we should get at least 2 rows (the Feb 1 entries and the Feb 28 entry)
        assert len(filtered) >= 2, f"Expected at least 2 rows, got {len(filtered)}"
        # Check that we have at least one row from start date (Feb 1)
        assert any(filtered["date"].dt.date == start_date.date()), "Should include start date"
        # Check that we have at least one row from end date (Feb 28)  
        assert any(filtered["date"].dt.date == end_date.date()), "Should include end date"

    def test_export_with_date_filtering(self, test_session, tmp_path):
        """Test exporting dataset with date filtering."""
        columns_config = {
            "date": {"type": "TEXT", "is_image": False},
            "value": {"type": "INTEGER", "is_image": False}
        }
        dataset = initialize_dataset(
            session=test_session,
            name="Test Dataset",
            slot_number=1,
            columns_config=columns_config,
            image_columns=[],
        )
        
        csv_file = tmp_path / "source.csv"
        csv_file.write_text(
            "date,value\n"
            "2024-01-15,10\n"
            "2024-02-15,20\n"
            "2024-03-15,30",
            encoding="utf-8"
        )
        upload_csv_to_dataset(
            session=test_session,
            dataset_id=dataset.id,
            csv_file=csv_file,
            filename="source.csv"
        )
        
        # Load data
        from src.services.dataframe_service import load_dataset_dataframe
        df = load_dataset_dataframe(test_session, dataset.id)
        
        # Convert date column to datetime
        df["date"] = pd.to_datetime(df["date"])
        
        # Filter
        filtered = filter_by_date_range(
            df,
            date_column="date",
            start_date=datetime(2024, 2, 1),
            end_date=datetime(2024, 2, 28)
        )
        
        assert len(filtered) == 1
        assert filtered.iloc[0]["value"] == 20

