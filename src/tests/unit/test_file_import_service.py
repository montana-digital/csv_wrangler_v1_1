"""
Unit tests for File Import service.

Tests unified file import functionality for CSV and Pickle files.
"""
from pathlib import Path

import pandas as pd
import pickle
import pytest

pytestmark = pytest.mark.unit

from src.services.file_import_service import detect_file_type, import_file
from src.utils.errors import FileProcessingError, ValidationError


class TestDetectFileType:
    """Test file type detection."""

    def test_detect_csv_file(self, tmp_path: Path):
        """Test detecting CSV file type."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("name,age\nJohn,30", encoding="utf-8")
        
        file_type = detect_file_type(csv_file)
        assert file_type == "CSV"

    def test_detect_pickle_file(self, tmp_path: Path):
        """Test detecting Pickle file type."""
        pickle_file = tmp_path / "test.pkl"
        df = pd.DataFrame({"name": ["John"], "age": [30]})
        with open(pickle_file, "wb") as f:
            pickle.dump(df, f)
        
        file_type = detect_file_type(pickle_file)
        assert file_type == "PICKLE"

    def test_detect_pickle_alternative_extension(self, tmp_path: Path):
        """Test detecting Pickle with .pickle extension."""
        pickle_file = tmp_path / "test.pickle"
        df = pd.DataFrame({"name": ["John"]})
        with open(pickle_file, "wb") as f:
            pickle.dump(df, f)
        
        file_type = detect_file_type(pickle_file)
        assert file_type == "PICKLE"

    def test_detect_unsupported_file_type(self, tmp_path: Path):
        """Test that unsupported file types raise ValidationError."""
        txt_file = tmp_path / "test.txt"
        txt_file.write_text("Not a CSV or Pickle file", encoding="utf-8")
        
        with pytest.raises(ValidationError) as exc_info:
            detect_file_type(txt_file)
        
        assert "Unsupported file type" in str(exc_info.value)
        assert ".txt" in str(exc_info.value)

    def test_detect_file_type_case_insensitive(self, tmp_path: Path):
        """Test that file extension detection is case insensitive."""
        csv_file = tmp_path / "test.CSV"
        csv_file.write_text("name,age\nJohn,30", encoding="utf-8")
        
        file_type = detect_file_type(csv_file)
        assert file_type == "CSV"


class TestImportFile:
    """Test unified file import."""

    def test_import_csv_file(self, tmp_path: Path):
        """Test importing a CSV file."""
        csv_file = tmp_path / "test.csv"
        csv_content = "name,age\nJohn,30\nJane,25"
        csv_file.write_text(csv_content, encoding="utf-8")
        
        df, file_type = import_file(csv_file, show_progress=False)
        
        assert file_type == "CSV"
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == ["name", "age"]
        assert df.iloc[0]["name"] == "John"
        # CSV parsing returns age as string by default
        assert str(df.iloc[0]["age"]) == "30" or df.iloc[0]["age"] == 30

    def test_import_pickle_file(self, tmp_path: Path):
        """Test importing a Pickle file."""
        pickle_file = tmp_path / "test.pkl"
        original_df = pd.DataFrame({
            "name": ["John", "Jane"],
            "age": [30, 25]
        })
        with open(pickle_file, "wb") as f:
            pickle.dump(original_df, f)
        
        df, file_type = import_file(pickle_file, show_progress=False)
        
        assert file_type == "PICKLE"
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == ["name", "age"]
        pd.testing.assert_frame_equal(df, original_df)

    def test_import_file_nonexistent(self, tmp_path: Path):
        """Test importing a non-existent file raises error."""
        nonexistent_file = tmp_path / "nonexistent.csv"
        
        with pytest.raises(FileProcessingError):
            import_file(nonexistent_file, show_progress=False)

    def test_import_file_invalid_csv(self, tmp_path: Path):
        """Test importing an invalid CSV file."""
        csv_file = tmp_path / "invalid.csv"
        csv_file.write_text("Not a valid CSV content", encoding="utf-8")
        
        # Should raise FileProcessingError or handle gracefully
        with pytest.raises((FileProcessingError, ValidationError)):
            import_file(csv_file, show_progress=False)

