"""
Unit tests for CSV service.

Following TDD: Tests written first (RED phase).
"""
import csv
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from src.services.csv_service import (
    detect_base64_image_columns,
    generate_unique_ids,
    parse_csv_file,
    validate_column_matching,
)
from src.utils.errors import FileProcessingError, SchemaMismatchError


class TestParseCSVFile:
    """Test CSV file parsing."""

    def test_parse_valid_csv_utf8(self, sample_csv_file: Path):
        """Test parsing valid CSV with UTF-8 encoding."""
        df = parse_csv_file(sample_csv_file)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert list(df.columns) == ["name", "age", "email", "image_data"]
        # Check that name column contains strings, not integers
        assert df["name"].dtype == "object"  # String columns are object dtype
        assert df.iloc[0]["name"] == "John Doe"

    def test_parse_csv_with_utf8_sig_bom(self, tmp_path: Path):
        """Test parsing CSV with UTF-8 BOM."""
        csv_file = tmp_path / "test_bom.csv"
        content = "\ufeffname,age\nJohn,30\nJane,25"
        csv_file.write_text(content, encoding="utf-8-sig")
        
        df = parse_csv_file(csv_file)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == ["name", "age"]

    def test_parse_csv_latin1_encoding(self, tmp_path: Path):
        """Test parsing CSV with Latin-1 encoding."""
        csv_file = tmp_path / "test_latin1.csv"
        content = "name,value\nJosé,30\nFrançois,25"
        csv_file.write_bytes(content.encode("latin-1"))
        
        df = parse_csv_file(csv_file)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

    def test_handle_empty_csv(self, tmp_path: Path):
        """Test handling empty CSV file."""
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("", encoding="utf-8")
        
        with pytest.raises(FileProcessingError) as exc_info:
            parse_csv_file(csv_file)
        
        assert "empty" in str(exc_info.value).lower()

    def test_handle_malformed_csv(self, tmp_path: Path):
        """Test handling malformed CSV gracefully."""
        csv_file = tmp_path / "malformed.csv"
        csv_file.write_text("name,age\nJohn,30\nJane", encoding="utf-8")
        
        # Should handle gracefully or raise FileProcessingError
        try:
            df = parse_csv_file(csv_file)
            # If it succeeds, should have handled the malformed row
            assert isinstance(df, pd.DataFrame)
        except FileProcessingError:
            # Also acceptable - malformed CSV should raise error
            pass

    def test_handle_csv_with_special_characters(self, tmp_path: Path):
        """Test handling CSV with special characters."""
        csv_file = tmp_path / "special.csv"
        content = 'name,description\n"John, Jr.","He said ""Hello"""\nJane,"Line 1\nLine 2"'
        csv_file.write_text(content, encoding="utf-8")
        
        df = parse_csv_file(csv_file)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2


class TestDetectBase64ImageColumns:
    """Test Base64 image column detection."""

    def test_detect_base64_image_column(self):
        """Test detecting Base64 image data."""
        df = pd.DataFrame({
            "name": ["John", "Jane"],
            "image": [
                "data:image/png;base64,iVBORw0KGgoAAAANS",
                "data:image/jpeg;base64,/9j/4AAQSkZJRg"
            ]
        })
        
        image_columns = detect_base64_image_columns(df)
        
        assert "image" in image_columns
        assert "name" not in image_columns

    def test_detect_no_image_columns(self):
        """Test when no image columns exist."""
        df = pd.DataFrame({
            "name": ["John", "Jane"],
            "age": [30, 25]
        })
        
        image_columns = detect_base64_image_columns(df)
        
        assert len(image_columns) == 0

    def test_detect_long_base64_string(self):
        """Test detecting long Base64 strings."""
        long_base64 = "A" * 200  # Long string
        df = pd.DataFrame({
            "name": ["John"],
            "data": [long_base64]
        })
        
        image_columns = detect_base64_image_columns(df)
        
        # Should not detect unless it has image prefix
        assert len(image_columns) == 0

    def test_detect_multiple_image_columns(self):
        """Test detecting multiple image columns."""
        df = pd.DataFrame({
            "image1": ["data:image/png;base64,ABC"],
            "image2": ["data:image/jpeg;base64,DEF"],
            "text": ["not an image"]
        })
        
        image_columns = detect_base64_image_columns(df)
        
        assert "image1" in image_columns
        assert "image2" in image_columns
        assert "text" not in image_columns


class TestGenerateUniqueIDs:
    """Test unique ID generation."""

    def test_generate_unique_ids_for_dataframe(self):
        """Test generating unique IDs for DataFrame."""
        df = pd.DataFrame({
            "name": ["John", "Jane", "Bob"],
            "age": [30, 25, 35]
        })
        
        df_with_ids = generate_unique_ids(df)
        
        from src.config.settings import UNIQUE_ID_COLUMN_NAME
        assert UNIQUE_ID_COLUMN_NAME in df_with_ids.columns
        assert len(df_with_ids) == 3
        # All IDs should be unique
        assert df_with_ids[UNIQUE_ID_COLUMN_NAME].nunique() == 3
        # IDs should be strings (UUIDs)
        assert all(isinstance(id_val, str) for id_val in df_with_ids[UNIQUE_ID_COLUMN_NAME])

    def test_generate_ids_preserves_existing_columns(self):
        """Test that existing columns are preserved."""
        df = pd.DataFrame({
            "name": ["John"],
            "age": [30]
        })
        
        df_with_ids = generate_unique_ids(df)
        
        assert "name" in df_with_ids.columns
        assert "age" in df_with_ids.columns
        assert df_with_ids.iloc[0]["name"] == "John"

    def test_generate_ids_empty_dataframe(self):
        """Test generating IDs for empty DataFrame."""
        df = pd.DataFrame()
        
        df_with_ids = generate_unique_ids(df)
        
        from src.config.settings import UNIQUE_ID_COLUMN_NAME
        assert UNIQUE_ID_COLUMN_NAME in df_with_ids.columns
        assert len(df_with_ids) == 0


class TestValidateColumnMatching:
    """Test column matching validation."""

    def test_validate_matching_columns(self):
        """Test validation when columns match."""
        expected = ["name", "age", "email"]
        actual = ["name", "age", "email"]
        
        # Should not raise
        validate_column_matching(expected, actual)

    def test_validate_mismatched_columns_raises_error(self):
        """Test validation raises error when columns don't match."""
        expected = ["name", "age", "email"]
        actual = ["name", "age"]
        
        with pytest.raises(SchemaMismatchError) as exc_info:
            validate_column_matching(expected, actual)
        
        assert "mismatch" in str(exc_info.value).lower()

    def test_validate_different_column_order(self):
        """Test validation with different column order."""
        expected = ["name", "age", "email"]
        actual = ["email", "name", "age"]
        
        with pytest.raises(SchemaMismatchError) as exc_info:
            validate_column_matching(expected, actual)
        
        assert "order" in str(exc_info.value).lower() or "mismatch" in str(exc_info.value).lower()

    def test_validate_extra_columns(self):
        """Test validation with extra columns in actual."""
        expected = ["name", "age"]
        actual = ["name", "age", "email", "phone"]
        
        with pytest.raises(SchemaMismatchError) as exc_info:
            validate_column_matching(expected, actual)
        
        assert "mismatch" in str(exc_info.value).lower()

