"""
Unit tests for Pickle service.

Following TDD: Tests written first (RED phase).
"""
import pickle
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from src.services.pickle_service import (
    convert_pickle_to_dataframe,
    detect_base64_in_pickle_dataframe,
    parse_pickle_file,
    validate_pickle_dataframe,
)
from src.utils.errors import FileProcessingError, ValidationError


class TestParsePickleFile:
    """Test Pickle file parsing."""

    def test_parse_valid_pickle_dataframe(self, tmp_path: Path):
        """Test parsing valid pickle file containing DataFrame."""
        df = pd.DataFrame({
            "name": ["John", "Jane"],
            "age": [30, 25],
            "email": ["john@test.com", "jane@test.com"]
        })

        pickle_file = tmp_path / "test.pkl"
        with open(pickle_file, "wb") as f:
            pickle.dump(df, f)

        result_df = parse_pickle_file(pickle_file)

        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == 2
        assert list(result_df.columns) == ["name", "age", "email"]

    def test_parse_pickle_dict(self, tmp_path: Path):
        """Test parsing pickle file containing dictionary."""
        data = {"name": "John", "age": 30}

        pickle_file = tmp_path / "test.pkl"
        with open(pickle_file, "wb") as f:
            pickle.dump(data, f)

        result_df = parse_pickle_file(pickle_file)

        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == 1
        assert list(result_df.columns) == ["name", "age"]

    def test_parse_pickle_list_of_dicts(self, tmp_path: Path):
        """Test parsing pickle file containing list of dictionaries."""
        data = [
            {"name": "John", "age": 30},
            {"name": "Jane", "age": 25}
        ]

        pickle_file = tmp_path / "test.pkl"
        with open(pickle_file, "wb") as f:
            pickle.dump(data, f)

        result_df = parse_pickle_file(pickle_file)

        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == 2

    def test_parse_invalid_pickle_raises_error(self, tmp_path: Path):
        """Test that invalid pickle file raises error."""
        pickle_file = tmp_path / "invalid.pkl"
        pickle_file.write_bytes(b"not a pickle file")

        with pytest.raises(FileProcessingError):
            parse_pickle_file(pickle_file)

    def test_parse_empty_pickle_raises_error(self, tmp_path: Path):
        """Test that empty pickle file raises error."""
        pickle_file = tmp_path / "empty.pkl"
        pickle_file.write_bytes(b"")

        with pytest.raises(FileProcessingError):
            parse_pickle_file(pickle_file)

    def test_parse_unsupported_pickle_type(self, tmp_path: Path):
        """Test parsing unsupported pickle types."""
        # Pickle with unsupported type (set)
        data = {1, 2, 3}

        pickle_file = tmp_path / "test.pkl"
        with open(pickle_file, "wb") as f:
            pickle.dump(data, f)

        with pytest.raises(FileProcessingError) as exc_info:
            parse_pickle_file(pickle_file)

        assert "unsupported" in str(exc_info.value).lower() or "not supported" in str(exc_info.value).lower()


class TestConvertPickleToDataFrame:
    """Test converting pickle data to DataFrame."""

    def test_convert_dataframe(self):
        """Test converting DataFrame (already a DataFrame)."""
        df = pd.DataFrame({"name": ["John"], "age": [30]})

        result = convert_pickle_to_dataframe(df)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_convert_dict(self):
        """Test converting dictionary to DataFrame."""
        data = {"name": "John", "age": 30}

        result = convert_pickle_to_dataframe(data)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_convert_list_of_dicts(self):
        """Test converting list of dictionaries to DataFrame."""
        data = [
            {"name": "John", "age": 30},
            {"name": "Jane", "age": 25}
        ]

        result = convert_pickle_to_dataframe(data)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2


class TestValidatePickleDataFrame:
    """Test validating pickle DataFrame."""

    def test_validate_valid_dataframe(self):
        """Test validating a valid DataFrame."""
        df = pd.DataFrame({
            "name": ["John"],
            "age": [30]
        })

        # Should not raise
        validate_pickle_dataframe(df)

    def test_validate_empty_dataframe_raises_error(self):
        """Test that empty DataFrame raises error."""
        df = pd.DataFrame()

        with pytest.raises(ValidationError):
            validate_pickle_dataframe(df)

    def test_validate_non_dataframe_raises_error(self):
        """Test that non-DataFrame raises error."""
        with pytest.raises(ValidationError):
            validate_pickle_dataframe("not a dataframe")


class TestDetectBase64InPickleDataFrame:
    """Test Base64 detection in pickle DataFrame."""

    def test_detect_base64_columns(self):
        """Test detecting Base64 image columns."""
        df = pd.DataFrame({
            "name": ["John"],
            "image": ["data:image/png;base64,iVBORw0KGgoAAAANS"]
        })

        image_columns = detect_base64_in_pickle_dataframe(df)

        assert "image" in image_columns
        assert "name" not in image_columns

    def test_detect_no_base64_columns(self):
        """Test when no Base64 columns exist."""
        df = pd.DataFrame({
            "name": ["John"],
            "age": [30]
        })

        image_columns = detect_base64_in_pickle_dataframe(df)

        assert len(image_columns) == 0

