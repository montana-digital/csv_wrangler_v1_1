"""
Unit tests for visualization service.
"""
import pandas as pd
import pytest

from src.services.visualization_service import (
    CHART_BAR,
    CHART_BOX,
    CHART_CORRELATION,
    CHART_HEATMAP,
    CHART_HISTOGRAM,
    CHART_LINE,
    CHART_SCATTER,
    create_bar_chart,
    create_box_plot,
    create_chart,
    create_correlation_matrix,
    create_heatmap,
    create_histogram,
    create_line_chart,
    create_scatter_chart,
    detect_data_characteristics,
    suggest_chart_types,
)


class TestDetectDataCharacteristics:
    """Test data characteristics detection."""

    def test_detect_numeric_columns(self):
        """Test detection of numeric columns."""
        df = pd.DataFrame({
            "amount": [100, 200, 300],
            "name": ["A", "B", "C"],
        })
        
        chars = detect_data_characteristics(df)
        assert "amount" in chars["numeric_columns"]
        assert "name" in chars["categorical_columns"]

    def test_detect_datetime_columns(self):
        """Test detection of datetime columns."""
        df = pd.DataFrame({
            "date": pd.date_range("2024-01-01", periods=3),
            "value": [10, 20, 30],
        })
        
        chars = detect_data_characteristics(df)
        assert "date" in chars["datetime_columns"]
        assert chars["has_time_series"] is True

    def test_detect_categorical_columns(self):
        """Test detection of categorical columns."""
        df = pd.DataFrame({
            "category": ["A", "A", "B", "B", "C"],
            "value": [1, 2, 3, 4, 5],
        })
        
        chars = detect_data_characteristics(df)
        assert "category" in chars["categorical_columns"]


class TestSuggestChartTypes:
    """Test chart type suggestions."""

    def test_suggest_bar_chart(self):
        """Test suggestion of bar chart for categorical vs numeric."""
        df = pd.DataFrame({
            "category": ["A", "B", "C"],
            "sales": [100, 200, 300],
        })
        
        suggestions = suggest_chart_types(df)
        bar_suggestions = [s for s in suggestions if s["chart_type"] == CHART_BAR]
        assert len(bar_suggestions) > 0

    def test_suggest_line_chart(self):
        """Test suggestion of line chart for time series."""
        df = pd.DataFrame({
            "date": pd.date_range("2024-01-01", periods=5),
            "sales": [100, 200, 300, 400, 500],
        })
        
        suggestions = suggest_chart_types(df)
        line_suggestions = [s for s in suggestions if s["chart_type"] == CHART_LINE]
        assert len(line_suggestions) > 0

    def test_suggest_scatter_plot(self):
        """Test suggestion of scatter plot for two numeric columns."""
        df = pd.DataFrame({
            "x": [1, 2, 3, 4, 5],
            "y": [10, 20, 30, 40, 50],
        })
        
        suggestions = suggest_chart_types(df)
        scatter_suggestions = [s for s in suggestions if s["chart_type"] == CHART_SCATTER]
        assert len(scatter_suggestions) > 0

    def test_suggest_histogram(self):
        """Test suggestion of histogram for numeric distribution."""
        df = pd.DataFrame({
            "values": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        })
        
        suggestions = suggest_chart_types(df)
        hist_suggestions = [s for s in suggestions if s["chart_type"] == CHART_HISTOGRAM]
        assert len(hist_suggestions) > 0

    def test_suggest_correlation_matrix(self):
        """Test suggestion of correlation matrix for multiple numeric columns."""
        df = pd.DataFrame({
            "col1": [1, 2, 3, 4, 5],
            "col2": [10, 20, 30, 40, 50],
            "col3": [100, 200, 300, 400, 500],
        })
        
        suggestions = suggest_chart_types(df)
        corr_suggestions = [s for s in suggestions if s["chart_type"] == CHART_CORRELATION]
        assert len(corr_suggestions) > 0


class TestCreateCharts:
    """Test chart creation functions."""

    def test_create_bar_chart(self):
        """Test bar chart creation."""
        df = pd.DataFrame({
            "category": ["A", "B", "C"],
            "sales": [100, 200, 300],
        })
        
        fig = create_bar_chart(df, x_column="category", y_column="sales")
        assert fig is not None
        assert len(fig.data) > 0

    def test_create_line_chart(self):
        """Test line chart creation."""
        df = pd.DataFrame({
            "date": pd.date_range("2024-01-01", periods=5),
            "sales": [100, 200, 300, 400, 500],
        })
        
        fig = create_line_chart(df, x_column="date", y_column="sales")
        assert fig is not None
        assert len(fig.data) > 0

    def test_create_scatter_chart(self):
        """Test scatter chart creation."""
        df = pd.DataFrame({
            "x": [1, 2, 3, 4, 5],
            "y": [10, 20, 30, 40, 50],
        })
        
        fig = create_scatter_chart(df, x_column="x", y_column="y")
        assert fig is not None
        assert len(fig.data) > 0

    def test_create_histogram(self):
        """Test histogram creation."""
        df = pd.DataFrame({
            "values": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        })
        
        fig = create_histogram(df, x_column="values")
        assert fig is not None
        assert len(fig.data) > 0

    def test_create_box_plot(self):
        """Test box plot creation."""
        df = pd.DataFrame({
            "category": ["A", "A", "B", "B", "C", "C"],
            "values": [10, 20, 30, 40, 50, 60],
        })
        
        fig = create_box_plot(df, x_column="category", y_column="values")
        assert fig is not None
        assert len(fig.data) > 0

    def test_create_correlation_matrix(self):
        """Test correlation matrix creation."""
        df = pd.DataFrame({
            "col1": [1, 2, 3, 4, 5],
            "col2": [10, 20, 30, 40, 50],
            "col3": [100, 200, 300, 400, 500],
        })
        
        fig = create_correlation_matrix(df)
        assert fig is not None

    def test_create_chart_generic(self):
        """Test generic create_chart function."""
        df = pd.DataFrame({
            "category": ["A", "B", "C"],
            "sales": [100, 200, 300],
        })
        
        fig = create_chart(df, CHART_BAR, x_column="category", y_column="sales")
        assert fig is not None

    def test_create_chart_invalid_type(self):
        """Test create_chart with invalid type raises error."""
        df = pd.DataFrame({"x": [1, 2, 3]})
        
        with pytest.raises(ValueError):
            create_chart(df, "invalid_type", x_column="x")

    def test_create_chart_missing_columns(self):
        """Test create_chart with missing columns raises error."""
        df = pd.DataFrame({"x": [1, 2, 3]})
        
        with pytest.raises(ValueError):
            create_bar_chart(df, x_column="x", y_column="missing")

