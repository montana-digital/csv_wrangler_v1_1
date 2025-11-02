"""
Visualization service for CSV Wrangler Data Geek page.

Provides chart generation and intelligent chart type suggestions based on data characteristics.
Uses Plotly for interactive visualizations (optional package).
"""
from typing import Any, Optional

import pandas as pd

from src.utils.logging_config import get_logger
from src.utils.package_check import has_plotly

logger = get_logger(__name__)

# Lazy import plotly - only import if available
_plotly_available = False
px = None
go = None

try:
    if has_plotly():
        import plotly.express as px
        import plotly.graph_objects as go
        _plotly_available = True
except ImportError:
    pass


class PlotlyNotAvailableError(Exception):
    """Raised when Plotly is not available but visualization is requested."""
    
    def __init__(self):
        super().__init__(
            "Plotly package is not installed. Install it with: pip install plotly"
        )


def is_visualization_available() -> bool:
    """
    Check if visualization capabilities are available (Plotly installed).
    
    Returns:
        True if Plotly is available, False otherwise
    """
    return _plotly_available

# Chart type constants
CHART_BAR = "bar"
CHART_LINE = "line"
CHART_SCATTER = "scatter"
CHART_HISTOGRAM = "histogram"
CHART_HEATMAP = "heatmap"
CHART_BOX = "box"
CHART_VIOLIN = "violin"
CHART_CORRELATION = "correlation"


def detect_data_characteristics(df: pd.DataFrame) -> dict[str, Any]:
    """
    Analyze DataFrame to detect data characteristics.
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        Dictionary with detected characteristics:
        - numeric_columns: List of numeric column names
        - categorical_columns: List of categorical column names
        - datetime_columns: List of datetime column names
        - has_time_series: True if datetime column exists
        - row_count: Number of rows
        - column_count: Number of columns
    """
    characteristics = {
        "numeric_columns": [],
        "categorical_columns": [],
        "datetime_columns": [],
        "has_time_series": False,
        "row_count": len(df),
        "column_count": len(df.columns),
    }
    
    for col in df.columns:
        # Check numeric
        if pd.api.types.is_numeric_dtype(df[col]):
            characteristics["numeric_columns"].append(col)
        
        # Check datetime
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            characteristics["datetime_columns"].append(col)
            characteristics["has_time_series"] = True
        
        # Check categorical (string/object with limited unique values)
        elif df[col].dtype == "object" or df[col].dtype.name == "category":
            unique_count = df[col].nunique()
            total_count = len(df)
            unique_ratio = unique_count / total_count if total_count > 0 else 0
            # Consider categorical if:
            # - Less than 50% unique values, OR
            # - Few unique values (<= 10) and at least some repetition, OR
            # - All values are unique but it's a small dataset (might still be categorical)
            if unique_ratio < 0.5 or (unique_count <= 10 and unique_count < total_count) or (unique_count == total_count and total_count <= 5):
                characteristics["categorical_columns"].append(col)
    
    return characteristics


def suggest_chart_types(df: pd.DataFrame) -> list[dict[str, Any]]:
    """
    Suggest appropriate chart types based on DataFrame characteristics.
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        List of suggested chart configurations, each with:
        - chart_type: Chart type identifier
        - description: Human-readable description
        - recommended: True if highly recommended for this data
        - columns: Suggested columns for this chart type
    """
    if df.empty:
        return []
    
    suggestions = []
    chars = detect_data_characteristics(df)
    
    # Bar chart: categorical vs numeric
    if chars["categorical_columns"] and chars["numeric_columns"]:
        cat_col = chars["categorical_columns"][0]
        num_col = chars["numeric_columns"][0]
        suggestions.append({
            "chart_type": CHART_BAR,
            "description": f"Bar chart: {cat_col} vs {num_col}",
            "recommended": True,
            "x_column": cat_col,
            "y_column": num_col,
        })
    
    # Line chart: time series
    if chars["has_time_series"] and chars["numeric_columns"]:
        time_col = chars["datetime_columns"][0]
        num_col = chars["numeric_columns"][0]
        suggestions.append({
            "chart_type": CHART_LINE,
            "description": f"Line chart: {time_col} vs {num_col}",
            "recommended": True,
            "x_column": time_col,
            "y_column": num_col,
        })
    
    # Scatter plot: two numeric columns
    if len(chars["numeric_columns"]) >= 2:
        x_col = chars["numeric_columns"][0]
        y_col = chars["numeric_columns"][1]
        suggestions.append({
            "chart_type": CHART_SCATTER,
            "description": f"Scatter plot: {x_col} vs {y_col}",
            "recommended": True,
            "x_column": x_col,
            "y_column": y_col,
        })
    
    # Histogram: single numeric column distribution
    if chars["numeric_columns"]:
        num_col = chars["numeric_columns"][0]
        suggestions.append({
            "chart_type": CHART_HISTOGRAM,
            "description": f"Histogram: Distribution of {num_col}",
            "recommended": True,
            "x_column": num_col,
        })
    
    # Heatmap: correlation matrix (if multiple numeric columns)
    if len(chars["numeric_columns"]) >= 3:
        suggestions.append({
            "chart_type": CHART_CORRELATION,
            "description": "Correlation heatmap of numeric columns",
            "recommended": False,
            "columns": chars["numeric_columns"],
        })
    
    # Box plot: numeric distribution by category
    if chars["categorical_columns"] and chars["numeric_columns"]:
        cat_col = chars["categorical_columns"][0]
        num_col = chars["numeric_columns"][0]
        suggestions.append({
            "chart_type": CHART_BOX,
            "description": f"Box plot: {num_col} by {cat_col}",
            "recommended": False,
            "x_column": cat_col,
            "y_column": num_col,
        })
    
    # Heatmap: pivot table visualization (if data looks like pivot)
    if len(df.columns) <= 10 and len(df) <= 50:
        # Might be a pivot table result
        suggestions.append({
            "chart_type": CHART_HEATMAP,
            "description": "Heatmap: Matrix visualization",
            "recommended": False,
            "columns": list(df.columns[:3]),  # First 3 columns
        })
    
    return suggestions


def create_bar_chart(
    df: pd.DataFrame,
    x_column: str,
    y_column: str,
    title: Optional[str] = None,
    color_column: Optional[str] = None,
):
    """
    Create bar chart using Plotly.
    
    Args:
        df: DataFrame with data
        x_column: Column for x-axis (categorical)
        y_column: Column for y-axis (numeric)
        title: Optional chart title
        color_column: Optional column for color grouping
        
    Returns:
        Plotly Figure object
        
    Raises:
        PlotlyNotAvailableError: If Plotly is not installed
    """
    if not _plotly_available:
        raise PlotlyNotAvailableError()
    
    if x_column not in df.columns or y_column not in df.columns:
        raise ValueError(f"Columns {x_column} or {y_column} not found in DataFrame")
    
    # Aggregate if needed (in case of duplicate x values)
    if df[x_column].duplicated().any():
        df = df.groupby(x_column)[y_column].sum().reset_index()
    
    fig = px.bar(
        df,
        x=x_column,
        y=y_column,
        color=color_column if color_column and color_column in df.columns else None,
        title=title or f"{y_column} by {x_column}",
    )
    
    fig.update_layout(
        xaxis_title=x_column,
        yaxis_title=y_column,
        hovermode="x unified",
    )
    
    return fig


def create_line_chart(
    df: pd.DataFrame,
    x_column: str,
    y_column: str,
    title: Optional[str] = None,
    color_column: Optional[str] = None,
):
    """
    Create line chart using Plotly.
    
    Args:
        df: DataFrame with data
        x_column: Column for x-axis (datetime or numeric)
        y_column: Column for y-axis (numeric)
        title: Optional chart title
        color_column: Optional column for color grouping
        
    Returns:
        Plotly Figure object
        
    Raises:
        PlotlyNotAvailableError: If Plotly is not installed
    """
    if not _plotly_available:
        raise PlotlyNotAvailableError()
    
    if x_column not in df.columns or y_column not in df.columns:
        raise ValueError(f"Columns {x_column} or {y_column} not found in DataFrame")
    
    # Sort by x if datetime
    if pd.api.types.is_datetime64_any_dtype(df[x_column]):
        df = df.sort_values(x_column)
    
    fig = px.line(
        df,
        x=x_column,
        y=y_column,
        color=color_column if color_column and color_column in df.columns else None,
        title=title or f"{y_column} over {x_column}",
    )
    
    fig.update_layout(
        xaxis_title=x_column,
        yaxis_title=y_column,
        hovermode="x unified",
    )
    
    return fig


def create_scatter_chart(
    df: pd.DataFrame,
    x_column: str,
    y_column: str,
    title: Optional[str] = None,
    color_column: Optional[str] = None,
    size_column: Optional[str] = None,
):
    """
    Create scatter plot using Plotly.
    
    Args:
        df: DataFrame with data
        x_column: Column for x-axis (numeric)
        y_column: Column for y-axis (numeric)
        title: Optional chart title
        color_column: Optional column for color grouping
        size_column: Optional column for marker size
        
    Returns:
        Plotly Figure object
        
    Raises:
        PlotlyNotAvailableError: If Plotly is not installed
    """
    if not _plotly_available:
        raise PlotlyNotAvailableError()
    
    if x_column not in df.columns or y_column not in df.columns:
        raise ValueError(f"Columns {x_column} or {y_column} not found in DataFrame")
    
    fig = px.scatter(
        df,
        x=x_column,
        y=y_column,
        color=color_column if color_column and color_column in df.columns else None,
        size=size_column if size_column and size_column in df.columns else None,
        title=title or f"{y_column} vs {x_column}",
    )
    
    fig.update_layout(
        xaxis_title=x_column,
        yaxis_title=y_column,
        hovermode="closest",
    )
    
    return fig


def create_histogram(
    df: pd.DataFrame,
    x_column: str,
    title: Optional[str] = None,
    bins: Optional[int] = None,
):
    """
    Create histogram using Plotly.
    
    Args:
        df: DataFrame with data
        x_column: Column for histogram (numeric)
        title: Optional chart title
        bins: Optional number of bins
        
    Returns:
        Plotly Figure object
        
    Raises:
        PlotlyNotAvailableError: If Plotly is not installed
    """
    if not _plotly_available:
        raise PlotlyNotAvailableError()
    
    if x_column not in df.columns:
        raise ValueError(f"Column {x_column} not found in DataFrame")
    
    fig = px.histogram(
        df,
        x=x_column,
        nbins=bins,
        title=title or f"Distribution of {x_column}",
    )
    
    fig.update_layout(
        xaxis_title=x_column,
        yaxis_title="Frequency",
        hovermode="x unified",
    )
    
    return fig


def create_heatmap(
    df: pd.DataFrame,
    x_column: Optional[str] = None,
    y_column: Optional[str] = None,
    values_column: Optional[str] = None,
    title: Optional[str] = None,
):
    """
    Create heatmap using Plotly.
    
    Args:
        df: DataFrame with data
        x_column: Column for x-axis (optional, will use first column if None)
        y_column: Column for y-axis (optional, will use second column if None)
        values_column: Column for values (optional, will use first numeric if None)
        title: Optional chart title
        
    Returns:
        Plotly Figure object
        
    Raises:
        PlotlyNotAvailableError: If Plotly is not installed
    """
    if not _plotly_available:
        raise PlotlyNotAvailableError()
    
    if df.empty:
        raise ValueError("DataFrame is empty")
    
    # Determine columns
    if x_column is None:
        x_column = df.columns[0]
    if y_column is None and len(df.columns) > 1:
        y_column = df.columns[1]
    if values_column is None:
        # Find first numeric column
        numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
        values_column = numeric_cols[0] if numeric_cols else df.columns[-1]
    
    # Pivot if needed
    if y_column and values_column:
        pivot_df = df.pivot_table(
            index=y_column,
            columns=x_column,
            values=values_column,
            aggfunc="sum",
            fill_value=0,
        )
    else:
        # Simple heatmap from numeric columns
        numeric_df = df.select_dtypes(include=["number"])
        pivot_df = numeric_df.T
    
    fig = px.imshow(
        pivot_df,
        title=title or "Heatmap",
        labels=dict(x=x_column, y=y_column or "Index", color=values_column or "Value"),
    )
    
    return fig


def create_box_plot(
    df: pd.DataFrame,
    x_column: str,
    y_column: str,
    title: Optional[str] = None,
):
    """
    Create box plot using Plotly.
    
    Args:
        df: DataFrame with data
        x_column: Column for x-axis (categorical)
        y_column: Column for y-axis (numeric)
        title: Optional chart title
        
    Returns:
        Plotly Figure object
        
    Raises:
        PlotlyNotAvailableError: If Plotly is not installed
    """
    if not _plotly_available:
        raise PlotlyNotAvailableError()
    
    if x_column not in df.columns or y_column not in df.columns:
        raise ValueError(f"Columns {x_column} or {y_column} not found in DataFrame")
    
    fig = px.box(
        df,
        x=x_column,
        y=y_column,
        title=title or f"Distribution of {y_column} by {x_column}",
    )
    
    fig.update_layout(
        xaxis_title=x_column,
        yaxis_title=y_column,
        hovermode="x unified",
    )
    
    return fig


def create_correlation_matrix(df: pd.DataFrame, title: Optional[str] = None):
    """
    Create correlation matrix heatmap.
    
    Args:
        df: DataFrame with numeric columns
        title: Optional chart title
        
    Returns:
        Plotly Figure object
        
    Raises:
        PlotlyNotAvailableError: If Plotly is not installed
    """
    if not _plotly_available:
        raise PlotlyNotAvailableError()
    
    # Select only numeric columns
    numeric_df = df.select_dtypes(include=["number"])
    
    if numeric_df.empty:
        raise ValueError("No numeric columns found for correlation matrix")
    
    # Calculate correlation
    corr_matrix = numeric_df.corr()
    
    fig = px.imshow(
        corr_matrix,
        title=title or "Correlation Matrix",
        labels=dict(color="Correlation"),
        color_continuous_scale="RdBu",
        aspect="auto",
    )
    
    return fig


def create_chart(
    df: pd.DataFrame,
    chart_type: str,
    x_column: Optional[str] = None,
    y_column: Optional[str] = None,
    title: Optional[str] = None,
    **kwargs: Any,
):
    """
    Create chart based on chart type.
    
    Args:
        df: DataFrame with data
        chart_type: Type of chart to create
        x_column: Column for x-axis
        y_column: Column for y-axis
        title: Optional chart title
        **kwargs: Additional chart-specific parameters
        
    Returns:
        Plotly Figure object
        
    Raises:
        PlotlyNotAvailableError: If Plotly is not installed
        ValueError: If chart_type is invalid or required columns missing
    """
    if not _plotly_available:
        raise PlotlyNotAvailableError()
    
    if df.empty:
        raise ValueError("Cannot create chart from empty DataFrame")
    
    if chart_type == CHART_BAR:
        if not x_column or not y_column:
            raise ValueError("Bar chart requires x_column and y_column")
        return create_bar_chart(df, x_column, y_column, title, kwargs.get("color_column"))
    
    elif chart_type == CHART_LINE:
        if not x_column or not y_column:
            raise ValueError("Line chart requires x_column and y_column")
        return create_line_chart(df, x_column, y_column, title, kwargs.get("color_column"))
    
    elif chart_type == CHART_SCATTER:
        if not x_column or not y_column:
            raise ValueError("Scatter chart requires x_column and y_column")
        return create_scatter_chart(
            df, x_column, y_column, title, kwargs.get("color_column"), kwargs.get("size_column")
        )
    
    elif chart_type == CHART_HISTOGRAM:
        if not x_column:
            raise ValueError("Histogram requires x_column")
        return create_histogram(df, x_column, title, kwargs.get("bins"))
    
    elif chart_type == CHART_HEATMAP:
        return create_heatmap(df, x_column, y_column, kwargs.get("values_column"), title)
    
    elif chart_type == CHART_BOX:
        if not x_column or not y_column:
            raise ValueError("Box plot requires x_column and y_column")
        return create_box_plot(df, x_column, y_column, title)
    
    elif chart_type == CHART_CORRELATION:
        return create_correlation_matrix(df, title)
    
    else:
        raise ValueError(f"Unknown chart type: {chart_type}")

