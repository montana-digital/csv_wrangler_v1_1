"""
Unit tests for image table viewer selection logic.

Tests multiple selection handling and selection state management.
"""
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from src.ui.components.image_table_viewer import render_image_table_viewer


@pytest.fixture
def mock_session():
    """Create a mock database session."""
    return MagicMock()


@pytest.fixture
def sample_table_info():
    """Create sample table info for testing."""
    return {
        "type": "dataset",
        "id": 1,
        "name": "Test Dataset",
        "table_name": "test_dataset_1",
        "image_columns": ["photo"],
    }


@pytest.fixture
def sample_dataframe():
    """Create sample DataFrame for testing."""
    return pd.DataFrame({
        "name": ["John Doe", "Jane Smith", "Bob Johnson"],
        "email": ["john@test.com", "jane@test.com", "bob@test.com"],
        "photo": ["data:image/png;base64,img1", "data:image/png;base64,img2", None],
    })


class TestMultipleSelectionHandling:
    """Test multiple selection warning behavior."""

    @patch("src.ui.components.image_table_viewer.load_dataset_dataframe")
    @patch("streamlit.session_state", new_callable=dict)
    @patch("streamlit.warning")
    @patch("streamlit.data_editor")
    def test_multiple_selection_shows_warning(
        self,
        mock_data_editor,
        mock_warning,
        mock_session_state,
        mock_load_df,
        mock_session,
        sample_table_info,
        sample_dataframe,
    ):
        """Test that selecting 2 rows shows warning and returns None."""
        # Setup
        mock_load_df.return_value = sample_dataframe
        
        # Create edited_df with 2 rows selected
        edited_df = sample_dataframe.copy()
        edited_df.insert(0, "Select", False)
        edited_df.loc[0, "Select"] = True
        edited_df.loc[1, "Select"] = True
        mock_data_editor.return_value = edited_df
        
        # Mock session state
        cache_key = f"image_table_data_{sample_table_info['type']}_{sample_table_info['id']}"
        mock_session_state[cache_key] = sample_dataframe
        mock_session_state["_streamlit"] = MagicMock()
        
        # Call function
        result = render_image_table_viewer(
            session=mock_session,
            table_info=sample_table_info,
        )
        
        # Verify warning was shown
        mock_warning.assert_called_once()
        warning_message = mock_warning.call_args[0][0]
        assert "Only one row can be selected" in warning_message
        assert "deselect other rows" in warning_message.lower()
        
        # Verify function returns None (no selection)
        assert result is None

    @patch("src.ui.components.image_table_viewer.load_dataset_dataframe")
    @patch("streamlit.session_state", new_callable=dict)
    @patch("streamlit.data_editor")
    def test_single_selection_works(
        self,
        mock_data_editor,
        mock_session_state,
        mock_load_df,
        mock_session,
        sample_table_info,
        sample_dataframe,
    ):
        """Test that single selection works correctly."""
        # Setup
        mock_load_df.return_value = sample_dataframe
        
        # Create edited_df with 1 row selected
        edited_df = sample_dataframe.copy()
        edited_df.insert(0, "Select", False)
        edited_df.loc[0, "Select"] = True
        mock_data_editor.return_value = edited_df
        
        # Mock session state
        cache_key = f"image_table_data_{sample_table_info['type']}_{sample_table_info['id']}"
        mock_session_state[cache_key] = sample_dataframe
        mock_session_state["_streamlit"] = MagicMock()
        
        # Call function
        result = render_image_table_viewer(
            session=mock_session,
            table_info=sample_table_info,
        )
        
        # Verify function returns the selected index
        assert result == 0

    @patch("src.ui.components.image_table_viewer.load_dataset_dataframe")
    @patch("streamlit.session_state", new_callable=dict)
    @patch("streamlit.data_editor")
    def test_no_selection_returns_none(
        self,
        mock_data_editor,
        mock_session_state,
        mock_load_df,
        mock_session,
        sample_table_info,
        sample_dataframe,
    ):
        """Test that no selection returns None."""
        # Setup
        mock_load_df.return_value = sample_dataframe
        
        # Create edited_df with no rows selected
        edited_df = sample_dataframe.copy()
        edited_df.insert(0, "Select", False)
        mock_data_editor.return_value = edited_df
        
        # Mock session state
        cache_key = f"image_table_data_{sample_table_info['type']}_{sample_table_info['id']}"
        mock_session_state[cache_key] = sample_dataframe
        mock_session_state["_streamlit"] = MagicMock()
        
        # Call function
        result = render_image_table_viewer(
            session=mock_session,
            table_info=sample_table_info,
        )
        
        # Verify function returns None
        assert result is None

    @patch("src.ui.components.image_table_viewer.load_dataset_dataframe")
    @patch("streamlit.session_state", new_callable=dict)
    @patch("streamlit.data_editor")
    def test_reset_flag_clears_checkboxes(
        self,
        mock_data_editor,
        mock_session_state,
        mock_load_df,
        mock_session,
        sample_table_info,
        sample_dataframe,
    ):
        """Test that reset flag clears all checkboxes in table editor."""
        # Setup
        mock_load_df.return_value = sample_dataframe
        
        # Create edited_df with all rows unselected (after reset)
        edited_df = sample_dataframe.copy()
        edited_df.insert(0, "Select", False)
        mock_data_editor.return_value = edited_df
        
        # Set reset flag (simulating Close button was clicked)
        reset_key = f"reset_editor_{sample_table_info['type']}_{sample_table_info['id']}"
        mock_session_state[reset_key] = True
        
        # Mock session state
        cache_key = f"image_table_data_{sample_table_info['type']}_{sample_table_info['id']}"
        mock_session_state[cache_key] = sample_dataframe
        mock_session_state["_streamlit"] = MagicMock()
        
        # Call function
        result = render_image_table_viewer(
            session=mock_session,
            table_info=sample_table_info,
        )
        
        # Verify reset flag was cleared
        assert mock_session_state.get(reset_key) == False
        
        # Verify editor state was reset (no selections)
        editor_key = f"image_table_editor_{sample_table_info['type']}_{sample_table_info['id']}"
        # Editor should have been reset, so no selections should exist
        assert result is None  # No row selected after reset

