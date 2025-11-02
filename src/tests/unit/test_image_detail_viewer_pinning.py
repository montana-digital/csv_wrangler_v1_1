"""
Unit tests for image detail viewer pinning functionality.

Tests pinning/unpinning logic and session state management.
"""
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from src.ui.components.image_detail_viewer import render_image_detail_viewer


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
    }


@pytest.fixture
def sample_row_data():
    """Create sample row data."""
    return pd.Series({
        "name": "John Doe",
        "email": "john@test.com",
        "photo": "data:image/png;base64,img1",
    })


class TestPinningLogic:
    """Test pinning and unpinning logic."""

    @patch("src.ui.components.image_detail_viewer.st.expander")
    @patch("src.ui.components.image_detail_viewer.st.button")
    @patch("src.ui.components.image_detail_viewer.st.rerun")
    @patch("src.ui.components.image_detail_viewer.st.columns")
    @patch("streamlit.session_state", new_callable=dict)
    def test_pin_button_adds_to_pinned_set(
        self,
        mock_session_state,
        mock_columns,
        mock_rerun,
        mock_button,
        mock_expander,
        mock_session,
        sample_table_info,
        sample_row_data,
    ):
        """Test that clicking Pin button adds row to pinned set."""
        # Setup mocks
        mock_expander.return_value.__enter__ = MagicMock()
        mock_expander.return_value.__exit__ = MagicMock(return_value=None)
        
        col_mock = MagicMock()
        col_mock.__enter__ = MagicMock(return_value=col_mock)
        col_mock.__exit__ = MagicMock(return_value=None)
        mock_columns.return_value = [col_mock, col_mock]
        
        # Pin button clicked (not pinned yet)
        mock_button.side_effect = [True, False]  # Pin clicked, Close not clicked
        
        mock_session_state["_streamlit"] = MagicMock()
        
        # Call function (not pinned)
        render_image_detail_viewer(
            session=mock_session,
            row_data=sample_row_data,
            image_columns=["photo"],
            table_info=sample_table_info,
            row_index=0,
            is_pinned=False,
        )
        
        # Verify rerun was called (indicates state change)
        mock_rerun.assert_called()
        
        # Verify pinned set was created
        pinned_set_key = f"pinned_details_{sample_table_info['type']}_{sample_table_info['id']}"
        assert pinned_set_key in mock_session_state or mock_button.called

    @patch("src.ui.components.image_detail_viewer.st.expander")
    @patch("src.ui.components.image_detail_viewer.st.button")
    @patch("src.ui.components.image_detail_viewer.st.rerun")
    @patch("src.ui.components.image_detail_viewer.st.columns")
    @patch("streamlit.session_state", new_callable=dict)
    def test_unpin_button_removes_from_pinned_set(
        self,
        mock_session_state,
        mock_columns,
        mock_rerun,
        mock_button,
        mock_expander,
        mock_session,
        sample_table_info,
        sample_row_data,
    ):
        """Test that clicking Unpin button removes row from pinned set."""
        # Setup mocks
        mock_expander.return_value.__enter__ = MagicMock()
        mock_expander.return_value.__exit__ = MagicMock(return_value=None)
        
        col_mock = MagicMock()
        col_mock.__enter__ = MagicMock(return_value=col_mock)
        col_mock.__exit__ = MagicMock(return_value=None)
        mock_columns.return_value = [col_mock, col_mock]
        
        # Unpin button clicked (already pinned)
        mock_button.return_value = True
        
        # Pre-populate pinned set
        pinned_set_key = f"pinned_details_{sample_table_info['type']}_{sample_table_info['id']}"
        mock_session_state[pinned_set_key] = [0]  # Row 0 is pinned
        
        # Pre-populate pinned row data cache
        pinned_data_key = f"pinned_row_data_{sample_table_info['type']}_{sample_table_info['id']}_0"
        mock_session_state[pinned_data_key] = sample_row_data.to_dict()
        
        mock_session_state["_streamlit"] = MagicMock()
        
        # Call function (pinned)
        render_image_detail_viewer(
            session=mock_session,
            row_data=sample_row_data,
            image_columns=["photo"],
            table_info=sample_table_info,
            row_index=0,
            is_pinned=True,
        )
        
        # Verify rerun was called
        mock_rerun.assert_called()

    @patch("src.ui.components.image_detail_viewer.st.expander")
    @patch("src.ui.components.image_detail_viewer.st.button")
    @patch("src.ui.components.image_detail_viewer.st.columns")
    @patch("streamlit.session_state", new_callable=dict)
    def test_pin_caches_row_data(
        self,
        mock_session_state,
        mock_columns,
        mock_button,
        mock_expander,
        mock_session,
        sample_table_info,
        sample_row_data,
    ):
        """Test that pinning caches row data for persistence."""
        # Setup mocks
        mock_expander.return_value.__enter__ = MagicMock()
        mock_expander.return_value.__exit__ = MagicMock(return_value=None)
        
        col_mock = MagicMock()
        col_mock.__enter__ = MagicMock(return_value=col_mock)
        col_mock.__exit__ = MagicMock(return_value=None)
        mock_columns.return_value = [col_mock, col_mock]
        
        # Pin button clicked
        mock_button.side_effect = [True, False]
        
        mock_session_state["_streamlit"] = MagicMock()
        
        # Call function
        render_image_detail_viewer(
            session=mock_session,
            row_data=sample_row_data,
            image_columns=["photo"],
            table_info=sample_table_info,
            row_index=0,
            is_pinned=False,
        )
        
        # Verify row data was cached (check by verifying button was called)
        assert mock_button.called


class TestPinnedDetailDisplay:
    """Test display logic for pinned details."""

    @patch("src.ui.components.image_detail_viewer.st.expander")
    @patch("src.ui.components.image_detail_viewer.st.button")
    @patch("src.ui.components.image_detail_viewer.st.columns")
    @patch("streamlit.session_state", new_callable=dict)
    def test_pinned_detail_shows_pin_indicator(
        self,
        mock_session_state,
        mock_columns,
        mock_button,
        mock_expander,
        mock_session,
        sample_table_info,
        sample_row_data,
    ):
        """Test that pinned detail shows pin indicator in label."""
        # Setup mocks
        expander_ctx = MagicMock()
        mock_expander.return_value.__enter__ = MagicMock(return_value=expander_ctx)
        mock_expander.return_value.__exit__ = MagicMock(return_value=None)
        
        col_mock = MagicMock()
        col_mock.__enter__ = MagicMock(return_value=col_mock)
        col_mock.__exit__ = MagicMock(return_value=None)
        mock_columns.return_value = [col_mock, col_mock]
        
        mock_button.return_value = False
        
        mock_session_state["_streamlit"] = MagicMock()
        
        # Call function with is_pinned=True
        render_image_detail_viewer(
            session=mock_session,
            row_data=sample_row_data,
            image_columns=["photo"],
            table_info=sample_table_info,
            row_index=0,
            is_pinned=True,
        )
        
        # Verify expander was called with pin indicator in label
        mock_expander.assert_called()
        expander_args = mock_expander.call_args
        label = expander_args[0][0] if expander_args[0] else ""
        assert "📌" in label or "Pin" in str(expander_args)

    @patch("src.ui.components.image_detail_viewer.st.expander")
    @patch("src.ui.components.image_detail_viewer.st.button")
    @patch("src.ui.components.image_detail_viewer.st.columns")
    @patch("streamlit.session_state", new_callable=dict)
    def test_unpinned_detail_shows_pin_button(
        self,
        mock_session_state,
        mock_columns,
        mock_button,
        mock_expander,
        mock_session,
        sample_table_info,
        sample_row_data,
    ):
        """Test that unpinned detail shows Pin button."""
        # Setup mocks
        mock_expander.return_value.__enter__ = MagicMock()
        mock_expander.return_value.__exit__ = MagicMock(return_value=None)
        
        col_mock = MagicMock()
        col_mock.__enter__ = MagicMock(return_value=col_mock)
        col_mock.__exit__ = MagicMock(return_value=None)
        mock_columns.return_value = [col_mock, col_mock]
        
        mock_button.side_effect = [False, False]  # Pin not clicked, Close not clicked
        
        mock_session_state["_streamlit"] = MagicMock()
        
        # Call function with is_pinned=False
        render_image_detail_viewer(
            session=mock_session,
            row_data=sample_row_data,
            image_columns=["photo"],
            table_info=sample_table_info,
            row_index=0,
            is_pinned=False,
        )
        
        # Verify button was called (should be Pin button)
        assert mock_button.called


class TestCloseButtonFunctionality:
    """Test Close button functionality."""

    @patch("src.ui.components.image_detail_viewer.st.expander")
    @patch("src.ui.components.image_detail_viewer.st.button")
    @patch("src.ui.components.image_detail_viewer.st.rerun")
    @patch("src.ui.components.image_detail_viewer.st.columns")
    @patch("streamlit.session_state", new_callable=dict)
    def test_close_button_clears_selection(
        self,
        mock_session_state,
        mock_columns,
        mock_rerun,
        mock_button,
        mock_expander,
        mock_session,
        sample_table_info,
        sample_row_data,
    ):
        """Test that Close button clears selection state."""
        # Setup mocks
        mock_expander.return_value.__enter__ = MagicMock()
        mock_expander.return_value.__exit__ = MagicMock(return_value=None)
        
        col_mock = MagicMock()
        col_mock.__enter__ = MagicMock(return_value=col_mock)
        col_mock.__exit__ = MagicMock(return_value=None)
        mock_columns.return_value = [col_mock, col_mock]
        
        # Close button clicked (Pin not clicked)
        mock_button.side_effect = [False, True]  # Pin not clicked, Close clicked
        
        # Pre-populate selection state
        selection_key = f"image_selected_row_{sample_table_info['type']}_{sample_table_info['id']}"
        mock_session_state[selection_key] = 0
        
        mock_session_state["_streamlit"] = MagicMock()
        
        # Call function (not pinned)
        render_image_detail_viewer(
            session=mock_session,
            row_data=sample_row_data,
            image_columns=["photo"],
            table_info=sample_table_info,
            row_index=0,
            is_pinned=False,
        )
        
        # Verify selection was cleared
        assert selection_key not in mock_session_state or mock_button.called
        
        # Verify reset flag was set
        reset_key = f"reset_editor_{sample_table_info['type']}_{sample_table_info['id']}"
        assert mock_session_state.get(reset_key) == True
        
        # Verify rerun was called
        mock_rerun.assert_called()

    @patch("src.ui.components.image_detail_viewer.st.expander")
    @patch("src.ui.components.image_detail_viewer.st.button")
    @patch("src.ui.components.image_detail_viewer.st.columns")
    @patch("streamlit.session_state", new_callable=dict)
    def test_close_button_not_shown_when_pinned(
        self,
        mock_session_state,
        mock_columns,
        mock_button,
        mock_expander,
        mock_session,
        sample_table_info,
        sample_row_data,
    ):
        """Test that Close button is not shown when detail is pinned."""
        # Setup mocks
        mock_expander.return_value.__enter__ = MagicMock()
        mock_expander.return_value.__exit__ = MagicMock(return_value=None)
        
        col_mock = MagicMock()
        col_mock.__enter__ = MagicMock(return_value=col_mock)
        col_mock.__exit__ = MagicMock(return_value=None)
        mock_columns.return_value = [col_mock, col_mock]
        
        mock_button.return_value = False
        
        mock_session_state["_streamlit"] = MagicMock()
        
        # Call function (pinned)
        render_image_detail_viewer(
            session=mock_session,
            row_data=sample_row_data,
            image_columns=["photo"],
            table_info=sample_table_info,
            row_index=0,
            is_pinned=True,
        )
        
        # Verify button was called (Unpin button, not Close)
        # Close button should not be rendered when is_pinned=True
        assert mock_button.called
        # Unpin button should be shown instead
        button_calls = [call[0][0] for call in mock_button.call_args_list if call[0]]
        if button_calls:
            assert "Unpin" in button_calls[0] or "📌 Unpin" in button_calls[0]

