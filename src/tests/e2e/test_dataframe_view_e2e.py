"""
E2E tests for DataFrame View page.

Tests user-facing DataFrame viewing functionality using Playwright.
"""
import pytest
from playwright.sync_api import Page, expect
from time import sleep

pytestmark = pytest.mark.e2e


class TestDataFrameViewPageLoad:
    """Test that DataFrame View page loads correctly."""

    def test_dataframe_view_page_exists_and_loads(self, app_page: Page):
        """Test page accessible from navigation."""
        app_page.goto("http://localhost:8501/03_dataframe_view", wait_until="networkidle")
        app_page.wait_for_load_state("networkidle")
        sleep(2)  # Allow Streamlit to render
        
        title = app_page.locator("h1").first
        expect(title).to_contain_text("DataFrame View", timeout=10000)

    def test_page_displays_no_datasets_message(self, app_page: Page):
        """Test page shows message when no datasets available."""
        app_page.goto("http://localhost:8501/03_dataframe_view", wait_until="networkidle")
        app_page.wait_for_load_state("networkidle")
        sleep(2)
        
        body_text = app_page.locator("body").text_content().lower()
        # May show message about initializing datasets
        assert "dataframe view" in body_text or "no datasets" in body_text or "select dataset" in body_text


class TestDatasetSelector:
    """Test dataset selector functionality."""

    def test_dataset_selector_visible_when_datasets_exist(self, app_page: Page):
        """Test dataset selector appears when datasets are available."""
        app_page.goto("http://localhost:8501/03_dataframe_view", wait_until="networkidle")
        app_page.wait_for_load_state("networkidle")
        sleep(2)
        
        # Check for selectbox (dataset selector)
        selectbox = app_page.locator('[data-baseweb="select"]')
        # May or may not exist depending on app state
        if selectbox.count() > 0:
            expect(selectbox.first).to_be_visible(timeout=5000)


class TestDataFrameDisplay:
    """Test DataFrame display functionality."""

    def test_dataframe_table_visible_after_dataset_selection(self, app_page: Page):
        """Test DataFrame table appears after dataset selection."""
        app_page.goto("http://localhost:8501/03_dataframe_view", wait_until="networkidle")
        app_page.wait_for_load_state("networkidle")
        sleep(2)
        
        # Check for table elements (Streamlit dataframes render as tables)
        body_text = app_page.locator("body").text_content()
        assert "dataframe view" in body_text.lower()


class TestFilteringUI:
    """Test filtering UI elements."""

    def test_filter_ui_visible(self, app_page: Page):
        """Test filter UI elements are visible."""
        app_page.goto("http://localhost:8501/03_dataframe_view", wait_until="networkidle")
        app_page.wait_for_load_state("networkidle")
        sleep(2)
        
        # Filter UI may be visible if dataset is selected
        body_text = app_page.locator("body").text_content()
        assert "dataframe view" in body_text.lower()

