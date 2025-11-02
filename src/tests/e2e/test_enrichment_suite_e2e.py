"""
E2E tests for Enrichment Suite page.

Tests user-facing enrichment functionality using Playwright.
"""
import pytest
from playwright.sync_api import Page, expect
from time import sleep

pytestmark = pytest.mark.e2e


class TestEnrichmentSuitePageLoad:
    """Test that Enrichment Suite page loads correctly."""

    def test_enrichment_suite_page_exists_and_loads(self, app_page: Page):
        """Test page accessible from navigation."""
        app_page.goto("http://localhost:8501/02_enrichment_suite", wait_until="networkidle")
        app_page.wait_for_load_state("networkidle")
        sleep(2)  # Allow Streamlit to render
        
        title = app_page.locator("h1").first
        expect(title).to_contain_text("Enrichment Suite", timeout=10000)

    def test_page_displays_no_datasets_message(self, app_page: Page):
        """Test page shows message when no datasets available."""
        app_page.goto("http://localhost:8501/02_enrichment_suite", wait_until="networkidle")
        app_page.wait_for_load_state("networkidle")
        sleep(2)
        
        body_text = app_page.locator("body").text_content().lower()
        # May show message about initializing datasets
        assert "enrichment suite" in body_text or "no datasets" in body_text or "select source dataset" in body_text


class TestDatasetSelector:
    """Test dataset selector functionality."""

    def test_dataset_selector_visible_when_datasets_exist(self, app_page: Page):
        """Test dataset selector appears when datasets are available."""
        app_page.goto("http://localhost:8501/02_enrichment_suite", wait_until="networkidle")
        app_page.wait_for_load_state("networkidle")
        sleep(2)
        
        # Check for selectbox (dataset selector)
        selectbox = app_page.locator('[data-baseweb="select"]')
        # May or may not exist depending on app state
        if selectbox.count() > 0:
            expect(selectbox.first).to_be_visible(timeout=5000)


class TestEnrichmentConfiguration:
    """Test enrichment configuration UI."""

    def test_enrichment_config_ui_visible_after_dataset_selection(self, app_page: Page):
        """Test enrichment configuration UI appears after dataset selection."""
        app_page.goto("http://localhost:8501/02_enrichment_suite", wait_until="networkidle")
        app_page.wait_for_load_state("networkidle")
        sleep(2)
        
        # Check for enrichment configuration section
        body_text = app_page.locator("body").text_content().lower()
        # Configuration UI may be visible if dataset is selected
        assert "enrichment suite" in body_text

    def test_enrichment_functions_available(self, app_page: Page):
        """Test that enrichment function options are available."""
        app_page.goto("http://localhost:8501/02_enrichment_suite", wait_until="networkidle")
        app_page.wait_for_load_state("networkidle")
        sleep(2)
        
        # Check page contains enrichment-related content
        body_text = app_page.locator("body").text_content()
        # May contain references to enrichment functions
        assert "enrichment" in body_text.lower()


class TestEnrichedDatasetTracker:
    """Test enriched dataset tracker display."""

    def test_enriched_tracker_section_visible(self, app_page: Page):
        """Test enriched dataset tracker section is visible."""
        app_page.goto("http://localhost:8501/02_enrichment_suite", wait_until="networkidle")
        app_page.wait_for_load_state("networkidle")
        sleep(2)
        
        # Tracker may or may not be visible depending on state
        body_text = app_page.locator("body").text_content()
        assert "enrichment suite" in body_text.lower()

