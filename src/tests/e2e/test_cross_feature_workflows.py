"""
E2E tests for cross-feature workflows.

Tests complete user workflows that span multiple features.
"""
import pytest
from playwright.sync_api import Page, expect
from time import sleep

pytestmark = pytest.mark.e2e


class TestDatasetToEnrichmentToViewWorkflow:
    """Test workflow: Dataset upload -> Enrichment -> DataFrame View."""

    def test_navigation_between_workflow_pages(self, app_page: Page):
        """Test navigation between pages in dataset-enrichment-view workflow."""
        # Navigate to Dataset 1
        app_page.goto("http://localhost:8501/10_dataset_1", wait_until="networkidle")
        sleep(2)
        body_text = app_page.locator("body").text_content()
        assert "dataset" in body_text.lower()
        
        # Navigate to Enrichment Suite
        app_page.goto("http://localhost:8501/02_enrichment_suite", wait_until="networkidle")
        sleep(2)
        body_text = app_page.locator("body").text_content()
        assert "enrichment" in body_text.lower()
        
        # Navigate to DataFrame View
        app_page.goto("http://localhost:8501/03_dataframe_view", wait_until="networkidle")
        sleep(2)
        body_text = app_page.locator("body").text_content()
        assert "dataframe" in body_text.lower()


class TestBulkUploadToEnrichmentWorkflow:
    """Test workflow: Bulk upload -> Enrichment -> Search."""

    def test_navigation_bulk_upload_to_enrichment(self, app_page: Page):
        """Test navigation from bulk uploader to enrichment suite."""
        # Navigate to Bulk Uploader
        app_page.goto("http://localhost:8501/09_bulk_uploader", wait_until="networkidle")
        sleep(2)
        body_text = app_page.locator("body").text_content()
        assert "bulk uploader" in body_text.lower()
        
        # Navigate to Enrichment Suite
        app_page.goto("http://localhost:8501/02_enrichment_suite", wait_until="networkidle")
        sleep(2)
        body_text = app_page.locator("body").text_content()
        assert "enrichment" in body_text.lower()
        
        # Navigate to Knowledge Search
        app_page.goto("http://localhost:8501/06_knowledge_search", wait_until="networkidle")
        sleep(2)
        body_text = app_page.locator("body").text_content()
        assert "search" in body_text.lower() or "knowledge" in body_text.lower()


class TestKnowledgeBaseToSearchWorkflow:
    """Test workflow: Knowledge Base -> Knowledge Search -> Image Search."""

    def test_navigation_knowledge_workflow(self, app_page: Page):
        """Test navigation through knowledge-related pages."""
        # Navigate to Knowledge Base
        app_page.goto("http://localhost:8501/05_knowledge_base", wait_until="networkidle")
        sleep(2)
        body_text = app_page.locator("body").text_content()
        assert "knowledge" in body_text.lower()
        
        # Navigate to Knowledge Search
        app_page.goto("http://localhost:8501/06_knowledge_search", wait_until="networkidle")
        sleep(2)
        body_text = app_page.locator("body").text_content()
        assert "search" in body_text.lower() or "knowledge" in body_text.lower()
        
        # Navigate to Image Search
        app_page.goto("http://localhost:8501/07_image_search", wait_until="networkidle")
        sleep(2)
        body_text = app_page.locator("body").text_content()
        assert "image" in body_text.lower() or "search" in body_text.lower()


class TestEnrichmentToDataGeekWorkflow:
    """Test workflow: Enrichment -> Data Geek analysis."""

    def test_navigation_enrichment_to_analysis(self, app_page: Page):
        """Test navigation from enrichment to data analysis."""
        # Navigate to Enrichment Suite
        app_page.goto("http://localhost:8501/02_enrichment_suite", wait_until="networkidle")
        sleep(2)
        body_text = app_page.locator("body").text_content()
        assert "enrichment" in body_text.lower()
        
        # Navigate to Data Geek
        app_page.goto("http://localhost:8501/04_data_geek", wait_until="networkidle")
        sleep(2)
        body_text = app_page.locator("body").text_content()
        assert "data geek" in body_text.lower() or "analysis" in body_text.lower()


class TestCompleteFeatureNavigation:
    """Test complete navigation through all major features."""

    def test_navigate_through_all_major_features(self, app_page: Page):
        """Test navigation through all major feature pages."""
        features = [
            ("Home", "http://localhost:8501/01_home", "wrangler"),
            ("Enrichment Suite", "http://localhost:8501/02_enrichment_suite", "enrichment"),
            ("DataFrame View", "http://localhost:8501/03_dataframe_view", "dataframe"),
            ("Data Geek", "http://localhost:8501/04_data_geek", "geek"),
            ("Knowledge Base", "http://localhost:8501/05_knowledge_base", "knowledge"),
            ("Knowledge Search", "http://localhost:8501/06_knowledge_search", "search"),
            ("Image Search", "http://localhost:8501/07_image_search", "image"),
            ("Settings", "http://localhost:8501/08_settings", "settings"),
            ("Bulk Uploader", "http://localhost:8501/09_bulk_uploader", "bulk"),
        ]
        
        for feature_name, url, keyword in features:
            app_page.goto(url, wait_until="networkidle")
            sleep(2)
            body_text = app_page.locator("body").text_content().lower()
            assert keyword in body_text, f"Failed to load {feature_name} page"

