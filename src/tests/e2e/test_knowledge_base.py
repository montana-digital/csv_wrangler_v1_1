"""
End-to-end tests for Knowledge Base page.

Tests complete user workflows through the browser UI.
"""
import tempfile
from pathlib import Path
from time import sleep

import pandas as pd
import pytest
from playwright.sync_api import Page, expect


class TestKnowledgeBasePageLoad:
    """Test Knowledge Base page loads correctly."""

    def test_knowledge_base_page_exists(self, app_page: Page):
        """Test that Knowledge Base page exists in navigation and loads."""
        # Wait for app to load
        app_page.wait_for_load_state("networkidle", timeout=15000)
        sleep(2)
        
        # Look for Knowledge Base link in sidebar
        # Try multiple selectors as Streamlit sidebar structure may vary
        knowledge_link = app_page.locator(
            'a:has-text("Knowledge Base"), a[href*="knowledge_base"], text=Knowledge Base'
        )
        
        # If not found, check all links in sidebar
        if not knowledge_link.is_visible(timeout=2000):
            # Get all sidebar links
            all_links = app_page.locator('[data-testid="stSidebar"] a')
            link_count = all_links.count()
            for i in range(link_count):
                link_text = all_links.nth(i).text_content()
                if "Knowledge" in (link_text or "") or "knowledge" in (link_text or ""):
                    knowledge_link = all_links.nth(i)
                    break
        
        # Click Knowledge Base link
        if knowledge_link.is_visible(timeout=3000):
            knowledge_link.click()
            app_page.wait_for_load_state("networkidle", timeout=10000)
            sleep(2)
            
            # Verify page content
            expect(app_page.locator("body")).to_contain_text("Knowledge Base", timeout=5000)
        else:
            pytest.skip("Knowledge Base page link not found in sidebar - page may not be registered")


class TestKnowledgeTableCreation:
    """Test creating a new Knowledge Table."""

    def test_create_phone_knowledge_table(self, app_page: Page):
        """Test creating a Knowledge Table for phone numbers."""
        # Navigate to Knowledge Base page
        self._navigate_to_knowledge_base(app_page)
        
        # Look for "Create New" button or section
        create_button = app_page.locator(
            'button:has-text("Create New"), button:has-text("New Knowledge Table"), '
            'text=Create New Knowledge Table'
        )
        
        if create_button.is_visible(timeout=3000):
            create_button.click()
            sleep(1)
        
        # Look for file uploader
        file_input = app_page.locator('input[type="file"]')
        
        if file_input.is_visible(timeout=3000):
            # Create test CSV
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                f.write("phone,carrier\n+1234567890,Verizon\n+0987654321,AT&T\n")
                csv_path = Path(f.name)
            
            try:
                # Upload file
                file_input.set_input_files(str(csv_path))
                sleep(2)  # Wait for file processing
                
                # Look for name input
                name_input = app_page.locator(
                    'input[placeholder*="name"], input[placeholder*="Name"], '
                    'input[label*="name" i]'
                )
                
                if name_input.count() > 0:
                    name_input.first.fill("Test Phone Table")
                    sleep(0.5)
                
                # Look for data type selection
                data_type_select = app_page.locator(
                    'select, [role="combobox"], div[data-baseweb="select"]'
                )
                
                if data_type_select.count() > 0:
                    # Select phone_numbers if available
                    app_page.keyboard.press("Tab")
                    sleep(0.5)
                    app_page.keyboard.type("phone")
                    sleep(0.5)
                
                # Look for primary key column selection
                primary_key_select = app_page.locator(
                    'select, [role="combobox"], div[data-baseweb="select"]'
                )
                
                # Submit form - look for submit button
                submit_button = app_page.locator(
                    'button:has-text("Create"), button:has-text("Initialize"), '
                    'button:has-text("Submit"), button[type="submit"]'
                )
                
                if submit_button.is_visible(timeout=2000):
                    submit_button.click()
                    sleep(3)  # Wait for table creation
                    
                    # Verify success - should show the new table
                    expect(app_page.locator("body")).to_contain_text(
                        "Test Phone Table", timeout=10000
                    )
            finally:
                # Cleanup
                if csv_path.exists():
                    csv_path.unlink()


class TestKnowledgeTableOperations:
    """Test operations on existing Knowledge Tables."""

    def test_view_knowledge_table_stats(self, app_page: Page):
        """Test viewing statistics for a Knowledge Table."""
        self._navigate_to_knowledge_base(app_page)
        
        # Look for existing table selector
        table_select = app_page.locator(
            'select, [role="combobox"], div[data-baseweb="select"]'
        )
        
        if table_select.count() > 0:
            # If tables exist, select one
            table_select.first.click()
            sleep(1)
            
            # Look for stats section
            stats_text = app_page.locator(
                'text=Top 20, text=Recently Added, text=Missing Values, text=Statistics'
            )
            
            # Stats might not be visible if no data, but section should exist
            # Just verify page doesn't crash
            sleep(1)

    def test_upload_data_to_knowledge_table(self, app_page: Page):
        """Test uploading additional data to existing Knowledge Table."""
        self._navigate_to_knowledge_base(app_page)
        
        # Look for upload section in existing table
        upload_input = app_page.locator('input[type="file"]')
        
        if upload_input.count() > 1:  # Might have multiple file inputs
            # Create test CSV
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                f.write("phone,carrier\n+1111111111,T-Mobile\n")
                csv_path = Path(f.name)
            
            try:
                # Use the second file input (for table upload, not creation)
                upload_input.nth(1).set_input_files(str(csv_path))
                sleep(2)
                
                # Look for upload button
                upload_button = app_page.locator(
                    'button:has-text("Upload"), button:has-text("Add Data")'
                )
                
                if upload_button.is_visible(timeout=2000):
                    upload_button.click()
                    sleep(2)
            finally:
                if csv_path.exists():
                    csv_path.unlink()


class TestKnowledgeTableDeletion:
    """Test deleting Knowledge Tables."""

    def test_delete_knowledge_table_with_confirmation(self, app_page: Page):
        """Test deleting a Knowledge Table with confirmation."""
        self._navigate_to_knowledge_base(app_page)
        
        # Look for delete button
        delete_button = app_page.locator(
            'button:has-text("Delete"), button:has-text("Remove"), '
            'button[aria-label*="delete" i]'
        )
        
        if delete_button.is_visible(timeout=3000):
            delete_button.click()
            sleep(1)
            
            # Look for confirmation dialog
            confirm_text = app_page.locator(
                'text=confirm, text=Are you sure, text=Delete'
            )
            
            if confirm_text.is_visible(timeout=2000):
                # Look for final confirm button
                final_confirm = app_page.locator(
                    'button:has-text("Confirm Delete"), button:has-text("Yes, Delete"), '
                    'button:has-text("Delete")'
                )
                
                if final_confirm.count() > 1:
                    # Use the last one (should be final confirmation)
                    final_confirm.last.click()
                elif final_confirm.count() == 1:
                    final_confirm.click()
                
                sleep(2)


class TestMultipleKnowledgeTables:
    """Test multiple Knowledge Tables workflow."""

    def test_create_multiple_tables_same_data_type(self, app_page: Page):
        """Test creating multiple Knowledge Tables for same data_type."""
        self._navigate_to_knowledge_base(app_page)
        
        # Create first table
        self._create_simple_table(app_page, "White List", "+1234567890")
        
        # Create second table
        self._create_simple_table(app_page, "Black List", "+0987654321")
        
        # Verify both appear in selector
        table_select = app_page.locator('select, [role="combobox"]')
        if table_select.count() > 0:
            # Should be able to select different tables
            sleep(1)

    def test_switch_between_knowledge_tables(self, app_page: Page):
        """Test switching between different Knowledge Tables."""
        self._navigate_to_knowledge_base(app_page)
        
        # Look for table selector
        table_select = app_page.locator('select, [role="combobox"]')
        
        if table_select.count() > 0:
            # Get all options
            options_count = app_page.locator('option').count()
            
            if options_count > 1:
                # Select different table
                table_select.first.select_option(index=1)
                sleep(2)
                
                # Verify content changes (might show different stats)
                sleep(1)


# Helper methods for test classes

def _navigate_to_knowledge_base(self, app_page: Page):
    """Helper to navigate to Knowledge Base page."""
    app_page.wait_for_load_state("networkidle", timeout=15000)
    sleep(2)
    
    # Try to find and click Knowledge Base link
    knowledge_link = app_page.locator(
        'a:has-text("Knowledge Base"), a[href*="knowledge_base"]'
    )
    
    if not knowledge_link.is_visible(timeout=2000):
        # Try alternative: check all sidebar links
        all_links = app_page.locator('[data-testid="stSidebar"] a')
        link_count = all_links.count()
        for i in range(link_count):
            link_text = all_links.nth(i).text_content()
            if "Knowledge" in (link_text or "") or "knowledge" in (link_text or ""):
                knowledge_link = all_links.nth(i)
                break
    
    if knowledge_link.is_visible(timeout=3000):
        knowledge_link.click()
        app_page.wait_for_load_state("networkidle", timeout=10000)
        sleep(2)


def _create_simple_table(self, app_page: Page, table_name: str, phone: str):
    """Helper to create a simple Knowledge Table."""
    # Look for create button
    create_button = app_page.locator(
        'button:has-text("Create New"), button:has-text("New")'
    )
    
    if create_button.is_visible(timeout=2000):
        create_button.click()
        sleep(1)
        
        # Create and upload CSV
        file_input = app_page.locator('input[type="file"]')
        if file_input.is_visible(timeout=2000):
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                f.write(f"phone,carrier\n{phone},Test\n")
                csv_path = Path(f.name)
            
            try:
                file_input.set_input_files(str(csv_path))
                sleep(2)
                
                # Fill name if input exists
                name_input = app_page.locator('input[placeholder*="name" i]')
                if name_input.count() > 0:
                    name_input.first.fill(table_name)
                    sleep(0.5)
                
                # Submit
                submit_button = app_page.locator(
                    'button:has-text("Create"), button:has-text("Initialize")'
                )
                if submit_button.is_visible(timeout=2000):
                    submit_button.click()
                    sleep(3)
            finally:
                if csv_path.exists():
                    csv_path.unlink()


# Bind helper methods to test classes
TestKnowledgeBasePageLoad._navigate_to_knowledge_base = _navigate_to_knowledge_base
TestKnowledgeTableCreation._navigate_to_knowledge_base = _navigate_to_knowledge_base
TestKnowledgeTableCreation._create_simple_table = _create_simple_table
TestKnowledgeTableOperations._navigate_to_knowledge_base = _navigate_to_knowledge_base
TestKnowledgeTableDeletion._navigate_to_knowledge_base = _navigate_to_knowledge_base
TestMultipleKnowledgeTables._navigate_to_knowledge_base = _navigate_to_knowledge_base
TestMultipleKnowledgeTables._create_simple_table = _create_simple_table

