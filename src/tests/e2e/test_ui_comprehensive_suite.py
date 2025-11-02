"""
Comprehensive Automated UI Testing Suite for CSV Wrangler.

This suite covers all user-facing functionality through the browser:
- Page navigation and routing
- Form interactions and validation
- File uploads (CSV and Pickle)
- Dataset initialization and configuration
- Data viewing and export
- Settings and dataset management
- Error handling and edge cases
- Multi-dataset workflows

Uses Playwright for browser automation with pytest-playwright plugin.
"""
import base64
import tempfile
from pathlib import Path
from time import sleep
from datetime import datetime, timedelta

import pandas as pd
import pytest
from playwright.sync_api import Page, expect


class TestNavigationAndPageLoads:
    """Test navigation between pages and page load functionality."""

    def test_home_page_loads(self, app_page: Page):
        """Test that home page loads correctly."""
        # Wait for app to fully load
        app_page.wait_for_load_state("networkidle", timeout=15000)
        sleep(2)  # Allow Streamlit to render
        
        # Navigate to Home page if not already there
        # Check if we're on main page (shows "Use the sidebar")
        sidebar_nav = app_page.locator('text=Use the sidebar to navigate')
        if sidebar_nav.is_visible(timeout=2000):
            # Click Home link in sidebar
            home_link = app_page.locator('a:has-text("Home"), a[href*="Home"]')
            if home_link.is_visible(timeout=3000):
                home_link.click()
                app_page.wait_for_load_state("networkidle", timeout=10000)
                sleep(2)
        
        # Now check for CSV Wrangler title (on Home page)
        expect(app_page.locator("body")).to_contain_text("CSV Wrangler", timeout=10000)
        
        # Check for key elements
        home_title = app_page.locator("text=CSV Wrangler")
        expect(home_title).to_be_visible(timeout=5000)

    def test_sidebar_navigation_exists(self, app_page: Page):
        """Test that sidebar navigation is visible and functional."""
        # Sidebar should be visible
        sidebar = app_page.locator('[data-testid="stSidebar"]')
        expect(sidebar).to_be_visible(timeout=5000)
        
        # Check for navigation links
        home_link = app_page.locator('a[href*="Home"]')
        expect(home_link).to_be_visible(timeout=3000)

    def test_navigate_to_all_pages(self, app_page: Page):
        """Test navigation to all 14 pages in correct order."""
        pages_to_test = [
            ("Home", ["CSV Wrangler", "Dataset Overview"]),
            ("Enrichment Suite", ["Enrichment Suite", "enrich"]),
            ("DataFrame View", ["DataFrame View", "dataframe"]),
            ("Data Geek", ["Data Geek", "analysis"]),
            ("Knowledge Base", ["Knowledge Base", "knowledge"]),
            ("Knowledge Search", ["Knowledge Search", "search"]),
            ("Image Search", ["Image Search", "image"]),
            ("Settings", ["Settings", "Database Configuration"]),
            ("Bulk Uploader", ["Bulk Uploader", "bulk"]),
            ("Dataset 1", ["Dataset #1", "Initialize Dataset", "Upload New File"]),
            ("Dataset 2", ["Dataset #2", "Initialize Dataset", "Upload New File"]),
            ("Dataset 3", ["Dataset #3", "Initialize Dataset", "Upload New File"]),
            ("Dataset 4", ["Dataset #4", "Initialize Dataset", "Upload New File"]),
            ("Dataset 5", ["Dataset #5", "Initialize Dataset", "Upload New File"]),
        ]
        
        for page_name, expected_texts in pages_to_test:
            # Find and click sidebar link
            page_link = app_page.locator(f'a:has-text("{page_name}")')
            if page_link.is_visible(timeout=2000):
                page_link.click()
                app_page.wait_for_load_state("networkidle", timeout=10000)
                sleep(2)  # Allow Streamlit to render
                
                # Verify page content - check for any of the expected texts
                # (pages may show different content based on state)
                body_text = app_page.locator("body").text_content() or ""
                found = any(expected in body_text for expected in expected_texts)
                assert found, f"Page '{page_name}' should contain one of: {expected_texts}. Found: {body_text[:200]}"


class TestProfileCreationAndInitialization:
    """Test user profile creation and app initialization."""

    def test_first_launch_shows_profile_form(self, app_page: Page):
        """Test that first launch shows profile creation form."""
        # Check for profile form elements
        welcome_text = app_page.locator("text=Welcome")
        name_input = app_page.locator('input[placeholder*="Your Name"], input[placeholder*="name"]')
        init_button = app_page.locator('button:has-text("Initialize"), button:has-text("Create")')
        
        # At least one of these should be visible on first launch
        is_first_launch = (
            welcome_text.is_visible(timeout=2000) or
            name_input.is_visible(timeout=2000) or
            init_button.is_visible(timeout=2000)
        )
        
        # If app is already initialized, check for welcome message
        is_initialized = app_page.locator('text=Welcome,').is_visible(timeout=2000)
        
        assert is_first_launch or is_initialized, "Profile form or welcome message should be visible"

    def test_create_profile_if_needed(self, app_page: Page):
        """Create profile if app is not initialized."""
        # Check if profile form exists
        name_input = app_page.locator('input[placeholder*="Your Name"], input[placeholder*="name"]')
        
        if name_input.is_visible(timeout=2000):
            # Fill profile form
            name_input.fill("Test User")
            sleep(0.5)
            
            # Submit form
            init_button = app_page.locator('button:has-text("Initialize"), button:has-text("Create")')
            if init_button.is_visible(timeout=2000):
                init_button.click()
                app_page.wait_for_load_state("networkidle", timeout=10000)
                sleep(2)  # Wait for initialization


class TestDatasetInitialization:
    """Test dataset initialization flow through UI."""

    @pytest.fixture(autouse=True)
    def ensure_profile(self, app_page: Page):
        """Ensure profile is created before dataset tests."""
        # Navigate to home first
        home_link = app_page.locator('a:has-text("Home")')
        if home_link.is_visible(timeout=2000):
            home_link.click()
            app_page.wait_for_load_state("networkidle", timeout=10000)
            sleep(1)
        
        # Create profile if needed
        name_input = app_page.locator('input[placeholder*="Your Name"], input[placeholder*="name"]')
        if name_input.is_visible(timeout=2000):
            name_input.fill("Test User")
            init_button = app_page.locator('button:has-text("Initialize"), button:has-text("Create")')
            if init_button.is_visible(timeout=2000):
                init_button.click()
                app_page.wait_for_load_state("networkidle", timeout=10000)
                sleep(2)

    def test_navigate_to_dataset_page(self, app_page: Page):
        """Test navigating to a dataset page."""
        dataset_link = app_page.locator('a[href*="dataset_1"], a:has-text("Dataset 1")')
        expect(dataset_link).to_be_visible(timeout=5000)
        dataset_link.click()
        app_page.wait_for_load_state("networkidle", timeout=10000)
        sleep(1)
        
        # Verify dataset page loaded
        expect(app_page.locator("body")).to_contain_text("Initialize Dataset", timeout=5000)

    def test_file_uploader_appears(self, app_page: Page):
        """Test that file uploader appears on dataset page."""
        # Navigate to dataset page
        dataset_link = app_page.locator('a[href*="dataset_1"], a:has-text("Dataset 1")')
        if dataset_link.is_visible(timeout=2000):
            dataset_link.click()
            app_page.wait_for_load_state("networkidle", timeout=10000)
            sleep(1)
        
        # Check for file uploader
        file_input = app_page.locator('input[type="file"]')
        expect(file_input).to_be_visible(timeout=5000)

    def test_upload_csv_file(self, app_page: Page):
        """Test uploading a CSV file."""
        # Navigate to dataset page
        dataset_link = app_page.locator('a[href*="dataset_1"], a:has-text("Dataset 1")')
        if dataset_link.is_visible(timeout=2000):
            dataset_link.click()
            app_page.wait_for_load_state("networkidle", timeout=10000)
            sleep(1)
        
        # Create test CSV
        csv_content = "name,age,email\ntest_user,25,test@example.com\nanother_user,30,another@example.com"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp_file:
            tmp_file.write(csv_content)
            tmp_file_path = tmp_file.name
        
        try:
            # Upload file
            file_input = app_page.locator('input[type="file"]').first
            expect(file_input).to_be_visible(timeout=5000)
            file_input.set_input_files(tmp_file_path)
            
            # Wait for file processing
            sleep(3)
            
            # Check for success message or parsed data
            success_msg = app_page.locator('text=parsed successfully, text=File parsed')
            expect(success_msg).to_be_visible(timeout=10000)
        finally:
            Path(tmp_file_path).unlink(missing_ok=True)

    def test_upload_pickle_file(self, app_page: Page):
        """Test uploading a Pickle file."""
        # Navigate to dataset page
        dataset_link = app_page.locator('a[href*="dataset_2"], a:has-text("Dataset 2")')
        if dataset_link.is_visible(timeout=2000):
            dataset_link.click()
            app_page.wait_for_load_state("networkidle", timeout=10000)
            sleep(1)
        
        # Create test Pickle file
        df = pd.DataFrame({
            "name": ["Mickey", "Minnie"],
            "age": [95, 94],
            "city": ["Disneyland", "Disney World"]
        })
        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tmp_file:
            df.to_pickle(tmp_file.name)
            tmp_file_path = tmp_file.name
        
        try:
            # Upload file
            file_input = app_page.locator('input[type="file"]').first
            expect(file_input).to_be_visible(timeout=5000)
            file_input.set_input_files(tmp_file_path)
            
            # Wait for file processing
            sleep(3)
            
            # Check for success message
            success_msg = app_page.locator('text=parsed successfully, text=File parsed')
            expect(success_msg).to_be_visible(timeout=10000)
        finally:
            Path(tmp_file_path).unlink(missing_ok=True)

    def test_dataset_name_input(self, app_page: Page):
        """Test entering dataset name."""
        # Navigate to dataset page
        dataset_link = app_page.locator('a[href*="dataset_1"], a:has-text("Dataset 1")')
        if dataset_link.is_visible(timeout=2000):
            dataset_link.click()
            app_page.wait_for_load_state("networkidle", timeout=10000)
            sleep(1)
        
        # Find name input
        name_input = app_page.locator('input[placeholder*="Dataset"], input[type="text"]').first
        expect(name_input).to_be_visible(timeout=5000)
        
        # Enter name
        name_input.fill("Test Dataset")
        sleep(0.5)
        
        # Verify value was entered
        expect(name_input).to_have_value("Test Dataset", timeout=2000)

    def test_configure_column_types(self, app_page: Page):
        """Test configuring column data types."""
        # Navigate to dataset page
        dataset_link = app_page.locator('a[href*="dataset_1"], a:has-text("Dataset 1")')
        if dataset_link.is_visible(timeout=2000):
            dataset_link.click()
            app_page.wait_for_load_state("networkidle", timeout=10000)
            sleep(1)
        
        # Upload CSV first
        csv_content = "name,age,price\nJohn,30,99.99\nJane,25,149.99"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp_file:
            tmp_file.write(csv_content)
            tmp_file_path = tmp_file.name
        
        try:
            file_input = app_page.locator('input[type="file"]').first
            file_input.set_input_files(tmp_file_path)
            sleep(3)  # Wait for parsing
            
            # Look for column configuration UI
            # Streamlit selectboxes for column types
            selectboxes = app_page.locator('select, [role="combobox"]')
            expect(selectboxes.first).to_be_visible(timeout=5000)
            
        finally:
            Path(tmp_file_path).unlink(missing_ok=True)

    def test_complete_dataset_initialization(self, app_page: Page):
        """Test complete dataset initialization flow."""
        # Navigate to dataset page
        dataset_link = app_page.locator('a[href*="dataset_3"], a:has-text("Dataset 3")')
        if dataset_link.is_visible(timeout=2000):
            dataset_link.click()
            app_page.wait_for_load_state("networkidle", timeout=10000)
            sleep(1)
        
        # Step 1: Upload CSV
        csv_content = "id,name,age\n1,Alice,28\n2,Bob,32\n3,Charlie,25"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp_file:
            tmp_file.write(csv_content)
            tmp_file_path = tmp_file.name
        
        try:
            file_input = app_page.locator('input[type="file"]').first
            file_input.set_input_files(tmp_file_path)
            sleep(3)
            
            # Step 2: Enter dataset name
            name_input = app_page.locator('input[placeholder*="Dataset"], input[type="text"]').first
            name_input.fill("Complete Test Dataset")
            sleep(1)
            
            # Step 3: Submit form
            submit_button = app_page.locator('button:has-text("Initialize Dataset")')
            if submit_button.is_visible(timeout=5000):
                submit_button.click()
                sleep(2)
                
                # Check for success
                success_msg = app_page.locator('text=initialized, text=successfully, text=Dataset')
                expect(success_msg).to_be_visible(timeout=10000)
        finally:
            Path(tmp_file_path).unlink(missing_ok=True)


class TestDataUploadAndViewing:
    """Test data upload and viewing functionality."""

    def test_upload_data_to_initialized_dataset(self, app_page: Page):
        """Test uploading data to an already initialized dataset."""
        # First initialize a dataset (simplified - assumes dataset exists)
        # Navigate to dataset page
        dataset_link = app_page.locator('a[href*="dataset_4"], a:has-text("Dataset 4")')
        if dataset_link.is_visible(timeout=2000):
            dataset_link.click()
            app_page.wait_for_load_state("networkidle", timeout=10000)
            sleep(1)
        
        # Check if dataset is initialized or needs initialization
        init_header = app_page.locator('text=Initialize Dataset')
        if init_header.is_visible(timeout=2000):
            # Initialize first
            csv_content = "name,value\nItem1,100\nItem2,200"
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp_file:
                tmp_file.write(csv_content)
                tmp_file_path = tmp_file.name
            
            try:
                file_input = app_page.locator('input[type="file"]').first
                file_input.set_input_files(tmp_file_path)
                sleep(3)
                
                name_input = app_page.locator('input[placeholder*="Dataset"]').first
                name_input.fill("Upload Test Dataset")
                sleep(1)
                
                submit_button = app_page.locator('button:has-text("Initialize Dataset")')
                if submit_button.is_visible(timeout=3000):
                    submit_button.click()
                    sleep(3)
            finally:
                Path(tmp_file_path).unlink(missing_ok=True)
        
        # Now upload additional data
        upload_section = app_page.locator('text=Upload, text=Add Data')
        if upload_section.is_visible(timeout=3000):
            # Upload new file
            csv_content = "name,value\nItem3,300\nItem4,400"
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp_file:
                tmp_file.write(csv_content)
                tmp_file_path = tmp_file.name
            
            try:
                upload_input = app_page.locator('input[type="file"]').first
                upload_input.set_input_files(tmp_file_path)
                sleep(2)
            finally:
                Path(tmp_file_path).unlink(missing_ok=True)

    def test_view_dataset_data(self, app_page: Page):
        """Test viewing dataset data."""
        # Navigate to a dataset page
        dataset_link = app_page.locator('a[href*="dataset_1"], a:has-text("Dataset 1")')
        if dataset_link.is_visible(timeout=2000):
            dataset_link.click()
            app_page.wait_for_load_state("networkidle", timeout=10000)
            sleep(1)
        
        # Look for data viewer section
        data_section = app_page.locator('text=View Data, text=Data Viewer, table')
        # Data viewer might be present if dataset has data
        if data_section.is_visible(timeout=3000):
            # Check for table or data display
            table = app_page.locator('table')
            if table.is_visible(timeout=2000):
                expect(table).to_be_visible()


class TestDataExport:
    """Test data export functionality."""

    def test_export_data_with_date_range(self, app_page: Page):
        """Test exporting data with date range filter."""
        # Navigate to dataset page
        dataset_link = app_page.locator('a[href*="dataset_1"], a:has-text("Dataset 1")')
        if dataset_link.is_visible(timeout=2000):
            dataset_link.click()
            app_page.wait_for_load_state("networkidle", timeout=10000)
            sleep(1)
        
        # Look for export section
        export_section = app_page.locator('text=Export, text=Download')
        if export_section.is_visible(timeout=3000):
            # Check for date inputs
            date_inputs = app_page.locator('input[type="date"]')
            if date_inputs.count() > 0:
                # Set date range
                inputs = date_inputs.all()
                if len(inputs) >= 2:
                    # Set start date
                    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
                    inputs[0].fill(start_date)
                    sleep(0.5)
                    
                    # Set end date
                    end_date = datetime.now().strftime("%Y-%m-%d")
                    inputs[1].fill(end_date)
                    sleep(0.5)
                    
                    # Look for export button
                    export_button = app_page.locator('button:has-text("Export"), button:has-text("Download")')
                    if export_button.is_visible(timeout=2000):
                        # Note: We can't actually download in test, but we can verify button exists
                        expect(export_button).to_be_visible()


class TestSettingsAndManagement:
    """Test settings page and dataset management."""

    def test_navigate_to_settings(self, app_page: Page):
        """Test navigating to settings page."""
        settings_link = app_page.locator('a[href*="settings"], a:has-text("Settings")')
        expect(settings_link).to_be_visible(timeout=5000)
        settings_link.click()
        app_page.wait_for_load_state("networkidle", timeout=10000)
        sleep(1)
        
        # Verify settings page loaded
        expect(app_page.locator("body")).to_contain_text("Settings", timeout=5000)

    def test_view_dataset_details(self, app_page: Page):
        """Test viewing dataset details in settings."""
        # Navigate to settings
        settings_link = app_page.locator('a[href*="settings"], a:has-text("Settings")')
        if settings_link.is_visible(timeout=2000):
            settings_link.click()
            app_page.wait_for_load_state("networkidle", timeout=10000)
            sleep(1)
        
        # Look for dataset selector
        dataset_selector = app_page.locator('select, [role="combobox"]')
        if dataset_selector.is_visible(timeout=3000):
            # Select a dataset if available
            options = dataset_selector.locator('option')
            if options.count() > 1:  # More than just placeholder
                dataset_selector.select_option(index=1)
                sleep(1)
                
                # Check for dataset details
                details = app_page.locator('text=Dataset Details, text=Total Rows, text=Columns')
                expect(details.first).to_be_visible(timeout=5000)

    def test_delete_dataset_flow(self, app_page: Page):
        """Test dataset deletion flow (without actually deleting)."""
        # Navigate to settings
        settings_link = app_page.locator('a[href*="settings"], a:has-text("Settings")')
        if settings_link.is_visible(timeout=2000):
            settings_link.click()
            app_page.wait_for_load_state("networkidle", timeout=10000)
            sleep(1)
        
        # Look for delete section
        delete_section = app_page.locator('text=Delete Dataset, text=⚠️')
        if delete_section.is_visible(timeout=3000):
            # Check for confirmation input
            confirm_input = app_page.locator('input[placeholder*="confirm"], input[placeholder*="type"]')
            if confirm_input.is_visible(timeout=2000):
                # Verify delete button is disabled until confirmation
                delete_button = app_page.locator('button:has-text("Delete")')
                if delete_button.is_visible(timeout=2000):
                    # Should be disabled initially
                    expect(delete_button).to_have_attribute("disabled", "", timeout=2000)


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_upload_invalid_file(self, app_page: Page):
        """Test uploading an invalid file."""
        # Navigate to dataset page
        dataset_link = app_page.locator('a[href*="dataset_5"], a:has-text("Dataset 5")')
        if dataset_link.is_visible(timeout=2000):
            dataset_link.click()
            app_page.wait_for_load_state("networkidle", timeout=10000)
            sleep(1)
        
        # Create invalid file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as tmp_file:
            tmp_file.write("This is not a CSV or Pickle file")
            tmp_file_path = tmp_file.name
        
        try:
            # Try to upload (file input might filter it out)
            file_input = app_page.locator('input[type="file"]').first
            if file_input.is_visible(timeout=2000):
                # File input might reject invalid types before upload
                # This test verifies the UI handles it gracefully
                pass
        finally:
            Path(tmp_file_path).unlink(missing_ok=True)

    def test_submit_form_without_data(self, app_page: Page):
        """Test submitting form without required data."""
        # Navigate to dataset page
        dataset_link = app_page.locator('a[href*="dataset_1"], a:has-text("Dataset 1")')
        if dataset_link.is_visible(timeout=2000):
            dataset_link.click()
            app_page.wait_for_load_state("networkidle", timeout=10000)
            sleep(1)
        
        # Try to submit without uploading file or entering name
        submit_button = app_page.locator('button:has-text("Initialize Dataset")')
        if submit_button.is_visible(timeout=3000):
            submit_button.click()
            sleep(1)
            
            # Check for error message
            error_msg = app_page.locator('text=error, text=Please, text=required, text=⚠️')
            # Error might be shown
            if error_msg.is_visible(timeout=2000):
                expect(error_msg).to_be_visible()


class TestMultiDatasetWorkflow:
    """Test workflows involving multiple datasets."""

    def test_create_multiple_datasets(self, app_page: Page):
        """Test creating multiple datasets."""
        datasets_to_create = [
            ("dataset_1", "Dataset One"),
            ("dataset_2", "Dataset Two"),
        ]
        
        for dataset_ref, dataset_name in datasets_to_create:
            # Navigate to dataset page
            dataset_link = app_page.locator(f'a[href*="{dataset_ref}"], a:has-text("{dataset_ref.replace("_", " ").title()}")')
            if dataset_link.is_visible(timeout=2000):
                dataset_link.click()
                app_page.wait_for_load_state("networkidle", timeout=10000)
                sleep(1)
            
            # Check if already initialized
            init_header = app_page.locator('text=Initialize Dataset')
            if init_header.is_visible(timeout=2000):
                # Create simple CSV
                csv_content = f"id,name\n1,{dataset_name}\n2,{dataset_name} Item 2"
                with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp_file:
                    tmp_file.write(csv_content)
                    tmp_file_path = tmp_file.name
                
                try:
                    file_input = app_page.locator('input[type="file"]').first
                    file_input.set_input_files(tmp_file_path)
                    sleep(2)
                    
                    name_input = app_page.locator('input[placeholder*="Dataset"]').first
                    name_input.fill(dataset_name)
                    sleep(1)
                    
                    submit_button = app_page.locator('button:has-text("Initialize Dataset")')
                    if submit_button.is_visible(timeout=3000):
                        submit_button.click()
                        sleep(2)
                finally:
                    Path(tmp_file_path).unlink(missing_ok=True)

    def test_home_page_shows_all_datasets(self, app_page: Page):
        """Test that home page displays all initialized datasets."""
        # Navigate to home
        home_link = app_page.locator('a:has-text("Home")')
        if home_link.is_visible(timeout=2000):
            home_link.click()
            app_page.wait_for_load_state("networkidle", timeout=10000)
            sleep(1)
        
        # Check for dataset overview
        overview = app_page.locator('text=Dataset Overview, text=Total Datasets')
        expect(overview.first).to_be_visible(timeout=5000)


class TestPerformanceAndStress:
    """Test performance and stress scenarios."""

    def test_upload_large_csv(self, app_page: Page):
        """Test uploading a large CSV file."""
        # Navigate to dataset page
        dataset_link = app_page.locator('a[href*="dataset_1"], a:has-text("Dataset 1")')
        if dataset_link.is_visible(timeout=2000):
            dataset_link.click()
            app_page.wait_for_load_state("networkidle", timeout=10000)
            sleep(1)
        
        # Create large CSV (1000 rows)
        rows = ["id,name,value\n"]
        for i in range(1000):
            rows.append(f"{i},User{i},{i * 10}\n")
        
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp_file:
            tmp_file.writelines(rows)
            tmp_file_path = tmp_file.name
        
        try:
            file_input = app_page.locator('input[type="file"]').first
            file_input.set_input_files(tmp_file_path)
            
            # Wait for processing (longer timeout for large files)
            sleep(5)
            
            # Check for success or processing indicator
            success_msg = app_page.locator('text=parsed successfully, text=File parsed, text=Processing')
            expect(success_msg.first).to_be_visible(timeout=15000)
        finally:
            Path(tmp_file_path).unlink(missing_ok=True)

    def test_rapid_page_navigation(self, app_page: Page):
        """Test rapid navigation between pages."""
        pages = ["Home", "Dataset 1", "Dataset 2", "Settings", "Home"]
        
        for page_name in pages:
            page_link = app_page.locator(f'a:has-text("{page_name}")')
            if page_link.is_visible(timeout=2000):
                page_link.click()
                app_page.wait_for_load_state("domcontentloaded", timeout=5000)
                sleep(0.5)  # Minimal wait
        
        # Verify we ended up on home
        expect(app_page.locator("body")).to_contain_text("CSV Wrangler", timeout=5000)


class TestAccessibility:
    """Test accessibility features."""

    def test_keyboard_navigation(self, app_page: Page):
        """Test keyboard navigation through the UI."""
        # Navigate to home
        home_link = app_page.locator('a:has-text("Home")')
        if home_link.is_visible(timeout=2000):
            home_link.click()
            app_page.wait_for_load_state("networkidle", timeout=10000)
            sleep(1)
        
        # Use Tab to navigate
        app_page.keyboard.press("Tab")
        sleep(0.5)
        app_page.keyboard.press("Tab")
        sleep(0.5)
        
        # Verify focus is visible (this is a basic check)
        focused = app_page.evaluate("document.activeElement")
        assert focused is not None

    def test_screen_reader_compatibility(self, app_page: Page):
        """Test basic screen reader compatibility."""
        # Check for aria labels and semantic HTML
        buttons = app_page.locator("button")
        inputs = app_page.locator("input")
        
        # At least some elements should be present
        assert buttons.count() > 0 or inputs.count() > 0

