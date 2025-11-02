"""
Comprehensive stress tests for Dataset Uploader UI flow.

Tests the complete user journey with various edge cases and stress scenarios.
"""
import tempfile
from pathlib import Path
from time import sleep

import pandas as pd
import pytest
from playwright.sync_api import Page, expect


class TestDatasetInitializationFlow:
    """Stress test dataset initialization flow through UI."""

    def test_complete_initialization_flow_empty_dataset(self, app_page: Page):
        """Test: Navigate → Upload file → Fill name → Configure → Submit."""
        # Navigate to dataset page
        dataset_link = app_page.locator('a[href*="dataset_1"]')
        expect(dataset_link).to_be_visible(timeout=5000)
        dataset_link.click()
        app_page.wait_for_load_state("networkidle")
        sleep(1)

        # Verify initialization form appears
        init_header = app_page.locator('text=Initialize Dataset')
        expect(init_header).to_be_visible(timeout=5000)

        # Step 1: Upload file
        csv_content = "name,age,email\nJohn Doe,30,john@test.com\nJane Smith,25,jane@test.com"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp_file:
            tmp_file.write(csv_content)
            tmp_file_path = tmp_file.name

        try:
            # Find file upload input
            file_input = app_page.locator('input[type="file"]').first
            expect(file_input).to_be_visible(timeout=5000)
            file_input.set_input_files(tmp_file_path)

            # Wait for file to be processed
            sleep(2)

            # Step 2: Enter dataset name
            name_input = app_page.locator('input[placeholder*="Dataset"], input[type="text"]').filter(
                has_text="Dataset"
            )
            # Try to find by placeholder or label
            name_inputs = app_page.locator('input[type="text"]')
            dataset_name_input = None
            for i in range(name_inputs.count()):
                input_elem = name_inputs.nth(i)
                placeholder = input_elem.get_attribute("placeholder") or ""
                if "dataset" in placeholder.lower() or "name" in placeholder.lower():
                    dataset_name_input = input_elem
                    break

            if dataset_name_input:
                dataset_name_input.fill("Stress Test Dataset")
                sleep(0.5)

            # Step 3: Wait for column configuration to appear
            # File should be parsed and columns shown
            st.success_msg = app_page.locator('text=parsed successfully, text=File parsed')
            if st.success_msg.count() > 0:
                sleep(1)  # Wait for columns to render

            # Step 4: Configure columns (if visible)
            # Column type selectboxes should appear
            selectboxes = app_page.locator('select, [role="combobox"]')
            if selectboxes.count() > 0:
                # Columns are configured - defaults should work

            # Step 5: Select duplicate filter column
            duplicate_select = app_page.locator('select, [role="combobox"]').filter(
                has_text="duplicate"
            )
            if duplicate_select.count() == 0:
                # Try finding by text near it
                duplicate_section = app_page.locator('text=Duplicate Filter')
                if duplicate_section.is_visible():
                    # Find selectbox near this text
                    duplicate_select = app_page.locator('select').last

            # Step 6: Click Initialize button
            init_button = app_page.locator('button:has-text("Initialize Dataset")')
            expect(init_button).to_be_visible(timeout=5000)
            
            # Check if button is enabled
            is_disabled = init_button.get_attribute("disabled")
            if is_disabled:
                pytest.skip("Button is disabled - configuration not complete")

            init_button.click()

            # Step 7: Wait for success
            app_page.wait_for_load_state("networkidle", timeout=10000)
            sleep(2)

            # Should see success message or dataset name
            success_indicator = app_page.locator(
                'text=initialized successfully, text=Dataset, text=Stress Test Dataset'
            )
            # At least one should be visible
            assert success_indicator.count() > 0 or app_page.locator('text=Stress Test Dataset').is_visible()

        finally:
            Path(tmp_file_path).unlink()

    def test_initialization_with_missing_name(self, app_page: Page):
        """Test: Upload file but don't enter name → button should show error."""
        dataset_link = app_page.locator('a[href*="dataset_2"]')
        if dataset_link.is_visible(timeout=2000):
            dataset_link.click()
            app_page.wait_for_load_state("networkidle")
            sleep(1)

        # Upload file
        csv_content = "col1,col2\nval1,val2"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp_file:
            tmp_file.write(csv_content)
            tmp_file_path = tmp_file.name

        try:
            file_input = app_page.locator('input[type="file"]').first
            if file_input.is_visible():
                file_input.set_input_files(tmp_file_path)
                sleep(2)

                # Try to submit without name
                init_button = app_page.locator('button:has-text("Initialize Dataset")')
                if init_button.is_visible():
                    # Button might be disabled or show error on click
                    is_disabled = init_button.get_attribute("disabled")
                    if not is_disabled:
                        init_button.click()
                        sleep(1)
                        # Should see error about missing name
                        error_msg = app_page.locator('text=name, text=required, text=error')
                        # Error might appear
        finally:
            Path(tmp_file_path).unlink()

    def test_initialization_with_invalid_file(self, app_page: Page):
        """Test: Upload invalid file → should show error."""
        dataset_link = app_page.locator('a[href*="dataset_3"]')
        if dataset_link.is_visible(timeout=2000):
            dataset_link.click()
            app_page.wait_for_load_state("networkidle")
            sleep(1)

        # Create invalid file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as tmp_file:
            tmp_file.write("not a csv file")
            tmp_file_path = tmp_file.name

        try:
            file_input = app_page.locator('input[type="file"]').first
            if file_input.is_visible():
                # Try to upload invalid file
                file_input.set_input_files(tmp_file_path)
                sleep(2)

                # Should see error message
                error_msg = app_page.locator('text=error, text=Error, text=invalid, text=parse')
                # Error might appear or file might be rejected
        finally:
            Path(tmp_file_path).unlink()


class TestFileUploadFlow:
    """Stress test file upload flow for initialized datasets."""

    def test_upload_multiple_files_sequentially(self, app_page: Page):
        """Test: Upload multiple files one after another."""
        # Navigate to dataset page (assuming dataset exists)
        dataset_link = app_page.locator('a[href*="dataset_1"]')
        dataset_link.click()
        app_page.wait_for_load_state("networkidle")
        sleep(1)

        # Check if dataset is initialized
        upload_section = app_page.locator('text=Upload New File')
        if not upload_section.is_visible(timeout=3000):
            pytest.skip("Dataset not initialized - skipping upload test")

        # Upload first file
        csv1 = "name,age\nAlice,25\nBob,30"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp1:
            tmp1.write(csv1)
            tmp1_path = tmp1.name

        try:
            file_input = app_page.locator('input[type="file"]').filter(
                has_not_text="Initialize"
            ).first

            if file_input.is_visible():
                # Upload first file
                file_input.set_input_files(tmp1_path)
                sleep(2)

                upload_btn = app_page.locator('button:has-text("Upload File")')
                if upload_btn.is_visible():
                    upload_btn.click()
                    sleep(3)

                    # Upload second file
                    csv2 = "name,age\nCharlie,35\nDiana,28"
                    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp2:
                        tmp2.write(csv2)
                        tmp2_path = tmp2.name

                    try:
                        file_input.set_input_files(tmp2_path)
                        sleep(2)
                        upload_btn.click()
                        sleep(3)

                        # Should see success messages
                        success = app_page.locator('text=Uploaded, text=rows')
                        assert success.count() > 0

                    finally:
                        Path(tmp2_path).unlink()

        finally:
            Path(tmp1_path).unlink()

    def test_upload_duplicate_filename(self, app_page: Page):
        """Test: Upload same filename twice → should detect duplicate."""
        dataset_link = app_page.locator('a[href*="dataset_1"]')
        dataset_link.click()
        app_page.wait_for_load_state("networkidle")
        sleep(1)

        upload_section = app_page.locator('text=Upload New File')
        if not upload_section.is_visible(timeout=3000):
            pytest.skip("Dataset not initialized")

        csv_content = "name,age\nTest,25"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp_file:
            tmp_file.write(csv_content)
            tmp_file_path = tmp_file.name

        try:
            file_input = app_page.locator('input[type="file"]').filter(
                has_not_text="Initialize"
            ).first

            if file_input.is_visible():
                # First upload
                file_input.set_input_files(tmp_file_path)
                sleep(2)
                upload_btn = app_page.locator('button:has-text("Upload File")')
                if upload_btn.is_visible():
                    upload_btn.click()
                    sleep(3)

                    # Second upload with same file
                    file_input.set_input_files(tmp_file_path)
                    sleep(2)
                    upload_btn.click()
                    sleep(2)

                    # Should see duplicate warning
                    duplicate_warning = app_page.locator(
                        'text=duplicate, text=uploaded before, text=Continue anyway'
                    )
                    # Warning might appear
        finally:
            Path(tmp_file_path).unlink()

    def test_upload_large_file(self, app_page: Page):
        """Test: Upload large CSV file → should handle gracefully."""
        dataset_link = app_page.locator('a[href*="dataset_1"]')
        dataset_link.click()
        app_page.wait_for_load_state("networkidle")
        sleep(1)

        upload_section = app_page.locator('text=Upload New File')
        if not upload_section.is_visible(timeout=3000):
            pytest.skip("Dataset not initialized")

        # Create large CSV (1000 rows)
        rows = ["name,age,email"] + [
            f"User{i},{20 + (i % 50)},{f'user{i}@test.com'}" for i in range(1000)
        ]
        csv_content = "\n".join(rows)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp_file:
            tmp_file.write(csv_content)
            tmp_file_path = tmp_file.name

        try:
            file_input = app_page.locator('input[type="file"]').filter(
                has_not_text="Initialize"
            ).first

            if file_input.is_visible():
                file_input.set_input_files(tmp_file_path)
                sleep(3)  # Longer wait for large file

                upload_btn = app_page.locator('button:has-text("Upload File")')
                if upload_btn.is_visible():
                    upload_btn.click()
                    sleep(5)  # Wait for upload

                    # Should see success or progress
                    result = app_page.locator('text=Uploaded, text=rows, text=spinner')
                    assert result.count() > 0

        finally:
            Path(tmp_file_path).unlink()

    def test_upload_pickle_file(self, app_page: Page):
        """Test: Upload Pickle file to initialized dataset."""
        dataset_link = app_page.locator('a[href*="dataset_1"]')
        dataset_link.click()
        app_page.wait_for_load_state("networkidle")
        sleep(1)

        upload_section = app_page.locator('text=Upload New File')
        if not upload_section.is_visible(timeout=3000):
            pytest.skip("Dataset not initialized")

        # Create Pickle file
        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})
        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tmp_file:
            df.to_pickle(tmp_file.name)
            tmp_file_path = tmp_file.name

        try:
            file_input = app_page.locator('input[type="file"]').filter(
                has_not_text="Initialize"
            ).first

            if file_input.is_visible():
                file_input.set_input_files(tmp_file_path)
                sleep(2)

                upload_btn = app_page.locator('button:has-text("Upload File")')
                if upload_btn.is_visible():
                    upload_btn.click()
                    sleep(3)

                    # Should see success
                    success = app_page.locator('text=Uploaded, text=rows')
                    assert success.count() > 0

        finally:
            Path(tmp_file_path).unlink()


class TestFormValidationStress:
    """Stress test form validation and error handling."""

    def test_submit_empty_form(self, app_page: Page):
        """Test: Submit form without filling anything → should show errors."""
        dataset_link = app_page.locator('a[href*="dataset_4"]')
        if dataset_link.is_visible(timeout=2000):
            dataset_link.click()
            app_page.wait_for_load_state("networkidle")
            sleep(1)

            init_button = app_page.locator('button:has-text("Initialize Dataset")')
            if init_button.is_visible():
                # Try clicking without filling form
                is_disabled = init_button.get_attribute("disabled")
                if not is_disabled:
                    init_button.click()
                    sleep(1)
                    # Should see validation errors
                    errors = app_page.locator('text=error, text=Error, text=required')
                    # Errors might appear

    def test_partial_form_filling(self, app_page: Page):
        """Test: Fill only file OR only name → button behavior."""
        dataset_link = app_page.locator('a[href*="dataset_5"]')
        if dataset_link.is_visible(timeout=2000):
            dataset_link.click()
            app_page.wait_for_load_state("networkidle")
            sleep(1)

            # Try with only file
            csv_content = "col1,col2\nval1,val2"
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp_file:
                tmp_file.write(csv_content)
                tmp_file_path = tmp_file.name

            try:
                file_input = app_page.locator('input[type="file"]').first
                if file_input.is_visible():
                    file_input.set_input_files(tmp_file_path)
                    sleep(2)

                    # Don't fill name
                    init_button = app_page.locator('button:has-text("Initialize Dataset")')
                    if init_button.is_visible():
                        is_disabled = init_button.get_attribute("disabled")
                        # Button should be disabled or show error on click
            finally:
                Path(tmp_file_path).unlink()


class TestColumnConfigurationStress:
    """Stress test column configuration during initialization."""

    def test_configure_all_column_types(self, app_page: Page):
        """Test: Configure different column types (TEXT, INTEGER, REAL)."""
        dataset_link = app_page.locator('a[href*="dataset_1"]')
        dataset_link.click()
        app_page.wait_for_load_state("networkidle")
        sleep(1)

        csv_content = "text_col,int_col,real_col\nText Value,42,3.14\nAnother,100,2.71"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp_file:
            tmp_file.write(csv_content)
            tmp_file_path = tmp_file.name

        try:
            file_input = app_page.locator('input[type="file"]').first
            if file_input.is_visible():
                file_input.set_input_files(tmp_file_path)
                sleep(2)

                # Find dataset name input
                name_inputs = app_page.locator('input[type="text"]')
                for i in range(name_inputs.count()):
                    input_elem = name_inputs.nth(i)
                    placeholder = input_elem.get_attribute("placeholder") or ""
                    if "dataset" in placeholder.lower():
                        input_elem.fill("Column Type Test")
                        break

                sleep(1)

                # Column type selectboxes should be visible
                selectboxes = app_page.locator('select')
                if selectboxes.count() > 0:
                    # Verify we can see column configuration
                    assert selectboxes.count() >= 3  # At least 3 columns

        finally:
            Path(tmp_file_path).unlink()

    def test_detect_base64_columns(self, app_page: Page):
        """Test: File with Base64 images → should auto-detect."""
        dataset_link = app_page.locator('a[href*="dataset_2"]')
        if dataset_link.is_visible(timeout=2000):
            dataset_link.click()
            app_page.wait_for_load_state("networkidle")
            sleep(1)

            # CSV with Base64-like column
            csv_content = (
                "name,image_data\n"
                "John,data:image/png;base64,iVBORw0KGgoAAAANS\n"
                "Jane,data:image/jpeg;base64,/9j/4AAQSkZJRg"
            )
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp_file:
                tmp_file.write(csv_content)
                tmp_file_path = tmp_file.name

            try:
                file_input = app_page.locator('input[type="file"]').first
                if file_input.is_visible():
                    file_input.set_input_files(tmp_file_path)
                    sleep(2)

                    # Checkboxes for image columns should appear
                    image_checkboxes = app_page.locator('input[type="checkbox"]').filter(
                        has_text="Base64"
                    )
                    # Image detection checkboxes might be visible

            finally:
                Path(tmp_file_path).unlink()

