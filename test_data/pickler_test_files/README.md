# Pickler Test Files

This directory contains test pickle files for testing the Pickler page functionality.

## Generated Test Files

### 1. `simple_data.pkl`
- **Description**: Simple DataFrame without any date columns
- **Rows**: 5
- **Columns**: 5 (name, age, email, city, salary)
- **Use Case**: Test basic column selection and filtering without date functionality

### 2. `with_single_date.pkl`
- **Description**: DataFrame with a single date column (`purchase_date`)
- **Rows**: 5
- **Columns**: 5 (product_name, price, quantity, purchase_date, category)
- **Use Case**: Test date range filtering with a single date column

### 3. `with_multiple_dates.pkl`
- **Description**: DataFrame with multiple date columns (`created_date`, `updated_date`)
- **Rows**: 5
- **Columns**: 6 (order_id, customer_name, created_date, updated_date, total_amount, status)
- **Use Case**: Test date column selector when multiple date columns are detected

### 4. `large_dataset.pkl`
- **Description**: Large DataFrame with 1000 rows
- **Rows**: 1000
- **Columns**: 5 (id, name, age, score, active)
- **Use Case**: Test performance with larger datasets and verify pagination/preview works correctly

### 5. `with_datetime_type.pkl`
- **Description**: DataFrame with a datetime-typed column (not string)
- **Rows**: 10
- **Columns**: 4 (event_name, event_datetime, attendees, venue)
- **Use Case**: Test date detection with pandas datetime dtype columns

### 6. `mixed_types_with_dates.pkl`
- **Description**: DataFrame with mixed data types (int, string, float, bool, datetime)
- **Rows**: 10
- **Columns**: 7 (id, name, price, quantity, in_stock, created_date, category)
- **Use Case**: Test filtering with various data types and ensure data types are preserved

### 7. `date_column_names.pkl`
- **Description**: DataFrame with date-like column names (`created_at`, `last_login`)
- **Rows**: 5
- **Columns**: 5 (user_id, username, created_at, last_login, email)
- **Use Case**: Test date column detection based on column name patterns

### 8. `minimal.pkl`
- **Description**: Minimal DataFrame with just 2 rows and 2 columns
- **Rows**: 2
- **Columns**: 2 (name, value)
- **Use Case**: Test edge cases with very small datasets

### 9. `list_of_dicts.pkl`
- **Description**: Pickle file containing a list of dictionaries (not a DataFrame)
- **Format**: List of 4 dictionaries
- **Use Case**: Test that the pickler service can handle different pickle formats (list of dicts)

### 10. `many_columns.pkl`
- **Description**: DataFrame with many columns (20 columns)
- **Rows**: 10
- **Columns**: 20 (col_1 through col_20)
- **Use Case**: Test column selection UI with many columns and verify scrolling/selection works

## How to Use

1. **Start the Streamlit app**:
   ```bash
   streamlit run src/main.py
   ```

2. **Navigate to Pickler page**:
   - Go to `http://localhost:8501/15_pickler`
   - Or use the sidebar navigation

3. **Upload a test file**:
   - Click "Upload Pickle File"
   - Select one of the test files from `test_data/pickler_test_files/`
   - Verify the file information displays correctly

4. **Test different scenarios**:
   - **No dates**: Use `simple_data.pkl` to test column-only filtering
   - **Single date**: Use `with_single_date.pkl` to test date range filtering
   - **Multiple dates**: Use `with_multiple_dates.pkl` to test date column selector
   - **Large file**: Use `large_dataset.pkl` to test performance
   - **Many columns**: Use `many_columns.pkl` to test column selection UI

## Test Workflows

### Basic Column Filtering
1. Upload `simple_data.pkl`
2. Deselect some columns
3. Export and verify only selected columns are in the output

### Date Range Filtering
1. Upload `with_single_date.pkl`
2. Enable date range filter
3. Set start and end dates
4. Export and verify rows are filtered correctly

### Combined Filtering
1. Upload `mixed_types_with_dates.pkl`
2. Select specific columns
3. Apply date range filter
4. Export and verify both filters are applied

### Large File Handling
1. Upload `large_dataset.pkl`
2. Verify file loads without errors
3. Test column selection
4. Export and verify file size is reasonable

## Regenerating Test Files

To regenerate all test files, run:

```bash
python test_data/scripts/generate_pickler_test_files.py
```

This will recreate all test files in this directory.

