# Loading Guide for Test Datasets

This guide explains how to load the test datasets into CSV Wrangler.

## Step-by-Step Loading Instructions

### 1. Load Regular Datasets (1-5)

For each dataset slot (1-5):

1. Navigate to the corresponding Dataset page (Dataset 1, Dataset 2, etc.)
2. Click "Initialize Dataset"
3. Upload the first CSV file (e.g., `dataset_1_customers_1.csv`)
4. The app will automatically detect columns and types
5. Upload the second CSV file (e.g., `dataset_1_customers_2.csv`)

**Recommended Loading Order:**
- Dataset 1: `testing_datasets/datasets/dataset_1_customers_1.csv` then `dataset_1_customers_2.csv`
- Dataset 2: `testing_datasets/datasets/dataset_2_products_1.csv` then `dataset_2_products_2.csv`
- Dataset 3: `testing_datasets/datasets/dataset_3_employees_1.csv` then `dataset_3_employees_2.csv`
- Dataset 4: `testing_datasets/datasets/dataset_4_sales_1.csv` then `dataset_4_sales_2.csv`
- Dataset 5: `testing_datasets/datasets/dataset_5_inventory_1.csv` then `dataset_5_inventory_2.csv`

### 2. Load Knowledge Base Tables

Navigate to the **Knowledge Base** page:

#### Create Emails Knowledge Table:
1. Click "Create Knowledge Table"
2. Name: "Test Emails"
3. Data Type: **emails**
4. Primary Key Column: **email**
5. Upload: `testing_datasets/knowledge_base/emails.csv`
6. The app will auto-detect columns

#### Create Phone Numbers Knowledge Table:
1. Click "Create Knowledge Table"
2. Name: "Test Phone Numbers"
3. Data Type: **phone_numbers**
4. Primary Key Column: **phone**
5. Upload: `testing_datasets/knowledge_base/phone_numbers.csv`

#### Create Web Domains Knowledge Table:
1. Click "Create Knowledge Table"
2. Name: "Test Web Domains"
3. Data Type: **web_domains**
4. Primary Key Column: **domain**
5. Upload: `testing_datasets/knowledge_base/web_domains.csv`

### 3. Load Images Dataset

1. Choose an available dataset slot (e.g., Dataset 1)
2. Initialize the dataset
3. Upload: `testing_datasets/images/products_with_images_1.csv`
4. **Important**: When initializing, mark `image_data` and `thumbnail` as image columns
5. Upload: `testing_datasets/images/products_with_images_2.csv`

The app will automatically detect Base64 encoded images and enable image viewing.

## Testing Scenarios

After loading all datasets, you can test:

1. **Enriched Dataset Creation**: 
   - Create enriched datasets from Dataset 1 (customers) using email/phone enrichment
   - Verify sync works when uploading new files

2. **Knowledge Base Linking**:
   - Search for email addresses from enriched datasets in Knowledge Base
   - Verify cross-referencing works

3. **Image Detection**:
   - View images in the Images dataset
   - Test image search functionality

4. **Bulk Upload**:
   - Use Bulk Uploader to upload multiple files at once
   - Test duplicate detection

5. **Data Analysis**:
   - Create analyses on loaded datasets
   - Test different analysis types

6. **Export**:
   - Export datasets with filters
   - Test date range filtering

## Notes

- All CSV files use UTF-8 encoding
- Dates are in YYYY-MM-DD format
- Phone numbers include country codes where applicable
- Email addresses are valid format
- Base64 images are minimal valid images for testing

