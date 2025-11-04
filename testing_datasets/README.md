# Testing Datasets Directory

This directory contains comprehensive test data for CSV Wrangler testing.

## Directory Structure

```
testing_datasets/
├── datasets/          # 5 datasets, 2 CSV files each (10 files total)
├── knowledge_base/      # Knowledge Base datasets (3 files)
├── images/            # Dataset with Base64 encoded images (2 files)
└── README.md          # This file
```

## Usage

### Datasets (5 datasets × 2 files = 10 CSV files)

Each dataset has 2 CSV files with different data:

- **Dataset 1** (Customer Data):
  - `dataset_1_customers_1.csv` - 8 records
  - `dataset_1_customers_2.csv` - 8 records
  - Columns: name, email, phone, company, join_date

- **Dataset 2** (Product Data):
  - `dataset_2_products_1.csv` - 8 records
  - `dataset_2_products_2.csv` - 8 records
  - Columns: product_name, description, price, category, stock_quantity

- **Dataset 3** (Employee Data):
  - `dataset_3_employees_1.csv` - 8 records
  - `dataset_3_employees_2.csv` - 8 records
  - Columns: employee_name, department, salary, hire_date, email

- **Dataset 4** (Sales Data):
  - `dataset_4_sales_1.csv` - 10 records
  - `dataset_4_sales_2.csv` - 10 records
  - Columns: sale_date, amount, region, customer_id, product_id

- **Dataset 5** (Inventory Data):
  - `dataset_5_inventory_1.csv` - 8 records
  - `dataset_5_inventory_2.csv` - 8 records
  - Columns: item_name, quantity, location, warehouse, last_restocked

### Knowledge Base (3 files)

Knowledge Base datasets for linking enriched data:

- **emails.csv**: 
  - 15 email addresses with metadata
  - Columns: email, company, status, verified_date, notes
  - Use with: primary_key_column = "email", data_type = "emails"

- **phone_numbers.csv**: 
  - 15 phone numbers with metadata
  - Columns: phone, owner, company, status, verified_date, country_code
  - Use with: primary_key_column = "phone", data_type = "phone_numbers"

- **web_domains.csv**: 
  - 15 web domains with metadata
  - Columns: domain, company, website_url, status, verified_date, ssl_enabled
  - Use with: primary_key_column = "domain", data_type = "web_domains"

### Images Dataset (2 files)

Dataset with Base64 encoded images for testing image column detection:

- **products_with_images_1.csv** - 4 records
- **products_with_images_2.csv** - 4 records
- Columns: product_name, description, price, image_data, thumbnail
- Both `image_data` and `thumbnail` contain Base64 encoded images
- Use these columns as `image_columns` when initializing the dataset

## Loading Test Data

### Load Datasets:

1. Initialize each dataset in the app (Dataset 1-5)
2. Upload the corresponding CSV files from `datasets/` directory

### Load Knowledge Base:

1. Go to Knowledge Base page
2. Create Knowledge Tables:
   - **Emails**: Use `knowledge_base/emails.csv`, primary_key_column = "email", data_type = "emails"
   - **Phone Numbers**: Use `knowledge_base/phone_numbers.csv`, primary_key_column = "phone", data_type = "phone_numbers"
   - **Web Domains**: Use `knowledge_base/web_domains.csv`, primary_key_column = "domain", data_type = "web_domains"

### Load Images Dataset:

1. Initialize a dataset slot
2. Upload CSV files from `images/` directory
3. The app will automatically detect Base64 encoded image columns

## Test Scenarios Covered

- Multiple dataset uploads
- Enriched dataset creation and sync
- Knowledge Base linking
- Image column detection and display
- Duplicate file handling
- Data enrichment workflows
- Cross-dataset relationships

