"""
Generate test datasets for Data Geek page testing.

Creates varied CSV and Pickle files with different data types and formats.
"""
import pickle
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import faker

# Initialize Faker for realistic data generation
fake = faker.Faker()

# Ensure test_data directory exists (one level up from scripts/)
script_dir = Path(__file__).parent
TEST_DATA_DIR = script_dir.parent
TEST_DATA_DIR.mkdir(exist_ok=True)


def generate_phone_formats():
    """Generate phone numbers in various formats."""
    formats = [
        lambda: f"+1-{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}",
        lambda: f"({random.randint(200, 999)}) {random.randint(200, 999)}-{random.randint(1000, 9999)}",
        lambda: f"{random.randint(200, 999)}.{random.randint(200, 999)}.{random.randint(1000, 9999)}",
        lambda: f"+44 {random.randint(20, 99)} {random.randint(1000, 9999)} {random.randint(1000, 9999)}",
        lambda: f"{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}",
        lambda: f"1.{random.randint(200, 999)}.{random.randint(200, 999)}.{random.randint(1000, 9999)}",
    ]
    return random.choice(formats)()


def generate_email_formats():
    """Generate emails in various formats."""
    domains = ["gmail.com", "yahoo.com", "company.com", "example.org", "test.net", "mail.io"]
    formats = [
        lambda: f"{fake.user_name()}@{random.choice(domains)}",
        lambda: f"{fake.first_name().lower()}.{fake.last_name().lower()}@{random.choice(domains)}",
        lambda: f"{fake.first_name().lower()}{random.randint(100, 999)}@{random.choice(domains)}",
        lambda: f"{fake.company().lower().replace(' ', '')}@{random.choice(domains)}",
    ]
    return random.choice(formats)()


def generate_url_formats():
    """Generate URLs in various formats."""
    formats = [
        lambda: f"https://www.{fake.domain_name()}",
        lambda: f"http://{fake.domain_name()}",
        lambda: f"https://{fake.domain_name()}/{fake.uri_path()}",
        lambda: f"https://{fake.domain_name()}/{fake.uri_path()}?{fake.uri_extension()}",
        lambda: f"www.{fake.domain_name()}",
        lambda: f"{fake.domain_name()}",
    ]
    return random.choice(formats)()


def generate_dataset_1():
    """Generate Dataset 1: Sales & Customer Data (50,000 rows)"""
    print("Generating Dataset 1: Sales & Customer Data (50,000 rows)...")
    
    rows = []
    categories = ["Electronics", "Clothing", "Food", "Books", "Toys", "Home", "Sports", "Automotive"]
    regions = ["North", "South", "East", "West", "Central"]
    
    for i in range(50000):
        purchase_date = fake.date_between(start_date="-2y", end_date="today")
        rows.append({
            "customer_id": f"CUST-{random.randint(1000, 99999):05d}",
            "customer_name": fake.name(),
            "email": generate_email_formats(),
            "phone": generate_phone_formats(),
            "category": random.choice(categories),
            "product_name": fake.catch_phrase(),
            "quantity": random.randint(1, 50),
            "unit_price": round(random.uniform(10.0, 1000.0), 2),
            "total_amount": 0,  # Will calculate
            "purchase_date": purchase_date,
            "region": random.choice(regions),
            "website": generate_url_formats(),
            "discount_percent": round(random.uniform(0, 30), 2),
            "payment_method": random.choice(["Credit Card", "Debit Card", "PayPal", "Cash", "Bank Transfer"]),
            "shipping_address": fake.address().replace("\n", ", "),
        })
    
    df = pd.DataFrame(rows)
    df["total_amount"] = df["quantity"] * df["unit_price"] * (1 - df["discount_percent"] / 100)
    df["total_amount"] = df["total_amount"].round(2)
    
    # Reorder columns for variety
    columns_order = [
        "customer_id", "customer_name", "email", "phone", "purchase_date",
        "category", "product_name", "quantity", "unit_price", "discount_percent",
        "total_amount", "region", "website", "payment_method", "shipping_address"
    ]
    df = df[columns_order]
    
    file_path = TEST_DATA_DIR / "dataset_1_sales_customers.csv"
    df.to_csv(file_path, index=False)
    print(f"[OK] Created {file_path} with {len(df)} rows")
    return file_path


def generate_dataset_2():
    """Generate Dataset 2: Employee & Performance Data (75,000 rows)"""
    print("Generating Dataset 2: Employee & Performance Data (75,000 rows)...")
    
    rows = []
    departments = ["Engineering", "Sales", "Marketing", "HR", "Finance", "Operations", "IT", "Support"]
    job_titles = ["Manager", "Developer", "Analyst", "Designer", "Coordinator", "Specialist", "Director", "Consultant"]
    
    for i in range(75000):
        hire_date = fake.date_between(start_date="-5y", end_date="-1y")
        review_date = fake.date_between(start_date=hire_date, end_date="today")
        rows.append({
            "employee_id": f"EMP-{random.randint(10000, 99999):05d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "work_email": generate_email_formats(),
            "personal_email": generate_email_formats(),
            "mobile_phone": generate_phone_formats(),
            "office_phone": generate_phone_formats(),
            "department": random.choice(departments),
            "job_title": random.choice(job_titles),
            "hire_date": hire_date,
            "salary": random.randint(40000, 150000),
            "bonus": round(random.uniform(0, 20000), 2),
            "performance_score": round(random.uniform(1.0, 5.0), 2),
            "review_date": review_date,
            "years_experience": random.randint(0, 20),
            "training_hours": random.randint(0, 200),
            "projects_completed": random.randint(0, 50),
            "linkedin_url": generate_url_formats(),
            "github_url": generate_url_formats(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "zip_code": fake.zipcode(),
        })
    
    df = pd.DataFrame(rows)
    
    # Reorder columns differently
    columns_order = [
        "employee_id", "first_name", "last_name", "department", "job_title",
        "hire_date", "salary", "bonus", "performance_score", "review_date",
        "work_email", "personal_email", "mobile_phone", "office_phone",
        "years_experience", "training_hours", "projects_completed",
        "linkedin_url", "github_url", "city", "state", "zip_code"
    ]
    df = df[columns_order]
    
    file_path = TEST_DATA_DIR / "dataset_2_employees_performance.csv"
    df.to_csv(file_path, index=False)
    print(f"[OK] Created {file_path} with {len(df)} rows")
    return file_path


def generate_dataset_3():
    """Generate Dataset 3: E-commerce Orders & Products (100,000 rows)"""
    print("Generating Dataset 3: E-commerce Orders & Products (100,000 rows)...")
    
    rows = []
    product_categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Beauty", "Toys", "Automotive"]
    order_statuses = ["Pending", "Processing", "Shipped", "Delivered", "Cancelled", "Returned"]
    countries = ["USA", "UK", "Canada", "Australia", "Germany", "France", "Japan", "Brazil"]
    
    for i in range(100000):
        order_date = fake.date_between(start_date="-1y", end_date="today")
        delivery_date = order_date + timedelta(days=random.randint(1, 14)) if random.random() > 0.2 else None
        rows.append({
            "order_id": f"ORD-{random.randint(100000, 999999):06d}",
            "order_date": order_date,
            "delivery_date": delivery_date,
            "customer_email": generate_email_formats(),
            "customer_phone": generate_phone_formats(),
            "billing_email": generate_email_formats(),
            "product_id": f"PROD-{random.randint(1000, 99999):05d}",
            "product_name": fake.catch_phrase(),
            "category": random.choice(product_categories),
            "brand": fake.company(),
            "price": round(random.uniform(5.0, 500.0), 2),
            "quantity": random.randint(1, 10),
            "subtotal": 0,  # Will calculate
            "tax_amount": 0,  # Will calculate
            "shipping_cost": round(random.uniform(0, 50), 2),
            "total_cost": 0,  # Will calculate
            "order_status": random.choice(order_statuses),
            "payment_url": generate_url_formats(),
            "tracking_url": generate_url_formats(),
            "customer_country": random.choice(countries),
            "shipping_address": fake.address().replace("\n", ", "),
            "billing_address": fake.address().replace("\n", ", "),
            "discount_code": fake.bothify(text="DISCOUNT-??##", letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
            "loyalty_points": random.randint(0, 5000),
        })
    
    df = pd.DataFrame(rows)
    df["subtotal"] = df["price"] * df["quantity"]
    df["tax_amount"] = (df["subtotal"] * 0.08).round(2)  # 8% tax
    df["total_cost"] = (df["subtotal"] + df["tax_amount"] + df["shipping_cost"]).round(2)
    
    # Different column order
    columns_order = [
        "order_id", "order_date", "delivery_date", "order_status",
        "customer_email", "customer_phone", "billing_email",
        "product_id", "product_name", "category", "brand",
        "price", "quantity", "subtotal", "tax_amount", "shipping_cost",
        "total_cost", "discount_code", "loyalty_points",
        "payment_url", "tracking_url",
        "customer_country", "shipping_address", "billing_address"
    ]
    df = df[columns_order]
    
    file_path = TEST_DATA_DIR / "dataset_3_ecommerce_orders.csv"
    df.to_csv(file_path, index=False)
    print(f"[OK] Created {file_path} with {len(df)} rows")
    return file_path


def generate_dataset_mismatch():
    """Generate Dataset with mismatched columns (for error testing)"""
    print("Generating Dataset with Mismatched Columns (25,000 rows)...")
    
    rows = []
    for i in range(25000):
        rows.append({
            "id": f"ID-{i:05d}",
            "name": fake.name(),
            "value": random.randint(100, 9999),
            "extra_column_1": fake.word(),
            "extra_column_2": random.randint(1, 100),
            "mismatch_field": fake.sentence(),
        })
    
    df = pd.DataFrame(rows)
    file_path = TEST_DATA_DIR / "dataset_mismatch_columns.csv"
    df.to_csv(file_path, index=False)
    print(f"[OK] Created {file_path} with {len(df)} rows (mismatched columns)")
    return file_path


def generate_dataset_pickle():
    """Generate Pickle dataset (30,000 rows)"""
    print("Generating Pickle Dataset (30,000 rows)...")
    
    rows = []
    product_types = ["A", "B", "C", "D", "E"]
    
    for i in range(30000):
        rows.append({
            "record_id": f"REC-{random.randint(10000, 99999):05d}",
            "product_type": random.choice(product_types),
            "description": fake.text(max_nb_chars=100),
            "price": round(random.uniform(10.0, 500.0), 2),
            "stock_quantity": random.randint(0, 1000),
            "supplier_email": generate_email_formats(),
            "supplier_phone": generate_phone_formats(),
            "supplier_website": generate_url_formats(),
            "created_date": fake.date_between(start_date="-1y", end_date="today"),
            "modified_date": fake.date_between(start_date="-6m", end_date="today"),
            "rating": round(random.uniform(1.0, 5.0), 2),
            "review_count": random.randint(0, 1000),
            "is_active": random.choice([True, False]),
            "category_code": fake.bothify(text="CAT-??##"),
        })
    
    df = pd.DataFrame(rows)
    file_path = TEST_DATA_DIR / "dataset_pickle_data.pkl"
    
    with open(file_path, "wb") as f:
        pickle.dump(df, f)
    
    print(f"[OK] Created {file_path} with {len(df)} rows")
    return file_path


if __name__ == "__main__":
    print("=" * 60)
    print("Generating Test Datasets for Data Geek Page")
    print("=" * 60)
    print()
    
    try:
        # Generate all datasets
        dataset1 = generate_dataset_1()
        print()
        
        dataset2 = generate_dataset_2()
        print()
        
        dataset3 = generate_dataset_3()
        print()
        
        dataset_mismatch = generate_dataset_mismatch()
        print()
        
        dataset_pickle = generate_dataset_pickle()
        print()
        
        print("=" * 60)
        print("[SUCCESS] All datasets generated successfully!")
        print("=" * 60)
        print()
        print("Generated files:")
        print(f"  - {dataset1.name} ({dataset1.stat().st_size / 1024 / 1024:.2f} MB)")
        print(f"  - {dataset2.name} ({dataset2.stat().st_size / 1024 / 1024:.2f} MB)")
        print(f"  - {dataset3.name} ({dataset3.stat().st_size / 1024 / 1024:.2f} MB)")
        print(f"  - {dataset_mismatch.name} ({dataset_mismatch.stat().st_size / 1024 / 1024:.2f} MB)")
        print(f"  - {dataset_pickle.name} ({dataset_pickle.stat().st_size / 1024 / 1024:.2f} MB)")
        print()
        print("Files are located in: test_data/")
        
    except Exception as e:
        print(f"[ERROR] Error generating datasets: {e}")
        import traceback
        traceback.print_exc()

