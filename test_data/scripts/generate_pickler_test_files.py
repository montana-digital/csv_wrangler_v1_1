"""
Generate test pickle files for Pickler page testing.

Creates various pickle files with different scenarios:
- Simple DataFrame without dates
- DataFrame with single date column
- DataFrame with multiple date columns
- Large DataFrame
- DataFrame with different data types
"""
import pickle
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# Create output directory
output_dir = Path(__file__).parent.parent / "pickler_test_files"
output_dir.mkdir(parents=True, exist_ok=True)

print(f"Generating test pickle files in: {output_dir}")
print("=" * 60)

# 1. Simple DataFrame without dates
print("\n1. Creating simple DataFrame without dates...")
df_simple = pd.DataFrame({
    "name": ["John Doe", "Jane Smith", "Bob Johnson", "Alice Williams", "Charlie Brown"],
    "age": [30, 25, 35, 28, 42],
    "email": [
        "john@example.com",
        "jane@example.com",
        "bob@example.com",
        "alice@example.com",
        "charlie@example.com"
    ],
    "city": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"],
    "salary": [75000, 65000, 80000, 70000, 90000]
})
output_file = output_dir / "simple_data.pkl"
with open(output_file, "wb") as f:
    pickle.dump(df_simple, f)
print(f"   [OK] Created: {output_file.name} ({len(df_simple)} rows, {len(df_simple.columns)} columns)")

# 2. DataFrame with single date column
print("\n2. Creating DataFrame with single date column...")
base_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
dates = [
    base_date - timedelta(days=30),
    base_date - timedelta(days=15),
    base_date - timedelta(days=7),
    base_date - timedelta(days=3),
    base_date - timedelta(days=1),
]
df_with_dates = pd.DataFrame({
    "product_name": ["Widget A", "Widget B", "Widget C", "Widget D", "Widget E"],
    "price": [19.99, 29.99, 39.99, 49.99, 59.99],
    "quantity": [100, 150, 200, 75, 120],
    "purchase_date": dates,
    "category": ["Electronics", "Electronics", "Home", "Home", "Sports"]
})
output_file = output_dir / "with_single_date.pkl"
with open(output_file, "wb") as f:
    pickle.dump(df_with_dates, f)
print(f"   [OK] Created: {output_file.name} ({len(df_with_dates)} rows, {len(df_with_dates.columns)} columns)")

# 3. DataFrame with multiple date columns
print("\n3. Creating DataFrame with multiple date columns...")
base_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
created_dates = [
    base_date - timedelta(days=60),
    base_date - timedelta(days=45),
    base_date - timedelta(days=30),
    base_date - timedelta(days=15),
    base_date - timedelta(days=5),
]
updated_dates = [
    base_date - timedelta(days=55),
    base_date - timedelta(days=40),
    base_date - timedelta(days=25),
    base_date - timedelta(days=10),
    base_date - timedelta(days=2),
]
df_multiple_dates = pd.DataFrame({
    "order_id": [1001, 1002, 1003, 1004, 1005],
    "customer_name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
    "created_date": created_dates,
    "updated_date": updated_dates,
    "total_amount": [150.50, 275.00, 89.99, 320.75, 199.99],
    "status": ["Completed", "Pending", "Completed", "Completed", "Pending"]
})
output_file = output_dir / "with_multiple_dates.pkl"
with open(output_file, "wb") as f:
    pickle.dump(df_multiple_dates, f)
print(f"   [OK] Created: {output_file.name} ({len(df_multiple_dates)} rows, {len(df_multiple_dates.columns)} columns)")

# 4. Large DataFrame (1000 rows)
print("\n4. Creating large DataFrame (1000 rows)...")
large_data = {
    "id": range(1, 1001),
    "name": [f"User_{i}" for i in range(1, 1001)],
    "age": [18 + (i % 50) for i in range(1000)],
    "score": [round(50 + (i * 0.1) % 50, 2) for i in range(1000)],
    "active": [i % 2 == 0 for i in range(1000)]
}
df_large = pd.DataFrame(large_data)
output_file = output_dir / "large_dataset.pkl"
with open(output_file, "wb") as f:
    pickle.dump(df_large, f)
print(f"   [OK] Created: {output_file.name} ({len(df_large)} rows, {len(df_large.columns)} columns)")

# 5. DataFrame with datetime column (datetime type, not string)
print("\n5. Creating DataFrame with datetime type column...")
datetime_values = pd.date_range(start="2024-01-01", end="2024-01-10", freq="D")
df_datetime_type = pd.DataFrame({
    "event_name": [f"Event {i+1}" for i in range(10)],
    "event_datetime": datetime_values,
    "attendees": [50 + i * 10 for i in range(10)],
    "venue": [f"Venue {chr(65+i)}" for i in range(10)]
})
output_file = output_dir / "with_datetime_type.pkl"
with open(output_file, "wb") as f:
    pickle.dump(df_datetime_type, f)
print(f"   [OK] Created: {output_file.name} ({len(df_datetime_type)} rows, {len(df_datetime_type.columns)} columns)")

# 6. DataFrame with mixed data types including dates
print("\n6. Creating DataFrame with mixed data types and dates...")
base_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
df_mixed = pd.DataFrame({
    "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "name": ["Product A", "Product B", "Product C", "Product D", "Product E",
             "Product F", "Product G", "Product H", "Product I", "Product J"],
    "price": [10.99, 20.50, 30.00, 40.75, 50.25, 60.00, 70.50, 80.99, 90.00, 100.00],
    "quantity": [100, 200, 150, 300, 250, 180, 220, 190, 210, 240],
    "in_stock": [True, True, False, True, False, True, True, False, True, True],
    "created_date": [base_date - timedelta(days=i*5) for i in range(10)],
    "category": ["A", "B", "A", "C", "B", "A", "C", "B", "A", "C"]
})
output_file = output_dir / "mixed_types_with_dates.pkl"
with open(output_file, "wb") as f:
    pickle.dump(df_mixed, f)
print(f"   [OK] Created: {output_file.name} ({len(df_mixed)} rows, {len(df_mixed.columns)} columns)")

# 7. DataFrame with date-like column names (timestamp, created_at, etc.)
print("\n7. Creating DataFrame with date-like column names...")
base_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
df_date_names = pd.DataFrame({
    "user_id": [101, 102, 103, 104, 105],
    "username": ["user1", "user2", "user3", "user4", "user5"],
    "created_at": [base_date - timedelta(days=i*10) for i in range(5)],
    "last_login": [base_date - timedelta(days=i*2) for i in range(5)],
    "email": [f"user{i+1}@example.com" for i in range(5)]
})
output_file = output_dir / "date_column_names.pkl"
with open(output_file, "wb") as f:
    pickle.dump(df_date_names, f)
print(f"   [OK] Created: {output_file.name} ({len(df_date_names)} rows, {len(df_date_names.columns)} columns)")

# 8. Minimal DataFrame (just 2 rows, 2 columns)
print("\n8. Creating minimal DataFrame...")
df_minimal = pd.DataFrame({
    "name": ["Test 1", "Test 2"],
    "value": [100, 200]
})
output_file = output_dir / "minimal.pkl"
with open(output_file, "wb") as f:
    pickle.dump(df_minimal, f)
print(f"   [OK] Created: {output_file.name} ({len(df_minimal)} rows, {len(df_minimal.columns)} columns)")

# 9. DataFrame as list of dicts format (alternative pickle format)
print("\n9. Creating DataFrame as list of dicts...")
data_as_list = [
    {"employee_id": 1, "name": "John", "department": "Sales", "salary": 50000},
    {"employee_id": 2, "name": "Jane", "department": "Marketing", "salary": 55000},
    {"employee_id": 3, "name": "Bob", "department": "IT", "salary": 60000},
    {"employee_id": 4, "name": "Alice", "department": "HR", "salary": 52000},
]
output_file = output_dir / "list_of_dicts.pkl"
with open(output_file, "wb") as f:
    pickle.dump(data_as_list, f)
print(f"   [OK] Created: {output_file.name} (list of {len(data_as_list)} dicts)")

# 10. DataFrame with many columns (for column selection testing)
print("\n10. Creating DataFrame with many columns...")
df_many_columns = pd.DataFrame({
    f"col_{i}": [j * i for j in range(10)] for i in range(1, 21)
})
output_file = output_dir / "many_columns.pkl"
with open(output_file, "wb") as f:
    pickle.dump(df_many_columns, f)
print(f"   [OK] Created: {output_file.name} ({len(df_many_columns)} rows, {len(df_many_columns.columns)} columns)")

print("\n" + "=" * 60)
print(f"[SUCCESS] All test pickle files generated successfully!")
print(f"\nFiles location: {output_dir}")
print(f"\nTotal files created: {len(list(output_dir.glob('*.pkl')))}")

