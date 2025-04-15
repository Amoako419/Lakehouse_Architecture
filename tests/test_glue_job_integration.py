# tests/test_glue_job_integration.py
import sys
import os
import pytest
from datetime import date, datetime # Make sure these are imported for test data

# --- Enhanced Debugging ---
print("\n--- DEBUG: Starting test_glue_job_integration.py ---")
print(f"--- DEBUG: Current working directory: {os.getcwd()}")
print(f"--- DEBUG: __file__ is: {__file__}")

# Calculate and print the expected path to the 'src' directory
test_dir = os.path.dirname(__file__)
project_root_guess = os.path.abspath(os.path.join(test_dir, '..'))
src_path = os.path.join(project_root_guess, 'src')
print(f"--- DEBUG: Calculated project_root_guess: {project_root_guess}")
print(f"--- DEBUG: Calculated src_path: {src_path}")

# Check if src_path exists and list contents
if os.path.isdir(src_path):
    print(f"--- DEBUG: src_path exists. Contents:")
    try:
        print(os.listdir(src_path))
    except Exception as e:
        print(f"--- DEBUG: Error listing src_path contents: {e}")
else:
    print(f"--- DEBUG: src_path DOES NOT EXIST or is not a directory.")

# Add src directory to Python's path
print(f"--- DEBUG: Inserting {src_path} into sys.path")
sys.path.insert(0, src_path)
print(f"--- DEBUG: sys.path after insert: {sys.path}")
# --- End Enhanced Debugging ---


# Try importing specific names
try:
    print("--- DEBUG: Attempting: from glue_job import ... ---")
    # Ensure the imports match exactly what's needed globally or by process_dataset
    from glue_job import (
        process_dataset,
        validate_data,
        log,
        products_schema,
        orders_schema,
        order_items_schema
        # Do NOT import spark, glueContext, job, etc. from glue_job
    )
    print("--- SUCCESS: Successfully imported required components from glue_job.py ---")

    # --- Add check immediately after import ---
    if 'process_dataset' in locals() or 'process_dataset' in globals():
         print("--- DEBUG: 'process_dataset' IS in locals/globals immediately after import.")
    else:
         print("--- WARNING: 'process_dataset' is NOT in locals/globals immediately after import!")
    # --- End check ---

except ImportError as e:
    print(f"--- ERROR: Caught ImportError importing from glue_job.py: {e} ---")
    import traceback
    traceback.print_exc()
    pytest.skip("Could not import glue_job.py components due to ImportError.", allow_module_level=True)
except Exception as e: # Catch other potential import-time errors
     print(f"--- ERROR: Caught unexpected error during import: {type(e).__name__}: {e} ---")
     import traceback
     traceback.print_exc()
     pytest.fail(f"Failed during import phase: {type(e).__name__}: {e}")


# Define test data (Needs ts1, ts2, d1, d2 etc defined - ensure they are)
ts1 = datetime(2023, 1, 10, 10, 0, 0)
ts2 = datetime(2023, 1, 11, 12, 30, 0)
d1 = date(2023, 1, 10)
d2 = date(2023, 1, 11)

products_data_initial = [
    (101, 10, "Electronics", "Laptop"),
    (102, 10, "Electronics", "Mouse"),
    (103, 20, "Clothing", "T-Shirt"),
    (None, 20, "Clothing", "Socks"), # Invalid: Null product_id
    (105, 30, "Home", None), # Invalid: Null product_name
]
products_headers = ["product_id", "department_id", "department", "product_name"]

orders_data_initial = [
    (1, 501, 1001, ts1.strftime('%Y-%m-%d %H:%M:%S'), 150.50, d1.strftime('%Y-%m-%d')),
    (2, 502, 1002, ts2.strftime('%Y-%m-%d %H:%M:%S'), 75.00, d2.strftime('%Y-%m-%d')),
    (3, None, 1003, ts1.strftime('%Y-%m-%d %H:%M:%S'), 50.00, d1.strftime('%Y-%m-%d')), # Invalid: Null order_id
    (4, 504, 1004, None, 25.00, d2.strftime('%Y-%m-%d')), # Invalid: Null timestamp
    (5, 505, 1005, ts2.strftime('%Y-%m-%d %H:%M:%S'), -10.00, d2.strftime('%Y-%m-%d')), # Invalid: Negative amount
]
orders_headers = ["order_num", "order_id", "user_id", "order_timestamp", "total_amount", "date"]

order_items_data_initial = [
    (10001, 501, 1001, 5, 101, 1, 0, ts1.strftime('%Y-%m-%d %H:%M:%S'), d1.strftime('%Y-%m-%d')), # Valid
    (10002, 501, 1001, 5, 102, 2, 0, ts1.strftime('%Y-%m-%d %H:%M:%S'), d1.strftime('%Y-%m-%d')), # Valid
    (10003, 502, 1002, None, 103, 1, 1, ts2.strftime('%Y-%m-%d %H:%M:%S'), d2.strftime('%Y-%m-%d')), # Valid (null days_since_prior)
    (None, 502, 1002, 10, 101, 2, 1, ts2.strftime('%Y-%m-%d %H:%M:%S'), d2.strftime('%Y-%m-%d')), # Invalid: Null id
    (10005, 501, 1001, 5, 999, 3, 0, ts1.strftime('%Y-%m-%d %H:%M:%S'), d1.strftime('%Y-%m-%d')), # Invalid: Product 999 not in products
    (10006, 999, 1001, 5, 101, 4, 0, ts1.strftime('%Y-%m-%d %H:%M:%S'), d1.strftime('%Y-%m-%d')), # Invalid: Order 999 not in orders
]
order_items_headers = ["id", "order_id", "user_id", "days_since_prior_order", "product_id", "add_to_cart_order", "reordered", "order_timestamp", "date"]


# --- Test Functions ---

def test_process_products_initial_load(spark_session, data_paths, create_csv_file):
    """Test initial load of products data."""
    create_csv_file("products.csv", products_data_initial, products_headers)
    raw_df = spark_session.read.csv(str(data_paths["raw"] / "products.csv"), header=True, inferSchema=True)

    processed_df = process_dataset(
        raw_df, products_schema, "products", str(data_paths["products_out"]),
        # Need to mock or setup these globals if process_dataset relies on them
        spark_session, # Pass spark session explicitly if needed
        log, # Pass log function explicitly if needed
        data_paths["rejected_out"], # Pass rejected path
        "test_job" # Pass job name
    )

    # Assertions for processed data
    assert os.path.exists(data_paths["products_out"] / "_delta_log")
    delta_df = spark_session.read.format("delta").load(str(data_paths["products_out"]))
    assert delta_df.count() == 3 # 5 initial - 2 invalid
    assert delta_df.filter("product_id = 101").count() == 1
    assert "department" in delta_df.columns # Check partitioning column exists

    # Assertions for rejected data
    rejected_full_path = data_paths["rejected_out"] / "products"
    assert os.path.exists(rejected_full_path / "_delta_log")
    rejected_df = spark_session.read.format("delta").load(str(rejected_full_path))
    assert rejected_df.count() == 2
    assert rejected_df.filter("product_id is NULL").count() == 1
    assert rejected_df.filter("product_name is NULL").count() == 1
    assert "validation_errors" in rejected_df.columns
    assert "rejection_time" in rejected_df.columns

def test_process_products_merge(spark_session, data_paths, create_csv_file):
    """Test merging (update/insert) products data."""
    # --- Run initial load first ---
    create_csv_file("products_initial.csv", products_data_initial[:3], products_headers) # Only valid ones initially
    raw_df_initial = spark_session.read.csv(str(data_paths["raw"] / "products_initial.csv"), header=True, inferSchema=True)
    process_dataset(
        raw_df_initial, products_schema, "products", str(data_paths["products_out"]),
        spark_session, log, data_paths["rejected_out"], "test_job"
    )
    initial_delta_df = spark_session.read.format("delta").load(str(data_paths["products_out"]))
    assert initial_delta_df.count() == 3

    # --- Prepare update/new data ---
    products_update_data = [
        (102, 10, "Electronics", "Wireless Mouse"), # Update product 102 name
        (104, 20, "Clothing", "Jeans"),             # New product 104
        (None, 40, "Books", "Novel"),               # Invalid new product
    ]
    create_csv_file("products_update.csv", products_update_data, products_headers)
    raw_df_update = spark_session.read.csv(str(data_paths["raw"] / "products_update.csv"), header=True, inferSchema=True)

    # --- Run process_dataset again for merge ---
    process_dataset(
        raw_df_update, products_schema, "products", str(data_paths["products_out"]),
        spark_session, log, data_paths["rejected_out"], "test_job"
    )

    # --- Assertions ---
    merged_delta_df = spark_session.read.format("delta").load(str(data_paths["products_out"]))
    # Expected: 3 initial + 1 new valid = 4
    assert merged_delta_df.count() == 4
    # Check updated record
    assert merged_delta_df.filter("product_id = 102").select("product_name").first()["product_name"] == "Wireless Mouse"
    # Check new record
    assert merged_delta_df.filter("product_id = 104").count() == 1
    # Check rejected records (should have 1 from this batch)
    rejected_df = spark_session.read.format("delta").load(str(data_paths["rejected_out"] / "products"))
    # Note: Rejected data is appended, so count might need adjustment if running multiple tests saving there
    assert rejected_df.filter("department = 'Books'").count() >= 1 # Check if the specific rejected record exists

# --- Add similar tests for orders (initial load, merge, validation checks) ---
def test_process_orders_initial_load(spark_session, data_paths, create_csv_file):
    """Test initial load of orders data."""
    create_csv_file("orders.csv", orders_data_initial, orders_headers)
    raw_df = spark_session.read.csv(str(data_paths["raw"] / "orders.csv"), header=True, inferSchema=True)

    processed_df = process_dataset(
        raw_df, orders_schema, "orders", str(data_paths["orders_out"]),
        spark_session, log, data_paths["rejected_out"], "test_job"
    )

    delta_df = spark_session.read.format("delta").load(str(data_paths["orders_out"]))
    assert delta_df.count() == 2 # 5 initial - 3 invalid
    assert delta_df.filter("order_id = 501").count() == 1
    assert "date" in delta_df.columns # Partition column

    rejected_full_path = data_paths["rejected_out"] / "orders"
    assert os.path.exists(rejected_full_path / "_delta_log")
    rejected_df = spark_session.read.format("delta").load(str(rejected_full_path))
    assert rejected_df.count() == 3
    assert rejected_df.filter("order_id is NULL").count() == 1
    assert rejected_df.filter("validation_errors like '%Invalid timestamp%'").count() == 1
    assert rejected_df.filter("validation_errors like '%Non-positive total amount%'").count() == 1

# --- Test for Order Items including Referential Integrity ---
def test_process_order_items_with_references(spark_session, data_paths, create_csv_file):
    """Test order_items processing with referential integrity checks."""
    # 1. Load valid products and orders first to act as reference tables
    valid_products = [(101, 10, "Electronics", "Laptop"), (102, 10, "Electronics", "Mouse"), (103, 20, "Clothing", "T-Shirt")]
    valid_orders = [(1, 501, 1001, ts1.strftime('%Y-%m-%d %H:%M:%S'), 150.50, d1.strftime('%Y-%m-%d')), (2, 502, 1002, ts2.strftime('%Y-%m-%d %H:%M:%S'), 75.00, d2.strftime('%Y-%m-%d'))]

    create_csv_file("ref_products.csv", valid_products, products_headers)
    create_csv_file("ref_orders.csv", valid_orders, orders_headers)

    raw_ref_prod = spark_session.read.csv(str(data_paths["raw"] / "ref_products.csv"), header=True, inferSchema=True)
    ref_prod_df = process_dataset(raw_ref_prod, products_schema, "products", str(data_paths["products_out"]), spark_session, log, data_paths["rejected_out"], "ref_job_prod")

    raw_ref_ord = spark_session.read.csv(str(data_paths["raw"] / "ref_orders.csv"), header=True, inferSchema=True)
    ref_ord_df = process_dataset(raw_ref_ord, orders_schema, "orders", str(data_paths["orders_out"]), spark_session, log, data_paths["rejected_out"], "ref_job_ord")

    # Ensure reference DFs are cached or readily available
    ref_prod_df.cache()
    ref_ord_df.cache()
    ref_prod_df.count() # Action to trigger cache
    ref_ord_df.count()  # Action to trigger cache


    # 2. Prepare order_items data including invalid references
    create_csv_file("order_items.csv", order_items_data_initial, order_items_headers)
    raw_oi_df = spark_session.read.csv(str(data_paths["raw"] / "order_items.csv"), header=True, inferSchema=True)

    # 3. Process order_items, passing reference data
    reference_data = {
        "products": ref_prod_df,
        "orders": ref_ord_df
    }
    processed_oi_df = process_dataset(
        raw_oi_df, order_items_schema, "order_items", str(data_paths["order_items_out"]),
        reference_data, # Pass the loaded DFs here
        spark_session, log, data_paths["rejected_out"], "test_oi_job"
    )

    # 4. Assertions
    delta_oi_df = spark_session.read.format("delta").load(str(data_paths["order_items_out"]))
    assert delta_oi_df.count() == 3 # 6 initial - 3 invalid (null id, bad product_id, bad order_id)

    rejected_oi_path = data_paths["rejected_out"] / "order_items"
    assert os.path.exists(rejected_oi_path / "_delta_log")
    rejected_oi_df = spark_session.read.format("delta").load(str(rejected_oi_path))
    assert rejected_oi_df.count() == 3
    assert rejected_oi_df.filter("id is NULL").count() == 1
    assert rejected_oi_df.filter("validation_errors like '%Invalid product_id reference%'").count() == 1
    assert rejected_oi_df.filter("validation_errors like '%Invalid order_id reference%'").count() == 1

    # Unpersist reference data
    ref_prod_df.unpersist()
    ref_ord_df.unpersist()