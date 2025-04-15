# tests/test_glue_etl_unit.py
"""
Unit tests for individual functions in the glue_etl.py script.
Focuses on testing the logic within functions like validate_data in isolation,
using a local SparkSession for DataFrame operations.
"""

import pytest
import sys
import os
from datetime import date, datetime

# --- Path Setup ---
# Add the 'src' directory to sys.path to allow importing 'glue_etl'
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../src'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)
# --- End Path Setup ---

# --- Imports ---
# Try importing the necessary components from the ETL script
# These imports happen *after* sys.path is modified
try:
    from glue_etl import (
        validate_data,
        log, # Import log if validate_data uses it implicitly or for direct testing
        products_schema,
        orders_schema,
        order_items_schema
    )
    print("Successfully imported components from glue_etl.py for unit tests.")
except ImportError as e:
    print(f"Error importing from glue_etl.py: {e}")
    pytest.skip(f"Could not import glue_etl.py components: {e}", allow_module_level=True)
except Exception as e:
     print(f"Unexpected error during import: {type(e).__name__}: {e}")
     pytest.fail(f"Failed during import phase: {type(e).__name__}: {e}")

# Import Spark types if needed for creating test data/schemas
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, DateType
# --- End Imports ---


# ==============================================================================
# Unit Tests for validate_data function
# ==============================================================================

# --- Products Validation Tests ---

def test_validate_products_valid(spark_session):
    """Test validate_data with valid product records."""
    # Arrange: Create a DataFrame with valid data using the imported schema
    valid_product_data = [
        Row(product_id=101, department_id=10, department="Electronics", product_name="Laptop"),
        Row(product_id=102, department_id=10, department="Electronics", product_name="Mouse")
    ]
    input_df = spark_session.createDataFrame(valid_product_data, schema=products_schema)

    # Act: Call the function under test
    valid_records, invalid_records = validate_data(input_df, "products", reference_data=None)

    # Assert: Check counts of returned DataFrames
    assert valid_records.count() == 2, "Should have 2 valid records"
    assert invalid_records.count() == 0, "Should have 0 invalid records"

def test_validate_products_invalid_nulls(spark_session):
    """Test validate_data with products having null primary key or name."""
    # Arrange: Create DataFrame with invalid data
    invalid_product_data = [
        Row(product_id=None, department_id=20, department="Clothing", product_name="Socks"), # Null product_id
        Row(product_id=105, department_id=30, department="Home", product_name=None),       # Null product_name
        Row(product_id=106, department_id=30, department="Home", product_name="Lamp")        # Valid row for comparison
    ]
    input_df = spark_session.createDataFrame(invalid_product_data, schema=products_schema)

    # Act
    valid_records, invalid_records = validate_data(input_df, "products", reference_data=None)

    # Assert
    assert valid_records.count() == 1, "Should have 1 valid record"
    assert invalid_records.count() == 2, "Should have 2 invalid records"
    # Optional: Check specific error messages
    errors = invalid_records.select("validation_errors").rdd.flatMap(lambda x: x).collect()
    assert "Null product_id primary key" in errors[0] # Order might vary depending on execution
    assert "Null product name" in errors[1]


# --- Orders Validation Tests ---

def test_validate_orders_valid(spark_session):
    """Test validate_data with valid order records."""
    # Arrange
    ts1 = datetime(2023, 1, 10, 10, 0, 0)
    d1 = date(2023, 1, 10)
    valid_order_data = [
        Row(order_num=1, order_id=501, user_id=1001, order_timestamp=ts1, total_amount=150.50, date=d1),
        Row(order_num=2, order_id=502, user_id=1002, order_timestamp=ts1, total_amount=75.00, date=d1)
    ]
    input_df = spark_session.createDataFrame(valid_order_data, schema=orders_schema)

    # Act
    valid_records, invalid_records = validate_data(input_df, "orders", reference_data=None)

    # Assert
    assert valid_records.count() == 2
    assert invalid_records.count() == 0

def test_validate_orders_invalid(spark_session):
    """Test validate_data with invalid order records."""
    # Arrange
    ts1 = datetime(2023, 1, 10, 10, 0, 0)
    d1 = date(2023, 1, 10)
    invalid_order_data = [
        Row(order_num=3, order_id=None, user_id=1003, order_timestamp=ts1, total_amount=50.00, date=d1),   # Null order_id
        Row(order_num=4, order_id=504, user_id=1004, order_timestamp=None, total_amount=25.00, date=d1),  # Null timestamp
        Row(order_num=5, order_id=505, user_id=1005, order_timestamp=ts1, total_amount=-10.00, date=d1), # Non-positive amount
        Row(order_num=6, order_id=506, user_id=1006, order_timestamp=ts1, total_amount=0.00, date=d1),   # Non-positive amount (zero)
        Row(order_num=7, order_id=507, user_id=1007, order_timestamp=ts1, total_amount=100.00, date=d1)  # Valid row
    ]
    input_df = spark_session.createDataFrame(invalid_order_data, schema=orders_schema)

    # Act
    valid_records, invalid_records = validate_data(input_df, "orders", reference_data=None)

    # Assert
    assert valid_records.count() == 1, "Only one order should be valid"
    assert invalid_records.count() == 4, "Four orders should be invalid"
    # Optional: Check specific error messages
    errors_df = invalid_records.select("order_id", "validation_errors").collect()
    errors_map = {row['order_id']: row['validation_errors'] for row in errors_df}
    assert errors_map[None] == "Null order_id primary key"
    assert errors_map[504] == "Invalid timestamp"
    assert errors_map[505] == "Non-positive total amount"
    assert errors_map[506] == "Non-positive total amount"


# --- Order Items Validation Tests (Including Referential Integrity) ---

def test_validate_order_items_valid(spark_session):
    """Test validate_data with valid order_item records and valid references."""
    # Arrange
    ts1 = datetime(2023, 1, 10, 10, 0, 0)
    d1 = date(2023, 1, 10)
    # Valid order items
    valid_oi_data = [
        Row(id=10001, order_id=501, user_id=1001, days_since_prior_order=5, product_id=101, add_to_cart_order=1, reordered=0, order_timestamp=ts1, date=d1),
        Row(id=10002, order_id=501, user_id=1001, days_since_prior_order=5, product_id=102, add_to_cart_order=2, reordered=0, order_timestamp=ts1, date=d1)
    ]
    input_df = spark_session.createDataFrame(valid_oi_data, schema=order_items_schema)

    # Mock reference data DataFrames
    mock_products_data = [Row(product_id=101), Row(product_id=102)]
    mock_orders_data = [Row(order_id=501)]
    mock_products_df = spark_session.createDataFrame(mock_products_data, schema=StructType([StructField("product_id", IntegerType())]))
    mock_orders_df = spark_session.createDataFrame(mock_orders_data, schema=StructType([StructField("order_id", IntegerType())]))
    reference_data = {"products": mock_products_df, "orders": mock_orders_df}

    # Act
    valid_records, invalid_records = validate_data(input_df, "order_items", reference_data=reference_data)

    # Assert
    assert valid_records.count() == 2
    assert invalid_records.count() == 0

def test_validate_order_items_invalid_nulls(spark_session):
    """Test validate_data with order_items having null required fields."""
    # Arrange
    ts1 = datetime(2023, 1, 10, 10, 0, 0)
    d1 = date(2023, 1, 10)
    invalid_oi_data = [
        Row(id=None, order_id=501, user_id=1001, days_since_prior_order=5, product_id=101, add_to_cart_order=1, reordered=0, order_timestamp=ts1, date=d1), # Null id
        Row(id=10002, order_id=None, user_id=1001, days_since_prior_order=5, product_id=101, add_to_cart_order=2, reordered=0, order_timestamp=ts1, date=d1), # Null order_id
        Row(id=10003, order_id=501, user_id=1001, days_since_prior_order=5, product_id=None, add_to_cart_order=3, reordered=0, order_timestamp=ts1, date=d1), # Null product_id
        Row(id=10004, order_id=501, user_id=1001, days_since_prior_order=5, product_id=101, add_to_cart_order=4, reordered=0, order_timestamp=None, date=d1), # Null timestamp
        Row(id=10005, order_id=501, user_id=1001, days_since_prior_order=5, product_id=101, add_to_cart_order=5, reordered=0, order_timestamp=ts1, date=d1) # Valid
    ]
    input_df = spark_session.createDataFrame(invalid_oi_data, schema=order_items_schema)
    # No reference data needed for these null checks

    # Act
    valid_records, invalid_records = validate_data(input_df, "order_items", reference_data=None)

    # Assert
    assert valid_records.count() == 1
    assert invalid_records.count() == 4
    errors = {row['id']: row['validation_errors'] for row in invalid_records.collect()}
    assert errors[None] == "Null primary identifier" # ID is primary key for items
    assert errors[10002] == "Null order_id"
    assert errors[10003] == "Null product_id"
    assert errors[10004] == "Invalid timestamp"


def test_validate_order_items_invalid_references(spark_session):
    """Test validate_data with order_items having invalid foreign key references."""
    # Arrange
    ts1 = datetime(2023, 1, 10, 10, 0, 0)
    d1 = date(2023, 1, 10)
    invalid_ref_oi_data = [
        Row(id=10001, order_id=501, user_id=1001, days_since_prior_order=5, product_id=101, add_to_cart_order=1, reordered=0, order_timestamp=ts1, date=d1), # Valid reference
        Row(id=10002, order_id=999, user_id=1001, days_since_prior_order=5, product_id=101, add_to_cart_order=2, reordered=0, order_timestamp=ts1, date=d1), # Invalid order_id
        Row(id=10003, order_id=501, user_id=1001, days_since_prior_order=5, product_id=999, add_to_cart_order=3, reordered=0, order_timestamp=ts1, date=d1), # Invalid product_id
        Row(id=10004, order_id=998, user_id=1001, days_since_prior_order=5, product_id=998, add_to_cart_order=4, reordered=0, order_timestamp=ts1, date=d1)  # Both invalid
    ]
    input_df = spark_session.createDataFrame(invalid_ref_oi_data, schema=order_items_schema)

    # Mock reference data DataFrames (containing only valid IDs)
    mock_products_df = spark_session.createDataFrame([Row(product_id=101)], schema=StructType([StructField("product_id", IntegerType())]))
    mock_orders_df = spark_session.createDataFrame([Row(order_id=501)], schema=StructType([StructField("order_id", IntegerType())]))
    reference_data = {"products": mock_products_df, "orders": mock_orders_df}

    # Act
    valid_records, invalid_records = validate_data(input_df, "order_items", reference_data=reference_data)

    # Assert
    assert valid_records.count() == 1, "Only row 10001 should be valid"
    assert invalid_records.count() == 3, "Rows 10002, 10003, 10004 should be invalid"
    errors = {row['id']: row['validation_errors'] for row in invalid_records.collect()}
    assert errors[10002] == "Invalid order_id reference"
    assert errors[10003] == "Invalid product_id reference"
    # Check row with multiple errors (order might vary, check for substrings)
    assert "Invalid order_id reference" in errors[10004]
    assert "Invalid product_id reference" in errors[10004]