
"""
Unit tests for individual functions extracted into etl_utils.py.
Focuses on testing the logic within functions like validate_data in isolation,
using a local SparkSession for DataFrame operations.
"""

import pytest
import sys
import os
from datetime import date, datetime


# Add the 'src' directory to sys.path to allow importing 'etl_utils'
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../src'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

try:
    from etl_utils import (
        validate_data,
        log,
        products_schema,
        orders_schema,
        order_items_schema
    )
    print("Successfully imported components from etl_utils.py for unit tests.")
except ImportError as e:
    print(f"Error importing from etl_utils.py: {e}")
    pytest.skip(f"Could not import etl_utils.py components: {e}", allow_module_level=True)
except Exception as e:
     print(f"Unexpected error during import: {type(e).__name__}: {e}")
     pytest.fail(f"Failed during import phase: {type(e).__name__}: {e}")

# Import Spark types
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, DateType, BooleanType





# --- Products Validation Tests ---

def test_validate_products_valid(spark_session):
    # ... (test logic unchanged) ...
    valid_product_data = [
        Row(product_id=101, department_id=10, department="Electronics", product_name="Laptop"),
        Row(product_id=102, department_id=10, department="Electronics", product_name="Mouse")
    ]
    input_df = spark_session.createDataFrame(valid_product_data, schema=products_schema)
    valid_records, invalid_records = validate_data(input_df, "products", reference_data=None)
    assert valid_records.count() == 2
    assert invalid_records.count() == 0


def test_validate_products_invalid_nulls(spark_session):
    nullable_products_schema_for_test = StructType([
        StructField("product_id", IntegerType(), nullable=True), # Allow null for test input
        StructField("department_id", IntegerType(), nullable=True),
        StructField("department", StringType(), nullable=True),
        StructField("product_name", StringType(), nullable=True) # Allow null for test input
    ])
    invalid_product_data = [
        Row(product_id=None, department_id=20, department="Clothing", product_name="Socks"),
        Row(product_id=105, department_id=30, department="Home", product_name=None),
        Row(product_id=106, department_id=30, department="Home", product_name="Lamp")
    ]
    input_df = spark_session.createDataFrame(invalid_product_data, schema=nullable_products_schema_for_test)
    valid_records, invalid_records = validate_data(input_df, "products", reference_data=None) # Validate against original schema
    assert valid_records.count() == 1
    assert invalid_records.count() == 2


# --- Orders Validation Tests ---

def test_validate_orders_valid(spark_session):
    ts1 = datetime(2023, 1, 10, 10, 0, 0)
    d1 = date(2023, 1, 10)
    valid_order_data = [
        Row(order_num=1, order_id=501, user_id=1001, order_timestamp=ts1, total_amount=150.50, date=d1),
        Row(order_num=2, order_id=502, user_id=1002, order_timestamp=ts1, total_amount=75.00, date=d1)
    ]
    input_df = spark_session.createDataFrame(valid_order_data, schema=orders_schema)
    valid_records, invalid_records = validate_data(input_df, "orders", reference_data=None)
    assert valid_records.count() == 2
    assert invalid_records.count() == 0


def test_validate_orders_invalid(spark_session):
    nullable_orders_schema_for_test = StructType([
        StructField("order_num", IntegerType(), nullable=True),
        StructField("order_id", IntegerType(), nullable=True), # Allow null
        StructField("user_id", IntegerType(), nullable=False),
        StructField("order_timestamp", TimestampType(), nullable=True), # Allow null
        StructField("total_amount", DoubleType(), nullable=True),
        StructField("date", DateType(), nullable=False)
    ])
    ts1 = datetime(2023, 1, 10, 10, 0, 0)
    d1 = date(2023, 1, 10)
    invalid_order_data = [
        Row(order_num=3, order_id=None, user_id=1003, order_timestamp=ts1, total_amount=50.00, date=d1),
        Row(order_num=4, order_id=504, user_id=1004, order_timestamp=None, total_amount=25.00, date=d1),
        Row(order_num=5, order_id=505, user_id=1005, order_timestamp=ts1, total_amount=-10.00, date=d1),
        Row(order_num=6, order_id=506, user_id=1006, order_timestamp=ts1, total_amount=0.00, date=d1),
        Row(order_num=7, order_id=507, user_id=1007, order_timestamp=ts1, total_amount=100.00, date=d1)
    ]
    input_df = spark_session.createDataFrame(invalid_order_data, schema=nullable_orders_schema_for_test)
    valid_records, invalid_records = validate_data(input_df, "orders", reference_data=None) # Validate against original schema
    assert valid_records.count() == 1
    assert invalid_records.count() == 4


# --- Order Items Validation Tests ---

def test_validate_order_items_valid(spark_session):
    ts1 = datetime(2023, 1, 10, 10, 0, 0)
    d1 = date(2023, 1, 10)
    valid_oi_data = [
        Row(id=10001, order_id=501, user_id=1001, days_since_prior_order=5, product_id=101, add_to_cart_order=1, reordered=0, order_timestamp=ts1, date=d1),
        Row(id=10002, order_id=501, user_id=1001, days_since_prior_order=5, product_id=102, add_to_cart_order=2, reordered=0, order_timestamp=ts1, date=d1)
    ]
    input_df = spark_session.createDataFrame(valid_oi_data, schema=order_items_schema)
    mock_products_df = spark_session.createDataFrame([Row(product_id=101), Row(product_id=102)], schema=StructType([StructField("product_id", IntegerType())]))
    mock_orders_df = spark_session.createDataFrame([Row(order_id=501)], schema=StructType([StructField("order_id", IntegerType())]))
    reference_data = {"products": mock_products_df, "orders": mock_orders_df}
    valid_records, invalid_records = validate_data(input_df, "order_items", reference_data=reference_data)
    assert valid_records.count() == 2
    assert invalid_records.count() == 0


def test_validate_order_items_invalid_nulls(spark_session):
    nullable_order_items_schema_for_test = StructType([
        StructField("id", IntegerType(), nullable=True), # Allow null
        StructField("order_id", IntegerType(), nullable=True), # Allow null
        StructField("user_id", IntegerType(), nullable=False),
        StructField("days_since_prior_order", IntegerType(), nullable=True),
        StructField("product_id", IntegerType(), nullable=True), # Allow null
        StructField("add_to_cart_order", IntegerType(), nullable=True),
        StructField("reordered", IntegerType(), nullable=True),
        StructField("order_timestamp", TimestampType(), nullable=True), # Allow null
        StructField("date", DateType(), nullable=False)
    ])
    ts1 = datetime(2023, 1, 10, 10, 0, 0)
    d1 = date(2023, 1, 10)
    invalid_oi_data = [
        Row(id=None, order_id=501, user_id=1001, days_since_prior_order=5, product_id=101, add_to_cart_order=1, reordered=0, order_timestamp=ts1, date=d1),
        Row(id=10002, order_id=None, user_id=1001, days_since_prior_order=5, product_id=101, add_to_cart_order=2, reordered=0, order_timestamp=ts1, date=d1),
        Row(id=10003, order_id=501, user_id=1001, days_since_prior_order=5, product_id=None, add_to_cart_order=3, reordered=0, order_timestamp=ts1, date=d1),
        Row(id=10004, order_id=501, user_id=1001, days_since_prior_order=5, product_id=101, add_to_cart_order=4, reordered=0, order_timestamp=None, date=d1),
        Row(id=10005, order_id=501, user_id=1001, days_since_prior_order=5, product_id=101, add_to_cart_order=5, reordered=0, order_timestamp=ts1, date=d1)
    ]
    input_df = spark_session.createDataFrame(invalid_oi_data, schema=nullable_order_items_schema_for_test)
    valid_records, invalid_records = validate_data(input_df, "order_items", reference_data=None) # Validate against original schema
    assert valid_records.count() == 1
    assert invalid_records.count() == 4
    


def test_validate_order_items_invalid_references(spark_session):
    # ... (test logic unchanged) ...
    ts1 = datetime(2023, 1, 10, 10, 0, 0)
    d1 = date(2023, 1, 10)
    invalid_ref_oi_data = [
        Row(id=10001, order_id=501, user_id=1001, days_since_prior_order=5, product_id=101, add_to_cart_order=1, reordered=0, order_timestamp=ts1, date=d1),
        Row(id=10002, order_id=999, user_id=1001, days_since_prior_order=5, product_id=101, add_to_cart_order=2, reordered=0, order_timestamp=ts1, date=d1),
        Row(id=10003, order_id=501, user_id=1001, days_since_prior_order=5, product_id=999, add_to_cart_order=3, reordered=0, order_timestamp=ts1, date=d1),
        Row(id=10004, order_id=998, user_id=1001, days_since_prior_order=5, product_id=998, add_to_cart_order=4, reordered=0, order_timestamp=ts1, date=d1)
    ]
    input_df = spark_session.createDataFrame(invalid_ref_oi_data, schema=order_items_schema)
    mock_products_df = spark_session.createDataFrame([Row(product_id=101)], schema=StructType([StructField("product_id", IntegerType())]))
    mock_orders_df = spark_session.createDataFrame([Row(order_id=501)], schema=StructType([StructField("order_id", IntegerType())]))
    reference_data = {"products": mock_products_df, "orders": mock_orders_df}
    valid_records, invalid_records = validate_data(input_df, "order_items", reference_data=reference_data)
    assert valid_records.count() == 1
    assert invalid_records.count() == 3
  

