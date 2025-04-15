import pytest
import os
import sys
import tempfile
import shutil
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, DateType
import pandas as pd
from datetime import datetime, date

# Import the script to test - adjust path as needed
# We'll mock the imports that aren't available in the testing environment
sys.modules['awsglue.transforms'] = MagicMock()
sys.modules['awsglue.utils'] = MagicMock()
sys.modules['awsglue.context'] = MagicMock()
sys.modules['awsglue.job'] = MagicMock()
sys.modules['delta.tables'] = MagicMock()

# Path to your ETL script
# Import only the schemas and functions for testing to avoid initialization code
from glue_etl import (
    order_items_schema,
    orders_schema,
    products_schema,
    validate_data,
    process_dataset,
    log_dataframe_info,
    timed_execution
)

# Fixture for SparkSession - will be reused across tests
@pytest.fixture(scope="module")
def spark():
    spark = (SparkSession.builder
             .master("local[2]")
             .appName("ETL_Unit_Tests")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.default.parallelism", "2")
             .config("spark.sql.adaptive.enabled", "true")
             .getOrCreate())
    yield spark
    spark.stop()

# Fixture for temporary directory to store test data
@pytest.fixture(scope="module")
def temp_dir():
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)

# Fixture to create sample test data
@pytest.fixture(scope="module")
def test_data(spark, temp_dir):
    # Create test data directories
    input_dir = os.path.join(temp_dir, "input")
    output_dir = os.path.join(temp_dir, "output")
    rejected_dir = os.path.join(temp_dir, "rejected")
    
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(rejected_dir, exist_ok=True)
    
    # Products test data
    products_data = [
        (1, 101, "Dairy", "Milk"),
        (2, 102, "Bakery", "Bread"),
        (3, 103, "Produce", "Apples"),
        (4, 104, "Meat", "Chicken"),
        (5, None, None, "Invalid Product")  # Invalid due to missing department
    ]
    products_df = pd.DataFrame(products_data, columns=["product_id", "department_id", "department", "product_name"])
    products_path = os.path.join(input_dir, "products.csv")
    products_df.to_csv(products_path, index=False)
    
    # Orders test data
    current_date = date.today().isoformat()
    timestamp = datetime.now().isoformat()
    orders_data = [
        (1, 1001, 501, timestamp, 25.99, current_date),
        (2, 1002, 502, timestamp, 15.50, current_date),
        (3, 1003, 503, timestamp, 42.75, current_date),
        (4, None, 504, timestamp, 10.25, current_date),  # Invalid due to missing order_id
        (5, 1005, 505, timestamp, -5.00, current_date)   # Invalid due to negative amount
    ]
    orders_df = pd.DataFrame(orders_data, columns=["order_num", "order_id", "user_id", "order_timestamp", "total_amount", "date"])
    orders_path = os.path.join(input_dir, "orders.csv")
    orders_df.to_csv(orders_path, index=False)
    
    # Order items test data
    order_items_data = [
        (10001, 1001, 501, 2, 1, 1, 0, timestamp, current_date),
        (10002, 1001, 501, 2, 2, 2, 0, timestamp, current_date),
        (10003, 1002, 502, 5, 3, 1, 0, timestamp, current_date),
        (10004, 1003, 503, 1, 4, 1, 0, timestamp, current_date),
        (10005, 9999, 506, 3, 1, 1, 0, timestamp, current_date),  # Invalid order_id reference
        (10006, 1003, 503, 1, 999, 2, 0, timestamp, current_date),  # Invalid product_id reference
        (None, 1002, 502, 5, 3, 1, 0, timestamp, current_date)    # Invalid due to missing id
    ]
    order_items_df = pd.DataFrame(order_items_data, columns=["id", "order_id", "user_id", "days_since_prior_order", 
                                                            "product_id", "add_to_cart_order", "reordered", 
                                                            "order_timestamp", "date"])
    order_items_path = os.path.join(input_dir, "order_items.csv")
    order_items_df.to_csv(order_items_path, index=False)
    
    return {
        "input_dir": input_dir,
        "output_dir": output_dir,
        "rejected_dir": rejected_dir,
        "products_path": products_path,
        "orders_path": orders_path,
        "order_items_path": order_items_path
    }

# Mock class for DeltaTable
class MockDeltaTable:
    def __init__(self):
        self.merge_called = False
        self.merge_condition = None
        
    def alias(self, alias_name):
        return self
    
    def merge(self, source_df, condition):
        self.merge_called = True
        self.merge_condition = condition
        return self
    
    def whenMatchedUpdateAll(self):
        return self
    
    def whenNotMatchedInsertAll(self):
        return self
    
    def execute(self):
        return None

# Test validation function
def test_validation(spark, test_data):
    # Read the test data
    products_raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(test_data["products_path"])
    
    # Test validation logic
    valid_products, invalid_products = validate_data(products_raw, "products")
    
    # Verify validation results
    assert valid_products.count() == 4  # 4 valid records
    assert invalid_products.count() == 1  # 1 invalid record
    
    # Check specific validation error for invalid product
    error_message = invalid_products.filter(invalid_products.product_name == "Invalid Product").select("validation_errors").collect()[0][0]
    assert "department" in error_message.lower()  # Error should mention department

# Test the processing pipeline with mocked Delta operations
@patch('delta.tables.DeltaTable')
def test_process_dataset(mock_delta_table, spark, test_data):
    # Set up mock for DeltaTable
    mock_delta_instance = MockDeltaTable()
    mock_delta_table.forPath.return_value = mock_delta_instance
    
    # Read the test data
    products_raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(test_data["products_path"])
    
    # Process the products dataset
    output_path = os.path.join(test_data["output_dir"], "products")
    processed_data = process_dataset(products_raw, products_schema, "products", output_path)
    
    # Verify the processed data
    assert processed_data.count() == 4  # Should have 4 valid records after processing
    
    # Verify that Delta merge was called
    assert mock_delta_instance.merge_called
    assert "product_id" in mock_delta_instance.merge_condition

# Test full ETL pipeline integration
@patch('delta.tables.DeltaTable')
def test_full_pipeline_integration(mock_delta_table, spark, test_data):
    # Configure mock for DeltaTable to simulate table doesn't exist
    mock_delta_table.forPath.side_effect = Exception("Delta table does not exist")
    
    # Read all test datasets
    products_raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(test_data["products_path"])
    orders_raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(test_data["orders_path"])
    order_items_raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(test_data["order_items_path"])
    
    # Process products
    products_output = os.path.join(test_data["output_dir"], "products")
    products_data = process_dataset(products_raw, products_schema, "products", products_output)
    products_data_cached = products_data.cache()
    
    # Process orders
    orders_output = os.path.join(test_data["output_dir"], "orders")
    orders_data = process_dataset(orders_raw, orders_schema, "orders", orders_output)
    orders_data_cached = orders_data.cache()
    
    # Setup reference data for order_items validation
    reference_data = {
        "products": products_data_cached,
        "orders": orders_data_cached
    }
    
    # Process order_items with reference data
    order_items_output = os.path.join(test_data["output_dir"], "order_items")
    rejected_path = test_data["rejected_dir"]
    order_items_data = process_dataset(order_items_raw, order_items_schema, "order_items", order_items_output, reference_data)
    
    # Verify results
    assert products_data.count() == 4  # 4 valid products
    assert orders_data.count() == 3    # 3 valid orders
    assert order_items_data.count() == 4  # 4 valid order items
    
    # Clean up cached data
    products_data_cached.unpersist()
    orders_data_cached.unpersist()

# Test edge cases
def test_empty_dataset(spark, test_data):
    # Create an empty DataFrame with the products schema
    empty_data = spark.createDataFrame([], products_schema)
    
    # Process empty dataset
    output_path = os.path.join(test_data["output_dir"], "empty_products")
    processed_data = process_dataset(empty_data, products_schema, "products", output_path)
    
    # Verify results
    assert processed_data.count() == 0  # Should result in empty DataFrame

# Test duplicate handling
def test_duplicate_handling(spark, test_data):
    # Create dataset with duplicates
    duplicate_data = [
        (1, 101, "Dairy", "Milk"),
        (1, 101, "Dairy", "Milk"),  # Duplicate product_id
        (2, 102, "Bakery", "Bread"),
        (3, 103, "Produce", "Apples"),
        (3, 103, "Produce", "Different Apples")  # Duplicate product_id with different details
    ]
    
    # Convert to Spark DataFrame
    duplicate_df = spark.createDataFrame(duplicate_data, ["product_id", "department_id", "department", "product_name"])
    
    # Process dataset with duplicates
    output_path = os.path.join(test_data["output_dir"], "duplicate_products")
    processed_data = process_dataset(duplicate_df, products_schema, "products", output_path)
    
    # Verify results
    assert processed_data.count() == 3  # Should have 3 unique product_ids after deduplication