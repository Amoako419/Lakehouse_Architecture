import pytest
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
import os

from .glue_etl import (
    process_dataset,
    validate_data,
    products_schema,
    orders_schema,
    order_items_schema,
    log
)

@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing."""
    spark = (SparkSession.builder
            .master("local[2]")
            .appName("unit-tests")
            .getOrCreate())
    yield spark
    spark.stop()


@pytest.fixture
def test_data_paths(tmp_path):
    """Create temporary paths for test data."""
    paths = {
        "raw": tmp_path / "raw",
        "products_out": tmp_path / "products",
        "orders_out": tmp_path / "orders",
        "order_items_out": tmp_path / "order_items",
        "rejected_out": tmp_path / "rejected"
    }
    for path in paths.values():
        path.mkdir(exist_ok=True)
    return paths

@pytest.fixture
def create_csv_file(spark_session):
    """Fixture to create CSV files from test data."""
    def _create_csv(filename, data, headers):
        df = spark_session.createDataFrame(data, headers)
        df.write.csv(filename, header=True, mode="overwrite")
    return _create_csv

def test_validate_data_products(spark_session):
    """Test validate_data function with products data."""
    test_data = [
        (101, 10, "Electronics", "Laptop"),
        (None, 20, "Clothing", "T-Shirt"),  # Invalid due to null product_id
        (103, 30, "Home", None)  # Invalid due to null product_name
    ]
    
    df = spark_session.createDataFrame(test_data, ["product_id", "department_id", "department", "product_name"])
    valid_df, invalid_df = validate_data(df, "products")
    
    assert valid_df.count() == 1
    assert invalid_df.count() == 2
    assert valid_df.filter("product_id = 101").count() == 1

def test_validate_data_orders(spark_session):
    """Test validate_data function with orders data."""
    ts = datetime.now()
    d = date.today()
    
    test_data = [
        (1, 501, 1001, ts, 150.50, d),
        (2, None, 1002, ts, 75.00, d),  # Invalid due to null order_id
        (3, 503, 1003, ts, -10.00, d)   # Invalid due to negative amount
    ]
    
    df = spark_session.createDataFrame(
        test_data,
        ["order_num", "order_id", "user_id", "order_timestamp", "total_amount", "date"]
    )
    
    valid_df, invalid_df = validate_data(df, "orders")
    
    assert valid_df.count() == 1
    assert invalid_df.count() == 2
    assert valid_df.filter("order_id = 501").count() == 1

def test_process_dataset_products(spark_session, test_data_paths):
    """Test process_dataset function with products data."""
    test_data = [
        (101, 10, "Electronics", "Laptop"),
        (102, 20, "Clothing", "T-Shirt")
    ]
    
    df = spark_session.createDataFrame(test_data, ["product_id", "department_id", "department", "product_name"])
    
    result = process_dataset(
        df,
        products_schema,
        "products",
        str(test_data_paths["products_out"]),
        str(test_data_paths["rejected_out"]),
        "test_job",
        spark_session
    )
    
    assert result.count() == 2
    assert os.path.exists(test_data_paths["products_out"] / "_delta_log")

def test_referential_integrity(spark_session, test_data_paths):
    """Test referential integrity validation in order_items processing."""
    # Create reference data
    products_data = [(101, 10, "Electronics", "Laptop")]
    orders_data = [(1, 501, 1001, datetime.now(), 150.50, date.today())]
    
    products_df = spark_session.createDataFrame(products_data, ["product_id", "department_id", "department", "product_name"])
    orders_df = spark_session.createDataFrame(
        orders_data,
        ["order_num", "order_id", "user_id", "order_timestamp", "total_amount", "date"]
    )
    
    # Test data with valid and invalid references
    order_items_data = [
        (1, 501, 1001, 5, 101, 1, 0, datetime.now(), date.today()),  # Valid
        (2, 502, 1001, 5, 999, 1, 0, datetime.now(), date.today())   # Invalid product_id
    ]
    
    order_items_df = spark_session.createDataFrame(
        order_items_data,
        ["id", "order_id", "user_id", "days_since_prior_order", "product_id", 
         "add_to_cart_order", "reordered", "order_timestamp", "date"]
    )
    
    reference_data = {
        "products": products_df,
        "orders": orders_df
    }
    
    _, invalid_df = validate_data(order_items_df, "order_items", reference_data)
    
    assert invalid_df.count() == 1
    assert invalid_df.filter("product_id = 999").count() == 1

def test_error_logging(spark_session, caplog):
    """Test error logging functionality."""
    invalid_data = [(None, None, None, None)]
    df = spark_session.createDataFrame(invalid_data, ["product_id", "department_id", "department", "product_name"])
    
    validate_data(df, "products")
    
    assert "validation errors" in caplog.text.lower()