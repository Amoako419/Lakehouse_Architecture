import pytest
import os
import sys
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, DateType
from pyspark.sql.functions import col, lit
import datetime

# Mock modules that aren't available during testing
sys.modules['awsglue.transforms'] = MagicMock()
sys.modules['awsglue.utils'] = MagicMock()
sys.modules['awsglue.context'] = MagicMock()
sys.modules['awsglue.job'] = MagicMock()
sys.modules['delta.tables'] = MagicMock()

# Import our code with mocked dependencies
# This would be replaced with your actual module import
# For testing purposes, we'll use a mock

# Create fixture for Spark session
@pytest.fixture(scope="session")
def spark():
    spark = (SparkSession.builder
             .master("local[2]")
             .appName("GlueScriptTest")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.default.parallelism", "2")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .getOrCreate())
    
    yield spark
    spark.stop()

# Create fixture for sample data
@pytest.fixture
def sample_data(spark):
    # Create sample order_items data
    order_items_data = [
        (1, 10000, 1990, 10, 988, 1, 0, "2025-04-01T11:27:00", "2025-04-01"),
        (2, 10000, 1990, 10, 659, 2, 1, "2025-04-01T11:27:00", "2025-04-01"),
        (3, 10000, 1990, 22, 676, 3, 0, "2025-04-01T11:27:00", "2025-04-01"),
        (4, 10000, 1990, 14, 1, 4, 0, "2025-04-01T11:27:00", "2025-04-01"),
        # Add one with missing product_id for validation testing
        (5, 10000, 1990, 14, None, 4, 0, "2025-04-01T11:27:00", "2025-04-01"),
        # Add a duplicate id for deduplication testing
        (1, 10001, 1990, 10, 988, 1, 0, "2025-04-01T11:27:00", "2025-04-01"),
    ]
    
    order_items_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("days_since_prior_order", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("add_to_cart_order", IntegerType(), True),
        StructField("reordered", IntegerType(), True),
        StructField("order_timestamp", StringType(), True),
        StructField("date", StringType(), True)
    ])
    
    # Create sample orders data
    orders_data = [
        (90, 10000, 1990, "2025-04-01T11:27:00", 229.53, "2025-04-01"),
        (41, 10001, 5057, "2025-04-01T17:53:00", 131.93, "2025-04-01"),
        (22, 10002, 7864, "2025-04-01T02:26:00", 251.9, "2025-04-01"),
        (99, 10003, 3131, "2025-04-01T01:24:00", 487.49, "2025-04-01"),
        # Add one with negative amount for validation testing
        (100, 10004, 3132, "2025-04-01T01:24:00", -487.49, "2025-04-01"),
        # Add one with missing timestamp for validation testing
        (101, 10005, 3133, None, 487.49, "2025-04-01"),
    ]
    
    orders_schema = StructType([
        StructField("order_num", IntegerType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("order_timestamp", StringType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("date", StringType(), True)
    ])
    
    # Create sample products data
    products_data = [
        (1, 4, "Books", "Product_1_Store"),
        (2, 2, "Books", "Product_2_There"),
        (3, 4, "Books", "Product_3_Hand"),
        (4, 6, "Sports", "Product_4_Tv"),
        (988, 7, "Electronics", "Product_988_Phone"),
        (659, 7, "Electronics", "Product_659_Tablet"),
        (676, 8, "Home", "Product_676_Lamp"),
        # Add one with missing product_name for validation testing
        (999, 8, "Home", None),
    ]
    
    products_schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("department_id", IntegerType(), True),
        StructField("department", StringType(), True),
        StructField("product_name", StringType(), True)
    ])
    
    # Create DataFrames
    order_items_df = spark.createDataFrame(order_items_data, order_items_schema)
    orders_df = spark.createDataFrame(orders_data, orders_schema)
    products_df = spark.createDataFrame(products_data, products_schema)
    
    return {
        "order_items": order_items_df,
        "orders": orders_df,
        "products": products_df
    }

# Import functions to test
# Since we can't directly import the Glue script, we'll recreate the core validation function
def validate_data(df, schema_name, reference_data=None):
    """Simplified validate_data function for testing"""
    validated = df.withColumn("validation_errors", lit(None).cast(StringType()))
    
    if schema_name == "order_items":
        # No null primary identifiers
        validated = validated.withColumn(
            "validation_errors",
            when(col("id").isNull(), "Null primary identifier")
            .otherwise(col("validation_errors"))
        )
        
        # Product ID must not be null
        validated = validated.withColumn(
            "validation_errors",
            when(col("product_id").isNull(), 
                 when(col("validation_errors").isNull(), "Null product_id")
                 .otherwise(col("validation_errors") + "; Null product_id"))
            .otherwise(col("validation_errors"))
        )
        
        # Check referential integrity if reference data is provided
        if reference_data and "products" in reference_data:
            product_ids = reference_data["products"].select("product_id").distinct().rdd.flatMap(lambda x: x).collect()
            validated = validated.withColumn(
                "validation_errors",
                when((col("product_id").isNotNull()) & (~col("product_id").isin(product_ids)), 
                     when(col("validation_errors").isNull(), "Invalid product_id reference")
                     .otherwise(col("validation_errors") + "; Invalid product_id reference"))
                .otherwise(col("validation_errors"))
            )
    
    elif schema_name == "orders":
        # Valid timestamp check
        validated = validated.withColumn(
            "validation_errors",
            when(col("order_timestamp").isNull(), 
                 when(col("validation_errors").isNull(), "Invalid timestamp")
                 .otherwise(col("validation_errors") + "; Invalid timestamp"))
            .otherwise(col("validation_errors"))
        )
        
        # Total amount should be positive if present
        validated = validated.withColumn(
            "validation_errors",
            when((col("total_amount").isNotNull()) & (col("total_amount") <= 0), 
                 when(col("validation_errors").isNull(), "Non-positive total amount")
                 .otherwise(col("validation_errors") + "; Non-positive total amount"))
            .otherwise(col("validation_errors"))
        )
    
    elif schema_name == "products":
        # Product name must not be null
        validated = validated.withColumn(
            "validation_errors",
            when(col("product_name").isNull(), 
                 when(col("validation_errors").isNull(), "Null product name")
                 .otherwise(col("validation_errors") + "; Null product name"))
            .otherwise(col("validation_errors"))
        )
    
    # Split into valid and invalid records
    valid_records = validated.filter(col("validation_errors").isNull()).drop("validation_errors")
    invalid_records = validated.filter(col("validation_errors").isNotNull())
    
    return valid_records, invalid_records

# Test validation function for order_items
def test_order_items_validation(spark, sample_data):
    order_items_df = sample_data["order_items"]
    products_df = sample_data["products"]
    
    reference_data = {"products": products_df}
    valid, invalid = validate_data(order_items_df, "order_items", reference_data)
    
    # Check counts
    assert valid.count() == 5  # 6 total - 1 invalid (with null product_id)
    assert invalid.count() == 1
    
    # Check specific error
    errors = invalid.select("validation_errors").collect()
    assert "Null product_id" in errors[0][0]

# Test deduplication for order_items
def test_order_items_deduplication(spark, sample_data):
    order_items_df = sample_data["order_items"]
    
    # Count before deduplication
    pre_count = order_items_df.count()
    assert pre_count == 6
    
    # After deduplication on id
    deduped = order_items_df.dropDuplicates(["id"])
    post_count = deduped.count()
    assert post_count == 5
    
    # Check that we still have all unique ids
    unique_ids = deduped.select("id").distinct().count()
    assert unique_ids == 5

# Test orders validation
def test_orders_validation(spark, sample_data):
    orders_df = sample_data["orders"]
    
    valid, invalid = validate_data(orders_df, "orders")
    
    # Check counts (2 invalid - negative amount + null timestamp)
    assert valid.count() == 4
    assert invalid.count() == 2
    
    # Check specific errors
    error_types = [row["validation_errors"] for row in invalid.collect()]
    assert any("Non-positive total amount" in error for error in error_types)
    assert any("Invalid timestamp" in error for error in error_types)

# Test products validation
def test_products_validation(spark, sample_data):
    products_df = sample_data["products"]
    
    valid, invalid = validate_data(products_df, "products")
    
    # Check counts (1 invalid - null product name)
    assert valid.count() == 7
    assert invalid.count() == 1
    
    # Check specific error
    errors = invalid.select("validation_errors").collect()
    assert "Null product name" in errors[0][0]

# Test referential integrity
def test_referential_integrity(spark, sample_data):
    order_items_df = sample_data["order_items"]
    products_df = sample_data["products"]
    
    # Add an order item with a non-existent product_id
    invalid_item = [(999, 10000, 1990, 10, 9999, 1, 0, "2025-04-01T11:27:00", "2025-04-01")]
    invalid_schema = order_items_df.schema
    invalid_df = spark.createDataFrame(invalid_item, invalid_schema)
    
    # Combine with existing data
    combined_df = order_items_df.union(invalid_df)
    
    # Validate with referential integrity check
    reference_data = {"products": products_df}
    valid, invalid = validate_data(combined_df, "order_items", reference_data)
    
    # The invalid item with non-existent product_id should be rejected
    # But only if the product_id is not null
    valid_with_valid_product_ids = valid.filter(col("product_id").isNotNull())
    all_product_ids = valid_with_valid_product_ids.select("product_id").distinct().collect()
    all_product_ids = [row["product_id"] for row in all_product_ids]
    
    # Check that all product_ids in valid records exist in products table
    for pid in all_product_ids:
        assert pid in [row["product_id"] for row in products_df.collect()]
    
    # Check that we don't have the invalid product_id in valid records
    assert 9999 not in all_product_ids

# Integration test for the entire validation process
def test_full_validation_process(spark, sample_data):
    # Process products first
    products_df = sample_data["products"]
    valid_products, invalid_products = validate_data(products_df, "products")
    
    # Then process orders
    orders_df = sample_data["orders"]
    valid_orders, invalid_orders = validate_data(orders_df, "orders")
    
    # Finally process order_items with reference data
    order_items_df = sample_data["order_items"]
    reference_data = {
        "products": valid_products,
        "orders": valid_orders
    }
    
    valid_items, invalid_items = validate_data(order_items_df, "order_items", reference_data)
    
    # Verify counts
    assert valid_products.count() == 7
    assert invalid_products.count() == 1
    
    assert valid_orders.count() == 4
    assert invalid_orders.count() == 2
    
    assert valid_items.count() == 5
    assert invalid_items.count() == 1
    
    # Verify referential integrity is maintained
    product_ids_in_items = valid_items.select("product_id").distinct().collect()
    product_ids_in_items = [row["product_id"] for row in product_ids_in_items]
    
    for pid in product_ids_in_items:
        if pid is not None:
            matching_products = valid_products.filter(col("product_id") == pid).count()
            assert matching_products > 0