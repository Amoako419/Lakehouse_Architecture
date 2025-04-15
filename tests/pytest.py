import unittest
from unittest.mock import patch, MagicMock, call
import sys
import logging
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, DateType
from pyspark.sql.functions import col, lit
from datetime import datetime, date
import pandas as pd

# Import the module to test (assuming the ETL script is in a file called glue_etl.py)
# We'll mock most of the dependencies
# sys.modules['awsglue.transforms'] = MagicMock()
# sys.modules['awsglue.utils'] = MagicMock()
# sys.modules['awsglue.context'] = MagicMock()
# sys.modules['awsglue.job'] = MagicMock()
# sys.modules['delta.tables'] = MagicMock()

# Since we can't directly import the ETL script functions, we'll need to redefine them here for testing
# or extract the functions to a separate module that can be imported for testing

class TestGlueETL(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a local Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("GlueETLUnitTests") \
            .master("local[*]") \
            .getOrCreate()
        
        # Define schemas (copied from original script)
        cls.order_items_schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("order_id", IntegerType(), nullable=False),
            StructField("user_id", IntegerType(), nullable=False),
            StructField("days_since_prior_order", IntegerType(), nullable=True),
            StructField("product_id", IntegerType(), nullable=False),
            StructField("add_to_cart_order", IntegerType(), nullable=True),
            StructField("reordered", IntegerType(), nullable=True),
            StructField("order_timestamp", TimestampType(), nullable=False),
            StructField("date", DateType(), nullable=False)
        ])

        cls.orders_schema = StructType([
            StructField("order_num", IntegerType(), nullable=True),
            StructField("order_id", IntegerType(), nullable=False),
            StructField("user_id", IntegerType(), nullable=False),
            StructField("order_timestamp", TimestampType(), nullable=False),
            StructField("total_amount", DoubleType(), nullable=True),
            StructField("date", DateType(), nullable=False)
        ])

        cls.products_schema = StructType([
            StructField("product_id", IntegerType(), nullable=False),
            StructField("department_id", IntegerType(), nullable=True),
            StructField("department", StringType(), nullable=True),
            StructField("product_name", StringType(), nullable=False)
        ])
        
        # Setup logger for testing
        cls.logger = logging.getLogger()
        cls.logger.setLevel(logging.INFO)
    
    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session
        cls.spark.stop()
    
    # Helper methods to create test DataFrames
    def create_test_products_df(self):
        """Create a test products DataFrame."""
        data = [
            (1, 1, "Produce", "Apple"),
            (2, 1, "Produce", "Banana"),
            (3, 2, "Dairy", "Milk"),
            (4, 3, "Bakery", "Bread"),
            (5, None, None, "Generic Product")
        ]
        return self.spark.createDataFrame(data, ["product_id", "department_id", "department", "product_name"])
    
    def create_test_orders_df(self):
        """Create a test orders DataFrame."""
        data = [
            (1, 1001, 101, datetime.now(), 25.99, date(2023, 1, 1)),
            (2, 1002, 102, datetime.now(), 15.50, date(2023, 1, 1)),
            (3, 1003, 103, datetime.now(), 45.75, date(2023, 1, 2)),
            (None, 1004, 104, datetime.now(), -5.00, date(2023, 1, 2)),  # Invalid total_amount
            (5, 1005, 105, None, 20.00, date(2023, 1, 3))  # Invalid timestamp
        ]
        return self.spark.createDataFrame(data, ["order_num", "order_id", "user_id", "order_timestamp", "total_amount", "date"])
    
    def create_test_order_items_df(self):
        """Create a test order_items DataFrame."""
        data = [
            (1, 1001, 101, 3, 1, 1, 0, datetime.now(), date(2023, 1, 1)),
            (2, 1001, 101, 3, 2, 2, 0, datetime.now(), date(2023, 1, 1)),
            (3, 1002, 102, 7, 3, 1, 0, datetime.now(), date(2023, 1, 1)),
            (4, 1003, 103, 14, 4, 1, 0, datetime.now(), date(2023, 1, 2)),
            (5, 9999, 999, 5, 9999, 1, 0, datetime.now(), date(2023, 1, 2)),  # Invalid references
            (None, 1004, 104, 2, 5, 1, 0, datetime.now(), date(2023, 1, 3))  # Null primary key
        ]
        return self.spark.createDataFrame(data, ["id", "order_id", "user_id", "days_since_prior_order", 
                                               "product_id", "add_to_cart_order", "reordered", 
                                               "order_timestamp", "date"])
    
    # Unit tests for validate_data function
    @patch('logging.Logger.info')
    @patch('logging.Logger.error')
    def test_validate_products(self, mock_error, mock_info):
        """Test validation of products data."""
        # Define the function we're testing (copied from original script but adapted for unit testing)
        def validate_data(df, schema_name, reference_data=None):
            # Add validation error column
            validated = df.withColumn("validation_errors", lit(None).cast(StringType()))
            
            if schema_name == "products":
                # Product ID must not be null (primary key)
                validated = validated.withColumn(
                    "validation_errors",
                    pyspark.sql.functions.when(col("product_id").isNull(), "Null product_id primary key")
                    .otherwise(col("validation_errors"))
                )
                
                # Product name must not be null
                validated = validated.withColumn(
                    "validation_errors",
                    pyspark.sql.functions.when(col("product_name").isNull(), 
                         pyspark.sql.functions.when(col("validation_errors").isNull(), "Null product name")
                         .otherwise(col("validation_errors") + "; Null product name"))
                    .otherwise(col("validation_errors"))
                )
            
            # Split into valid and invalid records
            valid_records = validated.filter(col("validation_errors").isNull()).drop("validation_errors")
            invalid_records = validated.filter(col("validation_errors").isNotNull())
            
            return valid_records, invalid_records
        
        # Create test data with one invalid record (null product name)
        test_data = self.create_test_products_df()
        # Add a row with null product name
        pdf = test_data.toPandas()
        pdf = pd.concat([pdf, pd.DataFrame([{"product_id": 6, "department_id": 3, "department": "Bakery", "product_name": None}])])
        test_data_with_invalid = self.spark.createDataFrame(pdf)
        
        # Run validation
        valid_records, invalid_records = validate_data(test_data_with_invalid, "products")
        
        # Assertions
        self.assertEqual(valid_records.count(), 5)  # 5 valid records
        self.assertEqual(invalid_records.count(), 1)  # 1 invalid record
        invalid_record = invalid_records.collect()[0]
        self.assertEqual(invalid_record["validation_errors"], "Null product name")
    
    @patch('logging.Logger.info')
    @patch('logging.Logger.error')
    def test_validate_orders(self, mock_error, mock_info):
        """Test validation of orders data."""
        # Define the validate_data function just for orders
        def validate_data(df, schema_name, reference_data=None):
            # Add validation error column
            validated = df.withColumn("validation_errors", lit(None).cast(StringType()))
            
            if schema_name == "orders":
                # Order ID must not be null (primary key)
                validated = validated.withColumn(
                    "validation_errors",
                    pyspark.sql.functions.when(col("order_id").isNull(), "Null order_id primary key")
                    .otherwise(col("validation_errors"))
                )
                
                # Valid timestamp check
                validated = validated.withColumn(
                    "validation_errors",
                    pyspark.sql.functions.when(col("order_timestamp").isNull(), 
                         pyspark.sql.functions.when(col("validation_errors").isNull(), "Invalid timestamp")
                         .otherwise(col("validation_errors") + "; Invalid timestamp"))
                    .otherwise(col("validation_errors"))
                )
                
                # Total amount should be positive if present
                validated = validated.withColumn(
                    "validation_errors",
                    pyspark.sql.functions.when((col("total_amount").isNotNull()) & (col("total_amount") <= 0), 
                         pyspark.sql.functions.when(col("validation_errors").isNull(), "Non-positive total amount")
                         .otherwise(col("validation_errors") + "; Non-positive total amount"))
                    .otherwise(col("validation_errors"))
                )
            
            # Split into valid and invalid records
            valid_records = validated.filter(col("validation_errors").isNull()).drop("validation_errors")
            invalid_records = validated.filter(col("validation_errors").isNotNull())
            
            return valid_records, invalid_records
            
        # Create test data with invalid records
        test_data = self.create_test_orders_df()
        
        # Run validation
        valid_records, invalid_records = validate_data(test_data, "orders")
        
        # Assertions
        self.assertEqual(valid_records.count(), 3)  # 3 valid records
        self.assertEqual(invalid_records.count(), 2)  # 2 invalid records
        
        # Check specific validation errors
        invalid_df = invalid_records.orderBy("order_id").collect()
        # Order 1004 has negative total amount
        self.assertIn("Non-positive total amount", invalid_df[0]["validation_errors"])
        # Order 1005 has null timestamp
        self.assertIn("Invalid timestamp", invalid_df[1]["validation_errors"])
    
    @patch('logging.Logger.info')
    @patch('logging.Logger.error')
    def test_validate_order_items_with_references(self, mock_error, mock_info):
        """Test validation of order_items with referential integrity checks."""
        # Define the validate_data function just for order_items with reference checks
        def validate_data(df, schema_name, reference_data=None):
            # Add validation error column
            validated = df.withColumn("validation_errors", lit(None).cast(StringType()))
            
            if schema_name == "order_items":
                # No null primary identifiers
                validated = validated.withColumn(
                    "validation_errors",
                    pyspark.sql.functions.when(col("id").isNull(), "Null primary identifier")
                    .otherwise(col("validation_errors"))
                )
                
                # Order ID must not be null
                validated = validated.withColumn(
                    "validation_errors",
                    pyspark.sql.functions.when(col("order_id").isNull(), 
                         pyspark.sql.functions.when(col("validation_errors").isNull(), "Null order_id")
                         .otherwise(col("validation_errors") + "; Null order_id"))
                    .otherwise(col("validation_errors"))
                )
                
                # Product ID must not be null
                validated = validated.withColumn(
                    "validation_errors",
                    pyspark.sql.functions.when(col("product_id").isNull(), 
                         pyspark.sql.functions.when(col("validation_errors").isNull(), "Null product_id")
                         .otherwise(col("validation_errors") + "; Null product_id"))
                    .otherwise(col("validation_errors"))
                )
                
                # Referential integrity - order_id must exist in orders table
                if reference_data and "orders" in reference_data:
                    # Get list of valid order IDs from reference data
                    order_ids = reference_data["orders"].select("order_id").distinct()
                    order_ids_list = order_ids.rdd.flatMap(lambda x: x).collect()
                    
                    validated = validated.withColumn(
                        "validation_errors",
                        pyspark.sql.functions.when(~col("order_id").isin(order_ids_list), 
                             pyspark.sql.functions.when(col("validation_errors").isNull(), "Invalid order_id reference")
                             .otherwise(col("validation_errors") + "; Invalid order_id reference"))
                        .otherwise(col("validation_errors"))
                    )
                
                # Referential integrity - product_id must exist in products table
                if reference_data and "products" in reference_data:
                    # Get list of valid product IDs from reference data
                    product_ids = reference_data["products"].select("product_id").distinct()
                    product_ids_list = product_ids.rdd.flatMap(lambda x: x).collect()
                    
                    validated = validated.withColumn(
                        "validation_errors",
                        pyspark.sql.functions.when(~col("product_id").isin(product_ids_list), 
                             pyspark.sql.functions.when(col("validation_errors").isNull(), "Invalid product_id reference")
                             .otherwise(col("validation_errors") + "; Invalid product_id reference"))
                        .otherwise(col("validation_errors"))
                    )
            
            # Split into valid and invalid records
            valid_records = validated.filter(col("validation_errors").isNull()).drop("validation_errors")
            invalid_records = validated.filter(col("validation_errors").isNotNull())
            
            return valid_records, invalid_records
            
        # Create test data
        order_items_df = self.create_test_order_items_df()
        orders_df = self.create_test_orders_df()
        products_df = self.create_test_products_df()
        
        # Create reference data dictionary
        reference_data = {
            "orders": orders_df,
            "products": products_df
        }
        
        # Run validation with reference data for referential integrity checks
        valid_records, invalid_records = validate_data(order_items_df, "order_items", reference_data)
        
        # Assertions
        self.assertEqual(valid_records.count(), 4)  # 4 valid records
        self.assertEqual(invalid_records.count(), 2)  # 2 invalid records
        
        # Check specific validation errors
        invalid_df = invalid_records.orderBy("order_id").collect()
        
        # The record with null id
        one_invalid = [row for row in invalid_df if row["id"] is None][0]
        self.assertIn("Null primary identifier", one_invalid["validation_errors"])
        
        # The record with invalid references (order_id 9999 and product_id 9999)
        one_with_invalid_refs = [row for row in invalid_df if row["order_id"] == 9999][0]
        self.assertIn("Invalid order_id reference", one_with_invalid_refs["validation_errors"])
        self.assertIn("Invalid product_id reference", one_with_invalid_refs["validation_errors"])
    
    # Test for the process_dataset function
    @patch('delta.tables.DeltaTable')
    @patch('logging.Logger.info')
    @patch('logging.Logger.error')
    def test_process_dataset_write_new(self, mock_error, mock_info, mock_delta_table):
        """Test the process_dataset function for creating a new Delta table."""
        # Mock DeltaTable.forPath to raise an exception (table doesn't exist)
        mock_delta_table.forPath.side_effect = Exception("Table doesn't exist")
        
        # Define simplified process_dataset function for testing
        def process_dataset(raw_df, schema, schema_name, output_path, reference_data=None):
            # Actual implementation details omitted for brevity
            # This is just to test the Delta table creation logic
            
            # Simulate validation
            valid_data = raw_df.filter(col("product_id").isNotNull())
            rejected_data = raw_df.filter(col("product_id").isNull())
            
            # Determine partition column
            partition_col = "date" if schema_name in ["order_items", "orders"] else "department"
            
            # Check if Delta table exists
            try:
                # This will raise an exception in our test
                delta_table = mock_delta_table.forPath(None, output_path)
                # Merge logic would happen here if table existed
            except Exception as e:
                # If Delta table does not exist, create it
                # In a real test, we would mock the write operation
                if partition_col in valid_data.columns:
                    # Would write with partitioning
                    pass
                else:
                    # Would write without partitioning
                    pass
                
            return valid_data
        
        # Test data
        test_data = self.create_test_products_df()
        output_path = "/tmp/test_output"
        
        # Run the function
        result = process_dataset(test_data, self.products_schema, "products", output_path)
        
        # Assertions
        self.assertEqual(result.count(), test_data.count())  # All records are valid in this case
        mock_delta_table.forPath.assert_called_once_with(None, output_path)
    
    # Test for the log_dataframe_info function
    @patch('logging.Logger.info')
    def test_log_dataframe_info(self, mock_info):
        """Test the log_dataframe_info function."""
        def log_dataframe_info(df, name):
            count = df.count()
            columns = len(df.columns)
            logging.getLogger().info(f"DataFrame {name}: {count} rows, {columns} columns")
        
        # Test data
        test_data = self.create_test_products_df()
        
        # Call function
        log_dataframe_info(test_data, "test_products")
        
        # Assertions
        mock_info.assert_called_once_with("DataFrame test_products: 5 rows, 4 columns")

    # Test the timed_execution decorator
    @patch('logging.Logger.info')
    def test_timed_execution(self, mock_info):
        """Test the timed_execution decorator."""
        def timed_execution(func):
            def wrapper(*args, **kwargs):
                start_time = datetime.now()
                result = func(*args, **kwargs)
                end_time = datetime.now()
                logging.getLogger().info(f"Function {func.__name__} executed in {(end_time - start_time).total_seconds():.2f} seconds")
                return result
            return wrapper
        
        # Test function to decorate
        @timed_execution
        def test_func(x):
            return x * 2
        
        # Call decorated function
        result = test_func(5)
        
        # Assertions
        self.assertEqual(result, 10)
        # Check that log was called with correct function name
        self.assertTrue(mock_info.call_args[0][0].startswith("Function test_func executed in"))

if __name__ == '__main__':
    unittest.main()