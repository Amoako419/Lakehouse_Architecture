import sys
import time
import logging
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, current_timestamp, isnan, when, count, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, DateType

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Add timestamp to logs
def log(message, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if level == "INFO":
        logger.info(f"[{timestamp}] {message}")
    elif level == "ERROR":
        logger.error(f"[{timestamp}] {message}")
    elif level == "WARNING":
        logger.warning(f"[{timestamp}] {message}")
    else:
        logger.info(f"[{timestamp}] {message}")

# Initialize Spark and Glue contexts
log("Initializing Spark and Glue contexts")
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Set Spark configuration for better performance
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.statistics.histogram.enabled", "true")

# Initialize Job
job = Job(glueContext)

# Resolve input arguments
log("Resolving job arguments")
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 
    'S3_BUCKET_PATH', 
    'ORDER_ITEMS_OUTPUT_PATH', 
    'ORDERS_OUTPUT_PATH', 
    'PRODUCTS_OUTPUT_PATH',
    'REJECTED_PATH',
    'LOG_LEVEL'
])

# Set log level from job parameter, default to INFO
log_level = args.get('LOG_LEVEL', 'INFO').upper()
if log_level in ['DEBUG', 'INFO', 'WARNING', 'ERROR']:
    logger.setLevel(getattr(logging, log_level))
    log(f"Log level set to {log_level}")

s3_bucket_path = args['S3_BUCKET_PATH']
order_items_output = args['ORDER_ITEMS_OUTPUT_PATH']
orders_output = args['ORDERS_OUTPUT_PATH']
products_output = args['PRODUCTS_OUTPUT_PATH']
rejected_path = args['REJECTED_PATH']
job_name = args['JOB_NAME']

job.init(job_name, args)
log(f"Starting job: {job_name}")
log(f"S3 bucket path: {s3_bucket_path}")
log(f"Output paths - Order Items: {order_items_output}, Orders: {orders_output}, Products: {products_output}")
log(f"Rejected records path: {rejected_path}")

# Define schemas for the three datasets
log("Defining data schemas")
order_items_schema = StructType([
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

orders_schema = StructType([
    StructField("order_num", IntegerType(), nullable=True),
    StructField("order_id", IntegerType(), nullable=False),
    StructField("user_id", IntegerType(), nullable=False),
    StructField("order_timestamp", TimestampType(), nullable=False),
    StructField("total_amount", DoubleType(), nullable=True),
    StructField("date", DateType(), nullable=False)
])

products_schema = StructType([
    StructField("product_id", IntegerType(), nullable=False),
    StructField("department_id", IntegerType(), nullable=True),
    StructField("department", StringType(), nullable=True),
    StructField("product_name", StringType(), nullable=False)
])

# Function to measure execution time
def timed_execution(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        log(f"Function {func.__name__} executed in {end_time - start_time:.2f} seconds")
        return result
    return wrapper

# Function to log DataFrame information
def log_dataframe_info(df, name):
    count = df.count()
    columns = len(df.columns)
    log(f"DataFrame {name}: {count} rows, {columns} columns")
    if logger.level <= logging.DEBUG:
        log(f"DataFrame {name} schema:", "DEBUG")
        for field in df.schema.fields:
            log(f"  - {field.name}: {field.dataType} (nullable: {field.nullable})", "DEBUG")

# Function to apply validation rules and separate valid from invalid records
@timed_execution
def validate_data(df, schema_name, reference_data=None):
    log(f"Validating {schema_name} dataset")
    
    # Add validation error column
    validated = df.withColumn("validation_errors", lit(None).cast(StringType()))
    
    # Apply common validation rules based on schema
    if schema_name == "order_items":
        log("Applying order_items validation rules")
        # No null primary identifiers
        validated = validated.withColumn(
            "validation_errors",
            when(col("id").isNull(), "Null primary identifier")
            .otherwise(col("validation_errors"))
        )
        
        # Order ID must not be null
        validated = validated.withColumn(
            "validation_errors",
            when(col("order_id").isNull(), 
                 when(col("validation_errors").isNull(), "Null order_id")
                 .otherwise(col("validation_errors") + "; Null order_id"))
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
        
        # Valid timestamp check
        validated = validated.withColumn(
            "validation_errors",
            when(col("order_timestamp").isNull(), 
                 when(col("validation_errors").isNull(), "Invalid timestamp")
                 .otherwise(col("validation_errors") + "; Invalid timestamp"))
            .otherwise(col("validation_errors"))
        )
        
        # Referential integrity - order_id must exist in orders table
        if reference_data and "orders" in reference_data:
            log("Checking order_id referential integrity")
            # Cache order IDs for better performance
            order_ids = reference_data["orders"].select("order_id").distinct()
            order_ids_cached = order_ids.cache()
            order_ids_list = order_ids_cached.rdd.flatMap(lambda x: x).collect()
            
            validated = validated.withColumn(
                "validation_errors",
                when(~col("order_id").isin(order_ids_list), 
                     when(col("validation_errors").isNull(), "Invalid order_id reference")
                     .otherwise(col("validation_errors") + "; Invalid order_id reference"))
                .otherwise(col("validation_errors"))
            )
            
            # Release cache after use
            order_ids_cached.unpersist()
        
        # Referential integrity - product_id must exist in products table
        if reference_data and "products" in reference_data:
            log("Checking product_id referential integrity")
            # Cache product IDs for better performance
            product_ids = reference_data["products"].select("product_id").distinct()
            product_ids_cached = product_ids.cache()
            product_ids_list = product_ids_cached.rdd.flatMap(lambda x: x).collect()
            
            validated = validated.withColumn(
                "validation_errors",
                when(~col("product_id").isin(product_ids_list), 
                     when(col("validation_errors").isNull(), "Invalid product_id reference")
                     .otherwise(col("validation_errors") + "; Invalid product_id reference"))
                .otherwise(col("validation_errors"))
            )
            
            # Release cache after use
            product_ids_cached.unpersist()
    
    elif schema_name == "orders":
        log("Applying orders validation rules")
        # Order ID must not be null (primary key)
        validated = validated.withColumn(
            "validation_errors",
            when(col("order_id").isNull(), "Null order_id primary key")
            .otherwise(col("validation_errors"))
        )
        
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
        log("Applying products validation rules")
        # Product ID must not be null (primary key)
        validated = validated.withColumn(
            "validation_errors",
            when(col("product_id").isNull(), "Null product_id primary key")
            .otherwise(col("validation_errors"))
        )
        
        # Product name must not be null
        validated = validated.withColumn(
            "validation_errors",
            when(col("product_name").isNull(), 
                 when(col("validation_errors").isNull(), "Null product name")
                 .otherwise(col("validation_errors") + "; Null product name"))
            .otherwise(col("validation_errors"))
        )
    
    # Cache validated dataframe for better performance on split operations
    validated_cached = validated.cache()
    
    # Split into valid and invalid records
    log("Splitting into valid and invalid records")
    valid_records = validated_cached.filter(col("validation_errors").isNull()).drop("validation_errors")
    invalid_records = validated_cached.filter(col("validation_errors").isNotNull())
    
    # Log validation results
    valid_count = valid_records.count()
    invalid_count = invalid_records.count()
    total_count = valid_count + invalid_count
    log(f"Validation results for {schema_name}: {valid_count}/{total_count} valid records ({(valid_count/total_count)*100:.2f}%)")
    
    # Unpersist the cached dataframe
    validated_cached.unpersist()
    
    return valid_records, invalid_records

# Function to process and write a dataset
@timed_execution
def process_dataset(raw_df, schema, schema_name, output_path, reference_data=None):
    try:
        log(f"Processing {schema_name} dataset")
        log_dataframe_info(raw_df, f"{schema_name}_raw")
        
        # Cache raw dataframe if it will be used multiple times
        raw_df_cached = raw_df.cache()
        
        # Cast data to match schema
        log(f"Converting {schema_name} data types to match schema")
        typed_df = raw_df_cached
        for field in schema.fields:
            if field.name == "order_timestamp":
                typed_df = typed_df.withColumn(field.name, to_timestamp(col(field.name)))
            else:
                typed_df = typed_df.withColumn(field.name, col(field.name).cast(field.dataType))
        
        # Cache typed dataframe as it will be used in validation
        typed_df_cached = typed_df.cache()
        log_dataframe_info(typed_df_cached, f"{schema_name}_typed")
        
        # Apply validation rules
        valid_data, rejected_data = validate_data(typed_df_cached, schema_name, reference_data)
        
        # Release the cached raw dataframe
        raw_df_cached.unpersist()
        
        # Log rejected records
        rejected_count = rejected_data.count()
        if rejected_count > 0:
            log(f"Found {rejected_count} rejected {schema_name} records")
            
            # Enhance rejected records with metadata
            rejected_data = rejected_data.withColumn("rejection_time", current_timestamp())
            rejected_data = rejected_data.withColumn("source", lit(schema_name))
            rejected_data = rejected_data.withColumn("job_name", lit(job_name))
            
            # Write rejected records to S3
            rejected_path_full = f"{rejected_path}/{schema_name}"
            log(f"Writing rejected {schema_name} records to {rejected_path_full}")
            
            # Write with partitioning by date if available
            if "date" in rejected_data.columns:
                rejected_data.write.format("delta").mode("append").partitionBy("date").save(rejected_path_full)
            else:
                rejected_data.write.format("delta").mode("append").save(rejected_path_full)
            
            # Print rejection summary
            log(f"Rejected {rejected_count} {schema_name} records due to validation errors")
            
            # Get detailed rejection counts by error type
            rejection_counts = rejected_data.groupBy("validation_errors").count().orderBy(col("count").desc())
            rejection_counts.show(truncate=False)
            
            # Log top error types
            error_rows = rejection_counts.collect()
            for row in error_rows[:5]:  # Log top 5 error types
                log(f"Error type: {row['validation_errors']} - Count: {row['count']}")
        else:
            log(f"No rejected records found in {schema_name} dataset")
        
        # Cache valid data for deduplication
        valid_data_cached = valid_data.cache()
        
        # Deduplicate valid data based on primary key
        primary_key = "id" if schema_name == "order_items" else "order_id" if schema_name == "orders" else "product_id"
        log(f"Deduplicating {schema_name} data on {primary_key}")
        pre_dedup_count = valid_data_cached.count()
        deduplicated_data = valid_data_cached.dropDuplicates([primary_key])
        post_dedup_count = deduplicated_data.count()
        
        # Release the cached valid dataframe
        valid_data_cached.unpersist()
        
        # Release the cached typed dataframe
        typed_df_cached.unpersist()
        
        if pre_dedup_count > post_dedup_count:
            log(f"Removed {pre_dedup_count - post_dedup_count} duplicate {schema_name} records")
        else:
            log(f"No duplicates found in {schema_name} dataset")
        
        # Cache deduplicated data for partitioning and writing
        deduplicated_data_cached = deduplicated_data.cache()
        
        # Determine partition column
        partition_col = "date" if schema_name in ["order_items", "orders"] else "department"
        
        # Check if partition column exists before partitioning
        if partition_col in deduplicated_data_cached.columns:
            log(f"Partitioning {schema_name} data by {partition_col}")
            partitioned_data = deduplicated_data_cached.repartition(col(partition_col))
        else:
            partitioned_data = deduplicated_data_cached
            log(f"Warning: Partition column {partition_col} not found in {schema_name} dataset", "WARNING")
        
        # Write to Delta Lake (initial load or upsert)
        try:
            # Check if Delta table exists
            log(f"Checking if Delta table exists at {output_path}")
            delta_table = DeltaTable.forPath(spark, output_path)
            
            # Determine merge condition based on primary key
            merge_condition = f"existing.{primary_key} = updates.{primary_key}"
            
            # Merge/upsert logic
            log(f"Performing Delta merge operation for {schema_name}")
            merge_start = time.time()
            delta_table.alias("existing").merge(
                partitioned_data.alias("updates"),
                merge_condition
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            merge_end = time.time()
            
            log(f"Successfully merged {partitioned_data.count()} {schema_name} records into the Delta table in {merge_end - merge_start:.2f} seconds")
        except Exception as e:
            # If Delta table does not exist, create it
            log(f"Delta table for {schema_name} does not exist, creating at {output_path}")
            
            write_start = time.time()
            # Write with partitioning if partition column exists
            if partition_col in partitioned_data.columns:
                log(f"Writing {schema_name} data with partitioning by {partition_col}")
                partitioned_data.write.format("delta").mode("overwrite").partitionBy(partition_col).save(output_path)
            else:
                log(f"Writing {schema_name} data without partitioning")
                partitioned_data.write.format("delta").mode("overwrite").save(output_path)
            write_end = time.time()
                
            log(f"Successfully wrote {partitioned_data.count()} {schema_name} records to new Delta table in {write_end - write_start:.2f} seconds")
        
        # Release the cached deduplicated dataframe
        deduplicated_data_cached.unpersist()
        
        return deduplicated_data
        
    except Exception as e:
        log(f"Error processing {schema_name} dataset: {str(e)}", "ERROR")
        raise e

try:
    # Set job start time
    job_start_time = time.time()
    
    # Read products first as it's referenced by order_items
    log("Reading products data")
    products_path = f"{s3_bucket_path}/product.csv"
    products_raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(products_path)
    products_data = process_dataset(products_raw, products_schema, "products", products_output)
    
    # Cache products data for reference
    log("Caching products data for reference")
    products_data_cached = products_data.cache()
    
    # Read orders next as it's referenced by order_items
    log("Reading orders data")
    orders_path = f"{s3_bucket_path}/orders/"
    orders_raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(orders_path)
    orders_data = process_dataset(orders_raw, orders_schema, "orders", orders_output)
    
    # Cache orders data for reference
    log("Caching orders data for reference")
    orders_data_cached = orders_data.cache()
    
    # Read order_items last and validate against products and orders
    log("Reading order_items data")
    order_items_path = f"{s3_bucket_path}/order_items/"
    order_items_raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(order_items_path)
    
    # Create reference data dictionary for validation
    reference_data = {
        "products": products_data_cached,
        "orders": orders_data_cached
    }
    
    # Process order_items with referential integrity checks
    order_items_data = process_dataset(order_items_raw, order_items_schema, "order_items", order_items_output, reference_data)
    
    # Unpersist cached reference data
    log("Releasing cached reference data")
    products_data_cached.unpersist()
    orders_data_cached.unpersist()
    
    # Calculate job statistics
    job_end_time = time.time()
    job_duration = job_end_time - job_start_time
    
    log(f"All datasets processed successfully in {job_duration:.2f} seconds!")

except Exception as e:
    log(f"Error in main processing flow: {str(e)}", "ERROR")
    # Log the error and exit with failure
    raise e
finally:
    # Log job completion
    log("Job execution completed")
    # Commit the Glue job
    job.commit()