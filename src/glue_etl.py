import sys
import time
import logging
from datetime import datetime
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, current_timestamp, isnan, when, count, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, DateType



logger = logging.getLogger()
logger.setLevel(logging.INFO) # Default, can be overridden by job args later

# Add timestamp to logs (Keep global)
def log(message, level="INFO"):
    """Adds a timestamp to log messages."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if level == "INFO":
        logger.info(f"[{timestamp}] {message}")
    elif level == "ERROR":
        logger.error(f"[{timestamp}] {message}")
    elif level == "WARNING":
        logger.warning(f"[{timestamp}] {message}")
    else:
        logger.info(f"[{timestamp}] {message}")

# --- Schemas (Keep global for easy import/reference) ---
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

# --- Helper Functions (Keep global) ---
def timed_execution(func):
    """Decorator to measure execution time of a function."""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        log(f"Function {func.__name__} executed in {end_time - start_time:.2f} seconds")
        return result
    return wrapper

def log_dataframe_info(df, name):
    """Logs basic information about a DataFrame."""
    count = df.count()
    columns = len(df.columns)
    log(f"DataFrame {name}: {count} rows, {columns} columns")
    if logger.level <= logging.DEBUG:
        log(f"DataFrame {name} schema:", "DEBUG")
        for field in df.schema.fields:
            log(f"  - {field.name}: {field.dataType} (nullable: {field.nullable})", "DEBUG")

@timed_execution
def validate_data(df, schema_name, reference_data=None):
    """
    Applies validation rules based on the schema name and separates valid from invalid records.
    Assumes 'log' function is globally available.
    """
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
            order_ids = reference_data["orders"].select("order_id").distinct().cache()
            order_ids_list = order_ids.rdd.flatMap(lambda x: x).collect()
            validated = validated.withColumn(
                "validation_errors",
                when(~col("order_id").isin(order_ids_list),
                     when(col("validation_errors").isNull(), "Invalid order_id reference")
                     .otherwise(col("validation_errors") + "; Invalid order_id reference"))
                .otherwise(col("validation_errors"))
            )
            order_ids.unpersist()
        # Referential integrity - product_id must exist in products table
        if reference_data and "products" in reference_data:
            log("Checking product_id referential integrity")
            product_ids = reference_data["products"].select("product_id").distinct().cache()
            product_ids_list = product_ids.rdd.flatMap(lambda x: x).collect()
            validated = validated.withColumn(
                "validation_errors",
                when(~col("product_id").isin(product_ids_list),
                     when(col("validation_errors").isNull(), "Invalid product_id reference")
                     .otherwise(col("validation_errors") + "; Invalid product_id reference"))
                .otherwise(col("validation_errors"))
            )
            product_ids.unpersist()

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

    # Log validation results (Ensure DataFrame actions are performed)
    valid_count = valid_records.count()
    invalid_count = invalid_records.count()
    # Avoid division by zero if total_count is zero
    total_count = valid_count + invalid_count
    if total_count > 0:
        valid_percentage = (valid_count / total_count) * 100
        log(f"Validation results for {schema_name}: {valid_count}/{total_count} valid records ({valid_percentage:.2f}%)")
    else:
        log(f"Validation results for {schema_name}: 0/0 valid records")

    # Unpersist the cached dataframe
    validated_cached.unpersist()

    return valid_records, invalid_records

@timed_execution
def process_dataset(raw_df, schema, schema_name, output_path, rejected_path, job_name, spark, reference_data=None):
    """
    Processes a raw DataFrame: type casting, validation, deduplication, and writes to Delta Lake.
    Writes rejected records to a separate Delta path.
    Uses passed 'spark', 'rejected_path', 'job_name'. Assumes 'log', 'validate_data' are globally available.
    """
    try:
        log(f"Processing {schema_name} dataset")
        log_dataframe_info(raw_df, f"{schema_name}_raw") # Assumes log_dataframe_info is global

        # Cache raw dataframe if it will be used multiple times
        raw_df_cached = raw_df.cache()
        raw_df_cached.count() # Action to ensure caching

        # Cast data to match schema
        log(f"Converting {schema_name} data types to match schema")
        typed_df = raw_df_cached
        for field in schema.fields:
            if field.name == "order_timestamp":
                 typed_df = typed_df.withColumn(field.name, to_timestamp(col(field.name)))
            else:
                 # Ensure column exists before trying to cast
                 if field.name in typed_df.columns:
                     typed_df = typed_df.withColumn(field.name, col(field.name).cast(field.dataType))
                 else:
                     log(f"Warning: Column '{field.name}' not found in raw data for {schema_name}, skipping cast.", "WARNING")


        # Cache typed dataframe as it will be used in validation
        typed_df_cached = typed_df.cache()
        typed_df_cached.count() # Action to ensure caching
        log_dataframe_info(typed_df_cached, f"{schema_name}_typed") # Assumes log_dataframe_info is global

        # Apply validation rules
        valid_data, rejected_data = validate_data(typed_df_cached, schema_name, reference_data) # Assumes validate_data is global

        # Release the cached raw dataframe
        raw_df_cached.unpersist()

        # Log rejected records
        # Need an action to materialize rejected_data before count
        rejected_data_cached = rejected_data.cache()
        rejected_count = rejected_data_cached.count()

        if rejected_count > 0:
            log(f"Found {rejected_count} rejected {schema_name} records")

            # Enhance rejected records with metadata
            rejected_data_to_write = rejected_data_cached.withColumn("rejection_time", current_timestamp())
            rejected_data_to_write = rejected_data_to_write.withColumn("source", lit(schema_name))
            rejected_data_to_write = rejected_data_to_write.withColumn("job_name", lit(job_name)) # Use passed job_name

            # Write rejected records to S3 (using passed rejected_path)
            rejected_path_full = f"{rejected_path}/{schema_name}"
            log(f"Writing rejected {schema_name} records to {rejected_path_full}")

            # Write with partitioning by date if available
            if "date" in rejected_data_to_write.columns:
                rejected_data_to_write.write.format("delta").mode("append").partitionBy("date").save(rejected_path_full)
            else:
                rejected_data_to_write.write.format("delta").mode("append").save(rejected_path_full)

            # Print rejection summary
            log(f"Rejected {rejected_count} {schema_name} records due to validation errors")

            # Get detailed rejection counts by error type
            try:
                rejection_counts = rejected_data_to_write.groupBy("validation_errors").count().orderBy(col("count").desc())
                rejection_counts.show(truncate=False)
                # Log top error types
                error_rows = rejection_counts.collect()
                for row in error_rows[:5]:  # Log top 5 error types
                    log(f"Error type: {row['validation_errors']} - Count: {row['count']}")
            except Exception as show_err:
                 log(f"Could not show rejection counts: {show_err}", "WARNING")
        else:
            log(f"No rejected records found in {schema_name} dataset")

        rejected_data_cached.unpersist() # Unpersist rejected data cache

        # Cache valid data for deduplication
        valid_data_cached = valid_data.cache()
        pre_dedup_count = valid_data_cached.count() # Action needed before dropDuplicates

        # Deduplicate valid data based on primary key
        primary_key = "id" if schema_name == "order_items" else "order_id" if schema_name == "orders" else "product_id"
        log(f"Deduplicating {schema_name} data on {primary_key}")

        deduplicated_data = valid_data_cached.dropDuplicates([primary_key])
        # Cache after dropDuplicates if used multiple times below
        deduplicated_data_cached = deduplicated_data.cache()
        post_dedup_count = deduplicated_data_cached.count()

        valid_data_cached.unpersist() # Release the pre-dedup cache

        # Release the cached typed dataframe (Ensure it's unpersisted)
        typed_df_cached.unpersist()

        if pre_dedup_count > post_dedup_count:
            log(f"Removed {pre_dedup_count - post_dedup_count} duplicate {schema_name} records based on {primary_key}")
        else:
            log(f"No duplicates found in {schema_name} dataset based on {primary_key}")

        # Determine partition column
        partition_col = "date" if schema_name in ["order_items", "orders"] else "department"

        # Check if partition column exists before partitioning
        if partition_col in deduplicated_data_cached.columns:
            log(f"Partitioning {schema_name} data by {partition_col}")
            # Repartition might not be necessary if partitioning on write, depends on scale
            partitioned_data = deduplicated_data_cached # .repartition(col(partition_col)) # Consider removing repartition if using partitionBy below
        else:
            partitioned_data = deduplicated_data_cached
            log(f"Warning: Partition column '{partition_col}' not found in {schema_name} dataset", "WARNING")

        # Write to Delta Lake (initial load or upsert)
        try:
            # Check if Delta table exists (use passed spark instance)
            log(f"Checking if Delta table exists at {output_path}")
            delta_table = DeltaTable.forPath(spark, output_path)

            # Determine merge condition based on primary key
            merge_condition = f"existing.{primary_key} = updates.{primary_key}"

            # Merge/upsert logic
            log(f"Performing Delta merge operation for {schema_name} into {output_path}")
            merge_start = time.time()
            (delta_table.alias("existing").merge(
                partitioned_data.alias("updates"),
                merge_condition
            ).whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())
            merge_end = time.time()

            log(f"Successfully merged {post_dedup_count} {schema_name} records into the Delta table in {merge_end - merge_start:.2f} seconds")

        except Exception as e:
            # Check if the error indicates the path doesn't exist (common for initial write)
            if "Path does not exist" in str(e) or "is not a Delta table" in str(e):
                 log(f"Delta table for {schema_name} does not exist, creating at {output_path}. Error detail: {str(e)}")

                 write_start = time.time()
                 # Write with partitioning if partition column exists
                 if partition_col in partitioned_data.columns:
                     log(f"Writing initial {schema_name} data with partitioning by {partition_col}")
                     partitioned_data.write.format("delta").mode("overwrite").partitionBy(partition_col).save(output_path)
                 else:
                     log(f"Writing initial {schema_name} data without partitioning")
                     partitioned_data.write.format("delta").mode("overwrite").save(output_path)
                 write_end = time.time()

                 log(f"Successfully wrote {post_dedup_count} {schema_name} records to new Delta table in {write_end - write_start:.2f} seconds")
            else:
                 # Re-raise other exceptions during merge/write
                 log(f"Error during Delta merge/write operation for {schema_name}: {str(e)}", "ERROR")
                 raise e


        # Release the final cached dataframe
        deduplicated_data_cached.unpersist()

        return deduplicated_data # Or return the path, or success status

    except Exception as e:
        log(f"Error processing {schema_name} dataset: {str(e)}", "ERROR")
        # Ensure cleanup happens if possible
        if 'raw_df_cached' in locals() and raw_df_cached.is_cached: raw_df_cached.unpersist()
        if 'typed_df_cached' in locals() and typed_df_cached.is_cached: typed_df_cached.unpersist()
        if 'valid_data_cached' in locals() and valid_data_cached.is_cached: valid_data_cached.unpersist()
        if 'rejected_data_cached' in locals() and rejected_data_cached.is_cached: rejected_data_cached.unpersist()
        if 'deduplicated_data_cached' in locals() and deduplicated_data_cached.is_cached: deduplicated_data_cached.unpersist()
        raise e

def main():
    """Main function orchestrating the Glue job execution."""
    # Initialize Spark and Glue contexts *inside main*
    log("Initializing Spark and Glue contexts for job execution")
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)

    # Set Spark configuration for better performance
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.cbo.enabled", "true")
    spark.conf.set("spark.sql.statistics.histogram.enabled", "true")

    # Resolve input arguments *inside main*
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

    # Set log level from job parameter
    log_level = args.get('LOG_LEVEL', 'INFO').upper()
    if log_level in ['DEBUG', 'INFO', 'WARNING', 'ERROR']:
        logger.setLevel(getattr(logging, log_level))
        log(f"Log level set to {log_level}")
    else:
         logger.setLevel(logging.INFO) # Default if invalid level provided
         log(f"Invalid LOG_LEVEL '{args.get('LOG_LEVEL')}'. Defaulting to INFO.")

    # Extract args into variables
    s3_bucket_path = args['S3_BUCKET_PATH']
    order_items_output = args['ORDER_ITEMS_OUTPUT_PATH']
    orders_output = args['ORDERS_OUTPUT_PATH']
    products_output = args['PRODUCTS_OUTPUT_PATH']
    rejected_path = args['REJECTED_PATH']
    job_name = args['JOB_NAME']

    # Initialize Job *inside main*
    job.init(job_name, args)
    log(f"Starting job: {job_name}")
    log(f"S3 raw data path prefix: {s3_bucket_path}")
    log(f"Output paths - Order Items: {order_items_output}, Orders: {orders_output}, Products: {products_output}")
    log(f"Rejected records path: {rejected_path}")


    try:
        # Set job start time
        job_start_time = time.time()

        # Read products first as it's referenced by order_items
        log("Reading products data")
        products_path = f"{s3_bucket_path}/products.csv" # Assuming specific filename
        products_raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(products_path)
        # Pass necessary args to process_dataset
        products_data = process_dataset(
            raw_df=products_raw,
            schema=products_schema,
            schema_name="products",
            output_path=products_output,
            rejected_path=rejected_path, # from args
            job_name=job_name,           # from args
            spark=spark                  # from GlueContext
            # reference_data is None here
        )

        # Cache products data for reference
        log("Caching products data for reference")
        products_data_cached = products_data.cache()
        products_data_cached.count() # Action to trigger cache

        # Read orders next as it's referenced by order_items
        log("Reading orders data")
        orders_path = f"{s3_bucket_path}/orders/*.csv" # Assuming wildcard pattern
        orders_raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(orders_path)
        orders_data = process_dataset(
            raw_df=orders_raw,
            schema=orders_schema,
            schema_name="orders",
            output_path=orders_output,
            rejected_path=rejected_path, # from args
            job_name=job_name,           # from args
            spark=spark                  # from GlueContext
            # reference_data is None here
        )

        # Cache orders data for reference
        log("Caching orders data for reference")
        orders_data_cached = orders_data.cache()
        orders_data_cached.count() # Action to trigger cache


        # Read order_items last and validate against products and orders
        log("Reading order_items data")
        order_items_path = f"{s3_bucket_path}/order_items/*.csv" # Assuming wildcard pattern
        order_items_raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(order_items_path)

        # Create reference data dictionary for validation
        reference_data = {
            "products": products_data_cached,
            "orders": orders_data_cached
        }

        # Process order_items with referential integrity checks
        order_items_data = process_dataset(
            raw_df=order_items_raw,
            schema=order_items_schema,
            schema_name="order_items",
            output_path=order_items_output,
            rejected_path=rejected_path, # from args
            job_name=job_name,           
            spark=spark,                 # from GlueContext
            reference_data=reference_data
        )

        # Unpersist cached reference data
        log("Releasing cached reference data")
        products_data_cached.unpersist()
        orders_data_cached.unpersist()

        # Calculate job statistics
        job_end_time = time.time()
        job_duration = job_end_time - job_start_time

        log(f"All datasets processed successfully in {job_duration:.2f} seconds!")

    except Exception as e:
        log(f"Fatal error in main processing flow: {str(e)}", "ERROR")
        # Log traceback for debugging
        import traceback
        logger.error(traceback.format_exc())
        raise e # Re-raise exception to fail the job
    finally:
        # Log job completion
        log("Job execution completed")
        # Commit the Glue job
        job.commit()

if __name__ == "__main__":
    log("Starting Glue job execution via main entry point")
    main()