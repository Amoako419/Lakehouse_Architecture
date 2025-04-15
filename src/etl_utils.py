# src/etl_utils.py
"""
Utility functions and schema definitions for the E-commerce Lakehouse ETL process.
This module contains reusable logic independent of the AWS Glue runtime environment
and file I/O operations. Functions focus on transformations and validation.
"""

import logging
import time
from datetime import datetime

# Import necessary PySpark modules
from pyspark.sql.functions import col, lit, current_timestamp, when, count, to_timestamp, concat_ws, coalesce, array, array_union, size # Added size
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, DateType

# *** Removed DeltaTable import ***

# ==============================================================================
# Global Scope: Definitions available for import
# ==============================================================================

# Configure logging
logger = logging.getLogger(__name__)

def log(message, level="INFO"):
    """Adds a timestamp to log messages using the module's logger."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if level == "INFO":
        logger.info(f"[{timestamp}] {message}")
    elif level == "ERROR":
        logger.error(f"[{timestamp}] {message}")
    elif level == "WARNING":
        logger.warning(f"[{timestamp}] {message}")
    else:
        logger.info(f"[{timestamp}] {message}")

# --- Schemas ---
log("Defining data schemas in etl_utils")
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

# --- Helper Functions ---
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
    try:
        # Use isEmpty() which can be faster than count() for just checking existence
        if df.isEmpty():
             log(f"DataFrame {name}: 0 rows")
             return
        count_val = df.count() # Get count if not empty
        columns_val = len(df.columns)
        log(f"DataFrame {name}: {count_val} rows, {columns_val} columns")
        # if logger.isEnabledFor(logging.DEBUG):
        #    log(f"DataFrame {name} schema:", "DEBUG")
        #    df.printSchema()
    except Exception as e:
        log(f"Could not get info for DataFrame {name}: {e}", "WARNING")


@timed_execution
def validate_data(df, schema_name, reference_data=None):
    """
    Applies validation rules based on the schema name and separates valid from invalid records.
    Uses left-anti joins for referential integrity and array_union for error accumulation.
    Returns two DataFrames: valid_records, invalid_records.
    """
    log(f"Validating {schema_name} dataset")
    if df.isEmpty():
        log(f"Input DataFrame for {schema_name} validation is empty. Returning empty DataFrames.")
        # Return empty DFs matching expected output structure but without error columns
        empty_valid = df.sparkSession.createDataFrame([], schema=df.schema)
        # Invalid schema includes original cols + validation_errors string
        invalid_schema_fields = df.schema.fields + [StructField("validation_errors", StringType(), True)]
        empty_invalid = df.sparkSession.createDataFrame([], schema=StructType(invalid_schema_fields))
        return empty_valid, empty_invalid

    df.cache()
    df.count() # Action to trigger cache

    validated_df = df.withColumn("validation_errors_list", array().cast("array<string>"))

    current_schema = None
    if schema_name == "order_items":
        current_schema = order_items_schema
    elif schema_name == "orders":
        current_schema = orders_schema
    elif schema_name == "products":
        current_schema = products_schema
    else:
        df.unpersist()
        raise ValueError(f"Unknown schema_name: {schema_name}")

    log(f"Checking non-nullable fields for {schema_name}")
    for field in current_schema.fields:
        if not field.nullable:
             validated_df = validated_df.withColumn(
                 "validation_errors_list",
                 when(col(field.name).isNull(),
                      array_union(col("validation_errors_list"), array(lit(f"Null {field.name}"))))
                 .otherwise(col("validation_errors_list"))
             )

    if schema_name == "order_items":
        if reference_data and "orders" in reference_data and "order_id" in reference_data["orders"].columns:
            log("Checking order_id referential integrity using anti-join")
            ref_orders = reference_data["orders"].select("order_id").distinct()
            invalid_order_ids_df = validated_df.alias("oi") \
                .join(ref_orders.alias("o"), col("oi.order_id") == col("o.order_id"), "left_anti") \
                .select(col("oi.id").alias("invalid_id")).distinct()
            validated_df = validated_df.alias("v") \
                .join(invalid_order_ids_df.alias("inv"), col("v.id") == col("inv.invalid_id"), "left_outer") \
                .withColumn("validation_errors_list",
                    when(col("inv.invalid_id").isNotNull(),
                         array_union(col("v.validation_errors_list"), array(lit("Invalid order_id reference"))))
                    .otherwise(col("v.validation_errors_list"))
                ).select("v.*")
        if reference_data and "products" in reference_data and "product_id" in reference_data["products"].columns:
            log("Checking product_id referential integrity using anti-join")
            ref_products = reference_data["products"].select("product_id").distinct()
            invalid_product_ids_df = validated_df.alias("oi") \
                .join(ref_products.alias("p"), col("oi.product_id") == col("p.product_id"), "left_anti") \
                .select(col("oi.id").alias("invalid_id")).distinct()
            validated_df = validated_df.alias("v") \
                .join(invalid_product_ids_df.alias("inv"), col("v.id") == col("inv.invalid_id"), "left_outer") \
                .withColumn("validation_errors_list",
                    when(col("inv.invalid_id").isNotNull(),
                         array_union(col("v.validation_errors_list"), array(lit("Invalid product_id reference"))))
                    .otherwise(col("v.validation_errors_list"))
                ).select("v.*")

    elif schema_name == "orders":
        validated_df = validated_df.withColumn(
            "validation_errors_list",
            when((col("total_amount").isNotNull()) & (col("total_amount") <= 0),
                 array_union(col("validation_errors_list"), array(lit("Non-positive total amount"))))
            .otherwise(col("validation_errors_list"))
        )

    elif schema_name == "products":
        pass # No extra rules

    validated_df = validated_df.withColumn(
         "validation_errors",
         when(size(col("validation_errors_list")) > 0, concat_ws("; ", col("validation_errors_list")))
         .otherwise(lit(None).cast("string"))
    )

    validated_df.cache()
    validated_df.count()

    log("Splitting into valid and invalid records")
    valid_records = validated_df.filter(col("validation_errors").isNull()).drop("validation_errors_list", "validation_errors")
    invalid_records = validated_df.filter(col("validation_errors").isNotNull()).drop("validation_errors_list") # Keep validation_errors string

    valid_count = valid_records.count()
    invalid_count = invalid_records.count()
    total_count = valid_count + invalid_count
    if total_count > 0:
        valid_percentage = (valid_count / total_count) * 100
        log(f"Validation results for {schema_name}: {valid_count}/{total_count} valid records ({valid_percentage:.2f}%)")
    else:
        log(f"Validation results for {schema_name}: 0/0 valid records")

    validated_df.unpersist()
    df.unpersist()

    # Return both dataframes
    return valid_records, invalid_records


@timed_execution
def process_dataset(raw_df, schema, schema_name, job_name, spark, reference_data=None):
    """
    Processes a raw DataFrame: type casting, validation, deduplication.
    Returns two DataFrames: processed (valid, deduplicated) data and rejected data.
    Does NOT perform any file writing (Delta or otherwise).
    Requires SparkSession and job_name as arguments.
    Uses functions/schemas from this module.
    """
    # *** Removed output_path and rejected_path arguments ***
    try:
        log(f"Processing {schema_name} dataset for transformation (Job: {job_name})")
        if raw_df.isEmpty():
             log(f"Input DataFrame for {schema_name} processing is empty.")
             # Return empty DFs matching expected output structure
             empty_processed = spark.createDataFrame([], schema=schema) # Processed has final schema
             # Invalid schema includes original cols + validation_errors string
             invalid_schema_fields = schema.fields + [StructField("validation_errors", StringType(), True)]
             empty_rejected = spark.createDataFrame([], schema=StructType(invalid_schema_fields))
             return empty_processed, empty_rejected

        log_dataframe_info(raw_df, f"{schema_name}_raw")

        raw_df_cached = raw_df.cache()
        raw_df_cached.count()

        log(f"Converting {schema_name} data types to match schema")
        typed_df = raw_df_cached
        for field in schema.fields:
            if field.name in typed_df.columns:
                 if field.name == "order_timestamp":
                     try:
                         typed_df = typed_df.withColumn(field.name, to_timestamp(col(field.name)))
                     except Exception as cast_err:
                         log(f"Warning: Could not cast column '{field.name}' to TimestampType for {schema_name}. Error: {cast_err}", "WARNING")
                         typed_df = typed_df.withColumn(field.name, lit(None).cast(TimestampType()))
                 else:
                     try:
                         typed_df = typed_df.withColumn(field.name, col(field.name).cast(field.dataType))
                     except Exception as cast_err:
                         log(f"Warning: Could not cast column '{field.name}' to {field.dataType} for {schema_name}. Error: {cast_err}", "WARNING")
                         typed_df = typed_df.withColumn(field.name, lit(None).cast(field.dataType))
            else:
                 log(f"Warning: Column '{field.name}' not found in raw data for {schema_name}, skipping cast.", "WARNING")

        typed_df_cached = typed_df.cache()
        typed_df_cached.count()
        log_dataframe_info(typed_df_cached, f"{schema_name}_typed")

        # Call validate_data from this module
        valid_data, rejected_data = validate_data(typed_df_cached, schema_name, reference_data)

        raw_df_cached.unpersist()
        typed_df_cached.unpersist() # Unpersist typed DF earlier

        # Enhance rejected records with metadata (but don't write)
        rejected_data_enhanced = rejected_data # Start with rejected data
        if not rejected_data.isEmpty():
             rejected_data_enhanced = rejected_data.withColumn("rejection_time", current_timestamp())
             rejected_data_enhanced = rejected_data_enhanced.withColumn("source", lit(schema_name))
             rejected_data_enhanced = rejected_data_enhanced.withColumn("job_name", lit(job_name))
             log(f"Identified {rejected_data_enhanced.count()} rejected {schema_name} records")
             # Optional: Show rejection counts if needed for debugging during tests/runs
             # try:
             #     if logger.isEnabledFor(logging.INFO):
             #         rejected_data_enhanced.groupBy("validation_errors").count().orderBy(col("count").desc()).show(truncate=False)
             # except Exception as show_err:
             #     log(f"Could not show rejection counts: {show_err}", "WARNING")
        else:
             log(f"No rejected records found in {schema_name} dataset")

        # Cache valid data before deduplication
        valid_data_cached = valid_data.cache()
        pre_dedup_count = valid_data_cached.count()

        primary_key = "id" if schema_name == "order_items" else "order_id" if schema_name == "orders" else "product_id"
        log(f"Deduplicating {schema_name} data on {primary_key}")

        if primary_key not in valid_data_cached.columns:
             log(f"Primary key '{primary_key}' not found for deduplication in {schema_name}. Skipping deduplication.", "ERROR")
             deduplicated_data = valid_data_cached # Pass through without deduplication
        else:
             # Perform deduplication only if count > 0
             if pre_dedup_count > 0:
                 deduplicated_data = valid_data_cached.dropDuplicates([primary_key])
             else:
                 deduplicated_data = valid_data_cached # Avoid dropDuplicates on empty DF

        # No need to cache deduplicated_data unless further transforms happen here
        post_dedup_count = deduplicated_data.count()

        valid_data_cached.unpersist() # Unpersist cache

        if pre_dedup_count > post_dedup_count:
            log(f"Removed {pre_dedup_count - post_dedup_count} duplicate {schema_name} records based on {primary_key}")
        elif pre_dedup_count > 0: # Only log if there was data to begin with
            log(f"No duplicates found in {schema_name} dataset based on {primary_key}")

        # *** REMOVED ALL DELTA/PARQUET WRITING LOGIC ***

        log(f"Finished processing {schema_name}. Returning {post_dedup_count} processed records and {rejected_data_enhanced.count()} rejected records.")

        # Return the two dataframes
        return deduplicated_data, rejected_data_enhanced

    except Exception as e:
        log(f"Error processing {schema_name} dataset: {str(e)}", "ERROR")
        # Ensure cleanup happens if possible
        if 'raw_df_cached' in locals() and raw_df_cached.is_cached: raw_df_cached.unpersist()
        if 'typed_df_cached' in locals() and typed_df_cached.is_cached: typed_df_cached.unpersist()
        if 'valid_data_cached' in locals() and valid_data_cached.is_cached: valid_data_cached.unpersist()
        # rejected_data_cached was potentially created inside validate_data, harder to track here
        # if 'deduplicated_data_cached' in locals() and deduplicated_data_cached.is_cached: deduplicated_data_cached.unpersist() # Not caching this anymore
        raise e


