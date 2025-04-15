# src/etl_utils.py
"""
Utility functions and schema definitions for the E-commerce Lakehouse ETL process.
This module contains reusable logic independent of the AWS Glue runtime environment.
"""

import logging
import time
from datetime import datetime

# Import necessary PySpark modules
from pyspark.sql.functions import col, lit, current_timestamp, when, count, to_timestamp, concat_ws, coalesce
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, DateType

# Import DeltaTable class if process_dataset handles Delta writing
try:
    from delta.tables import DeltaTable
except ImportError:
    # Allow importing even if delta-spark isn't installed everywhere,
    # though process_dataset might fail later if it needs it.
    # Alternatively, make process_dataset more generic (e.g., return DF to be written by caller)
    print("Warning: delta-spark library not found. DeltaTable functionality might be unavailable.")
    DeltaTable = None # Define as None if not found

# ==============================================================================
# Global Scope: Definitions available for import
# ==============================================================================

# Configure logging (Get logger by name for better isolation)
logger = logging.getLogger(__name__) # Use module name for logger
# Note: Logging level might need to be configured by the calling script/environment

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
        count_val = df.count()
        columns_val = len(df.columns)
        log(f"DataFrame {name}: {count_val} rows, {columns_val} columns")
        # Optionally add schema logging back if needed, controlled by logger level
        # if logger.isEnabledFor(logging.DEBUG):
        #    log(f"DataFrame {name} schema:", "DEBUG")
        #    df.printSchema()
    except Exception as e:
        log(f"Could not get info for DataFrame {name}: {e}", "WARNING")


@timed_execution
def validate_data(df, schema_name, reference_data=None):
    """
    Applies validation rules based on the schema name and separates valid from invalid records.
    Uses left-anti joins for more robust referential integrity checks.
    Relies on globally defined schemas within this module.
    """
    log(f"Validating {schema_name} dataset")
    df.cache()
    df.count()

    validated_df = df.withColumn("validation_errors_list", lit(None).cast("array<string>"))

    # Determine the correct schema based on schema_name
    current_schema = None
    if schema_name == "order_items":
        current_schema = order_items_schema
    elif schema_name == "orders":
        current_schema = orders_schema
    elif schema_name == "products":
        current_schema = products_schema
    else:
        raise ValueError(f"Unknown schema_name: {schema_name}")

    # --- Apply common validation rules based on schema ---
    # Check non-nullable fields defined in the schema
    log(f"Checking non-nullable fields for {schema_name}")
    for field in current_schema.fields:
        if not field.nullable:
             validated_df = validated_df.withColumn(
                 "validation_errors_list",
                 when(col(field.name).isNull(),
                      coalesce(col("validation_errors_list"), lit([]).cast("array<string>")) + [lit(f"Null {field.name}")])
                 .otherwise(col("validation_errors_list"))
             )

    # --- Specific Rules ---
    if schema_name == "order_items":
        # Referential integrity using left-anti joins
        if reference_data and "orders" in reference_data:
            log("Checking order_id referential integrity using anti-join")
            invalid_order_ids_df = validated_df.alias("oi") \
                .join(reference_data["orders"].alias("o"), col("oi.order_id") == col("o.order_id"), "left_anti") \
                .select(col("oi.id").alias("invalid_id")).distinct() # Ensure distinct invalid IDs

            validated_df = validated_df.alias("v") \
                .join(invalid_order_ids_df.alias("inv"), col("v.id") == col("inv.invalid_id"), "left_outer") \
                .withColumn("validation_errors_list",
                    when(col("inv.invalid_id").isNotNull(),
                         coalesce(col("v.validation_errors_list"), lit([]).cast("array<string>")) + [lit("Invalid order_id reference")])
                    .otherwise(col("v.validation_errors_list"))
                ).select("v.*")

        if reference_data and "products" in reference_data:
            log("Checking product_id referential integrity using anti-join")
            invalid_product_ids_df = validated_df.alias("oi") \
                .join(reference_data["products"].alias("p"), col("oi.product_id") == col("p.product_id"), "left_anti") \
                .select(col("oi.id").alias("invalid_id")).distinct() # Ensure distinct invalid IDs

            validated_df = validated_df.alias("v") \
                .join(invalid_product_ids_df.alias("inv"), col("v.id") == col("inv.invalid_id"), "left_outer") \
                .withColumn("validation_errors_list",
                    when(col("inv.invalid_id").isNotNull(),
                         coalesce(col("v.validation_errors_list"), lit([]).cast("array<string>")) + [lit("Invalid product_id reference")])
                    .otherwise(col("v.validation_errors_list"))
                ).select("v.*")


    elif schema_name == "orders":
        # Total amount check
        validated_df = validated_df.withColumn(
            "validation_errors_list",
            when((col("total_amount").isNotNull()) & (col("total_amount") <= 0),
                 coalesce(col("validation_errors_list"), lit([]).cast("array<string>")) + [lit("Non-positive total amount")])
            .otherwise(col("validation_errors_list"))
        )

    elif schema_name == "products":
        # No extra rules specified beyond non-nullable checks
        pass

    # Convert error list to semicolon-separated string
    validated_df = validated_df.withColumn(
         "validation_errors",
         when(col("validation_errors_list").isNotNull(), concat_ws("; ", col("validation_errors_list")))
         .otherwise(lit(None).cast("string"))
    )

    validated_df.cache()
    validated_df.count()

    log("Splitting into valid and invalid records")
    valid_records = validated_df.filter(col("validation_errors").isNull()).drop("validation_errors_list", "validation_errors")
    invalid_records = validated_df.filter(col("validation_errors").isNotNull()).drop("validation_errors_list")

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

    return valid_records, invalid_records


@timed_execution
def process_dataset(raw_df, schema, schema_name, output_path, rejected_path, job_name, spark, reference_data=None):
    """
    Processes a raw DataFrame: type casting, validation, deduplication, and writes to Delta Lake.
    Writes rejected records to a separate Delta path. Uses functions/schemas from this module.
    Requires SparkSession, paths, and job_name as arguments.
    Requires delta-spark library to be installed if writing Delta tables.
    """
    if DeltaTable is None and output_path.startswith("s3"): # Basic check
         log("DeltaTable class not found (delta-spark library likely missing). Cannot write Delta output.", "ERROR")
         raise ImportError("DeltaTable class not found. Install delta-spark library.")

    try:
        log(f"Processing {schema_name} dataset")
        log_dataframe_info(raw_df, f"{schema_name}_raw") # Uses log_dataframe_info from this module

        raw_df_cached = raw_df.cache()
        raw_df_cached.count()

        log(f"Converting {schema_name} data types to match schema")
        typed_df = raw_df_cached
        for field in schema.fields:
            if field.name in typed_df.columns:
                 if field.name == "order_timestamp":
                     # Attempt conversion, handle potential errors if format is wrong
                     try:
                         typed_df = typed_df.withColumn(field.name, to_timestamp(col(field.name)))
                     except Exception as cast_err:
                         log(f"Warning: Could not cast column '{field.name}' to TimestampType for {schema_name}. Error: {cast_err}", "WARNING")
                         # Keep original or set to null? Decide based on requirements. Setting to null for now.
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

        # ---> Call validate_data from this module <---
        valid_data, rejected_data = validate_data(typed_df_cached, schema_name, reference_data)

        raw_df_cached.unpersist()

        rejected_data_cached = rejected_data.cache()
        rejected_count = rejected_data_cached.count()

        if rejected_count > 0:
            log(f"Found {rejected_count} rejected {schema_name} records")
            rejected_data_to_write = rejected_data_cached.withColumn("rejection_time", current_timestamp())
            rejected_data_to_write = rejected_data_to_write.withColumn("source", lit(schema_name))
            rejected_data_to_write = rejected_data_to_write.withColumn("job_name", lit(job_name))

            rejected_path_full = f"{rejected_path}/{schema_name}"
            log(f"Writing rejected {schema_name} records to {rejected_path_full}")

            # Write rejected data (assuming Delta format here, could be Parquet/etc.)
            try:
                if "date" in rejected_data_to_write.columns:
                    rejected_data_to_write.write.format("delta").mode("append").partitionBy("date").save(rejected_path_full)
                else:
                    rejected_data_to_write.write.format("delta").mode("append").save(rejected_path_full)
            except Exception as write_err:
                 log(f"Error writing rejected data to {rejected_path_full}: {write_err}", "ERROR")
                 # Decide if this should halt processing or just log

            log(f"Rejected {rejected_count} {schema_name} records due to validation errors")
            try:
                # Show errors only if logger level allows (can be expensive)
                if logger.isEnabledFor(logging.INFO):
                    rejection_counts = rejected_data_to_write.groupBy("validation_errors").count().orderBy(col("count").desc())
                    rejection_counts.show(truncate=False)
                    error_rows = rejection_counts.collect()
                    for row in error_rows[:5]:
                        log(f"Error type: {row['validation_errors']} - Count: {row['count']}")
            except Exception as show_err:
                 log(f"Could not show rejection counts: {show_err}", "WARNING")
        else:
            log(f"No rejected records found in {schema_name} dataset")

        rejected_data_cached.unpersist()

        valid_data_cached = valid_data.cache()
        pre_dedup_count = valid_data_cached.count()

        primary_key = "id" if schema_name == "order_items" else "order_id" if schema_name == "orders" else "product_id"
        log(f"Deduplicating {schema_name} data on {primary_key}")

        # Ensure primary key exists before dropping duplicates
        if primary_key not in valid_data_cached.columns:
             log(f"Primary key '{primary_key}' not found for deduplication in {schema_name}. Skipping deduplication.", "ERROR")
             deduplicated_data = valid_data_cached # Pass through without deduplication
        else:
             deduplicated_data = valid_data_cached.dropDuplicates([primary_key])

        deduplicated_data_cached = deduplicated_data.cache()
        post_dedup_count = deduplicated_data_cached.count()

        valid_data_cached.unpersist()
        typed_df_cached.unpersist()

        if pre_dedup_count > post_dedup_count:
            log(f"Removed {pre_dedup_count - post_dedup_count} duplicate {schema_name} records based on {primary_key}")
        else:
            log(f"No duplicates found in {schema_name} dataset based on {primary_key}")

        partition_col = "date" if schema_name in ["order_items", "orders"] else "department"

        if partition_col in deduplicated_data_cached.columns:
            log(f"Partition column for {schema_name} is {partition_col}")
            partitioned_data = deduplicated_data_cached
        else:
            partitioned_data = deduplicated_data_cached
            log(f"Warning: Partition column '{partition_col}' not found in {schema_name} dataset", "WARNING")
            partition_col = None # Set partition_col to None if not found

        # --- Delta Write Logic ---
        # Check if DeltaTable class was imported successfully before trying to use it
        if DeltaTable:
            try:
                log(f"Checking if Delta table exists at {output_path}")
                delta_table = DeltaTable.forPath(spark, output_path)
                merge_condition = f"existing.{primary_key} = updates.{primary_key}"
                log(f"Performing Delta merge operation for {schema_name} into {output_path}")
                merge_start = time.time()
                (delta_table.alias("existing").merge(
                    partitioned_data.alias("updates"), merge_condition
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
                merge_end = time.time()
                log(f"Successfully merged {post_dedup_count} {schema_name} records into the Delta table in {merge_end - merge_start:.2f} seconds")

            except Exception as e:
                # Improved check for "table not found" type errors
                if ("Path does not exist" in str(e)
                    or "is not a Delta table" in str(e)
                    or "java.io.FileNotFoundException" in str(e)): # Add specific Java exception if needed
                     log(f"Delta table for {schema_name} does not exist, creating at {output_path}. Error detail: {str(e)}")
                     write_start = time.time()
                     writer = partitioned_data.write.format("delta").mode("overwrite")
                     if partition_col:
                         log(f"Writing initial {schema_name} data with partitioning by {partition_col}")
                         writer = writer.partitionBy(partition_col)
                     else:
                         log(f"Writing initial {schema_name} data without partitioning")
                     writer.save(output_path)
                     write_end = time.time()
                     log(f"Successfully wrote {post_dedup_count} {schema_name} records to new Delta table in {write_end - write_start:.2f} seconds")
                else:
                     log(f"Error during Delta merge/write operation for {schema_name}: {str(e)}", "ERROR")
                     raise e
        else:
             # Fallback if DeltaTable is not available (e.g., write as Parquet)
             log("DeltaTable not available. Writing data as Parquet instead.", "WARNING")
             write_start = time.time()
             writer = partitioned_data.write.format("parquet").mode("overwrite")
             if partition_col:
                 writer = writer.partitionBy(partition_col)
             writer.save(output_path)
             write_end = time.time()
             log(f"Successfully wrote {post_dedup_count} {schema_name} records as Parquet to {output_path} in {write_end - write_start:.2f} seconds")
        # --- End Delta Write Logic ---


        deduplicated_data_cached.unpersist()
        # Return the DataFrame that was processed (before writing)
        return deduplicated_data

    except Exception as e:
        log(f"Error processing {schema_name} dataset: {str(e)}", "ERROR")
        # Ensure cleanup happens if possible
        if 'raw_df_cached' in locals() and raw_df_cached.is_cached: raw_df_cached.unpersist()
        if 'typed_df_cached' in locals() and typed_df_cached.is_cached: typed_df_cached.unpersist()
        if 'valid_data_cached' in locals() and valid_data_cached.is_cached: valid_data_cached.unpersist()
        if 'rejected_data_cached' in locals() and rejected_data_cached.is_cached: rejected_data_cached.unpersist()
        if 'deduplicated_data_cached' in locals() and deduplicated_data_cached.is_cached: deduplicated_data_cached.unpersist()
        raise e


