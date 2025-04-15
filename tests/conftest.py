# tests/conftest.py
import pytest
from pyspark.sql import SparkSession
import os
import shutil # Keep shutil in case you want to add other cleanup later

@pytest.fixture(scope="session")
def spark_session(tmp_path_factory):
    """
    Creates a standard SparkSession configured for local testing,
    without Delta Lake specific configurations.
    """
    # Create a temporary directory for Spark's warehouse (stores table metadata)
    # Using tmp_path_factory ensures it's cleaned up after the test session
    warehouse_dir = tmp_path_factory.mktemp("spark_warehouse")
    print(f"Creating SparkSession for unit tests (Warehouse: {warehouse_dir})")

    spark = (
        SparkSession.builder
        .appName("pytest-local-spark-unit-tests") # Changed app name slightly
        .master("local[*]") # Run Spark locally using all available cores
        # --- Removed Delta Lake Configurations ---
        # .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") # REMOVED
        # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") # REMOVED
        # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") # REMOVED
        # --- Standard Configurations for Testing ---
        .config("spark.sql.shuffle.partitions", "2") # Keep low for local testing
        .config("spark.sql.warehouse.dir", str(warehouse_dir)) # Use temporary warehouse
        .config("spark.driver.memory", "1g") # Limit memory for local runs
        .config("spark.executor.memory", "1g") # Limit memory for local runs
        .config("spark.ui.showConsoleProgress", "false") # Disable verbose progress bars
        .config("spark.log.level", "WARN") # Keep Spark logs quieter during tests
        # --- Removed Ivy Cache Handling ---
        # No longer needed as spark.jars.packages was removed
        .getOrCreate() # Get or create the SparkSession
    )

    print(f"SparkSession created.")
    yield spark # Provide the SparkSession object to the tests

    # Teardown: Stop the SparkSession after tests are done
    spark.stop()
    print("SparkSession stopped.")
    # Warehouse directory is automatically cleaned up by pytest tmp_path_factory

# --- Utility Fixtures (Can remain as they might be useful) ---

@pytest.fixture
def data_paths(tmp_path):
    """
    Provides temporary paths using pytest's tmp_path fixture.
    Useful even if not writing Delta tables (e.g., for CSVs, Parquet).
    """
    paths = {
        "raw": tmp_path / "raw",
        "processed": tmp_path / "processed", # Generic processed path
        "rejected": tmp_path / "rejected",   # Generic rejected path
        # Specific output paths might not be needed for unit tests
        # but can be kept or removed as desired.
        "generic_output": tmp_path / "output"
    }
    # Create directories if tests expect them
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return paths

@pytest.fixture
def create_csv_file(data_paths):
    """Helper fixture to create sample CSV files in a temporary 'raw' directory."""
    def _create_csv(filename, data, headers):
        # Ensure data is a list of tuples/lists for consistent processing
        if not isinstance(data, list):
             raise TypeError("Input 'data' must be a list of sequences (tuples/lists).")
        if data and not isinstance(data[0], (list, tuple)):
             raise TypeError("Elements inside 'data' must be sequences (tuples/lists).")

        filepath = data_paths["raw"] / filename
        try:
            with open(filepath, "w", newline="") as f:
                # Write header
                f.write(",".join(map(str, headers)) + "\n")
                # Write data rows
                for row in data:
                    f.write(",".join(map(str, row)) + "\n")
            print(f"Created test CSV: {filepath}")
            return str(filepath) # Return path as string
        except Exception as e:
            print(f"Error creating CSV file {filepath}: {e}")
            raise # Re-raise the exception
    return _create_csv

