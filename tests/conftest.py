# tests/conftest.py
import pytest
from pyspark.sql import SparkSession
import os
import shutil

@pytest.fixture(scope="session")
def spark_session(tmp_path_factory):
    """Creates a SparkSession for testing."""
    warehouse_dir = tmp_path_factory.mktemp("spark_warehouse")
    local_ivy_dir = tmp_path_factory.mktemp("ivy")

    # Clean up existing Ivy directory if it exists to avoid conflicts
    default_ivy_path = os.path.expanduser("~/.ivy2")
    if os.path.exists(default_ivy_path):
         print(f"Temporarily removing existing Ivy cache at {default_ivy_path}")
         # Consider backing up instead of removing if needed
         # shutil.rmtree(default_ivy_path) # Use with caution


    spark = (
        SparkSession.builder.appName("pytest-local-spark-delta")
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") 
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # --- Performance/Testing Configuration ---
        .config("spark.sql.shuffle.partitions", "4") # Keep low for local testing
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.log.level", "WARN") # Keep logs quieter during tests
        .getOrCreate()
    )
    print(f"SparkSession created with warehouse: {warehouse_dir}, ivy: {local_ivy_dir}")
    yield spark
    spark.stop()
    print("SparkSession stopped.")
    # Optional: Clean up temporary Ivy directory if desired, though tmp_path_factory should handle it
    # shutil.rmtree(local_ivy_dir)

# Fixture to provide paths for test data using pytest's tmp_path
@pytest.fixture
def data_paths(tmp_path):
    """Provides temporary paths for raw, processed, and rejected data."""
    paths = {
        "raw": tmp_path / "raw",
        "processed": tmp_path / "processed",
        "rejected": tmp_path / "rejected",
        "order_items_out": tmp_path / "processed" / "order_items",
        "orders_out": tmp_path / "processed" / "orders",
        "products_out": tmp_path / "processed" / "products",
        "rejected_out": tmp_path / "rejected", # Base path for rejected data
    }
    # Create directories
    paths["raw"].mkdir()
    paths["processed"].mkdir()
    paths["rejected"].mkdir()
    return paths

# Helper to create sample CSV files
@pytest.fixture
def create_csv_file(data_paths):
    def _create_csv(filename, data, headers):
        filepath = data_paths["raw"] / filename
        with open(filepath, "w", newline="") as f:
            f.write(",".join(headers) + "\n")
            for row in data:
                f.write(",".join(map(str, row)) + "\n")
        print(f"Created test CSV: {filepath}")
        return str(filepath) # Return path as string
    return _create_csv