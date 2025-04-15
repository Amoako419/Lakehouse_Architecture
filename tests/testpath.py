import pytest
from datetime import date, datetime
import sys  # Import sys
import os   # Import os
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../src'))
sys.path.insert(0, src_path)
print(f"--- DEBUG: Added {src_path} to sys.path ---")
# --- End block ---

# Now the import should work if src/glue_etl.py exists
from glue_etl import process_dataset, validate_data

# ... rest of your test file ...
from glue_etl import process_dataset, validate_data

# Test data setup
ts1 = datetime(2023, 1, 10, 10, 0, 0)
ts2 = datetime(2023, 1, 11, 12, 30, 0)
d1 = date(2023, 1, 10)
d2 = date(2023, 1, 11)

@pytest.fixture
def test_data():
    products_data = [
        (101, 10, "Electronics", "Laptop"),
        (102, 10, "Electronics", "Mouse"), 
        (103, 20, "Clothing", "T-Shirt"),
        (None, 20, "Clothing", "Socks"),
        (105, 30, "Home", None)
    ]
    return products_data

def test_validate_data(test_data):
    """Test data validation"""
    # Test valid record
    assert validate_data(test_data[0]) == True
    
    # Test invalid records
    assert validate_data(test_data[3]) == False # Null product_id
    assert validate_data(test_data[4]) == False # Null product_name

def test_process_dataset(spark_session, test_data):
    """Test dataset processing"""
    # Create test dataframe
    columns = ["product_id", "department_id", "department", "product_name"]
    test_df = spark_session.createDataFrame(test_data, columns)
    
    # Process dataset
    output_path = "test_output"
    rejected_path = "test_rejected"
    
    result_df = process_dataset(
        test_df,
        schema="test_schema",
        dataset_name="products",
        output_path=output_path,
        spark=spark_session,
        rejected_path=rejected_path
    )
    
    # Verify results
    assert result_df.count() == 3 # Valid records only
    assert "department" in result_df.columns
    
    # Check rejected data
    rejected_df = spark_session.read.format("delta").load(rejected_path + "/products")
    assert rejected_df.count() == 2 # Invalid records