import os
import sys
 
import pytest
from pyspark.sql import SparkSession

_TRANSFORMATIONS_DIR = os.path.join(os.path.dirname(__file__), "..", "transformations")
sys.path.insert(0, os.path.abspath(_TRANSFORMATIONS_DIR))

@pytest.fixture(scope="session")
def local_spark():
   
    active = SparkSession.getActiveSession()
    if active is not None:
        yield active
        
        return
    
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("frontrunner-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")  
        .getOrCreate()
    )
    yield spark
    spark.stop()
