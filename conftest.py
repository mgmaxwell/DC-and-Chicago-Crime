

import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session") #makes spark session available to test file 
def spark():
    spark = (
        SparkSession.builder #tests are running locally not on databricks 
        .master("local[1]")
        .appName("pytest-pyspark")
        .getOrCreate()
    )
    yield spark #prevents spark from staying open after tests are ran 
    spark.stop()

