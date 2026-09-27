import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    # Plain local Spark: the pure functions under test don't need Iceberg or AWS.
    session = SparkSession.builder.master("local[1]").config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false").getOrCreate()
    yield session
    session.stop()
