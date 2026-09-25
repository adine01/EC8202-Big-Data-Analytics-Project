import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import BinaryType, IntegerType, LongType, StringType, StructField, StructType

KAFKA_SCHEMA = StructType([
    StructField("key", BinaryType()),
    StructField("value", BinaryType()),
    StructField("topic", StringType()),
    StructField("partition", IntegerType()),
    StructField("offset", LongType()),
])


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("ward-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def kafka_rows(spark):
    """Build a DataFrame shaped like the Kafka source from (key, payload-bytes) pairs."""
    def build(payloads, topic="vitals.raw"):
        rows = [(b"k", p, topic, 0, i) for i, p in enumerate(payloads)]
        return spark.createDataFrame(rows, KAFKA_SCHEMA)
    return build
