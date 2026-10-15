import os
import time

import pytest

# PySpark converts timestamps using the process time zone; pin it so assertions are deterministic.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession  # lazy: the Airflow job runs without PySpark

    session = (
        SparkSession.builder.master("local[1]")
        .appName("rta-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def kafka_records(spark):
    """Builds a DataFrame shaped like Spark's Kafka source (value + headers) from (event_type, json) pairs."""
    from pyspark.sql import types as T

    schema = T.StructType([
        T.StructField("value", T.BinaryType()),
        T.StructField("headers", T.ArrayType(T.StructType([
            T.StructField("key", T.StringType()),
            T.StructField("value", T.BinaryType()),
        ]))),
    ])

    def build(records):
        rows = []
        for event_type, value in records:
            headers = [] if event_type is None else [("event-type", bytearray(event_type.encode()))]
            rows.append((bytearray(value.encode()), headers))
        return spark.createDataFrame(rows, schema)

    return build
