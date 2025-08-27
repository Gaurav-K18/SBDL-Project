import sys
import uuid

from pyspark.sql.functions import struct, col, to_json

from lib import ConfigLoader, Utils, DataLoader, Transformations
from lib.logger import Log4j
#from test_pytest_sbdl import spark

if __name__ == '__main__':
    spark = Utils.get_spark_session("LOCAL")

    # Example DataFrame with key/value (must be string or binary)
    df = spark.createDataFrame(
        [(1, "Hello Kafka"), (2, "This is local test")],
        ["id", "message"]
    )

    # Convert into Kafka-compatible format
    kafka_kv_df = df.selectExpr(
        "CAST(id AS STRING) AS key",
        "CAST(message AS STRING) AS value"
    )

    # Write to local Kafka (PLAINTEXT, no auth)
    kafka_kv_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "firsttopic") \
        .save()
