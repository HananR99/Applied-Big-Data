import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    from_json,
    sum as sum_,
    to_timestamp,
    window,
    to_json,
    struct,
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "172.28.0.3:9092")
POSTGRES_URL = os.getenv("POSTGRES_JDBC_URL", "jdbc:postgresql://172.28.0.4:5432/traffic")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

RAW_TOPIC = "traffic_raw"
ALERT_TOPIC = "critical_traffic"

schema = StructType(
    [
        StructField("sensor_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("vehicle_count", IntegerType(), False),
        StructField("avg_speed", DoubleType(), False),
    ]
)

spark = (
    SparkSession.builder.appName("traffic-stream")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", RAW_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    raw.select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("timestamp")))
    .withWatermark("event_time", "10 minutes")
)

# Write raw events to Postgres

def write_raw(batch_df, batch_id):
    (
        batch_df.select("sensor_id", "event_time", "vehicle_count", "avg_speed")
        .write.jdbc(
            url=POSTGRES_URL,
            table="traffic_raw",
            mode="append",
            properties={
                "user": POSTGRES_USER,
                "password": POSTGRES_PASSWORD,
                "driver": "org.postgresql.Driver",
            },
        )
    )

raw_query = (
    parsed.writeStream
    .foreachBatch(write_raw)
    .option("checkpointLocation", "/tmp/checkpoints/raw")
    .start()
)

# Windowed aggregation for congestion index
windowed = (
    parsed.groupBy(window(col("event_time"), "5 minutes"), col("sensor_id"))
    .agg(
        avg(col("avg_speed")).alias("avg_speed"),
        sum_(col("vehicle_count")).alias("total_vehicles"),
    )
    .withColumn("congestion_index", col("total_vehicles") / col("avg_speed"))
    .select(
        col("sensor_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_speed"),
        col("total_vehicles"),
        col("congestion_index"),
    )
)


def write_windowed(batch_df, batch_id):
    (
        batch_df.write.jdbc(
            url=POSTGRES_URL,
            table="traffic_window_agg",
            mode="append",
            properties={
                "user": POSTGRES_USER,
                "password": POSTGRES_PASSWORD,
                "driver": "org.postgresql.Driver",
            },
        )
    )

agg_query = (
    windowed.writeStream
    .foreachBatch(write_windowed)
    .option("checkpointLocation", "/tmp/checkpoints/windowed")
    .start()
)

# Real-time alerts to Kafka if avg_speed < 10
alerts = (
    parsed.filter(col("avg_speed") < 10)
    .select(
        to_json(
            struct(
                col("sensor_id"),
                col("event_time").cast("string").alias("timestamp"),
                col("avg_speed"),
                col("vehicle_count"),
            )
        ).alias("value")
    )
)

alert_query = (
    alerts.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", ALERT_TOPIC)
    .option("checkpointLocation", "/tmp/checkpoints/alerts")
    .start()
)

spark.streams.awaitAnyTermination()
