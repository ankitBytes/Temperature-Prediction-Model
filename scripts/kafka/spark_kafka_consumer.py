from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
)
from src.data.data_cleaner import validate_reading

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "raw-sensor-readings"

sensor_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("is_faulty", BooleanType(), True),
    StructField("fault_type", StringType(), True),
    StructField("source", StringType(), True),
])

spark = (
    SparkSession.builder
    .appName("TemperatureSensorKafkaConsumer")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

decoded_df = kafka_df.select(
    col("value").cast("string").alias("value"),
    "topic",
    "partition",
    "offset",
    "timestamp",
)


parsed_df = decoded_df.select(
    from_json(col("value"), sensor_schema).alias("data"),
    "topic",
    "partition",
    "offset",
)

parsed_df = parsed_df.select(
    "data.*",
    "topic",
    "partition",
    "offset",
)

parsed_df = parsed_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)

valid_df, invalid_df = validate_reading(parsed_df)

valid_df = (
    valid_df
    .withWatermark("timestamp", "10 minutes")
    .dropDuplicates([
        "sensor_id",
        "timestamp",
        "source"
    ])
)

invalid_query = (
    invalid_df
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

valid_query = (
    valid_df
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

valid_query.awaitTermination()
