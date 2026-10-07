import sqlite3

import requests

from pyspark.sql.functions import col, expr, lit


# ---------------------------------------------------------
# 1. CSV SOURCE
# ---------------------------------------------------------

def load_from_csv(spark, input_path):
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )

    df = (
        df
        .withColumn(
            "sensor_id",
            lit("MANUFACTURING_01")
        )
        .withColumn(
            "timestamp",
            expr("try_cast(`Timestamp` AS TIMESTAMP)")
        )
        .withColumn(
            "temperature",
            col("Current Temperature (°C)").cast("double")
        )
        .withColumn(
            "humidity",
            col("Humidity (%)").cast("double")
        )
    )

    return df.select(
        "sensor_id",
        "timestamp",
        "temperature",
        "humidity"
    )


# ---------------------------------------------------------
# 2. REST API SOURCE
# ---------------------------------------------------------

def load_from_api(spark, api_url):
    response = requests.get(
        api_url,
        timeout=10
    )

    response.raise_for_status()

    data = response.json()

    if isinstance(data, dict):
        data = [data]

    rows = [
        (
            reading.get("sensor_id"),
            reading.get("timestamp"),
            reading.get("temperature"),
            reading.get("humidity"),
        )
        for reading in data
    ]

    df = spark.createDataFrame(
        rows,
        schema=[
            "sensor_id",
            "timestamp",
            "temperature",
            "humidity",
        ],
    )

    df = (
        df
        .withColumn(
            "timestamp",
            expr("try_cast(timestamp AS TIMESTAMP)")
        )
        .withColumn(
            "temperature",
            col("temperature").cast("double")
        )
        .withColumn(
            "humidity",
            col("humidity").cast("double")
        )
    )

    return df.select(
        "sensor_id",
        "timestamp",
        "temperature",
        "humidity"
    )


# ---------------------------------------------------------
# 3. SQLITE SOURCE
# ---------------------------------------------------------

def load_from_database(spark, database_path):
    connection = sqlite3.connect(database_path)

    cursor = connection.cursor()

    cursor.execute("""
        SELECT
            sensor_id,
            timestamp,
            temperature,
            humidity
        FROM raw_sensor_readings
    """)

    rows = cursor.fetchall()

    cursor.close()
    connection.close()

    return spark.createDataFrame(
        rows,
        schema=[
            "sensor_id",
            "timestamp",
            "temperature",
            "humidity"
        ]
    )


# ---------------------------------------------------------
# 4. MERGE ALL SOURCES
# ---------------------------------------------------------

def load_from_all_sources(
    spark,
    csv_path,
    api_url,
    database_path
):
    csv_df = load_from_csv(
        spark,
        csv_path
    )

    api_df = load_from_api(
        spark,
        api_url
    )

    database_df = load_from_database(
        spark,
        database_path
    )

    combined_df = (
        csv_df
        .unionByName(api_df)
        .unionByName(database_df)
    )

    return combined_df