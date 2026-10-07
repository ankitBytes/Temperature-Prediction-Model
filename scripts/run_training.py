from pyspark.sql import SparkSession
from src.data.data_loader import load_from_all_sources
from src.data.data_cleaner import validate_reading
from src.features.feature_engineer import (
    create_temperature_change,
    create_rolling_average,
    create_target
)
from src.features.feature_store import (
    create_feature_table,
    store_features,
    check_features
)
import sqlite3
import yaml
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pipelines.training_pipeline import run_training
from pyspark.sql.functions import corr

with open("/home/vvdn/Desktop/Projects/Python/Temperature-prediction-model-using-pyspark/config/database.yaml", "r") as file:
    config = yaml.safe_load(file)

db_path = config["database"]["path"]

print(db_path)

connection = sqlite3.connect(db_path)
cursor = connection.cursor()

CSV_PATH = "/home/vvdn/Desktop/Projects/Python/Temperature-prediction-model-using-pyspark/data/raw/ttemperature_regulation_smart_manufacturing.csv"
API_URL = "http://127.0.0.1:8000/sensor"


def data_split(df):

    split_window = (
        Window
        .partitionBy("sensor_id")
        .orderBy("timestamp")
    )

    # Number each row chronologically within each sensor
    df = df.withColumn(
        "row_num",
        row_number().over(split_window)
    )

    # Calculate total rows for each sensor
    sensor_counts = (
        df.groupBy("sensor_id")
        .count()
        .withColumnRenamed("count", "total_rows")
    )

    # Attach each sensor's total row count
    df = df.join(
        sensor_counts,
        on="sensor_id",
        how="left"
    )

    # Calculate 70/15/15 boundaries for each sensor
    df = df.withColumn(
        "train_end",
        (col("total_rows") * 0.70).cast("int")
    )

    df = df.withColumn(
        "validation_end",
        (
            col("total_rows") * 0.85
        ).cast("int")
    )

    # Chronological split per sensor
    train_df = df.filter(
        col("row_num") <= col("train_end")
    )

    validation_df = df.filter(
        (col("row_num") > col("train_end")) &
        (col("row_num") <= col("validation_end"))
    )

    test_df = df.filter(
        col("row_num") > col("validation_end")
    )

    # First row of each sensor has no temperature_change
    train_df = train_df.filter(
        col("temperature_change").isNotNull()
    )

    feature_columns = [
        "temperature",
        "humidity",
        "temperature_change",
        "rolling_avg_temperature"
    ]

    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="features"
    )

    train_df = assembler.transform(train_df)
    validation_df = assembler.transform(validation_df)
    test_df = assembler.transform(test_df)

    return train_df, validation_df, test_df

def main():
    spark = (
        SparkSession.builder
        .appName("Temperature prediction pipeline")
        .master("local[*]")
        .getOrCreate()
    )

    processed_data_path = "data/processed/invalid_data"

    # Import the data from .csv file to the spark dataframe
    df = load_from_all_sources(spark, CSV_PATH, API_URL, db_path)

    # Clean the data by removing the impurities
    valid_df, invalid_df = validate_reading(df)

    # Store invalid data to csv file and valid data to sqlite
    invalid_df.write.mode("overwrite").option("header", True).csv(processed_data_path)


    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sensor_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id TEXT,
            timestamp TEXT,
            temperature REAL,
            humidity REAL,
            UNIQUE(sensor_id, timestamp)
        )
    """)

    valid_rows = valid_df.collect()

    for row in valid_rows:
        cursor.execute("""
            INSERT OR IGNORE INTO sensor_readings
            (sensor_id, timestamp, temperature, humidity)
            VALUES (?, ?, ?, ?)
        """, (
            row["sensor_id"],
            row["timestamp"].isoformat() if row["timestamp"] else None,
            row["temperature"],
            row["humidity"]
        ))


    feature_df = create_temperature_change(valid_df)
    feature_df = create_rolling_average(feature_df)
    feature_df = create_target(feature_df)

    training_df = feature_df.filter(
        col("target_temperature").isNotNull()
    )

    train_df, validation_df, test_df = data_split(training_df)

    create_feature_table(connection)
    store_features(connection, feature_df)
    check_features(connection)

    print("Train rows:", train_df.count())
    print("Validation rows:", validation_df.count())
    print("Test rows:", test_df.count())

    model, validation_predictions = run_training(
        train_df,
        validation_df
    )

    connection.commit()
    connection.close()

    # print("Valid data stored in SQLite.")

    spark.stop()

if __name__ == "__main__":
    main()
