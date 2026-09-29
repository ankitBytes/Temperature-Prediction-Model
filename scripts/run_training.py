from pyspark.sql import SparkSession
from src.data.data_loader import load_sensor_data
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


def data_split(df):

    split_window = Window.partitionBy("sensor_id").orderBy("timestamp")

    training_df = df.withColumn(
        "row_num",
        row_number().over(split_window)
    )

    total_rows = training_df.count()

    train_end = int(total_rows * 0.70)
    validation_end = train_end + int(total_rows * 0.15)

    train_df = training_df.filter(
        col("row_num") <= train_end
    )

    validation_df = training_df.filter(
        (col("row_num") > train_end) &
        (col("row_num") <= validation_end)
    )

    test_df = training_df.filter(
        col("row_num") > validation_end
    )

    train_df = train_df.filter(
        col("temperature_change").isNotNull()
    )

    feature_columns = [
            "temperature",
            "humidity",
            "temperature_change",
            "rolling_avg_temperature"
    ]

    target_column = "target_temperature"

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
    df = load_sensor_data(spark)

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
