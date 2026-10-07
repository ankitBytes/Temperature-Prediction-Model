from pyspark.sql import SparkSession
import mlflow

from src.data.data_loader import load_from_all_sources
from src.data.data_cleaner import validate_reading

from pipelines.monitoring_pipeline import (
    calculate_statistics,
    calculate_drift,
    split_reference_current_data,
    evaluate_champion_model,
)


REFERENCE_PATH = (
    "data/raw/ttemperature_regulation_smart_manufacturing.csv"
)

API_URL = "http://127.0.0.1:8000/sensor"

DATABASE_PATH = "database/sensor_data.db"

mlflow.set_tracking_uri("http://127.0.0.1:5000")


def main():

    spark = (
        SparkSession.builder
        .appName("Temperature prediction monitoring")
        .master("local[*]")
        .getOrCreate()
    )

    # --------------------------------------------------
    # 1. Load data from all sources
    # --------------------------------------------------

    combined_df = load_from_all_sources(
        spark,
        REFERENCE_PATH,
        API_URL,
        DATABASE_PATH,
    )

    print("\nCombined data:")
    print(f"Total rows: {combined_df.count()}")

    combined_df.select(
        "sensor_id",
        "timestamp",
        "temperature",
        "humidity",
    ).show(10, truncate=False)

    # --------------------------------------------------
    # 2. Validate data
    # --------------------------------------------------

    combined_df, invalid_df = validate_reading(
        combined_df
    )

    print("\nInvalid rows:")
    print(invalid_df.count())

    # --------------------------------------------------
    # 3. Split using timestamp
    # --------------------------------------------------

    reference_df, current_df = split_reference_current_data(
        combined_df
    )

    print("\nReference data:")
    print(f"Rows: {reference_df.count()}")

    reference_df.select(
        "timestamp",
        "temperature",
        "humidity",
    ).show(5, truncate=False)

    print("\nCurrent data:")
    print(f"Rows: {current_df.count()}")

    current_df.select(
        "timestamp",
        "temperature",
        "humidity",
    ).show(5, truncate=False)

    # --------------------------------------------------
    # 4. Calculate statistics
    # --------------------------------------------------

    reference_stats = calculate_statistics(
        reference_df
    )

    current_stats = calculate_statistics(
        current_df
    )

    # --------------------------------------------------
    # 5. Start MLflow monitoring run
    # --------------------------------------------------

    with mlflow.start_run(
        run_name="model-monitoring"
    ):

        # --------------------------------------------------
        # 6. Calculate drift
        # --------------------------------------------------

        drift = calculate_drift(
            reference_stats,
            current_stats,
        )

        print("\nReference statistics:")
        print(reference_stats)

        print("\nCurrent statistics:")
        print(current_stats)

        print("\nDrift:")

        for column, values in drift.items():

            print(f"{column}:")

            print(
                f"  Mean change: "
                f"{values['mean_change_percent']:.2f}%"
            )

            print(
                f"  Mean status: "
                f"{values['mean_status']}"
            )

            print(
                f"  Stddev change: "
                f"{values['stddev_change_percent']:.2f}%"
            )

            print(
                f"  Stddev status: "
                f"{values['stddev_status']}"
            )

        # --------------------------------------------------
        # 7. Model performance
        # --------------------------------------------------

        print("\nModel performance:")

        mae, rmse = evaluate_champion_model(
            reference_df
        )

        print(f"  MAE: {mae:.2f} °C")
        print(f"  RMSE: {rmse:.2f} °C")

        # --------------------------------------------------
        # 8. Log metrics to MLflow
        # --------------------------------------------------

        mlflow.log_metric(
            "monitoring_mae",
            mae,
        )

        mlflow.log_metric(
            "monitoring_rmse",
            rmse,
        )

        mlflow.log_metric(
            "temperature_mean_change_percent",
            drift["temperature"]["mean_change_percent"],
        )

        mlflow.log_metric(
            "temperature_stddev_change_percent",
            drift["temperature"]["stddev_change_percent"],
        )

        mlflow.log_metric(
            "humidity_mean_change_percent",
            drift["humidity"]["mean_change_percent"],
        )

        mlflow.log_metric(
            "humidity_stddev_change_percent",
            drift["humidity"]["stddev_change_percent"],
        )

    spark.stop()


if __name__ == "__main__":
    main()