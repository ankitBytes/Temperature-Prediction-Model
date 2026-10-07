from pyspark.sql.functions import avg, stddev, col, min, max, row_number
from pyspark.sql.window import Window
import yaml

from src.features.feature_engineer import (
    create_temperature_change,
    create_rolling_average,
    create_target,
)
from src.model.model_registry import load_champion_model
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator


with open("config/training.yaml", "r") as file:
    config = yaml.safe_load(file)


DRIFT_THRESHOLD_PERCENT = config["monitoring"]["drift_threshold_percent"]

MONITORING_COLUMNS = [
    "temperature",
    "humidity",
]


def calculate_statistics(df):
    """
    Calculate basic statistics for monitoring columns.
    """

    aggregations = []

    for column in MONITORING_COLUMNS:
        aggregations.extend([
            avg(column).alias(f"{column}_mean"),
            stddev(column).alias(f"{column}_stddev"),
        ])

    return df.agg(*aggregations).collect()[0].asDict()


def calculate_drift(reference_stats, current_stats):
    """
    Compare statistics between reference and current datasets.
    """

    drift = {}

    for column in MONITORING_COLUMNS:

        reference_mean = reference_stats[f"{column}_mean"]
        current_mean = current_stats[f"{column}_mean"]

        reference_stddev = reference_stats[f"{column}_stddev"]
        current_stddev = current_stats[f"{column}_stddev"]

        mean_change = (
            (current_mean - reference_mean)
            / reference_mean
            * 100
        )

        stddev_change = (
            (current_stddev - reference_stddev)
            / reference_stddev
            * 100
        )

        drift[column] = {
            "mean_change_percent": mean_change,
            "stddev_change_percent": stddev_change,
            "mean_status": (
                "DRIFT"
                if abs(mean_change) > DRIFT_THRESHOLD_PERCENT
                else "OK"
            ),
            "stddev_status": (
                "DRIFT"
                if abs(stddev_change) > DRIFT_THRESHOLD_PERCENT
                else "OK"
            ),
        }

    return drift


def split_reference_current_data(df):
    """
    Split combined data into reference and current windows
    based on timestamp.

    The latest 20% of the data is treated as current data.
    The remaining 80% is treated as reference data.
    """

    window = Window.orderBy("timestamp")

    df = df.withColumn(
        "row_number",
        row_number().over(window)
    )

    total_rows = df.count()

    current_start = int(total_rows * 0.80)

    reference_df = df.filter(
        col("row_number") <= current_start
    )

    current_df = df.filter(
        col("row_number") > current_start
    )

    reference_df = reference_df.drop("row_number")
    current_df = current_df.drop("row_number")

    return reference_df, current_df


def evaluate_champion_model(df):
    """
    Generate predictions using the champion model
    and evaluate them against future actual temperature.
    """

    df = create_temperature_change(df)
    df = create_rolling_average(df)
    df = create_target(df)

    df = df.filter(
        col("temperature_change").isNotNull()
        & col("target_temperature").isNotNull()
    )

    assembler = VectorAssembler(
        inputCols=[
            "temperature",
            "humidity",
            "temperature_change",
            "rolling_avg_temperature",
        ],
        outputCol="features",
    )

    df = assembler.transform(df)

    model = load_champion_model()

    predictions = model.transform(df)

    mae_evaluator = RegressionEvaluator(
        labelCol="target_temperature",
        predictionCol="prediction",
        metricName="mae",
    )

    rmse_evaluator = RegressionEvaluator(
        labelCol="target_temperature",
        predictionCol="prediction",
        metricName="rmse",
    )

    mae = mae_evaluator.evaluate(predictions)
    rmse = rmse_evaluator.evaluate(predictions)

    return mae, rmse