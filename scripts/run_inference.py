from pyspark.sql import SparkSession
from pipelines.inference_pipeline import run_inference
import yaml

with open("/home/vvdn/Desktop/Projects/Python/Temperature-prediction-model-using-pyspark/config/database.yaml", "r") as file:
    config = yaml.safe_load(file)

db_path = config["database"]["path"]


CSV_PATH = "/home/vvdn/Desktop/Projects/Python/Temperature-prediction-model-using-pyspark/data/raw/ttemperature_regulation_smart_manufacturing.csv"
API_URL = "http://127.0.0.1:8000/sensor"
output_path = "data/processed/predictions"


def main():
    spark = (
        SparkSession.builder
        .appName("Temperature prediction inference")
        .master("local[*]")
        .getOrCreate()
    )

    predictions, invalid_df = run_inference(spark, CSV_PATH, API_URL, db_path)

    predictions.select(
        "timestamp",
        "temperature",
        "prediction"
    ).write.mode("overwrite").option("header", True).csv(output_path)

    print(f"Predictions saved to: {output_path}")

    predictions.select(
        "timestamp",
        "temperature",
        "prediction"
    ).show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()