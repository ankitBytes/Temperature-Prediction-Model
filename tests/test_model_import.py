import mlflow
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Model Loading Test")
    .master("local[*]")
    .getOrCreate()
)

model_uri = "models:/temperature-prediction-model/1"

model = mlflow.spark.load_model(model_uri)

print("Champion model loaded successfully!")
print(model)

spark.stop()