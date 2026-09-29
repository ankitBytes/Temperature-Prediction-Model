from pyspark.ml.regression import LinearRegression
import yaml

with open("config/training.yaml", "r") as file:
    config = yaml.safe_load(file)

FEATURE_COLUMNS = [
    "temperature",
    "humidity",
    "temperature_change",
    "rolling_avg_temperature"
]

TARGET_COLUMN = "target_temperature"


def train_model(train_df):

    lr = LinearRegression(
        featuresCol="features",
        labelCol=TARGET_COLUMN,
        predictionCol="prediction",
        regParam=config["model"]["regParam"]
    )

    model = lr.fit(train_df)

    return model

def predict(model, data_df):

    predictions = model.transform(data_df)

    return predictions