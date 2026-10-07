import mlflow


MODEL_URI = "models:/temperature-prediction-model@champion"


def load_champion_model():
    mlflow.set_tracking_uri("http://127.0.0.1:5000")

    model = mlflow.spark.load_model(MODEL_URI)


    return model