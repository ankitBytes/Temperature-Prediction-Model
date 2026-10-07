from src.model.model_trainer import train_model, predict, FEATURE_COLUMNS, TARGET_COLUMN
from src.model.model_evaluator import evaluate_model
import mlflow
from mlflow.models import infer_signature
from mlflow import MlflowClient
from src.mlflow.model_quality import passes_quality_gate
import yaml

with open("config/training.yaml", "r") as file:
    config = yaml.safe_load(file)

MAX_VALIDATION_MAE = config["validation"]["max_mae"]

mlflow.autolog()
mlflow.set_tracking_uri("http://127.0.0.1:5000")

client = MlflowClient()

def run_training(train_df, validation_df):

    with mlflow.start_run() as run:

        model = train_model(train_df)

        mlflow.log_param("target", TARGET_COLUMN)
        mlflow.log_param("model_type", "LinearRegression")
        mlflow.log_param("regParam", model.getRegParam())
        mlflow.log_param("elasticNetParam", model.getElasticNetParam())
        mlflow.log_param("features", ",".join(FEATURE_COLUMNS))
        mlflow.log_param("max_validation_mae", MAX_VALIDATION_MAE)

        validation_predictions = predict(
            model,
            validation_df
        )

        mse, mae, rmse = evaluate_model(validation_predictions)

        mlflow.log_metric("validation_mse", mse)
        mlflow.log_metric("validation_mae", mae)
        mlflow.log_metric("validation_rmse", rmse)

        signature = infer_signature(
            validation_predictions.select("features"),
            validation_predictions.select("prediction")
        )


        if passes_quality_gate(mae):
            print(f"Model passed validation: MAE = {mae:.2f}")

            model_info = mlflow.spark.log_model(
                model,
                "model",
                registered_model_name="temperature-prediction-model"
            )

            registered_version = model_info.registered_model_version

            client.set_registered_model_alias(
                "temperature-prediction-model",
                "champion",
                registered_version
            )

            print(f"Model version {registered_version} promoted to @champion")
        else:
            print(f"Model failed validation: MAE = {mae:.2f}")

        print("Run ID:", run.info.run_id)

        return model, validation_predictions