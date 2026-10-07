from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import corr


def evaluate_model(predictions_df):
    mse_evaluator = RegressionEvaluator(
        labelCol="target_temperature",
        predictionCol="prediction",
        metricName="mse"
    )

    mae_evaluator = RegressionEvaluator(
        labelCol="target_temperature",
        predictionCol="prediction",
        metricName="mae"
    )

    rmse_evaluator = RegressionEvaluator(
        labelCol="target_temperature",
        predictionCol="prediction",
        metricName="rmse"
    )

    mse = mse_evaluator.evaluate(predictions_df)
    mae = mae_evaluator.evaluate(predictions_df)
    rmse = rmse_evaluator.evaluate(predictions_df)

    return mse, mae, rmse