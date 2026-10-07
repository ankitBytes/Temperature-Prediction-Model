from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col

from src.data.data_loader import load_from_all_sources
from src.data.data_cleaner import validate_reading

from src.features.feature_engineer import (
    create_temperature_change,
    create_rolling_average,
)

from src.model.model_registry import load_champion_model


FEATURE_COLUMNS = [
    "temperature",
    "humidity",
    "temperature_change",
    "rolling_avg_temperature",
]


def run_inference(
    spark,
    csv_path,
    api_url,
    database_path,
):

    # ---------------------------------------------------------
    # 1. Load data from all sources
    # ---------------------------------------------------------

    df = load_from_all_sources(
        spark,
        csv_path,
        api_url,
        database_path,
    )

    # ---------------------------------------------------------
    # 2. Validate incoming data
    # ---------------------------------------------------------

    valid_df, invalid_df = validate_reading(df)

    # ---------------------------------------------------------
    # 3. Create features required by the model
    # ---------------------------------------------------------

    feature_df = create_temperature_change(valid_df)

    feature_df = create_rolling_average(feature_df)

    # The first reading of each sensor cannot calculate
    # temperature_change because there is no previous reading.
    feature_df = feature_df.filter(
        col("temperature_change").isNotNull()
    )

    # ---------------------------------------------------------
    # 4. Create feature vector
    # ---------------------------------------------------------

    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features",
    )

    feature_df = assembler.transform(feature_df)

    # ---------------------------------------------------------
    # 5. Load champion model from MLflow
    # ---------------------------------------------------------

    model = load_champion_model()

    # ---------------------------------------------------------
    # 6. Generate predictions
    # ---------------------------------------------------------

    predictions = model.transform(feature_df)

    return predictions, invalid_df