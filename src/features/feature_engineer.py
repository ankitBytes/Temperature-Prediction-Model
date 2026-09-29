from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, avg, lead


def create_temperature_change(df):

    window = (
        Window
        .partitionBy("sensor_id")
        .orderBy("timestamp")
    )

    df = df.withColumn(
        "previous_temperature",
        lag("temperature", 1).over(window)
    )

    df = df.withColumn(
        "temperature_change",
        col("temperature") - col("previous_temperature")
    )

    return df.drop("previous_temperature")

def create_rolling_average(df):
    window = (
        Window
        .partitionBy("sensor_id")
        .orderBy(col("timestamp").cast("long"))
        .rangeBetween(-30 * 60, 0)
    )

    df = df.withColumn(
        "rolling_avg_temperature",
        avg("temperature").over(window)
    )

    return df

def create_target(df):

    window = (
        Window
        .partitionBy("sensor_id")
        .orderBy("timestamp")
    )

    df = df.withColumn(
        "target_temperature",
        lead("temperature", 12).over(window)
    )

    return df