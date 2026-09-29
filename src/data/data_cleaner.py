from pyspark.sql.functions import (
    col,
    array,
    concat_ws,
    lit,
    when,
    filter as spark_filter
)

temperature_range = [300, 450]
humidity_range = [30, 80]

def validate_reading(df):

    reasons = array(
        when(
            col("sensor_id").isNull() | (col("sensor_id") == " "),
            lit("Sensor ID is missing")
        ),
        when(
            col("timestamp").isNull(),
            lit("Timestamp is missing")
        ),
        when(
            col("temperature").isNull(),
            lit("Temperature is missing")
        ),
        when(
            col("temperature").isNotNull()
            & (
                (col("temperature") < temperature_range[0])
                | (col("temperature") > temperature_range[1])
            ),
            lit("Temperature is out of range")
        ),
        when(
            col("humidity").isNull(),
            lit("Humidity is missing")
        ),
        when(
            col("humidity").isNotNull()
            & (
                (col("humidity") < humidity_range[0])
                | (col("humidity") > humidity_range[1])
            ),
            lit("Humidity is out of range")
        ),
    )

    df_with_reasons = (
        df
        .withColumn("reasons", reasons)
        .withColumn(
            "reasons",
            spark_filter(
                col("reasons"),
                lambda reason: reason.isNotNull()
            )
        )
        .withColumn(
            "reasons",
            concat_ws(", ", col("reasons"))
        )
    )
    
    invalid_df = df_with_reasons.filter(col("reasons") != "")

    valid_df = df_with_reasons.filter(col("reasons") == "").drop("reasons")

    invalid_df = invalid_df.select(
        "sensor_id",
        "timestamp",
        "temperature",
        "humidity",
        "reasons"
    )

    return valid_df, invalid_df