from pyspark.sql.functions import col, lit, expr

def load_sensor_data(spark):
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("/home/vvdn/Desktop/Projects/Python/Temperature-prediction-model-using-pyspark/data/raw/ttemperature_regulation_smart_manufacturing.csv")
    )

    df = (
        df
        .withColumn(
            "sensor_id",
            lit("MANUFACTURING_01")
        )
        .withColumn(
            "timestamp",
            expr("try_cast(`Timestamp` AS TIMESTAMP)")
        )
        .withColumn(
            "temperature",
            col("Current Temperature (°C)").cast("double")
        )
        .withColumn(
            "humidity",
            col("Humidity (%)").cast("double")
        )
    )

    return df.select(
        "sensor_id",
        "timestamp",
        "temperature",
        "humidity"
    )