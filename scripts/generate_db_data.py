import sqlite3
import random
from datetime import datetime, timedelta


DB_PATH = "database/sensor_data.db"

SENSORS = [
    "MANUFACTURING_01",
    "MANUFACTURING_02",
    "MANUFACTURING_03",
]

START_TIME = datetime(2026, 10, 1, 0, 0, 0)
READINGS_PER_SENSOR = 2000
INTERVAL_MINUTES = 5


def create_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_sensor_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id TEXT NOT NULL,
            timestamp DATETIME NOT NULL,
            temperature REAL NOT NULL,
            humidity REAL NOT NULL,
            UNIQUE(sensor_id, timestamp)
        )
    """)


def generate_readings():
    readings = []

    for sensor_id in SENSORS:
        base_temperature = random.uniform(360, 380)
        base_humidity = random.uniform(50, 60)

        for i in range(READINGS_PER_SENSOR):
            timestamp = START_TIME + timedelta(
                minutes=i * INTERVAL_MINUTES
            )

            # Small gradual changes instead of completely random values
            temperature = (
                base_temperature
                + random.uniform(-2.0, 2.0)
                + 3 * (i / READINGS_PER_SENSOR)
            )

            humidity = (
                base_humidity
                + random.uniform(-1.5, 1.5)
            )

            readings.append(
                (
                    sensor_id,
                    timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    round(temperature, 2),
                    round(humidity, 2),
                )
            )

    return readings


def insert_readings(cursor, readings):
    cursor.executemany("""
        INSERT OR IGNORE INTO raw_sensor_readings (
            sensor_id,
            timestamp,
            temperature,
            humidity
        )
        VALUES (?, ?, ?, ?)
    """, readings)


def main():
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    create_table(cursor)

    readings = generate_readings()
    insert_readings(cursor, readings)

    connection.commit()

    cursor.execute("SELECT COUNT(*) FROM raw_sensor_readings")
    total_records = cursor.fetchone()[0]

    print(f"Generated readings: {len(readings)}")
    print(f"Total records in database: {total_records}")

    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()