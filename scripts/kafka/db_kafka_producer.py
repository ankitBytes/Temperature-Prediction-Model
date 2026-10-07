import json
import os
import sqlite3
import time

from kafka import KafkaProducer


DB_PATH = "database/simulator.db"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "raw-sensor-readings"

CHECK_INTERVAL = 1
STATE_FILE = "data/simulator/db_kafka_offset.txt"


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def load_offset():
    # 1. Ensure the directory path exists before creating the file
    dir_name = os.path.dirname(STATE_FILE)
    if dir_name and not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)

    # 2. If the file doesn't exist, create it with a default offset string '0'
    if not os.path.exists(STATE_FILE):
        with open(STATE_FILE, "w") as file:
            file.write("0")

    # 3. Read and safely parse the integer offset
    with open(STATE_FILE, "r") as file:
        content = file.read().strip()
        return int(content) if content else 0


def save_offset(offset):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)

    with open(STATE_FILE, "w") as file:
        file.write(str(offset))


def read_new_rows(last_id):
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            id,
            sensor_id,
            timestamp,
            temperature,
            humidity,
            is_faulty,
            fault_type,
            source
        FROM raw_sensor_readings
        WHERE id > ?
        ORDER BY id
        """,
        (last_id,),
    )

    rows = cursor.fetchall()

    cursor.close()
    connection.close()

    for row in rows:
        yield {
            "sensor_id": row[1],
            "timestamp": row[2],
            "temperature": row[3],
            "humidity": row[4],
            "is_faulty": bool(row[5]),
            "fault_type": row[6],
            "source": row[7],
            "source_id": row[0],
        }


def publish_reading(producer, reading):
    future = producer.send(
        KAFKA_TOPIC,
        value=reading,
    )

    metadata = future.get(timeout=10)

    print(
        f"DB → Kafka | "
        f"db_id={reading.get('source_id')} | "
        f"partition={metadata.partition} | "
        f"offset={metadata.offset} | "
        f"sensor={reading.get('sensor_id')}"
    )


def run():
    producer = create_producer()
    last_id = load_offset()

    print(f"Starting DB producer from id {last_id}")

    try:
        while True:
            new_rows = list(read_new_rows(last_id))

            for reading in new_rows:
                publish_reading(producer, reading)

                last_id = reading["source_id"]
                save_offset(last_id)

            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        print("\nDB Kafka producer shutting down.")

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run()