import csv
import json
import os
import time

from kafka import KafkaProducer


CSV_PATH = "data/simulator/sensor_readings.csv"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "raw-sensor-readings"

CHECK_INTERVAL = 1
STATE_FILE = "data/simulator/csv_kafka_offset.txt"


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def load_offset():
    if not os.path.exists(STATE_FILE):
        return 0

    with open(STATE_FILE, "r") as file:
        return int(file.read().strip())


def save_offset(offset):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)

    with open(STATE_FILE, "w") as file:
        file.write(str(offset))


def read_new_rows(last_line):
    with open(CSV_PATH, "r", newline="") as file:
        reader = csv.DictReader(file)

        for line_number, row in enumerate(reader, start=1):
            if line_number > last_line:
                yield line_number, row


def publish_reading(producer, reading):
    future = producer.send(
        KAFKA_TOPIC,
        value=reading,
    )

    metadata = future.get(timeout=10)

    print(
        f"CSV → Kafka | "
        f"partition={metadata.partition} | "
        f"offset={metadata.offset} | "
        f"sensor={reading.get('sensor_id')}"
    )


def run():
    producer = create_producer()
    last_line = load_offset()

    print(f"Starting CSV producer from line {last_line}")

    try:
        while True:
            new_rows = list(read_new_rows(last_line))

            for line_number, reading in new_rows:
                publish_reading(producer, reading)
                last_line = line_number
                save_offset(last_line)

            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        print("\nCSV Kafka producer shutting down.")

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run()