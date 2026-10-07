import json
import time

import requests
from kafka import KafkaProducer


API_URL = "http://127.0.0.1:8000/sensor"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "raw-sensor-readings"
INTERVAL = 5


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def fetch_sensor_reading():
    response = requests.get(API_URL, timeout=10)
    response.raise_for_status()

    return response.json()


def publish_reading(producer, reading):
    future = producer.send(
        KAFKA_TOPIC,
        value=reading,
    )

    metadata = future.get(timeout=10)

    print(
        f"API → Kafka | "
        f"topic={metadata.topic} | "
        f"partition={metadata.partition} | "
        f"offset={metadata.offset} | "
        f"sensor={reading.get('sensor_id')} | ",
        f"source={reading.get('source')}"
    )


def run():
    producer = create_producer()

    try:
        while True:
            reading = fetch_sensor_reading()

            print(
                f"API | "
                f"{reading.get('timestamp')} | "
                f"{reading.get('sensor_id')} | "
                f"temperature={reading.get('temperature')} | "
                f"humidity={reading.get('humidity')}"
            )

            publish_reading(producer, reading)

            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print("\nAPI Kafka producer shutting down.")

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run()