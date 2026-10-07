import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

reading = {
    "sensor_id": "TEST_SENSOR_01",
    "timestamp": "2026-10-06T12:00:00",
    "temperature": 370.50,
    "humidity": 55.00,
    "is_faulty": False,
    "fault_type": None,
    "source": "test",
}

print("Sending event 1...")
producer.send("raw-sensor-readings", value=reading).get()

print("Sending event 2 (duplicate)...")
producer.send("raw-sensor-readings", value=reading).get()

producer.flush()
producer.close()

print("Done.")