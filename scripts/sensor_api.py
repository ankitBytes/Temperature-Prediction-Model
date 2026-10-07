from fastapi import FastAPI
from sensor_simulator.sensor_simulator import (
    generate_api_data,
    SensorSimulator,
    SENSOR_IDS,
)

app = FastAPI(title="Manufacturing Sensor API")

sensors = [
    SensorSimulator(sensor_id=sensor_id)
    for sensor_id in SENSOR_IDS
]

@app.get("/sensor")
def get_sensor_reading():
    return generate_api_data(sensors)