"""
Manufacturing temperature/humidity sensor simulator.

The simulator generates independent readings for three source systems:
    1. CSV
    2. SQLite
    3. REST API

Each source has its own SensorSimulator instance, so every source receives
a different reading.

Run:
    python data/sensor_simulator.py

    python data/sensor_simulator.py --interval 1 --count 5

    python data/sensor_simulator.py \
        --interval 60 \
        --fault-rate 0.10 \
        --sensor-id MANUFACTURING_01

    python data/sensor_simulator.py \
        --csv data/raw/ttemperature_regulation_smart_manufacturing.csv

Options:
    --sensor-id
        Sensor ID. Default: MANUFACTURING_01

    --interval
        Seconds between reading cycles. Default: 60

    --csv
        Optional existing dataset used for calibration.

    --temp-col
        Temperature column name when using --csv.

    --hum-col
        Humidity column name when using --csv.

    --temp-min / --temp-max
        Temperature safe range.

    --hum-min / --hum-max
        Humidity safe range.

    --fault-rate
        Probability that a generated reading is faulty.
        Default: 0.05

    --count
        Number of reading cycles before stopping.
        Default: run forever.

    --seed
        Optional random seed.
"""

import argparse
import csv
import random
import signal
import statistics
import time
from datetime import datetime
import os
import sqlite3


# ---------------------------------------------------------------------------
# Default simulator configuration
# ---------------------------------------------------------------------------

DEFAULTS = dict(
    temp_mean=375.0,
    temp_std=15.0,
    temp_min=300.0,
    temp_max=450.0,
    hum_mean=55.0,
    hum_std=8.0,
    hum_min=30.0,
    hum_max=80.0,
    temp_step=1.0,
    hum_step=0.8,
    temp_noise=0.3,
    hum_noise=0.3,
    reversion=0.05,
    coupling=-0.5,
    fault_rate=0.05,
)


# ---------------------------------------------------------------------------
# Fault injection
# ---------------------------------------------------------------------------

FAULT_TYPES = (
    "missing_sensor_id",
    "missing_timestamp",
    "missing_temperature",
    "missing_humidity",
    "temperature_out_of_range",
    "humidity_out_of_range",
)

SENSOR_IDS = [
    "MANUFACTURING_01",
    "MANUFACTURING_02",
    "MANUFACTURING_03",
]

CSV_OUTPUT_PATH = "data/simulator/sensor_readings.csv"
DB_OUTPUT_PATH = "database/simulator.db"


def _inject_fault(reading, rng, config):
    """
    Mutate one field of a reading to simulate a sensor fault.
    """

    fault = rng.choice(FAULT_TYPES)

    if fault == "missing_sensor_id":
        reading["sensor_id"] = rng.choice([None, "", " "])

    elif fault == "missing_timestamp":
        reading["timestamp"] = None

    elif fault == "missing_temperature":
        reading["temperature"] = None

    elif fault == "missing_humidity":
        reading["humidity"] = None

    elif fault == "temperature_out_of_range":
        span = config["temp_max"] - config["temp_min"]

        reading["temperature"] = round(
            (
                config["temp_min"] - rng.uniform(1, 0.2 * span)
                if rng.random() < 0.5
                else config["temp_max"] + rng.uniform(1, 0.2 * span)
            ),
            2,
        )

    elif fault == "humidity_out_of_range":
        span = config["hum_max"] - config["hum_min"]

        reading["humidity"] = round(
            (
                config["hum_min"] - rng.uniform(1, 0.2 * span)
                if rng.random() < 0.5
                else config["hum_max"] + rng.uniform(1, 0.2 * span)
            ),
            2,
        )

    reading["is_faulty"] = True
    reading["fault_type"] = fault

    return reading


# ---------------------------------------------------------------------------
# Dataset calibration
# ---------------------------------------------------------------------------

def calibrate_from_csv(path, temp_col, hum_col):
    """
    Learn baseline statistics from an existing dataset.

    The dataset is read-only and is not modified.
    """

    temps = []
    hums = []

    with open(path, newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)

        for row in reader:
            try:
                temperature = float(row[temp_col])
                humidity = float(row[hum_col])
            except (KeyError, ValueError, TypeError):
                continue

            temps.append(temperature)
            hums.append(humidity)

    if len(temps) < 3:
        raise SystemExit(
            f"Could not read enough numeric rows from {path} "
            f"(columns: '{temp_col}', '{hum_col}')"
        )

    config = {
        "temp_mean": statistics.fmean(temps),
        "temp_std": statistics.pstdev(temps),
        "temp_min": min(temps),
        "temp_max": max(temps),
        "hum_mean": statistics.fmean(hums),
        "hum_std": statistics.pstdev(hums),
        "hum_min": min(hums),
        "hum_max": max(hums),
    }

    dt = [b - a for a, b in zip(temps, temps[1:])]
    dh = [b - a for a, b in zip(hums, hums[1:])]

    # Keep the simulated movement gradual.
    config["temp_step"] = (
        min(
            statistics.pstdev(dt),
            config["temp_std"] * 0.1,
        )
        or DEFAULTS["temp_step"]
    )

    config["hum_step"] = (
        min(
            statistics.pstdev(dh),
            config["hum_std"] * 0.1,
        )
        or DEFAULTS["hum_step"]
    )

    # Estimate temperature/humidity relationship.
    var_temperature = statistics.pvariance(temps)

    if var_temperature > 0:
        mean_temperature = config["temp_mean"]
        mean_humidity = config["hum_mean"]

        covariance = (
            sum(
                (temperature - mean_temperature)
                * (humidity - mean_humidity)
                for temperature, humidity in zip(temps, hums)
            )
            / len(temps)
        )

        config["coupling"] = max(
            -2.0,
            min(
                2.0,
                covariance / var_temperature,
            ),
        )

    return config


# ---------------------------------------------------------------------------
# Sensor simulator
# ---------------------------------------------------------------------------

class SensorSimulator:
    """
    Simulated manufacturing sensor.

    A real sensor can later replace this class as long as it exposes:

        read() -> dict
    """

    def __init__(
        self,
        sensor_id="MANUFACTURING_01",
        seed=None,
        **overrides,
    ):
        self.sensor_id = sensor_id

        self.config = {
            **DEFAULTS,
            **overrides,
        }

        self.rng = random.Random(seed)

        # Start near the configured mean.
        self.temperature = (
            self.config["temp_mean"]
            + self.rng.gauss(
                0,
                self.config["temp_std"] * 0.2,
            )
        )

        self.humidity = (
            self.config["hum_mean"]
            + self.rng.gauss(
                0,
                self.config["hum_std"] * 0.2,
            )
        )

        self._clamp_state()

    def _clamp_state(self):
        """Keep the internal healthy sensor state within safe ranges."""

        config = self.config

        self.temperature = min(
            max(
                self.temperature,
                config["temp_min"],
            ),
            config["temp_max"],
        )

        self.humidity = min(
            max(
                self.humidity,
                config["hum_min"],
            ),
            config["hum_max"],
        )

    def _step(self):
        """
        Move the internal sensor state by one simulation step.
        """

        config = self.config
        gaussian = self.rng.gauss

        old_temperature = self.temperature

        self.temperature += (
            config["reversion"]
            * (config["temp_mean"] - self.temperature)
            + gaussian(0, config["temp_step"])
        )

        self.humidity += (
            config["reversion"]
            * (config["hum_mean"] - self.humidity)
            + config["coupling"]
            * (self.temperature - old_temperature)
            + gaussian(0, config["hum_step"])
        )

        self._clamp_state()

    def read(self):
        """
        Generate one sensor reading.
        """

        self._step()

        config = self.config
        gaussian = self.rng.gauss

        temperature = min(
            max(
                self.temperature
                + gaussian(0, config["temp_noise"]),
                config["temp_min"],
            ),
            config["temp_max"],
        )

        humidity = min(
            max(
                self.humidity
                + gaussian(0, config["hum_noise"]),
                config["hum_min"],
            ),
            config["hum_max"],
        )

        reading = {
            "sensor_id": self.sensor_id,
            "timestamp": datetime.now()
            .astimezone()
            .isoformat(timespec="seconds"),
            "temperature": round(temperature, 2),
            "humidity": round(humidity, 2),
            "is_faulty": False,
            "fault_type": None,
        }

        # Randomly inject a fault.
        if self.rng.random() < config["fault_rate"]:
            reading = _inject_fault(
                reading,
                self.rng,
                config,
            )

        return reading


# ---------------------------------------------------------------------------
# Common random data generation
# ---------------------------------------------------------------------------

def generate_random_data(sensors):
    """
    Generate one reading using a SensorSimulator instance.

    This is the common generation function used by all three
    independent source generators.
    """

    sensor = random.choice(sensors)
    return sensor.read()

def write_to_csv(reading, output_path):
    """
    Append one sensor reading to a CSV file.

    Creates the directory and CSV header if they don't exist.
    """

    # Create the parent directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    file_exists = os.path.exists(output_path)

    fieldnames = [
        "sensor_id",
        "timestamp",
        "temperature",
        "humidity",
        "is_faulty",
        "fault_type",
        "source"
    ]

    with open(
        output_path,
        mode="a",
        newline="",
        encoding="utf-8",
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames,
        )

        # Write the header only for a new file
        if not file_exists:
            writer.writeheader()

        writer.writerow({
            field: reading.get(field)
            for field in fieldnames
        })

def write_to_db(reading, database_path):
    """
    Append one sensor reading to the simulator SQLite database.

    Creates the database and table if they don't exist.
    """

    os.makedirs(
        os.path.dirname(database_path),
        exist_ok=True,
    )

    connection = sqlite3.connect(database_path)

    cursor = connection.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_sensor_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id TEXT,
            timestamp TEXT,
            temperature REAL,
            humidity REAL,
            is_faulty BOOLEAN,
            fault_type TEXT,
            source TEXT
        )
    """)

    cursor.execute("""
        INSERT INTO raw_sensor_readings (
            sensor_id,
            timestamp,
            temperature,
            humidity,
            is_faulty,
            fault_type,
            source
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        reading.get("sensor_id"),
        reading.get("timestamp"),
        reading.get("temperature"),
        reading.get("humidity"),
        reading.get("is_faulty"),
        reading.get("fault_type"),
        reading.get("source"),
    ))

    connection.commit()

    cursor.close()
    connection.close()



# ---------------------------------------------------------------------------
# Independent source generators
# ---------------------------------------------------------------------------

def generate_csv_data(sensors, output_path):
    """
    Generate one independent reading for the CSV source.
    """

    reading = generate_random_data(sensors)
    reading["source"] = "csv"

    write_to_csv(
        reading,
        output_path
    )

    return reading


def generate_db_data(sensors):
    """
    Generate one independent reading for the SQLite source.
    """

    reading = generate_random_data(sensors)
    reading["source"] = "database"

    write_to_db(
        reading, 
        DB_OUTPUT_PATH
    )

    return reading


def generate_api_data(sensors):
    """
    Generate one independent reading for the REST API source.
    """

    reading = generate_random_data(sensors)
    reading["source"] = "api"

    print(reading)

    return reading


# ---------------------------------------------------------------------------
# Formatting helper
# ---------------------------------------------------------------------------

def format_reading(source, reading):
    """
    Format a reading for terminal output.
    """

    sensor_id = (
        reading["sensor_id"]
        if reading["sensor_id"]
        else "NULL"
    )

    timestamp = (
        reading["timestamp"]
        if reading["timestamp"]
        else "NULL"
    )

    temperature = (
        f"{reading['temperature']:6.2f}"
        if reading["temperature"] is not None
        else "  NULL"
    )

    humidity = (
        f"{reading['humidity']:6.2f}"
        if reading["humidity"] is not None
        else "  NULL"
    )

    tag = (
        f" [FAULTY: {reading['fault_type']}]"
        if reading["is_faulty"]
        else ""
    )

    return (
        f"{source:3} | "
        f"{timestamp} | "
        f"{sensor_id} | "
        f"temperature={temperature} | "
        f"humidity={humidity}"
        f"{tag}"
    )


# ---------------------------------------------------------------------------
# Main simulation loop
# ---------------------------------------------------------------------------

def run(
    sensors,
    interval=60.0,
    count=None,
):
    """
    Generate independent readings for CSV, DB and API.

    One simulation cycle produces:

        CSV → reading A
        DB  → reading B
        API → reading C
    """

    stop = {"flag": False}

    def _handle_signal(signum, frame):
        stop["flag"] = True

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # print(
    #     "Sensor simulator started | "
    #     f"sensor_id={sensors.sensor_id} | "
    #     f"interval={interval}s | "
    #     "sources=CSV,DB,API | "
    #     "Ctrl+C to stop"
    # )

    cycle = 0

    next_tick = time.monotonic()

    while not stop["flag"] and (
        count is None or cycle < count
    ):
        cycle += 1

        # ---------------------------------------------------------------
        # Generate three independent readings.
        # Each source has its own SensorSimulator instance.
        # ---------------------------------------------------------------

        csv_reading = generate_csv_data(
            sensors,
            CSV_OUTPUT_PATH,
        )
        db_reading = generate_db_data(sensors)
        api_reading = generate_api_data(sensors)

        print(
            f"\n[{cycle:05d}]"
        )

        print(
            format_reading(
                "CSV",
                csv_reading,
            )
        )

        print(
            format_reading(
                "DB ",
                db_reading,
            )
        )

        print(
            format_reading(
                "API",
                api_reading,
            ),
            flush=True,
        )

        # ---------------------------------------------------------------
        # Drift-free interval handling.
        # ---------------------------------------------------------------

        next_tick += interval

        while (
            not stop["flag"]
            and time.monotonic() < next_tick
        ):
            time.sleep(
                min(
                    0.1,
                    max(
                        0.0,
                        next_tick - time.monotonic(),
                    ),
                )
            )

    print(
        f"\nShutting down gracefully. "
        f"{cycle} reading cycle(s) emitted."
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Manufacturing sensor simulator"
    )

    parser.add_argument(
        "--sensor-id",
        default="MANUFACTURING_01",
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=60.0,
        help="seconds between reading cycles, default 60",
    )

    parser.add_argument(
        "--csv",
        help="existing dataset to calibrate from (read-only)",
    )

    parser.add_argument(
        "--temp-col",
        default="Current Temperature (°C)",
    )

    parser.add_argument(
        "--hum-col",
        default="Humidity (%)",
    )

    parser.add_argument(
        "--temp-min",
        type=float,
    )

    parser.add_argument(
        "--temp-max",
        type=float,
    )

    parser.add_argument(
        "--hum-min",
        type=float,
    )

    parser.add_argument(
        "--hum-max",
        type=float,
    )

    parser.add_argument(
        "--fault-rate",
        type=float,
        default=DEFAULTS["fault_rate"],
        help=(
            "probability (0-1) that a reading is faulty, "
            "default 0.05"
        ),
    )

    parser.add_argument(
        "--count",
        type=int,
    )

    parser.add_argument(
        "--seed",
        type=int,
    )

    args = parser.parse_args()

    # ---------------------------------------------------------------
    # Validate CLI arguments.
    # ---------------------------------------------------------------

    if args.interval <= 0:
        parser.error(
            "--interval must be > 0"
        )

    if not 0.0 <= args.fault_rate <= 1.0:
        parser.error(
            "--fault-rate must be between 0 and 1"
        )

    # ---------------------------------------------------------------
    # Optional calibration from CSV.
    # ---------------------------------------------------------------

    config = (
        calibrate_from_csv(
            args.csv,
            args.temp_col,
            args.hum_col,
        )
        if args.csv
        else {}
    )

    # Explicit CLI ranges override calibrated values.
    for key in (
        "temp_min",
        "temp_max",
        "hum_min",
        "hum_max",
    ):
        value = getattr(args, key)

        if value is not None:
            config[key] = value

    config["fault_rate"] = args.fault_rate

    merged_config = {
        **DEFAULTS,
        **config,
    }

    if (
        merged_config["temp_min"]
        >= merged_config["temp_max"]
    ):
        parser.error(
            "temperature min must be smaller than max"
        )

    if (
        merged_config["hum_min"]
        >= merged_config["hum_max"]
    ):
        parser.error(
            "humidity min must be smaller than max"
        )

    # Keep means inside safe ranges.
    merged_config["temp_mean"] = min(
        max(
            merged_config["temp_mean"],
            merged_config["temp_min"],
        ),
        merged_config["temp_max"],
    )

    merged_config["hum_mean"] = min(
        max(
            merged_config["hum_mean"],
            merged_config["hum_min"],
        ),
        merged_config["hum_max"],
    )

    # ---------------------------------------------------------------
    # Calibration information.
    # ---------------------------------------------------------------

    if args.csv:
        print(
            f"Calibrated from {args.csv}: "
            f"temp "
            f"{merged_config['temp_min']:.1f}-"
            f"{merged_config['temp_max']:.1f} °C "
            f"(mean {merged_config['temp_mean']:.1f}), "
            f"humidity "
            f"{merged_config['hum_min']:.1f}-"
            f"{merged_config['hum_max']:.1f} % "
            f"(mean {merged_config['hum_mean']:.1f})"
        )

    # ---------------------------------------------------------------
    # IMPORTANT:
    #
    # Three independent simulator instances.
    #
    # Therefore:
    #
    # CSV → independent state → reading A
    # DB  → independent state → reading B
    # API → independent state → reading C
    # ---------------------------------------------------------------

    sensors = [
        SensorSimulator(
            sensor_id=sensor_id,
            seed=None if args.seed is None else args.seed + index,
            **merged_config,
        )
        for index, sensor_id in enumerate(SENSOR_IDS)
    ]

    run(
        sensors,
        interval=args.interval,
        count=args.count,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()