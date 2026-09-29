"""
Manufacturing temperature/humidity sensor simulator (standalone).

Run:
    python sensor_simulator.py                          # defaults, Ctrl+C to stop
    python sensor_simulator.py --interval 2 --sensor-id MANUFACTURING_02
    python sensor_simulator.py --csv data/train.csv     # calibrate from your dataset
    python sensor_simulator.py --csv data/train.csv --temp-col "Current Temperature (°C)" --hum-col "Humidity (%)"

Options:
    --sensor-id   default MANUFACTURING_01
    --interval    seconds between readings, default 5
    --csv         optional existing dataset; mean/std/min/max, step size and
                  temp<->humidity relationship are learned from it (read-only)
    --temp-min/--temp-max/--hum-min/--hum-max   safe ranges (override CSV-derived)
                  defaults: temperature [300, 450], humidity [30, 80]
    --fault-rate  probability (0-1) that a reading is faulty, default 0.05
    --count       stop after N readings (handy for testing; default: run forever)

How it works:
    - True temperature is a mean-reverting random walk around a setpoint:
      small steps, pulled gently back toward the mean, so it drifts instead of jumping.
    - True humidity follows the same idea, plus a coupling term so it moves
      opposite to temperature (as in most factory environments).
    - Each reading adds small independent measurement noise on top of the true state.
    - Most readings are clamped to the configured safe ranges. A small,
      configurable fraction are deliberately left faulty instead (missing
      field or out-of-range value) so downstream validation has something
      real to catch — see FAULT_TYPES / _inject_fault below.

Swapping in a real sensor later:
    Anything with a read() -> dict returning
    {"sensor_id", "timestamp", "temperature", "humidity"} can replace SensorSimulator;
    run() only depends on that method.

Standard library only. Does not touch the batch pipeline.
"""

import argparse
import csv
import random
import signal
import statistics
import time
from datetime import datetime

DEFAULTS = dict(
    temp_mean=375.0, temp_std=15.0, temp_min=300.0, temp_max=450.0,
    hum_mean=55.0, hum_std=8.0, hum_min=30.0, hum_max=80.0,
    temp_step=1.0, hum_step=0.8,       # per-reading random-walk step (std)
    temp_noise=0.3, hum_noise=0.3,     # measurement noise (std)
    reversion=0.05,                    # pull toward mean per reading
    coupling=-0.5,                     # humidity change per 1-degree temp change
    fault_rate=0.05,                   # fraction of readings deliberately faulty
)

# Each fault mutates one field of an otherwise-normal reading.
FAULT_TYPES = (
    "missing_sensor_id",
    "missing_timestamp",
    "missing_temperature",
    "missing_humidity",
    "temperature_out_of_range",
    "humidity_out_of_range",
)


def _inject_fault(reading, rng, c):
    """Mutate one field of `reading` in place to simulate a real sensor fault."""
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
        span = c["temp_max"] - c["temp_min"]
        reading["temperature"] = round(
            c["temp_min"] - rng.uniform(1, 0.2 * span) if rng.random() < 0.5
            else c["temp_max"] + rng.uniform(1, 0.2 * span), 2)
    elif fault == "humidity_out_of_range":
        span = c["hum_max"] - c["hum_min"]
        reading["humidity"] = round(
            c["hum_min"] - rng.uniform(1, 0.2 * span) if rng.random() < 0.5
            else c["hum_max"] + rng.uniform(1, 0.2 * span), 2)

    reading["is_faulty"] = True
    reading["fault_type"] = fault
    return reading


def calibrate_from_csv(path, temp_col, hum_col):
    """Learn baseline stats from the existing dataset (read-only)."""
    temps, hums = [], []
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            try:
                t, h = float(row[temp_col]), float(row[hum_col])
            except (KeyError, ValueError, TypeError):
                continue
            temps.append(t)
            hums.append(h)
    if len(temps) < 3:
        raise SystemExit(f"Could not read enough numeric rows from {path} "
                         f"(columns: '{temp_col}', '{hum_col}')")

    cfg = {
        "temp_mean": statistics.fmean(temps), "temp_std": statistics.pstdev(temps),
        "temp_min": min(temps), "temp_max": max(temps),
        "hum_mean": statistics.fmean(hums), "hum_std": statistics.pstdev(hums),
        "hum_min": min(hums), "hum_max": max(hums),
    }
    dt = [b - a for a, b in zip(temps, temps[1:])]
    dh = [b - a for a, b in zip(hums, hums[1:])]
    # Dataset row spacing may differ from 5s, so cap step size to stay gradual.
    cfg["temp_step"] = min(statistics.pstdev(dt), cfg["temp_std"] * 0.1) or DEFAULTS["temp_step"]
    cfg["hum_step"] = min(statistics.pstdev(dh), cfg["hum_std"] * 0.1) or DEFAULTS["hum_step"]
    var_t = statistics.pvariance(temps)
    if var_t > 0:
        mt, mh = cfg["temp_mean"], cfg["hum_mean"]
        cov = sum((t - mt) * (h - mh) for t, h in zip(temps, hums)) / len(temps)
        cfg["coupling"] = max(-2.0, min(2.0, cov / var_t))
    return cfg


class SensorSimulator:
    """Simulated sensor. Replace with a real sensor class exposing read()."""

    def __init__(self, sensor_id="MANUFACTURING_01", seed=None, **overrides):
        self.sensor_id = sensor_id
        self.c = {**DEFAULTS, **overrides}
        self.rng = random.Random(seed)
        # Start near the mean (small random offset)
        self.temp = self.c["temp_mean"] + self.rng.gauss(0, self.c["temp_std"] * 0.2)
        self.hum = self.c["hum_mean"] + self.rng.gauss(0, self.c["hum_std"] * 0.2)
        self._clamp_state()

    def _clamp_state(self):
        c = self.c
        self.temp = min(max(self.temp, c["temp_min"]), c["temp_max"])
        self.hum = min(max(self.hum, c["hum_min"]), c["hum_max"])

    def _step(self):
        c, g = self.c, self.rng.gauss
        old_temp = self.temp
        self.temp += (c["reversion"] * (c["temp_mean"] - self.temp)
                      + g(0, c["temp_step"]))
        self.hum += (c["reversion"] * (c["hum_mean"] - self.hum)
                     + c["coupling"] * (self.temp - old_temp)
                     + g(0, c["hum_step"]))
        self._clamp_state()

    def read(self):
        self._step()
        c, g = self.c, self.rng.gauss
        temp = min(max(self.temp + g(0, c["temp_noise"]), c["temp_min"]), c["temp_max"])
        hum = min(max(self.hum + g(0, c["hum_noise"]), c["hum_min"]), c["hum_max"])
        reading = {
            "sensor_id": self.sensor_id,
            "timestamp": datetime.now().astimezone().isoformat(timespec="seconds"),
            "temperature": round(temp, 2),
            "humidity": round(hum, 2),
            "is_faulty": False,
            "fault_type": None,
        }
        if self.rng.random() < c["fault_rate"]:
            reading = _inject_fault(reading, self.rng, c)
        return reading


def run(sensor, interval=5.0, count=None):
    """Emit a reading every `interval` seconds until Ctrl+C (or SIGTERM)."""
    stop = {"flag": False}

    def _handle(signum, frame):
        stop["flag"] = True

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    print(f"Sensor simulator started | sensor_id={sensor.sensor_id} | "
          f"interval={interval}s | Ctrl+C to stop")
    n = 0
    next_tick = time.monotonic()
    while not stop["flag"] and (count is None or n < count):
        r = sensor.read()
        n += 1
        sensor_id = r["sensor_id"] if r["sensor_id"] else "NULL"
        timestamp = r["timestamp"] if r["timestamp"] else "NULL"
        temp = f"{r['temperature']:6.2f}" if r["temperature"] is not None else "  NULL"
        hum = f"{r['humidity']:6.2f}" if r["humidity"] is not None else "  NULL"
        tag = f" [FAULTY: {r['fault_type']}]" if r["is_faulty"] else ""
        print(f"[{n:05d}] {timestamp} | {sensor_id} | "
              f"temperature={temp} | humidity={hum}{tag}",
              flush=True)
        # Drift-free sleep, wakes early on shutdown
        next_tick += interval
        while not stop["flag"] and time.monotonic() < next_tick:
            time.sleep(min(0.1, max(0.0, next_tick - time.monotonic())))
    print(f"\nShutting down gracefully. {n} reading(s) emitted.")


def main():
    p = argparse.ArgumentParser(description="Manufacturing sensor simulator")
    p.add_argument("--sensor-id", default="MANUFACTURING_01")
    p.add_argument("--interval", type=float, default=5.0)
    p.add_argument("--csv", help="existing dataset to calibrate from (read-only)")
    p.add_argument("--temp-col", default="Current Temperature (°C)")
    p.add_argument("--hum-col", default="Humidity (%)")
    p.add_argument("--temp-min", type=float)
    p.add_argument("--temp-max", type=float)
    p.add_argument("--hum-min", type=float)
    p.add_argument("--hum-max", type=float)
    p.add_argument("--fault-rate", type=float, default=DEFAULTS["fault_rate"],
                    help="probability (0-1) that a reading is faulty, default 0.05")
    p.add_argument("--count", type=int)
    p.add_argument("--seed", type=int)
    a = p.parse_args()

    if a.interval <= 0:
        p.error("--interval must be > 0")
    if not 0.0 <= a.fault_rate <= 1.0:
        p.error("--fault-rate must be between 0 and 1")

    cfg = calibrate_from_csv(a.csv, a.temp_col, a.hum_col) if a.csv else {}
    for key in ("temp_min", "temp_max", "hum_min", "hum_max"):
        if getattr(a, key) is not None:
            cfg[key] = getattr(a, key)
    cfg["fault_rate"] = a.fault_rate
    merged = {**DEFAULTS, **cfg}
    if merged["temp_min"] >= merged["temp_max"] or merged["hum_min"] >= merged["hum_max"]:
        p.error("min range values must be smaller than max")
    # Keep the mean inside the safe range
    merged["temp_mean"] = min(max(merged["temp_mean"], merged["temp_min"]), merged["temp_max"])
    merged["hum_mean"] = min(max(merged["hum_mean"], merged["hum_min"]), merged["hum_max"])

    if a.csv:
        print(f"Calibrated from {a.csv}: temp {merged['temp_min']:.1f}-{merged['temp_max']:.1f} °C "
              f"(mean {merged['temp_mean']:.1f}), humidity {merged['hum_min']:.1f}-{merged['hum_max']:.1f} % "
              f"(mean {merged['hum_mean']:.1f})")

    run(SensorSimulator(a.sensor_id, seed=a.seed, **merged), a.interval, a.count)


if __name__ == "__main__":
    main()