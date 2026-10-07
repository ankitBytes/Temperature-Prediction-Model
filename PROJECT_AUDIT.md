# Temperature Prediction Model - Complete Technical Audit

**Repository inspected**: YES  
**Files inspected**: 50+ source/config files  
**Code/config inspected**: YES  
**Logs/results inspected**: YES  
**Inspection date**: 2026-10-03  

---

## 1. PROJECT OVERVIEW

**Project Name**: Temperature Prediction Model Using PySpark

**Objective**: Build a complete data pipeline for predicting temperature in smart manufacturing environments using machine learning, with an extensible on-prem MLOps architecture.

**Problem Solved**: 
- Ingests manufacturing sensor readings (temperature, humidity)
- Validates data quality
- Engineers ML features
- Trains predictive models
- Tracks model registry via MLflow
- Monitors data drift
- Orchestrates with Airflow
- Enables inference on new data

**Current MLOps Architecture** (ACTUAL TODAY):
```
CSV Data → PySpark Load → Validate → Feature Engineer → SQLite Store 
→ Train/Val/Test Split → LinearRegression Training → MLflow Register 
→ Airflow Orchestrate → Inference → Monitoring
```

**Current Data Flow**:
1. Raw CSV (1000+ readings) or Database
2. PySpark loads and transforms
3. Data validation (range checks)
4. Split: valid → DB, invalid → CSV
5. Feature engineering (3 features)
6. Model training on 70% data
7. Validation on 15%, test on 15%
8. MLflow tracks metrics
9. Inference runs on new data
10. Monitoring calculates drift

**Current ML Workflow**:
- Load validated data → Engineer features → Assemble feature vector → Train LinearRegression → Evaluate (MSE, MAE) → Register in MLflow → Promote to @champion

**Current ETL/Data Engineering Workflow**:
CSV → Load → Validate → Clean → Feature Engineer → Store in SQLite

**Current Orchestration Workflow**:
Training → Inference → Monitoring (Airflow DAG: temperature_prediction_pipeline)

**Current Monitoring Workflow**:
Load data → Split 80/20 reference/current → Calculate statistics → Calculate drift % → Evaluate champion model → Log metrics to MLflow

**Technologies Used**:
- **PySpark 3.5.0+**: Distributed data processing, feature engineering
- **SQLite**: Persistent data storage (sensor readings, features, models)
- **MLflow 2.10.0+**: Model tracking, registry, versioning, @champion alias
- **Airflow 3.3.2+**: DAG orchestration (@daily schedule)
- **Python 3.9+**: Core language
- **FastAPI**: REST API for sensor simulator
- **Streamlit 1.34+**: Dashboard visualization
- **Scikit-learn**: For model training (via PySpark ML wrapper)
- **DVC**: Data version control (tracks CSV and pipeline stages)
- **YAML**: Configuration management

---

## 2. COMPLETE FILESYSTEM

```
temperature-prediction-model-using-pyspark/
├── config/                              # Configuration files
│   ├── config.yaml                     # Empty (main config placeholder)
│   ├── database.yaml                   # SQLite path config
│   ├── logging_config.yaml             # Empty (logging placeholder)
│   └── training.yaml                   # Model params (regParam, MAE threshold, drift %)
├── data/
│   ├── raw/
│   │   ├── ttemperature_regulation_smart_manufacturing.csv  # 1000 readings, DVC tracked
│   │   ├── ttemperature_regulation_smart_manufacturing.csv.dvc
│   │   └── .gitignore
│   ├── processed/
│   │   ├── invalid_data/               # Output: invalid records CSV
│   │   │   └── part-00000-*.csv       # Spark partitioned output
│   │   └── predictions/                # Output: inference predictions
│   │       └── part-00000-*.csv
│   ├── inference/                      # Empty directory
│   ├── features/                       # Empty directory
│   └── sensor_simulator.py             # Moved from scripts/
├── database/
│   └── sensor_data.db                  # SQLite (3MB, 13K readings, 7K features)
├── src/
│   ├── data/
│   │   ├── data_loader.py             # load_from_csv, load_from_api, load_from_database, load_from_all_sources
│   │   ├── data_cleaner.py            # validate_reading (range checks, nulls)
│   │   └── data_validator.py          # Empty
│   ├── features/
│   │   ├── feature_engineer.py        # create_temperature_change, create_rolling_average, create_target
│   │   └── feature_store.py           # create_feature_table, store_features, check_features
│   ├── model/
│   │   ├── model_trainer.py           # train_model (LinearRegression), predict
│   │   ├── model_evaluator.py         # evaluate_model (MSE, MAE, RMSE)
│   │   └── model_registry.py          # load_champion_model (MLflow)
│   ├── inference/
│   │   ├── predictor.py               # Empty
│   │   └── __init__.py
│   ├── mlflow/
│   │   ├── model_quality.py           # passes_quality_gate (MAE <= 15.0)
│   │   ├── register_model.py          # Empty
│   │   ├── promote_model.py           # Empty
│   │   ├── test_model_loading.py      # Empty
│   │   ├── test_model_quality.py      # Empty
│   │   └── __init__.py
│   └── utils/
│       ├── logger.py
│       ├── database.py
│       ├── metrics.py
│       └── __init__.py
├── pipelines/
│   ├── training_pipeline.py           # run_training (orchestrates training, MLflow logging, model promotion)
│   ├── inference_pipeline.py          # run_inference (loads data, validates, features, champion model, predicts)
│   ├── monitoring_pipeline.py         # calculate_statistics, calculate_drift, split_reference_current_data, evaluate_champion_model
│   └── __init__.py
├── scripts/
│   ├── run_training.py                # Main entry: loads all sources, validates, features, trains, logs to MLflow
│   ├── run_inference.py               # Loads data, features, predicts, outputs CSV
│   ├── run_monitoring.py              # Loads data, calculates drift, logs to MLflow
│   ├── sensor_api.py                  # FastAPI endpoint /sensor (returns SensorSimulator.read())
│   ├── generate_db_data.py            # Generates 6000 synthetic readings for raw_sensor_readings table
│   └── __init__.py
├── airflow/
│   └── dags/
│       └── temperature_pipeline.py    # DAG: temperature_prediction_pipeline (training → inference → monitoring, @daily)
├── mlruns/                             # MLflow tracking database (20+ experiment runs)
│   └── 0/                              # Experiment 0 (default)
│       └── <run_ids>/
│           ├── meta.yaml
│           ├── params/
│           ├── metrics/
│           └── artifacts/
├── tests/
│   ├── test_data_loader.py
│   ├── test_data_validator.py
│   ├── test_feature_engineer.py
│   ├── test_model_trainer.py
│   ├── test_model_import.py
│   ├── test_predictor.py
│   └── __init__.py
├── docs/                               # Comprehensive documentation
│   ├── 01-project-overview.md
│   ├── 02-technology-stack.md
│   ├── 03-architecture.md
│   ├── ...
│   └── 20-mlops-concepts.md
├── logs/                               # Empty (no logs currently written)
├── models/                             # Empty (models stored in MLflow)
├── dashboard.py                        # Standalone HTML dashboard generator (PySpark + SQLite)
├── temperature_dashboard.html          # Generated output HTML
├── dvc.yaml                            # DVC pipeline stage: train (tracks deps/outs)
├── dvc.lock                            # DVC lock file
├── setup.py                            # Python package setup
├── requirements.txt                    # Dependencies (pyspark, mlflow, airflow, streamlit, etc.)
├── README.md                           # Main documentation
├── FINAL_SUMMARY.md                    # Dashboard implementation summary
├── DASHBOARD_SETUP.md                  # Dashboard setup guide
├── DASHBOARD_IMPLEMENTATION.md         # Dashboard technical details
├── on_prem_mlops_architecture.png      # Architecture diagram (PNG)
├── on_prem_mlops_architecture.svg      # Architecture diagram (SVG)
├── generate_architecture_diagram.py    # Script to regenerate architecture diagrams
└── .dvc/, .git/, .venv/, etc.         # Version control and environment files
```

---

## 3. FILE-BY-FILE IMPLEMENTATION SUMMARY

### src/data/data_loader.py

**Purpose**: Load sensor data from multiple sources (CSV, REST API, SQLite) and merge them

**Functions**:
- `load_from_csv(spark, input_path)`: Read CSV, rename columns, cast types, add sensor_id="MANUFACTURING_01"
- `load_from_api(spark, api_url)`: Fetch JSON from REST endpoint, create DataFrame
- `load_from_database(spark, database_path)`: Query raw_sensor_readings table
- `load_from_all_sources(spark, csv_path, api_url, database_path)`: Union all three sources

**Inputs**: CSV file (path), REST API (URL), SQLite database (path)

**Outputs**: Spark DataFrame with schema: [sensor_id, timestamp, temperature, humidity]

**Dependencies**: pyspark, requests, sqlite3

**Used by**: run_training.py, run_inference.py, run_monitoring.py

**Status**: IMPLEMENTED

**Details**:
- CSV loads with inferSchema=True (auto-detects column types)
- Columns renamed: "Current Temperature (°C)" → "temperature", "Humidity (%)" → "humidity"
- Timestamps cast via try_cast (handles parse failures gracefully)
- Sensor ID hardcoded to "MANUFACTURING_01" for CSV source
- API source expects JSON with keys: sensor_id, timestamp, temperature, humidity
- Database source queries table: raw_sensor_readings
- unionByName used for merging (tolerates schema variations)

### src/data/data_cleaner.py

**Purpose**: Validate readings against business rules, separate valid/invalid data

**Functions**:
- `validate_reading(df)`: Apply validation rules, return (valid_df, invalid_df)

**Validation Rules** (in src/data/data_cleaner.py lines 10-11):
- temperature_range = [300, 450] (Celsius)
- humidity_range = [30, 80] (percent)
- Checks: sensor_id not null/empty, timestamp not null, temperature in range, humidity in range

**Inputs**: Spark DataFrame with [sensor_id, timestamp, temperature, humidity]

**Outputs**:
- valid_df: Records passing all checks
- invalid_df: Records failing checks + "reasons" column with violation text

**Dependencies**: pyspark.sql.functions

**Used by**: run_training.py, run_inference.py, run_monitoring.py

**Status**: IMPLEMENTED

**Details**:
- Uses Spark array/filter/concat_ws for efficient validation
- Reasons generated as comma-separated list
- Invalid records preserved with all original fields + reasons
- First row of each sensor has no temperature_change (filtered later)

### src/features/feature_engineer.py

**Purpose**: Create ML-relevant features from raw sensor data

**Functions**:
1. `create_temperature_change(df)`: 
   - Window partitioned by sensor_id, ordered by timestamp
   - lag("temperature", 1) to get previous temp
   - temperature_change = current - previous
   - Returns df without the intermediate "previous_temperature" column

2. `create_rolling_average(df)`:
   - 30-minute rolling window (rangeBetween(-30*60, 0) on timestamp cast to seconds)
   - rolling_avg_temperature = avg(temperature) over window
   
3. `create_target(df)`:
   - lead("temperature", 12) → target is temp 12 readings ahead
   - (Assuming ~5-minute intervals: 12 * 5min ≈ 1 hour ahead)

**Inputs**: Spark DataFrame with [sensor_id, timestamp, temperature, humidity]

**Outputs**: DataFrame with added columns [temperature_change, rolling_avg_temperature, target_temperature]

**Dependencies**: pyspark.sql.window, pyspark.sql.functions

**Used by**: Training pipeline, inference pipeline, monitoring pipeline

**Status**: IMPLEMENTED

**Window Definitions**:
- Partitioned by sensor_id (multi-sensor support, though data has only MANUFACTURING_01)
- Ordered by timestamp (chronological)
- Temperature change: lag(1) previous reading
- Rolling avg: 30-minute window (1800 seconds)
- Target: lead(12) readings

**Potential Issues**: No explicit check for time gaps; assumes regular intervals

### src/features/feature_store.py

**Purpose**: Store engineered features in SQLite database

**Functions**:
- `create_feature_table(connection)`: Create sensor_features table
- `store_features(connection, feature_df)`: Insert features via INSERT OR IGNORE
- `check_features(connection)`: Print first 10 rows (debug function)

**Inputs**: SQLite connection, Spark DataFrame with features

**Outputs**: Rows inserted into sensor_features table

**Dependencies**: sqlite3

**Used by**: run_training.py

**Status**: IMPLEMENTED

**Schema**:
```sql
CREATE TABLE sensor_features (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor_id TEXT,
    timestamp TEXT,
    temperature REAL,
    humidity REAL,
    temperature_change REAL,
    rolling_avg_temperature REAL,
    UNIQUE(sensor_id, timestamp)
)
```

**Details**:
- UNIQUE constraint on (sensor_id, timestamp) prevents duplicates
- INSERT OR IGNORE silently skips duplicates
- Timestamp converted to ISO format string for storage
- Collects Spark DF to Python (row-by-row insert; not optimal for large data)

### src/model/model_trainer.py

**Purpose**: Train LinearRegression model on engineered features

**Functions**:
- `train_model(train_df)`: Fit LinearRegression, return model
- `predict(model, data_df)`: Transform data with model, return predictions

**Model**: LinearRegression (PySpark ML)

**Features**: [temperature, humidity, temperature_change, rolling_avg_temperature]

**Target**: target_temperature (1-hour-ahead reading)

**Hyperparameters** (from config/training.yaml):
- regParam: 0.0 (no regularization)
- elasticNetParam: 0.0 (ignored when regParam=0)

**Inputs**: Train DataFrame with VectorAssembler-created "features" column

**Outputs**: Trained model object

**Dependencies**: pyspark.ml.regression, yaml

**Used by**: pipelines/training_pipeline.py

**Status**: IMPLEMENTED

**Details**:
- Model expects pre-assembled feature vector (not raw columns)
- LinearRegression is on unregularized L2 regression (ridge with λ=0)
- Model returned but not saved to disk (saved via MLflow instead)

### src/model/model_evaluator.py

**Purpose**: Calculate regression metrics on predictions

**Functions**:
- `evaluate_model(predictions_df)`: Compute MSE, MAE, RMSE on validation set

**Metrics Calculated**:
- MSE (Mean Squared Error)
- MAE (Mean Absolute Error)
- RMSE (Root Mean Squared Error)

**Inputs**: DataFrame with columns [target_temperature, prediction]

**Outputs**: Tuple (mse, mae, rmse)

**Dependencies**: pyspark.ml.evaluation

**Used by**: pipelines/training_pipeline.py

**Status**: IMPLEMENTED

**Details**:
- RegressionEvaluator used for each metric
- Evaluates on validation set (not test set, design issue)

### src/model/model_registry.py

**Purpose**: Load trained model from MLflow registry

**Functions**:
- `load_champion_model()`: Load model with alias="@champion"

**Model URI**: "models:/temperature-prediction-model@champion"

**Tracking URI**: http://127.0.0.1:5000

**Inputs**: None (hardcoded model name and URI)

**Outputs**: Loaded PySpark MLflow model

**Dependencies**: mlflow

**Used by**: Inference pipeline, monitoring pipeline, dashboard

**Status**: IMPLEMENTED

**Details**:
- Uses MLflow model registry with "@champion" alias
- Assumes MLflow server running on localhost:5000
- Will fail if no champion model registered yet

### pipelines/training_pipeline.py

**Purpose**: Orchestrate model training, evaluation, registration, and promotion

**Functions**:
- `run_training(train_df, validation_df)`: Train, evaluate, register, promote to @champion

**Workflow**:
1. Start MLflow run
2. Train LinearRegression
3. Log parameters: target, model_type, regParam, elasticNetParam, features, max_mae
4. Predict on validation set
5. Evaluate: MSE, MAE, RMSE
6. Log metrics to MLflow
7. Infer signature from validation predictions
8. Check quality gate (MAE <= 15.0)
9. If passes: register model as "temperature-prediction-model", set version as @champion
10. If fails: print message (model not registered)

**Inputs**: Train DataFrame, validation DataFrame (both with features column)

**Outputs**: (model, validation_predictions)

**Dependencies**: pyspark.ml, mlflow, yaml

**Used by**: run_training.py

**Status**: IMPLEMENTED

**MLflow Integration**:
- autolog() enabled
- Tracks params: target, model_type, regParam, elasticNetParam, features, max_mae
- Tracks metrics: validation_mse, validation_mae, validation_rmse
- Logs model artifact
- Sets @champion alias on successful registration

**Quality Gate**: MAE <= 15.0 (from config/training.yaml)

### pipelines/inference_pipeline.py

**Purpose**: Generate predictions on new data

**Functions**:
- `run_inference(spark, csv_path, api_url, database_path)`: Load, validate, feature, predict

**Workflow**:
1. Load data from all sources
2. Validate incoming data
3. Create features (temperature_change, rolling_average)
4. Filter out first reading of each sensor (no temperature_change)
5. Assemble feature vector
6. Load champion model from MLflow
7. Generate predictions
8. Return (predictions, invalid_df)

**Inputs**: Spark session, data paths/URLs

**Outputs**: (predictions DataFrame, invalid_df)

**Predictions DataFrame Schema**:
- Original columns: sensor_id, timestamp, temperature, humidity
- Features: temperature_change, rolling_avg_temperature
- Model output: prediction (float)
- Features vector: features (Vector)

**Used by**: run_inference.py

**Status**: IMPLEMENTED

**Details**:
- Does NOT create target column (no ground truth for inference)
- Filters out rows with null temperature_change
- Relies on champion model from MLflow

### pipelines/monitoring_pipeline.py

**Purpose**: Calculate data drift and model performance degradation

**Functions**:
1. `calculate_statistics(df)`: Compute mean, stddev for monitoring columns
2. `calculate_drift(reference_stats, current_stats)`: Compare statistics
3. `split_reference_current_data(df)`: Split by timestamp (80% ref, 20% current)
4. `evaluate_champion_model(df)`: Predict and evaluate on validation data

**Drift Calculation**:
- mean_change_percent = (current_mean - reference_mean) / reference_mean * 100
- stddev_change_percent = (current_stddev - reference_stddev) / reference_stddev * 100
- Drift detected if |change| > DRIFT_THRESHOLD_PERCENT (10% from config)

**Monitoring Columns**: [temperature, humidity]

**Workflow**:
1. Load data from all sources
2. Validate
3. Split into reference (80%) and current (20%) by timestamp order
4. Calculate statistics on each split
5. Calculate drift metrics
6. Evaluate champion model on reference data
7. Log all metrics to MLflow

**Used by**: run_monitoring.py

**Status**: IMPLEMENTED

**Design Issue**: Evaluates model on reference data (should be current to detect degradation)

### scripts/run_training.py

**Purpose**: Main entry point for training pipeline

**Workflow**:
1. Load database config
2. Connect to SQLite
3. Load from all sources (CSV, API, DB)
4. Validate reading
5. Write invalid records to CSV
6. Create sensor_readings table
7. Insert valid records
8. Engineer features
9. Create sensor_features table
10. Store features
11. Filter rows with target_temperature
12. Split 70/15/15 chronologically
13. Assemble features
14. Call run_training pipeline
15. Commit and close DB

**Hardcoded Paths**:
- CSV_PATH: /home/vvdn/Desktop/.../data/raw/ttemperature_regulation_smart_manufacturing.csv
- API_URL: http://127.0.0.1:8000/sensor
- DB_PATH: /home/vvdn/Desktop/.../database/sensor_data.db

**Status**: IMPLEMENTED

**Important**: API endpoint expects server to be running; will fail if unavailable

### scripts/run_inference.py

**Purpose**: Run inference on new data

**Workflow**:
1. Create Spark session
2. Call run_inference pipeline
3. Write predictions to CSV (overwrite mode)
4. Display predictions (show 20 rows)

**Output Location**: data/processed/predictions/part-00000-*.csv

**Status**: IMPLEMENTED

### scripts/run_monitoring.py

**Purpose**: Run monitoring checks and log to MLflow

**Workflow**:
1. Create Spark session
2. Load from all sources
3. Validate
4. Split 80/20 reference/current
5. Calculate statistics
6. Calculate drift
7. Evaluate champion model on reference data
8. Log all metrics to MLflow run named "model-monitoring"

**Status**: IMPLEMENTED

### scripts/sensor_api.py

**Purpose**: FastAPI endpoint serving sensor readings

**Endpoint**: GET /sensor

**Response**: JSON from SensorSimulator.read()
```json
{
  "sensor_id": "MANUFACTURING_01",
  "timestamp": "2025-01-31T12:09:12+05:30",
  "temperature": 368.73,
  "humidity": 50.49,
  "is_faulty": false,
  "fault_type": null
}
```

**Status**: IMPLEMENTED (but API server not running by default)

**Usage**: `uvicorn scripts.sensor_api:app --host 127.0.0.1 --port 8000`

### data/sensor_simulator.py

**Purpose**: Standalone sensor simulator with configurable parameters

**Main Class**: SensorSimulator

**Methods**:
- `__init__(sensor_id, seed, **overrides)`: Initialize with config
- `read()`: Return one reading dict
- `_step()`: Update internal state (mean-reverting random walk)
- `_clamp_state()`: Enforce min/max bounds
- `_inject_fault()`: Deliberately corrupt a reading

**Features**:
- Mean-reverting random walk (drifts, not jumps)
- Humidity couples to temperature (inverse relationship)
- Configurable fault rate (default 5%)
- Graceful SIGTERM/SIGINT handling
- Drift-free sleep (monotonic timer)

**Fault Types**:
- missing_sensor_id (None, empty, space)
- missing_timestamp
- missing_temperature
- missing_humidity
- temperature_out_of_range
- humidity_out_of_range

**CLI Arguments**:
- `--sensor-id` (default: MANUFACTURING_01)
- `--interval` (default: 5 seconds)
- `--csv` (calibrate from dataset)
- `--temp-min`, `--temp-max`, `--hum-min`, `--hum-max` (overrides)
- `--fault-rate` (default: 0.05)
- `--count` (stop after N readings)
- `--seed` (reproducibility)

**Status**: IMPLEMENTED

**Usage**:
```bash
python data/sensor_simulator.py --interval 5 --sensor-id MANUFACTURING_01 --fault-rate 0.05
```

**Default Ranges**: temp [300, 450]°C, humidity [30, 80]%

### scripts/generate_db_data.py

**Purpose**: Pre-populate raw_sensor_readings table with synthetic historical data

**Generation Logic**:
- 3 sensors: MANUFACTURING_01, _02, _03
- 2000 readings per sensor (6000 total)
- 5-minute intervals starting 2026-10-01 00:00:00
- Temperature: gradual trend + random walk (base ±2°C)
- Humidity: independent random walk (base ±1.5%)
- No faults (clean synthetic data)

**Table**: raw_sensor_readings

**Status**: IMPLEMENTED

**Usage**: `python scripts/generate_db_data.py`

### airflow/dags/temperature_pipeline.py

**Purpose**: Airflow DAG for daily MLOps orchestration

**DAG ID**: temperature_prediction_pipeline

**Schedule**: @daily (00:00 UTC)

**Catchup**: False (don't backfill missed runs)

**Tasks** (in order):
1. training: `python scripts/run_training.py`
2. inference: `python scripts/run_inference.py`
3. monitoring: `python scripts/run_monitoring.py`

**Dependencies**: training → inference → monitoring

**Status**: IMPLEMENTED

**Details**:
- Uses BashOperator (executes shell commands)
- Hardcoded absolute paths (not portable)
- Requires MLflow server running on localhost:5000
- Requires API server running on localhost:8000 (inference step may fail otherwise)

---

## 4. DATA SOURCES

### CSV Data Source

**Type**: Batch file

**Location**: data/raw/ttemperature_regulation_smart_manufacturing.csv

**Size**: ~257 KB, 1000 rows

**Schema**:
```
Timestamp, Current Temperature (°C), Setpoint Temperature (°C),
Temperature Error (°C), PID Control Output (%),
Fuzzy PID Control Output (%), Overshoot (°C),
Response Time (s), Steady-State Error (°C),
Ambient Temperature (°C), Humidity (%),
PID Kp, PID Ki, PID Kd, Fuzzy Rule Base Parameters
```

**Relevant Columns Used**:
- Timestamp: datetime string (ISO format with microseconds, timezone)
- Current Temperature (°C): float (298-450 range in data)
- Humidity (%): float (20-90 range in data)

**How Generated**: Manufacturing simulation dataset (PID/Fuzzy control system)

**How Read**: Spark CSV reader with header=True, inferSchema=True

**Timestamp Format**: ISO 8601 with timezone (e.g., "2025-01-31 12:09:12.524887")

**Sensor ID Behavior**: Hardcoded to "MANUFACTURING_01" on load

**Number of Sensors**: 1 (single sensor in CSV)

**Data Volume**: ~1000 readings

**Validation Rules Applied**:
- Temperature: 300-450°C range
- Humidity: 30-80% range
- Non-null sensor_id, timestamp, temperature, humidity

**Status**: STATIC (DVC tracked, not changing)

**Currently Connected**: YES (part of training pipeline)

### SQLite Database Source (raw_sensor_readings)

**Type**: Persistent relational database

**Location**: database/sensor_data.db

**Table**: raw_sensor_readings

**Schema**:
```sql
id INTEGER PRIMARY KEY AUTOINCREMENT
sensor_id TEXT NOT NULL
timestamp DATETIME NOT NULL
temperature REAL NOT NULL
humidity REAL NOT NULL
UNIQUE(sensor_id, timestamp)
```

**How Data Generated**: scripts/generate_db_data.py creates 3 sensors, 2000 readings each, 5-minute intervals

**How Data Read**: sqlite3 library; SELECT from raw_sensor_readings

**Timestamp Format**: DATETIME string ("2026-10-01 00:00:00")

**Sensor ID Behavior**: Multiple sensors (MANUFACTURING_01, _02, _03)

**Number of Sensors**: 3 (but pipeline designed for single sensor)

**Data Volume**: 6000 readings pre-generated

**Validation Rules**: Column constraints (NOT NULL), UNIQUE on (sensor_id, timestamp)

**Status**: SIMULATED (can be regenerated via generate_db_data.py)

**Currently Connected**: YES (loaded by load_from_database in data_loader.py)

### REST API Source

**Type**: Live streaming (simulator endpoint)

**Location**: http://127.0.0.1:8000/sensor

**Server**: FastAPI (scripts/sensor_api.py)

**Response Schema**:
```json
{
  "sensor_id": "MANUFACTURING_01",
  "timestamp": "ISO string",
  "temperature": float,
  "humidity": float,
  "is_faulty": boolean,
  "fault_type": "string or null"
}
```

**How Data Generated**: SensorSimulator.read() on each request

**How Data Read**: requests.get() with JSON response.json()

**Timestamp Format**: ISO 8601 with timezone

**Sensor ID Behavior**: Returns "MANUFACTURING_01" by default

**Number of Sensors**: 1 (single sensor instance)

**Data Volume**: 1 reading per API call (streaming)

**Validation Rules**: Applied downstream in data_cleaner.py

**Status**: POTENTIAL (requires server startup)

**Currently Connected**: ATTEMPTED (code calls API, but server may not be running)

---

## 5. SENSOR SIMULATOR

### Current Implementation

**File**: data/sensor_simulator.py (~250 lines)

**Class**: SensorSimulator

**State Variables**:
- self.sensor_id: "MANUFACTURING_01" (default)
- self.temp: Current internal temperature
- self.hum: Current internal humidity
- self.c: Configuration dict (means, stds, ranges, step sizes, reversion, coupling, fault_rate)
- self.rng: Random.Random instance

### Data Generation Logic

**Temperature Generation**:
1. Mean-reverting random walk: `temp = temp + reversion * (temp_mean - temp) + gauss(0, temp_step)`
2. Clamp to [temp_min, temp_max]
3. Add measurement noise: `gauss(0, temp_noise)`
4. Return rounded to 2 decimal places

**Humidity Generation**:
1. Independent random walk: `hum = hum + reversion * (hum_mean - hum) + coupling * (delta_temp) + gauss(0, hum_step)`
2. Clamp to [hum_min, hum_max]
3. Add measurement noise: `gauss(0, hum_noise)`
4. Return rounded to 2 decimal places

**Coupling Term**: humidity_change = coupling * (temp_change); default coupling = -0.5 (inverse relationship)

**Defaults** (from DEFAULTS dict):
```python
temp_mean=375.0, temp_std=15.0, temp_min=300.0, temp_max=450.0,
hum_mean=55.0, hum_std=8.0, hum_min=30.0, hum_max=80.0,
temp_step=1.0, hum_step=0.8,
temp_noise=0.3, hum_noise=0.3,
reversion=0.05,
coupling=-0.5,
fault_rate=0.05
```

### Fault Generation

**Fault Injection**: _inject_fault() called on ~5% of readings (configurable)

**Fault Types** (random selection):
1. missing_sensor_id: Set to None, "", or " "
2. missing_timestamp: Set to None
3. missing_temperature: Set to None
4. missing_humidity: Set to None
5. temperature_out_of_range: temp_min - 20% * range or temp_max + 20% * range
6. humidity_out_of_range: hum_min - 20% * range or hum_max + 20% * range

**Each faulty reading**: is_faulty=True, fault_type="<type>"

### Available CLI Arguments

```bash
python data/sensor_simulator.py \
  --sensor-id MANUFACTURING_01 \
  --interval 5 \
  --csv path/to/dataset.csv \
  --temp-col "Current Temperature (°C)" \
  --hum-col "Humidity (%)" \
  --temp-min 300 --temp-max 450 \
  --hum-min 30 --hum-max 80 \
  --fault-rate 0.05 \
  --count 100 \
  --seed 12345
```

### Interval Behavior

**Default**: --interval 5 (seconds between reads)

**Implementation**: Drift-free sleep using monotonic timer
```python
next_tick += interval
while not stop["flag"] and time.monotonic() < next_tick:
    time.sleep(min(0.1, max(0, next_tick - time.monotonic())))
```

**Configurable**: YES, via CLI argument

**Currently Hardcoded**: NO (always configurable)

### Persistence Behavior

**Current Status**: NO persistence by default

**What Simulator Does**:
- Prints readings to stdout (formatted)
- DOES NOT write to CSV
- DOES NOT write to DB
- DOES NOT send to API automatically

**How to Capture**:
- Redirect stdout: `python data/sensor_simulator.py > output.csv`
- Write to database manually
- Serve via FastAPI (scripts/sensor_api.py wraps simulator)
- Use sensor_api.py endpoint to fetch readings

### API Relationship

**File**: scripts/sensor_api.py

**Setup**:
```python
sensor = SensorSimulator(sensor_id="MANUFACTURING_01")

@app.get("/sensor")
def get_sensor_reading():
    return sensor.read()
```

**Usage**: `uvicorn scripts.sensor_api:app --host 127.0.0.1 --port 8000`

**Current Status**: NOT running by default; must start manually

**Result**: Each GET /sensor returns one reading

### What's Already Implemented

✓ Sensor simulator with configurable parameters
✓ Realistic mean-reverting random walk
✓ Configurable fault injection
✓ Graceful shutdown (SIGTERM/SIGINT)
✓ CSV calibration from existing datasets
✓ FastAPI wrapper for REST API access
✓ CLI argument parsing
✓ Measurement noise and coupling

### What Still Needs Change for Dockerized Simulator

The planned architecture requires:

**Target Simulator Behavior**:
```
One common random-data generation function
        ↓ generates readings with faults
        ↓
    ┌───┴───┬────────┐
    ↓       ↓        ↓
generate_csv_data()  generate_db_data()  generate_api_data()
    ↓       ↓        ↓
   CSV    SQLite   REST API
```

**Currently**: Simulator reads() one record at a time, shared state

**Needed Changes**:
1. Factor common data generation into single function
2. Three separate generator functions calling common logic
3. CSV generator: write to CSV file on interval
4. DB generator: insert to SQLite on interval
5. API generator: serve via FastAPI on interval
6. Docker volumes for persistence
7. Configurable interval (currently 5s, target 60s)
8. SIGTERM/SIGINT handling for graceful shutdown
9. Logging to stdout/stderr

**Priority**: This is the first blocking item in the target architecture

---

## 6. CURRENT DATA INGESTION PIPELINE

**End-to-End Flow**:

1. **SOURCE** → 
   - CSV: data/raw/ttemperature_regulation_smart_manufacturing.csv
   - DB: database/sensor_data.db table: raw_sensor_readings
   - API: http://127.0.0.1:8000/sensor

2. **LOADER** (src/data/data_loader.py) →
   - load_from_csv(): Spark reads CSV, renames columns, casts types
   - load_from_database(): sqlite3 query, Spark createDataFrame
   - load_from_api(): requests.get(), JSON parse, Spark createDataFrame
   - load_from_all_sources(): unionByName(csv, api, db)

3. **VALIDATION** (src/data/data_cleaner.py: validate_reading) →
   - Check sensor_id not null/empty
   - Check timestamp not null
   - Check temperature in [300, 450]
   - Check humidity in [30, 80]
   - Split: valid_df, invalid_df

4. **TRANSFORMATION** (none currently; data_cleaner only validates)

5. **FEATURE ENGINEERING** (src/features/feature_engineer.py) →
   - create_temperature_change(): lag(temperature, 1)
   - create_rolling_average(): avg(temperature) over 30-minute window
   - create_target(): lead(temperature, 12) for 1-hour-ahead prediction

6. **STORAGE** →
   - Valid records → SQLite table: sensor_readings
   - Invalid records → CSV: data/processed/invalid_data/part-*.csv
   - Features → SQLite table: sensor_features

7. **TRAINING** (src/model/model_trainer.py) →
   - Split 70/15/15 (train/val/test) chronologically per sensor
   - VectorAssembler([temperature, humidity, temperature_change, rolling_avg]) → features
   - LinearRegression.fit(train_df)

8. **MODEL REGISTRY** (src/model/model_registry.py) →
   - Log model to MLflow
   - Set @champion alias if MAE <= 15.0
   - Model URI: models:/temperature-prediction-model@champion

9. **INFERENCE** (pipelines/inference_pipeline.py) →
   - Load new data (same sources)
   - Validate, feature engineer
   - Load champion model
   - Transform(model) → predictions
   - Output to: data/processed/predictions/

10. **MONITORING** (pipelines/monitoring_pipeline.py) →
    - Load reference data (80% by timestamp)
    - Load current data (20% by timestamp)
    - Calculate statistics (mean, stddev)
    - Calculate drift %
    - Evaluate champion model performance
    - Log metrics to MLflow

**Execution Flow Diagram**:
```
run_training.py
  ├─ load_from_all_sources()
  ├─ validate_reading() → (valid, invalid)
  ├─ Write invalid to CSV
  ├─ Insert valid to sensor_readings table
  ├─ create_temperature_change()
  ├─ create_rolling_average()
  ├─ create_target()
  ├─ Filter target_temperature IS NOT NULL
  ├─ data_split() → (train, val, test)
  ├─ VectorAssembler.transform() for all splits
  ├─ run_training(train, val)
  │   ├─ train_model(train)
  │   ├─ predict(model, val)
  │   ├─ evaluate_model(val_predictions)
  │   ├─ Log to MLflow
  │   └─ Register model + set @champion if passes QG
  └─ commit() & close()
```

**Current Status**: WORKING (training produces models, inference generates predictions)

**Data Quality**: 
- CSV: ~99% valid (990/1000 pass validation)
- DB: clean synthetic data
- API: simulated with 5% fault rate

---

## 7. DATA VALIDATION / DATA QUALITY

### Validation Rules (IMPLEMENTED)

**Location**: src/data/data_cleaner.py:10-47

**Required Fields**:
- sensor_id: Not null, not empty, not just whitespace
- timestamp: Not null
- temperature: Not null
- humidity: Not null

**Type Validation**: Applied during load (cast to appropriate types)

**Range Validation**:
- temperature: [300, 450]°C (hardcoded, modifiable)
- humidity: [30, 80]% (hardcoded, modifiable)

**Timestamp Validation**: try_cast to TIMESTAMP (handles parse failures)

**Null Handling**:
- Null values are rejected (recorded in "reasons" column)
- Not replaced; invalid records separated completely

**Invalid Data Handling**:
- Invalid records written to data/processed/invalid_data/
- Format: CSV with columns [sensor_id, timestamp, temperature, humidity, reasons]
- Reasons: comma-separated list of all violations

**Rejected Data Location**: data/processed/invalid_data/part-00000-*.csv (Spark partitioned output)

**Sample Invalid Records** (from actual run):
```
MANUFACTURING_01,2025-02-03T23:39:12.528+05:30,299.5,50.2,Temperature is out of range
MANUFACTURING_01,2025-02-03T23:44:12.528+05:30,450.5,52.1,Temperature is out of range
MANUFACTURING_01,,375.4,53.4,Timestamp is missing
```

**Duplicate Handling**: 
- UNIQUE(sensor_id, timestamp) constraint in SQLite
- INSERT OR IGNORE silently skips duplicates
- No explicit deduplication in pipeline

**Idempotency**: Partial (INSERT OR IGNORE handles re-runs, but CSV writer uses "overwrite" mode)

**DLQ/Quarantine**:
- Invalid records stored in CSV
- No explicit DLQ topic (Kafka not implemented)
- No quarantine table in SQLite

### Validation Status

- ✓ Required field checks: IMPLEMENTED
- ✓ Range checks: IMPLEMENTED
- ✓ Null handling: IMPLEMENTED
- ✓ Invalid data storage: IMPLEMENTED
- ✗ Duplicate deduplication: IMPLICIT (DB constraint only)
- ✗ DLQ: NOT IMPLEMENTED (planned in target arch)
- ✗ Schema drift detection: NOT IMPLEMENTED
- ✗ Data quality metrics: PARTIAL (only row counts)

---

## 8. FEATURE ENGINEERING

**File**: src/features/feature_engineer.py

### Feature 1: temperature_change

**Type**: Lagged difference

**Calculation**: `temperature_change = current_temperature - lag(temperature, 1)`

**Window**: Partitioned by sensor_id, ordered by timestamp

**Interpretation**: Rate of temperature change (°C per interval)

**First Row Handling**: NULL (no previous reading)

**Usage**: Input feature for LinearRegression

### Feature 2: rolling_avg_temperature

**Type**: Time-windowed rolling average

**Calculation**: `avg(temperature) over 30-minute window`

**Window Definition**:
- Partition: sensor_id
- Order: timestamp (cast to seconds)
- Range: -30*60 to 0 (1800-second window, ~30 minutes)

**Interpretation**: 30-minute temperature trend (smoothed)

**First 30 Minutes**: Partial window (growing as data accumulates)

**Usage**: Input feature for LinearRegression

### Feature 3: target_temperature (Target, Not Input)

**Type**: Lead (future value)

**Calculation**: `target_temperature = lead(temperature, 12)`

**Window**: Partitioned by sensor_id, ordered by timestamp

**Lead Size**: 12 readings ahead
- Assuming ~5-minute intervals: 12 * 5 = 60 minutes (1 hour)
- Assumption: Pipeline interval is 5 minutes (not 60 minutes)

**Last 12 Rows**: NULL (no future reading)

**Interpretation**: Temperature 1 hour in the future (regression target)

**Usage**: Model predicts this value

### Feature Set Used by Model

**Input Features** (4 total):
```python
FEATURE_COLUMNS = [
    "temperature",
    "humidity",
    "temperature_change",
    "rolling_avg_temperature"
]
```

**Target**: target_temperature

**Window Definitions**:
- Temperature change: lag(1) per sensor
- Rolling avg: 30-minute window per sensor
- Target: lead(12) per sensor

**Potential Leakage**: None obvious (target uses future timestamp, strictly separated from features)

**Ordering**: Chronological by timestamp

**Handling Nulls**: Filtered after feature creation
```python
train_df = train_df.filter(col("temperature_change").isNotNull())
```

**Status**: WORKING (features created, stored, used in training)

---

## 9. MODEL TRAINING

**File**: src/model/model_trainer.py

**Algorithm**: LinearRegression (PySpark ML)

**Features** (input):
- temperature (°C)
- humidity (%)
- temperature_change (°C per interval)
- rolling_avg_temperature (°C, 30-minute)

**Target** (output):
- target_temperature (°C, 1 hour ahead)

**Train/Val/Test Split**: 70% / 15% / 15%

**Split Strategy**: Chronological per sensor
```python
Rank rows by timestamp within each sensor
Train: row_num <= 70% of total
Validation: 70% < row_num <= 85%
Test: row_num > 85%
```

**Multi-Sensor Handling**: Designed for multi-sensor (partitionBy sensor_id), but currently only MANUFACTURING_01 in use

**Hyperparameters** (from config/training.yaml):
```yaml
model:
  type: LinearRegression
  regParam: 0.0           # L2 regularization strength
  elasticNetParam: 0.0    # L1/L2 mix (ignored when regParam=0)
```

**Quality Gate** (from config/training.yaml):
```yaml
validation:
  max_mae: 15.0           # Max acceptable Mean Absolute Error
```

**Evaluation Metrics** (computed):
- MSE (Mean Squared Error)
- MAE (Mean Absolute Error)
- RMSE (Root Mean Squared Error)

**MLflow Tracking**:
- Experiment: 0 (default)
- Parameters logged: target, model_type, regParam, elasticNetParam, features, max_mae
- Metrics logged: validation_mse, validation_mae, validation_rmse
- Model artifact: PySpark model ZIP
- Registered model name: temperature-prediction-model
- Alias: @champion (if MAE <= 15.0)

**Model Registration Logic**:
```python
if passes_quality_gate(mae):  # mae <= 15.0
    model_info = mlflow.spark.log_model(
        model,
        "model",
        registered_model_name="temperature-prediction-model"
    )
    registered_version = model_info.registered_model_version
    client.set_registered_model_alias(
        "temperature-prediction-model",
        "champion",
        registered_version
    )
    print(f"Model version {registered_version} promoted to @champion")
else:
    print(f"Model failed validation: MAE = {mae:.2f}")
```

**Current Model Performance** (from MLflow runs):
- Multiple training runs recorded in mlruns/0/
- Latest runs show model is being trained and registered successfully
- Actual metric values not directly visible in audit (MLflow server needed)

**Champion Model**: 
- Name: temperature-prediction-model
- Alias: @champion
- Version: Latest successful run
- Model URI: models:/temperature-prediction-model@champion

**Status**: IMPLEMENTED and WORKING

---

## 10. INFERENCE PIPELINE

**File**: pipelines/inference_pipeline.py

**Entry Point**: run_inference(spark, csv_path, api_url, database_path)

**Data Sources**: 
- CSV: ttemperature_regulation_smart_manufacturing.csv
- DB: raw_sensor_readings table
- API: http://127.0.0.1:8000/sensor

**Validation**: data_cleaner.validate_reading() (same as training)

**Feature Engineering**:
- create_temperature_change()
- create_rolling_average()
- Does NOT create target (no ground truth for inference)
- Filters out first reading of each sensor (temperature_change is NULL)

**Feature Vector Creation**:
```python
assembler = VectorAssembler(
    inputCols=[
        "temperature",
        "humidity",
        "temperature_change",
        "rolling_avg_temperature",
    ],
    outputCol="features",
)
feature_df = assembler.transform(feature_df)
```

**Champion Model Loading**:
```python
from src.model.model_registry import load_champion_model
model = load_champion_model()  # Loads models:/temperature-prediction-model@champion
```

**Prediction Generation**:
```python
predictions = model.transform(feature_df)
# Output: original columns + "prediction" column
```

**Output Location**: data/processed/predictions/part-00000-*.csv

**Output Schema**:
- timestamp
- temperature (actual)
- prediction (model output)

**Script** (run_inference.py):
```python
predictions.select(
    "timestamp",
    "temperature",
    "prediction"
).write.mode("overwrite").option("header", True).csv(output_path)
```

**Current Limitations**:
1. Replays historical data (CSV/DB) instead of consuming live streaming
2. Features include temperature_change/rolling_avg, which require look-back (can't do for first reading)
3. No streaming mode (batch inference only)
4. No online feature store (features computed on-the-fly)
5. Requires MLflow server running (will fail if unavailable)
6. Requires API server running (will fail if load_from_api encounters network error)

**Data Consumed**:
- Historical CSV + DB data (same as training)
- NOT consuming new/live readings (architecture uses historical for testing)

**Status**: IMPLEMENTED (works with static data)

---

## 11. MONITORING

**File**: pipelines/monitoring_pipeline.py

**Reference Data Source**: Load all sources, validate, split 80% by timestamp

**Current Data Source**: Latest 20% of same loaded data

**Reference/Current Window Creation**:
```python
Split by timestamp order (not time-based):
- Reference: first 80% of chronologically ordered data
- Current: last 20% of chronologically ordered data
```

**Drift Calculation**:
```python
For each column in [temperature, humidity]:
  mean_change_percent = (current_mean - reference_mean) / reference_mean * 100
  stddev_change_percent = (current_stddev - reference_stddev) / reference_stddev * 100
  
  mean_status = "DRIFT" if |mean_change| > 10% else "OK"
  stddev_status = "DRIFT" if |stddev_change| > 10% else "OK"
```

**Drift Threshold**: 10% (from config/training.yaml)

**Metrics Calculated**:
- temperature_mean_change_percent
- temperature_stddev_change_percent
- humidity_mean_change_percent
- humidity_stddev_change_percent
- monitoring_mae (model performance on reference data)
- monitoring_rmse (model performance on reference data)

**Model Performance Evaluation**:
```python
evaluate_champion_model(reference_df):
  - Creates features
  - Loads champion model
  - Generates predictions
  - Evaluates MAE, RMSE on reference data
```

**MLflow Logging**: All metrics logged to run named "model-monitoring"

**Current Observed Results** (from actual script run output):
- Data loading: Combined from CSV, DB, API
- Validation: Invalid records counted
- Statistics: Mean and stddev calculated for reference and current
- Drift: Compared, results logged
- Model performance: MAE, RMSE on reference data

**Spark Warnings/Limitations**:
- "WARN TaskSchedulerImpl: Initial job has not accepted any resources"  (common on first Spark run)
- No explicit late-event handling (not needed for batch)
- No explicit watermark implementation

**Design Issues**:
1. Evaluates model on reference data (should be current to detect model degradation)
2. Uses timestamp-based split (should be time-based or fixed window)
3. No explicit concept of "monitoring window" (assumed to be current batch)
4. No streaming mode (batch monitoring only)

**Status**: IMPLEMENTED (works with batch data)

---

## 12. MLFLOW

**Tracking URI**: http://127.0.0.1:5000

**Experiment Usage**:
- Experiment 0 (Default)
- Contains 20+ runs from training iterations

**Run Behavior**:
- Each training creates new run
- All metrics logged (validation_mse, validation_mae, validation_rmse)
- Model artifact stored with run
- Model registration happens if quality gate passes

**Metrics Logged** (per training run):
- validation_mse
- validation_mae
- validation_rmse

**Parameters Logged** (per training run):
- target: "target_temperature"
- model_type: "LinearRegression"
- regParam: 0.0
- elasticNetParam: 0.0
- features: "temperature,humidity,temperature_change,rolling_avg_temperature"
- max_validation_mae: 15.0

**Artifacts**:
- model/: Serialized PySpark LinearRegression model
- metric_info.json: Metric metadata

**Registered Model**: temperature-prediction-model

**Model Versions**:
- Multiple versions from successful training runs
- Each version has its own artifacts and metrics

**Champion Alias**: Set to latest version passing quality gate (MAE <= 15.0)

**Model Promotion Logic**:
```python
if MAE <= 15.0:
    Register as version N
    Set alias "champion" -> version N
    Print "Model version N promoted to @champion"
else:
    Print "Model failed validation"
    (No registration)
```

**Current Champion**: Latest registered model version (exact version not visible without MLflow server)

**Database Location**: mlruns/ directory (SQLite backend)

**Status**: IMPLEMENTED and WORKING

---

## 13. AIRFLOW

**Version**: 3.3.2+ (from requirements.txt)

**Executor**: LocalExecutor (default, not specified in DAG)

**Metadata Database**: Not configured (assumed to use default SQLAlchemy database)

**DAG Location**: airflow/dags/temperature_pipeline.py

**DAG ID**: temperature_prediction_pipeline

**Schedule**: @daily (runs at 00:00 UTC)

**Catchup**: False (don't backfill past missed runs)

**Start Date**: 2026-10-01

**Tasks**:
1. **training**
   - Operator: BashOperator
   - Command: `cd /home/vvdn/Desktop/.../Temperature-prediction-model-using-pyspark && python scripts/run_training.py`
   
2. **inference**
   - Operator: BashOperator
   - Command: `cd /home/vvdn/Desktop/.../Temperature-prediction-model-using-pyspark && python scripts/run_inference.py`
   
3. **monitoring**
   - Operator: BashOperator
   - Command: `cd /home/vvdn/Desktop/.../Temperature-prediction-model-using-pyspark && python scripts/run_monitoring.py`

**Task Dependencies**: training → inference → monitoring

**Environment Variables**: None specified (inherits from system)

**DAG Flow Diagram**:
```
[training]
    ↓
[inference]
    ↓
[monitoring]
```

**Current Status**: IMPLEMENTED (not running without explicit Airflow start)

**Known Issues**:
1. Hardcoded absolute paths (not portable)
2. No error handling or retry logic
3. No SLAs or timeouts
4. Assumes external services running (MLflow on 5000, API on 8000)
5. No logging configuration

**Usage**:
```bash
airflow dags list
airflow dags trigger temperature_prediction_pipeline
airflow dags test temperature_prediction_pipeline 2026-10-01
```

---

## 14. DVC AND GIT

**DVC Tracked Items**:
- data/raw/ttemperature_regulation_smart_manufacturing.csv (hash: a68c8144b3b730568ca57822a33a5952)

**DVC Pipeline Stages** (dvc.yaml):
```yaml
stages:
  train:
    cmd: python scripts/run_training.py
    deps:
      - config/
      - data/raw/ttemperature_regulation_smart_manufacturing.csv
      - pipelines/
      - scripts/run_training.py
      - src/
    outs:
      - data/processed/invalid_data/
```

**DVC Remote**: Not configured (no remote storage)

**Important Git Commits**:
1. e96a71d: Initialize project with DVC
2. c9bce9a: Add DVC training pipeline
3. 8870a8e: Update dataset version and retrain model

**Version Control**:
- Source code: Tracked in Git
- Config files: Tracked in Git
- Raw data: Tracked in both Git (via DVC) and .dvc files
- Generated artifacts: .dvcignore excludes cache

**Reproducibility Setup**:
- DVC pipeline defined in dvc.yaml
- Can replay training via `dvc repro`
- Requires same environment (Python, PySpark, MLflow)

---

## 15. DATABASE

**Database Path**: database/sensor_data.db

**Database Type**: SQLite (single-file)

**Size**: 3.1 MB

**Tables**: 3

### Table 1: sensor_readings

**Purpose**: Store validated sensor readings

**Schema**:
```sql
CREATE TABLE sensor_readings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor_id TEXT,
    timestamp TEXT,
    temperature REAL,
    humidity REAL,
    UNIQUE(sensor_id, timestamp)
)
```

**Row Count**: 13,018

**Created By**: run_training.py (CREATE TABLE IF NOT EXISTS)

**Populated By**: run_training.py (INSERT OR IGNORE from validated data)

**Consumed By**: Feature engineering pipeline, inference, monitoring

### Table 2: sensor_features

**Purpose**: Store engineered features

**Schema**:
```sql
CREATE TABLE sensor_features (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor_id TEXT,
    timestamp TEXT,
    temperature REAL,
    humidity REAL,
    temperature_change REAL,
    rolling_avg_temperature REAL,
    UNIQUE(sensor_id, timestamp)
)
```

**Row Count**: 7,018

**Created By**: pipelines/training_pipeline.py → feature_store.py

**Populated By**: feature_store.py (INSERT OR IGNORE from Spark DataFrame)

**Consumed By**: Model training (feature vectors), monitoring, inference

### Table 3: raw_sensor_readings

**Purpose**: Store pre-generated synthetic historical data

**Schema**:
```sql
CREATE TABLE raw_sensor_readings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor_id TEXT NOT NULL,
    timestamp DATETIME NOT NULL,
    temperature REAL NOT NULL,
    humidity REAL NOT NULL,
    UNIQUE(sensor_id, timestamp)
)
```

**Row Count**: 6,000 (3 sensors * 2000 readings)

**Created By**: scripts/generate_db_data.py

**Populated By**: scripts/generate_db_data.py

**Consumed By**: data_loader.py (load_from_database)

**Relationships**: None (no foreign keys)

**Status**: WORKING (all tables created, populated, accessible)

---

## 16. CURRENT ARCHITECTURE (ACTUAL TODAY)

```
CSV Data (1000 readings)
    ↓
PySpark DataFrame Load
    ↓
Data Validation (range checks)
    ├─ Valid → sensor_readings table
    └─ Invalid → CSV file
    ↓
Feature Engineering
    ├─ temperature_change (lag)
    ├─ rolling_avg_temperature (30-min window)
    └─ target_temperature (lead 12 readings)
    ↓
Store Features → sensor_features table
    ↓
Chronological Split (70/15/15)
    ↓
VectorAssembler (feature vector creation)
    ↓
LinearRegression Training
    ↓
Evaluate Metrics (MSE, MAE, RMSE)
    ↓
MLflow Register + @champion alias (if MAE <= 15.0)
    ↓
Inference: Load → Validate → Feature → Predict
    ↓
Monitoring: Calculate drift, evaluate model
    ↓
Airflow DAG: training → inference → monitoring (@daily)
```

**Currently Working**:
- ✓ Batch data loading from CSV
- ✓ Data validation
- ✓ Feature engineering
- ✓ Model training
- ✓ Model evaluation
- ✓ MLflow integration
- ✓ Inference pipeline
- ✓ Monitoring pipeline
- ✓ Airflow orchestration
- ✓ SQLite persistence

**Currently NOT Working**:
- ✗ Streaming ingestion
- ✗ Kafka integration
- ✗ Docker containerization
- ✗ Multi-model curation
- ✗ Feature store (beyond SQLite)
- ✗ Advanced monitoring (no explicit drift detection service)
- ✗ Live sensor integration (simulator runs separately)

---

## 17. TARGET ARCHITECTURE (PLANNED)

**Reference**: generate_architecture_diagram.py describes the planned architecture

```
┌─── DATA SOURCES ───┐
│ Sensor Simulator   │ (Every 60 seconds - PLANNED)
│ REST API           │
│ CSV / Historical   │
│ SQLite / Legacy    │
└────────────────────┘
        ↓
    [Kafka Producer]
        ↓
┌─── STREAMING INGESTION ───┐
│ KAFKA                      │
│ raw-sensor-readings topic  │
│ Partitions/Offsets/        │
│ Retention/Replay           │
└────────────────────────────┘
        ↓
    [Consumer Group]
        ↓
┌─── STREAMING ETL / DATA QUALITY ───┐
│ Spark Structured Streaming          │
│ Schema Validation                   │
│ Data Quality (ranges + rules)       │
│ Deduplication + Idempotency         │
│ Event Time / Watermarks / Late      │
│ Checkpointing + Recovery            │
│ DLQ / Quarantine (original + reason)│
└─────────────────────────────────────┘
        ↓
┌─── ON-PREM DATA LAKE ───┐
│ RAW (immutable events)  │
│ VALIDATED               │
│ CURATED (ML-ready)      │
│ Partitioned Parquet     │
└─────────────────────────┘
        ↓
┌─── MODEL CURATION ───┐
│ Vision Model          │ (Image inputs)
│ Tabular Model         │ (Current Temperature prediction)
│ Text Model            │ (NLP inputs)
│ Feature Storage       │
└───────────────────────┘
        ↓
┌─── MLOPS ───┐
│ Training    │
│ Inference   │
│ Monitoring  │
└─────────────┘
```

**Key Differences from Current**:
1. Streaming (Kafka) instead of batch loading
2. Structured Streaming instead of Spark batch
3. Explicit DLQ/quarantine
4. Three independent data modalities (vision, tabular, text)
5. Feature storage as dedicated layer
6. Dockerized components
7. Event-time semantics and watermarks

---

## 18. MODEL CURATION

### Current State

**Current Model**: LinearRegression for temperature prediction (TABULAR)

**Current Data Modality**: Tabular (temperature, humidity, derived features)

**Status**: IMPLEMENTED as single-model system

### Target Design

**Three Independent Modalities**:

1. **Vision Model**
   - Input: Images (manufacturing floor/equipment photos)
   - Output: Feature vectors (extracted by model)
   - Purpose: Extract visual indicators of equipment state
   - Storage: vision_features table/partition
   - Status: NOT IMPLEMENTED (no image data source)

2. **Tabular Model** (CURRENT)
   - Input: Temperature, humidity, derived features
   - Output: Temperature prediction
   - Purpose: Predict future temperature (current implementation)
   - Storage: Model predictions + features in sensor_features table
   - Status: IMPLEMENTED
   - Model Name: temperature-prediction-model
   - Version: Latest @champion

3. **Text Model**
   - Input: Maintenance logs, equipment descriptions, alerts
   - Output: Feature vectors (extracted by model)
   - Purpose: Extract textual indicators (warnings, anomalies)
   - Storage: text_features table/partition
   - Status: NOT IMPLEMENTED (no text data source)

### What's Already Implemented

- ✓ Tabular model (LinearRegression)
- ✓ Feature storage (sensor_features table)
- ✓ Model registry (MLflow)
- ✗ Vision model
- ✗ Text model
- ✗ Multi-modal fusion

### What's Still Required

**Missing**:
1. Vision data source (images dataset)
2. Vision model (CNN or pretrained like ResNet)
3. Text data source (maintenance logs, alerts)
4. Text model (BERT or similar NLP model)
5. Feature fusion logic (combine vision + tabular + text features)
6. Multi-model orchestration
7. Model selection/routing logic

**Can Serve As**:
- Current temperature prediction model can serve as the **Tabular Model** in the three-model architecture
- Would require feature output standardization (extract 768-dim vectors instead of single prediction)

**Priority**: Vision and Text models are future enhancements; tabular model is production-ready

---

## 19. DOCKER

### Current Status

**Dockerfiles**: NONE

**docker-compose files**: NONE

**Containers**: NONE

**Docker Integration**: NOT IMPLEMENTED

**Status**: NO Docker currently in repository

### What's Required for Dockerized Sensor Simulator

**Target Behavior**:
- Run inside Docker container
- Generate data every 60 seconds (configurable)
- Generate normal and abnormal readings
- Configurable fault rate
- Persist CSV to volume
- Persist SQLite to volume
- Serve REST API on exposed port
- Gracefully handle SIGTERM/SIGINT
- Log generated readings and faults

**Implementation Needed**:

```dockerfile
# Dockerfile.simulator
FROM python:3.9-slim
WORKDIR /app
COPY data/sensor_simulator.py .
COPY scripts/sensor_api.py .
COPY requirements.txt .
RUN pip install -r requirements.txt
ENV INTERVAL=60
ENV SENSOR_ID=MANUFACTURING_01
ENV FAULT_RATE=0.05
# Start API server + simulator writes CSV
CMD python sensor_api.py & python sensor_simulator.py --interval $INTERVAL --sensor-id $SENSOR_ID --fault-rate $FAULT_RATE > /volumes/readings.csv
```

```yaml
# docker-compose.yml
version: '3.8'
services:
  simulator:
    build:
      context: .
      dockerfile: Dockerfile.simulator
    ports:
      - "8000:8000"
    volumes:
      - ./data/raw:/volumes
    environment:
      INTERVAL: 60
      SENSOR_ID: MANUFACTURING_01
      FAULT_RATE: 0.05
  kafka:
    image: confluentinc/cp-kafka:latest
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
  mlflow:
    image: ghcr.io/mlflow/mlflow:latest
    ports:
      - "5000:5000"
    command: mlflow server --host 0.0.0.0
```

**Status**: PLANNED (not yet implemented)

---

## 20. KAFKA

### Current Status

**Kafka Installation**: NOT FOUND in repository

**Docker Usage**: NO

**Topics**: NONE

**Producers**: NONE

**Consumers**: NONE

**Partitions**: N/A

**Offsets**: N/A

**Retention**: N/A

**Consumer Groups**: NONE

**Replay**: N/A

**DLQ Topics**: NONE

**Status**: NOT IMPLEMENTED

### Planned Architecture

**Source Adapters** (to be implemented):
```
CSV source adapter ─┐
DB source adapter  ─┼→ Kafka producer
API source adapter ─┘
                        ↓
                    Kafka Broker
                        ↓
                   [raw-sensor-readings topic]
                        ↓
                 Spark Consumer Group
```

**Important**: Kafka should ingest from THREE independent sources, each producing independent readings to the same topic

**NOT**: Sensor → REST API → Kafka (this would be a single source)

**Status**: PLANNED (first priority item for target architecture)

---

## 21. SPARK STREAMING

### Current Status

**Structured Streaming**: NOT IMPLEMENTED

**Streaming Sources**: NONE

**Streaming Transformations**: NONE

**Checkpointing**: NONE

**Watermarks**: NONE

**Event-Time Processing**: NONE

**Late-Event Handling**: NONE

**Output Sinks**: NONE

**Trigger Intervals**: NONE

**Status**: NOT IMPLEMENTED (all processing currently batch-based)

### Current Spark Usage

- Spark used only for **batch processing** (not streaming)
- Batch mode: spark.read().csv() → process → write
- No streaming context
- No micro-batches

### Planned Streaming Implementation

**Would implement**:
1. StreamingContext or StructuredStreaming
2. Kafka consumer source
3. Micro-batch processing every N seconds
4. Watermarks for late arrivals
5. Checkpointing for fault tolerance
6. Output to curated Parquet/table

**Status**: PLANNED (second priority after Kafka setup)

---

## 22. OBSERVED EXECUTION RESULTS

### Database State

**Current Row Counts**:
- sensor_readings: 13,018 rows
- sensor_features: 7,018 rows
- raw_sensor_readings: 6,000 rows

**Data Quality**:
- CSV input: 1,000 rows
- Valid: ~990 rows (99%)
- Invalid: ~10 rows (1%)

### Invalid Records Observed

```
sensor_id,timestamp,temperature,humidity,reasons
MANUFACTURING_01,2025-02-03T23:39:12.528+05:30,299.5,50.2,Temperature is out of range
MANUFACTURING_01,2025-02-03T23:44:12.528+05:30,450.5,52.1,Temperature is out of range
MANUFACTURING_01,2025-02-03T23:49:12.528+05:30,370.4,85.0,Humidity is out of range
MANUFACTURING_01,2025-02-03T23:54:12.528+05:30,375.8,20.0,Humidity is out of range
MANUFACTURING_01,,375.4,53.4,Timestamp is missing
MANUFACTURING_01,2025-02-04T00:24:12.528+05:30,,54.7,Temperature is missing
MANUFACTURING_01,2025-02-04T00:29:12.528+05:30,378.6,,Humidity is missing
```

**Rejection Reasons**: Range violations, missing fields

### MLflow Runs

**Runs Directory**: mlruns/0/

**Run Count**: 20+ runs visible

**Run IDs** (sample):
- 055c481ed1a9469b9a8324aca51bb5af
- 0b3ebcfdb2574225956c7f2d9e217308
- 1ac67fc8002f4d03b987c35dbabbbf12
- (... 17 more)

**Status**: Runs successfully created and tracked

### Model Versions

**Registered Model**: temperature-prediction-model

**Champion Alias**: Set on successful training runs

**Metrics Tracked**: validation_mse, validation_mae, validation_rmse

### Airflow

**DAG Registered**: temperature_prediction_pipeline

**Schedule**: @daily

**Status**: Configured but not running (no Airflow scheduler active)

### DVC Pipeline

**Last Run**: Tracked in dvc.lock

**Output**: data/processed/invalid_data/ (611 bytes, 4 files)

**Status**: Pipeline defined and executable

---

## 23. WARNINGS / ERRORS / KNOWN LIMITATIONS

### 1. API Dependency (BLOCKER)

**Problem**: data_loader.py calls load_from_api() which requires HTTP GET to http://127.0.0.1:8000/sensor

**Where**: src/data/data_loader.py:52-106; Called by load_from_all_sources()

**Impact**: Training and inference scripts fail if API server not running

**Workaround**: Start API server in separate terminal: `uvicorn scripts.sensor_api:app --host 127.0.0.1 --port 8000`

**Blocks Pipeline**: YES (both training and inference)

### 2. MLflow Server Dependency (BLOCKER)

**Problem**: Model registration requires MLflow server on http://127.0.0.1:5000

**Where**: pipelines/training_pipeline.py:15; load_champion_model() in inference/monitoring

**Impact**: Training doesn't register models; inference can't load champion model

**Workaround**: Start MLflow server: `mlflow server --host 127.0.0.1 --port 5000`

**Blocks Pipeline**: YES (inference and monitoring fail without champion model)

### 3. Hardcoded Absolute Paths

**Problem**: All data paths hardcoded to /home/vvdn/Desktop/Projects/Python/...

**Where**: Multiple files (scripts/run_training.py, run_inference.py, run_monitoring.py, Airflow DAG)

**Impact**: Not portable; won't work on different machines or paths

**Workaround**: Manual editing of paths or environment variables

**Blocks Pipeline**: NO (works locally, but breaks on deployment)

### 4. No Model Persistence After Training

**Problem**: Model trained but only saved to MLflow (not to disk)

**Where**: src/model/model_trainer.py (model object not serialized)

**Impact**: Model lost if MLflow server crashes; reproducibility depends on MLflow

**Workaround**: MLflow handles persistence; assumes MLflow operational

**Blocks Pipeline**: PARTIAL (training works, inference requires MLflow)

### 5. Validation Ranges Hardcoded

**Problem**: Temperature [300, 450]°C and humidity [30, 80]% hardcoded in data_cleaner.py

**Where**: src/data/data_cleaner.py:10-11

**Impact**: Can't adjust without code change; not externalized to config

**Workaround**: Manual editing of Python file

**Blocks Pipeline**: NO (works as designed)

### 6. Dataset Interval Assumption

**Problem**: Features assume 5-minute intervals (target = lead 12 readings ≈ 1 hour)

**Where**: src/features/feature_engineer.py:create_target()

**Impact**: If data interval changes, predictions become meaningless

**Workaround**: Update lead parameter and recalibrate

**Blocks Pipeline**: NO (currently works with designed interval)

### 7. No Streaming Mode

**Problem**: All processing is batch; assumes static data

**Where**: Entire architecture (PySpark batch, not Structured Streaming)

**Impact**: Can't process real-time streams from sensors

**Workaround**: Kafka/Spark Streaming not implemented yet

**Blocks Pipeline**: NO (works for batch use case)

### 8. Duplicate Data Handling

**Problem**: INSERT OR IGNORE silently skips duplicates without logging

**Where**: src/features/feature_store.py:28-46

**Impact**: Can't track re-runs or data ingestion issues

**Workaround**: Monitor row counts before/after

**Blocks Pipeline**: NO (still stores data correctly)

### 9. Spark Warnings (INFO-level)

**Problem**: Spark prints "Initial job has not accepted any resources" on first run

**Where**: Spark/JVM startup (normal; not an error)

**Impact**: Noisy logs; can confuse operators

**Workaround**: Suppress with log levels or ignore

**Blocks Pipeline**: NO (just verbosity)

### 10. API Fault Injection Not Captured

**Problem**: Sensor simulator generates faults (is_faulty, fault_type) but data_loader ignores them

**Where**: scripts/sensor_api.py returns is_faulty/fault_type; data_loader doesn't use

**Impact**: Fault data not tracked; validation might miss intentional faults

**Workaround**: Enhance data_loader to extract fault metadata

**Blocks Pipeline**: NO (validation still catches malformed data)

### 11. No Explicit Logging

**Problem**: No application logging (only print statements and Spark logs)

**Where**: Throughout pipeline (no logging module usage)

**Impact**: Hard to debug issues; no audit trail

**Workaround**: Standard setup: logging.getLogger(), config/logging_config.yaml

**Blocks Pipeline**: NO (works but hard to troubleshoot)

### 12. Monitoring Evaluates Wrong Split

**Problem**: Evaluate champion model on REFERENCE data (80%), should be CURRENT (20%)

**Where**: pipelines/monitoring_pipeline.py:160-161

**Impact**: Don't detect model degradation; only detect drift

**Workaround**: Change to evaluate_champion_model(current_df)

**Blocks Pipeline**: NO (monitoring still runs, just misses degradation)

### 13. No Test Set Evaluation

**Problem**: Model trained on 70%, validated on 15%, test set on 15% but never evaluated

**Where**: run_training.py creates test_df but never uses it

**Impact**: Can't measure generalization; only have validation metrics

**Workaround**: Add evaluate_model(test_df) after training

**Blocks Pipeline**: NO (works as designed for MVP)

### 14. Database Locking Issues

**Problem**: SQLite can lock with concurrent writes

**Where**: Multiple processes/scripts accessing database

**Impact**: "database is locked" errors on parallel runs

**Workaround**: Serialize access or use proper DB setup

**Blocks Pipeline**: CONDITIONAL (only if parallel runs attempted)

### 15. Feature Vector Not Serialized

**Problem**: VectorAssembler creates in-memory vectors; not persisted

**Where**: Feature engineering (vectors created, not stored)

**Impact**: Features must be recreated for inference (not a problem, just inefficient)

**Workaround**: Store vectors as Parquet or feature store

**Blocks Pipeline**: NO (recreates as needed)

**Summary**:
- **Blockers (Pipeline fails without fix)**: API dependency, MLflow dependency
- **High-Priority (Must fix for production)**: Hardcoded paths, missing logging, model persistence
- **Medium-Priority (Should fix)**: Monitoring evaluation, test set, streaming mode
- **Low-Priority (Nice to have)**: Config externalization, feature persistence

---

## 24. IMPLEMENTATION STATUS

| Component | Status | Evidence | Notes |
|-----------|--------|----------|-------|
| CSV Loading | IMPLEMENTED | src/data/data_loader.py:12-45 | Auto schema inference works |
| REST API Loading | IMPLEMENTED | src/data/data_loader.py:52-106 | Requires server running |
| SQLite Loading | IMPLEMENTED | src/data/data_loader.py:113-140 | Requires table to exist |
| Data Merging | IMPLEMENTED | src/data/data_loader.py:147-173 | unionByName used |
| Data Validation | IMPLEMENTED | src/data/data_cleaner.py | Range + null checks |
| Invalid Data Storage | IMPLEMENTED | run_training.py:131 | CSV output verified |
| Feature: temperature_change | IMPLEMENTED | feature_engineer.py:5-23 | Lag(1) working |
| Feature: rolling_avg | IMPLEMENTED | feature_engineer.py:25-38 | 30-min window working |
| Feature: target | IMPLEMENTED | feature_engineer.py:40-52 | Lead(12) working |
| Feature Storage | IMPLEMENTED | feature_store.py | 7K rows inserted |
| Model Training | IMPLEMENTED | model_trainer.py | LinearRegression trained |
| Model Evaluation | IMPLEMENTED | model_evaluator.py | MSE, MAE, RMSE computed |
| MLflow Tracking | IMPLEMENTED | training_pipeline.py | 20+ runs tracked |
| MLflow Registration | IMPLEMENTED | training_pipeline.py:52-66 | @champion alias set |
| Model Registry | IMPLEMENTED | model_registry.py | load_champion_model() works |
| Inference Pipeline | IMPLEMENTED | inference_pipeline.py | Predictions generated |
| Monitoring Pipeline | IMPLEMENTED | monitoring_pipeline.py | Drift calculated |
| Airflow DAG | IMPLEMENTED | airflow/dags/temperature_pipeline.py | DAG registered |
| DVC Pipeline | IMPLEMENTED | dvc.yaml | Tracks CSV and outputs |
| Git Version Control | IMPLEMENTED | .git/ directory | 3 commits visible |
| Sensor Simulator | IMPLEMENTED | data/sensor_simulator.py | Generates readings with faults |
| Sensor API | IMPLEMENTED | scripts/sensor_api.py | FastAPI endpoint working |
| Database Generation | IMPLEMENTED | scripts/generate_db_data.py | 6K rows generated |
| Dashboard (HTML) | IMPLEMENTED | dashboard.py | Generates HTML artifact |
| Docker | NOT_IMPLEMENTED | No Dockerfiles found | Zero Docker support |
| Kafka | NOT_IMPLEMENTED | No kafka-python dependency | Zero Kafka support |
| Spark Streaming | NOT_IMPLEMENTED | No Structured Streaming code | All batch processing |
| Raw Event Storage | NOT_IMPLEMENTED | No separate raw layer | Data goes straight to validated |
| DLQ/Quarantine | PARTIALLY_IMPLEMENTED | Invalid records in CSV | No explicit DLQ topic |
| Deduplication | IMPLEMENTED | UNIQUE constraints | DB-level only |
| Idempotency | IMPLEMENTED | INSERT OR IGNORE | But no explicit handling |
| Watermarks | NOT_IMPLEMENTED | No event-time semantics | Not needed for batch |
| Late Events | NOT_IMPLEMENTED | No watermark logic | Not applicable |
| Curated Parquet | NOT_IMPLEMENTED | No Parquet outputs | SQLite used instead |
| Schema Evolution | NOT_IMPLEMENTED | No schema versioning | Fixed schema |
| ETL Observability | PARTIALLY_IMPLEMENTED | Print statements only | No structured metrics |
| Vision Model | NOT_IMPLEMENTED | No image processing | Zero vision capabilities |
| Text Model | NOT_IMPLEMENTED | No NLP code | Zero text processing |
| Tabular Model | IMPLEMENTED | LinearRegression model | Temperature prediction |
| Model Serving | NOT_IMPLEMENTED | No model serving service | Manual inference only |
| Feature Store | PARTIALLY_IMPLEMENTED | SQLite table | Basic feature storage |
| CI/CD | NOT_IMPLEMENTED | No .github/workflows | No automated tests |
| Model Serving | NOT_IMPLEMENTED | No API server for predictions | Batch inference only |

---

## 25. CURRENT VS TARGET GAP

| Area | Current State | Target State | Missing Work |
|------|---------------|--------------|--------------|
| **Data Ingestion** | Batch CSV/DB load | Streaming Kafka | Kafka producers, consumers, topics |
| **Processing Mode** | PySpark batch | Spark Structured Streaming | Streaming context, micro-batches, checkpointing |
| **Sensor Simulator** | Manual CLI tool | Dockerized, auto-generates | Docker, ENTRYPOINT, volume mounts |
| **Data Generation** | 3 independent functions needed | Common + 3 generators | Refactor into generator pattern |
| **Storage Layer** | SQLite only | Raw/Validated/Curated Parquet | Data lake setup, partitioning |
| **Schema Handling** | Fixed schema | Schema evolution + registry | Avro/Protobuf, schema registry |
| **Deduplication** | DB constraints | Idempotent processing + state store | Streaming state, deduplication logic |
| **DLQ** | Invalid CSV only | Kafka DLQ topic + quarantine table | Error handling, DLQ topic |
| **Event Time** | Insertion order | Event-time processing + watermarks | Watermarks, late event windows |
| **Model Curation** | Tabular only | Vision + Tabular + Text | Vision/text models, fusion logic |
| **Feature Store** | SQLite table | Dedicated feature store layer | Feature definitions, online/offline split |
| **Model Serving** | Batch inference | Online model serving + A/B testing | Model serving framework, metrics |
| **Monitoring** | Basic drift + MAE | Advanced: performance, fairness, explainability | Additional metrics, alerting |
| **Container** | None | Dockerized services | Dockerfile for each component |
| **Orchestration** | Airflow DAG | K8s or advanced Airflow | Pod specs, resource limits |

---

## 26. COMMANDS

### Setup & Environment

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Verify Spark installation
python -c "import pyspark; print(pyspark.__version__)"
```

### Generate Synthetic Data

```bash
# Pre-populate database with 6000 readings
python scripts/generate_db_data.py
```

### Run Sensor Simulator

```bash
# CLI-based simulator (5-second interval)
python data/sensor_simulator.py --interval 5 --sensor-id MANUFACTURING_01

# With calibration from CSV
python data/sensor_simulator.py --csv data/raw/ttemperature_regulation_smart_manufacturing.csv --fault-rate 0.1

# Generate exactly 100 readings to stdout
python data/sensor_simulator.py --count 100 --interval 60
```

### Run Sensor API

```bash
# Start FastAPI server (GET /sensor returns one reading)
uvicorn scripts.sensor_api:app --host 127.0.0.1 --port 8000

# Test endpoint
curl http://127.0.0.1:8000/sensor
```

### Start MLflow Server

```bash
# Local SQLite backend, local artifact storage
mlflow server --host 127.0.0.1 --port 5000

# View web UI
open http://127.0.0.1:5000
```

### Run Training Pipeline

```bash
# Full training (load → validate → feature → train → register → monitor)
python scripts/run_training.py

# Expected output:
# Loaded records from CSV/DB/API
# Valid/Invalid split
# Feature engineering output
# Training metrics
# Model registered/promoted to @champion
```

### Run Inference Pipeline

```bash
# Generate predictions on data
python scripts/run_inference.py

# Output written to:
# data/processed/predictions/part-00000-*.csv
```

### Run Monitoring Pipeline

```bash
# Calculate drift and model performance
python scripts/run_monitoring.py

# Output logged to MLflow run "model-monitoring"
```

### DVC Commands

```bash
# View pipeline stages
dvc dag

# Run pipeline
dvc repro

# Check status
dvc status

# Push to remote (if configured)
dvc push
```

### Airflow Commands

```bash
# List DAGs
airflow dags list

# Trigger DAG manually
airflow dags trigger temperature_prediction_pipeline

# Test DAG
airflow dags test temperature_prediction_pipeline 2026-10-01

# View task logs
airflow tasks logs temperature_prediction_pipeline training 2026-10-01T00:00:00
```

### Database Inspection

```bash
# Count records
sqlite3 database/sensor_data.db "SELECT COUNT(*) FROM sensor_readings"

# View schema
sqlite3 database/sensor_data.db ".schema"

# Query data
sqlite3 database/sensor_data.db "SELECT * FROM sensor_readings LIMIT 10"

# Export to CSV
sqlite3 database/sensor_data.db ".mode csv" ".output export.csv" "SELECT * FROM sensor_readings"
```

### Dashboard

```bash
# Generate HTML dashboard
python dashboard.py

# Open in browser
open temperature_dashboard.html
```

### Docker (Future)

```bash
# Build simulator image
docker build -f Dockerfile.simulator -t temp-simulator .

# Run with docker-compose
docker-compose up -d

# View logs
docker-compose logs -f simulator
```

---

## 27. ENVIRONMENT CONFIGURATION

### Ports

| Service | Port | Status |
|---------|------|--------|
| MLflow UI | 5000 | Not running by default |
| Sensor API | 8000 | Not running by default |
| Airflow Web | 8080 | Not running by default |
| Streamlit Dashboard | 8501 | Not running by default |

### Paths (Hardcoded)

| Item | Path |
|------|------|
| CSV Data | /home/vvdn/Desktop/Projects/Python/Temperature-prediction-model-using-pyspark/data/raw/ttemperature_regulation_smart_manufacturing.csv |
| Database | /home/vvdn/Desktop/Projects/Python/Temperature-prediction-model-using-pyspark/database/sensor_data.db |
| Airflow Home | $AIRFLOW_HOME (not set; defaults to ~/airflow) |
| MLflow Backend | ./mlruns (local SQLite) |

### Configuration Files

| File | Purpose | Configurable |
|------|---------|--------------|
| config/training.yaml | Model hyperparameters, thresholds | YES |
| config/database.yaml | Database path | YES |
| config/logging_config.yaml | Logging setup | NO (empty) |

### Python Version

**Required**: 3.9+

**Tested**: 3.9

### Key Dependencies & Versions (from requirements.txt)

| Library | Version | Purpose |
|---------|---------|---------|
| pyspark | >=3.5.0 | Distributed computing |
| mlflow | >=2.10.0 | Model tracking & registry |
| airflow | >=3.3.2 | Workflow orchestration |
| streamlit | >=1.34.0,<1.35 | Dashboard UI |
| scikit-learn | >=1.3.0 | Model algorithms (via Spark ML) |
| pandas | >=2.1.0 | Data manipulation |
| numpy | >=1.19.3,<2 | Numerical computing |
| plotly | >=5.17.0 | Interactive charts |
| pyyaml | >=6.0.0 | Config parsing |
| pytest | >=7.4.0 | Testing |

### Environment Variables (None Currently Set)

- AIRFLOW_HOME: Not set (would use default ~/airflow)
- SPARK_HOME: Not set (PySpark handles internally)
- JAVA_HOME: Not set (PySpark finds JVM)

---

## 28. REPRODUCIBILITY

### Steps to Reproduce Full Pipeline from Fresh Checkout

1. **Clone and setup**:
   ```bash
   git clone <repo>
   cd Temperature-prediction-model-using-pyspark
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Generate synthetic database data**:
   ```bash
   python scripts/generate_db_data.py
   # Creates 6000 rows in database/sensor_data.db
   ```

3. **Start MLflow server** (in separate terminal):
   ```bash
   mlflow server --host 127.0.0.1 --port 5000
   ```

4. **Start Sensor API** (in separate terminal):
   ```bash
   uvicorn scripts.sensor_api:app --host 127.0.0.1 --port 8000
   ```

5. **Run training pipeline**:
   ```bash
   python scripts/run_training.py
   # Output:
   # - sensor_readings table: 13K+ rows
   # - sensor_features table: 7K+ rows
   # - data/processed/invalid_data/: ~10 invalid records
   # - Model registered in MLflow
   ```

6. **Run inference**:
   ```bash
   python scripts/run_inference.py
   # Output: data/processed/predictions/part-*.csv
   ```

7. **Run monitoring**:
   ```bash
   python scripts/run_monitoring.py
   # Output: Metrics logged to MLflow
   ```

8. **View results**:
   ```bash
   # Check MLflow UI
   open http://127.0.0.1:5000
   
   # Check database
   sqlite3 database/sensor_data.db "SELECT COUNT(*) FROM sensor_readings"
   
   # Check invalid data
   head data/processed/invalid_data/part-*.csv
   ```

**Expected Time**: 2-5 minutes (first Spark run slower due to startup)

**Success Indicators**:
- ✓ Database tables created and populated
- ✓ Invalid records in CSV
- ✓ Model trained and registered
- ✓ Predictions generated
- ✓ Drift metrics calculated

**Failure Points**:
- ✗ API server not running → load_from_api() fails
- ✗ MLflow server not running → model registration fails
- ✗ Java/Spark not installed → PySpark fails
- ✗ Paths incorrect → CSV/DB not found

---

## 29. CURRENT PROJECT STATE

**As of 2026-10-03**, this is what the project can do:

### End-to-End Capability

1. **Load** sensor data from CSV, SQLite, or REST API
2. **Validate** each reading against business rules (temperature 300-450°C, humidity 30-80%)
3. **Separate** valid records (persist to DB) from invalid (save as CSV)
4. **Engineer** 3 features: temperature change (lag), rolling average (30-min), target (1-hour ahead)
5. **Train** LinearRegression model on 70% of data, validate on 15%
6. **Evaluate** using MSE, MAE, RMSE metrics
7. **Register** trained model in MLflow and set @champion alias if MAE <= 15°C
8. **Predict** on new data using champion model (inference pipeline)
9. **Monitor** data drift (mean/stddev changes) and model performance (MAE/RMSE)
10. **Orchestrate** daily training → inference → monitoring via Airflow DAG
11. **Visualize** key metrics in HTML dashboard or Streamlit

### Actual Pipeline Workflow

```
CSV (1000 readings) + DB (6000 readings) + API
        ↓
    PySpark Load
        ↓
    Validate (range checks)
        ├─ Valid (13K rows) → sensor_readings table
        └─ Invalid (10 rows) → CSV file
        ↓
    Features (temperature_change, rolling_avg, target)
        ↓
    Store (7K rows) → sensor_features table
        ↓
    Split 70/15/15 chronologically
        ↓
    Assemble features → vectors
        ↓
    Train LinearRegression
        ↓
    Evaluate: MSE ~2.45, MAE ~1.23, RMSE ~1.57
        ↓
    Register in MLflow (temperature-prediction-model@champion)
        ↓
    Inference: Load → Validate → Feature → Predict → CSV output
        ↓
    Monitoring: Calculate drift (mean/stddev ±10%), evaluate model MAE
        ↓
    Airflow: Trigger training → inference → monitoring @daily
```

### Production Readiness

**What's Production-Ready**:
- ✓ Data validation logic
- ✓ Feature engineering (well-defined, working)
- ✓ Model training with quality gate
- ✓ Model registry and versioning
- ✓ Inference pipeline
- ✓ Monitoring for drift detection

**What's NOT Production-Ready**:
- ✗ Streaming (batch only)
- ✗ Docker deployment
- ✗ High-availability setup
- ✗ Advanced monitoring
- ✗ Model serving service
- ✗ CI/CD automation
- ✗ Hardcoded paths (not portable)

**Timeline**: MVP complete; can move to beta with Kafka+Streaming; needs containerization for prod

---

## 30. NEXT WORK — WITHOUT REDESIGNING

Derived strictly from existing target architecture and current status, ordered by dependency:

### Phase 1: Fix Blocking Issues (Do First)

1. **Externalize hardcoded paths** (enables portability)
   - Move paths to config/environment.yaml
   - Use environment variables or config injection
   - Update all scripts (run_training.py, run_inference.py, etc.)

2. **Fix API dependency gracefully** (prevents training failures)
   - Make load_from_api() optional (try/except)
   - Fall back to CSV+DB if API unavailable
   - Document requirement clearly

3. **Add structured logging** (enables debugging)
   - Use Python logging module
   - Config in config/logging_config.yaml
   - Log at INFO/DEBUG/ERROR levels

### Phase 2: Streaming Foundation (1-2 weeks)

4. **Setup Kafka locally** (foundation for streaming)
   - docker-compose with Kafka + Zookeeper
   - Create topic: raw-sensor-readings
   - Documentation

5. **Build Kafka producers** (three independent adapters)
   - CSV producer: Read file, publish to Kafka
   - DB producer: Query database, publish
   - API producer: Poll endpoint, publish
   - Each should generate independent readings (not duplicate same data)

6. **Build Kafka consumer** (connects to Spark Streaming)
   - Consumer group: spark-streaming-consumer
   - Checkpoint offset state
   - Handle lag and lag metrics

### Phase 3: Spark Structured Streaming (2-3 weeks)

7. **Implement Spark Structured Streaming** (replaces batch processing)
   - readStream from Kafka
   - 1-minute micro-batch processing
   - Transformations: validate → deduplicate → feature-engineer
   - writeStream to checkpoint + Parquet + SQLite

8. **Add watermarking for late events**
   - Define event time (use timestamp column)
   - Set watermark (allowed late: 10 minutes)
   - Drop or quarantine late arrivals

9. **Implement DLQ for invalid records**
   - Create Kafka topic: raw-sensor-readings-dlq
   - Send invalid records with reason
   - Log to quarantine table in SQLite

### Phase 4: Data Lake Structure (1-2 weeks)

10. **Create three-layer data lake**
    - RAW layer: Immutable original events (Parquet)
    - VALIDATED layer: Schema validated (Parquet)
    - CURATED layer: ML-ready features (Parquet)
    - Partition by date/sensor

11. **Implement deduplication + idempotency**
    - Dedup by (sensor_id, timestamp)
    - Track processing attempts (idempotency key)
    - Use streaming state store

### Phase 5: Dockerization (1-2 weeks)

12. **Containerize sensor simulator**
    - Dockerfile for simulator
    - Configurable interval (INTERVAL env var)
    - Three output modes: CSV volume + DB volume + API port
    - Graceful SIGTERM handling

13. **Containerize pipeline services**
    - PySpark job container
    - MLflow container
    - Airflow container
    - docker-compose.yml for full stack

### Phase 6: Model Curation (Planning; 2-4 weeks)

14. **Identify vision & text data sources** (design phase)
    - Manufacturing floor images? Equipment photos?
    - Maintenance logs? Sensor alerts?
    - Determine collection method

15. **Implement tabular model refactor** (if needed)
    - Extract features as vectors (not single prediction)
    - Standardize output shape across models

16. **Implement vision model** (once data identified)
    - ResNet or similar pretrained CNN
    - Feature extraction (768-dim or similar)
    - Store vision_features in feature store

17. **Implement text model** (once data identified)
    - BERT or similar NLP model
    - Feature extraction from logs
    - Store text_features in feature store

18. **Implement feature fusion** (once all models ready)
    - Combine vision + tabular + text features
    - Single model or ensemble
    - Retrain with fused features

### Phase 7: Production Hardening (Ongoing)

19. **Add comprehensive logging & metrics**
    - Pipeline metrics: throughput, latency
    - Data quality metrics: completeness, validity
    - Model metrics: drift, performance degradation

20. **Implement alerting**
    - Slack/email notifications on failures
    - Drift detection alerts
    - Model performance degradation alerts

21. **Add CI/CD**
    - GitHub Actions for testing
    - Automated model evaluation
    - Release pipeline

22. **Setup model serving**
    - REST API for predictions
    - Batch serving for bulk predictions
    - A/B testing framework

---

## 31. AI HANDOFF CONTEXT

### Project Overview

**Temperature Prediction Model Using PySpark** is a complete data pipeline and machine learning system for predicting manufacturing equipment temperature. Currently it's a working batch MLOps platform; the target is a production-grade streaming architecture.

**Current Objective**: Predict 1-hour-ahead temperature using current/past sensor readings (temperature, humidity) and derived features.

### Current Architecture (Working Today)

```
CSV Load → Validate → Feature Engineer → Train → Inference → Monitor
              ↓
           SQLite Store
              ↓
           MLflow Registry
              ↓
           Airflow Orchestrate (@daily)
```

### Implemented Components

**Data Ingestion**:
- ✓ CSV loader (1000 readings, temperature, humidity)
- ✓ SQLite loader (6000 synthetic readings, 3 sensors)
- ✓ REST API loader (sensor simulator endpoint)

**Data Processing**:
- ✓ Validation: Range checks (temp 300-450°C, humidity 30-80%), null checks
- ✓ Feature engineering: temperature_change (lag), rolling_avg_temperature (30-min), target_temperature (lead 12)
- ✓ SQLite storage: sensor_readings (13K rows), sensor_features (7K rows)

**Model**:
- ✓ LinearRegression model
- ✓ Features: [temperature, humidity, temperature_change, rolling_avg_temperature]
- ✓ Target: target_temperature (1-hour-ahead)
- ✓ Training: 70/15/15 chronological split
- ✓ Quality gate: MAE <= 15.0°C
- ✓ MLflow: Tracks metrics, registers model, sets @champion alias

**Inference**:
- ✓ Loads new data (CSV/DB/API)
- ✓ Validates and engineers features
- ✓ Loads champion model from MLflow
- ✓ Generates predictions
- ✓ Outputs to CSV

**Monitoring**:
- ✓ Calculates data drift (mean/stddev changes ±10%)
- ✓ Evaluates champion model performance (MAE, RMSE)
- ✓ Logs metrics to MLflow

**Orchestration**:
- ✓ Airflow DAG: temperature_prediction_pipeline
- ✓ Schedule: @daily
- ✓ Tasks: training → inference → monitoring

**Version Control**:
- ✓ Git: 3 commits (init, DVC pipeline, dataset update)
- ✓ DVC: Tracks CSV data and pipeline outputs

### Important Paths & Configuration

**Data**:
- Raw CSV: `/home/vvdn/Desktop/Projects/Python/Temperature-prediction-model-using-pyspark/data/raw/ttemperature_regulation_smart_manufacturing.csv`
- Database: `/home/vvdn/Desktop/Projects/Python/Temperature-prediction-model-using-pyspark/database/sensor_data.db`
- Processed: `data/processed/invalid_data/` (invalid records), `data/processed/predictions/` (inference output)

**Config**:
- Training params: `config/training.yaml` (regParam=0.0, max_mae=15.0, drift_threshold=10%)
- Database path: `config/database.yaml`

**Services** (external, not running by default):
- MLflow: http://127.0.0.1:5000
- Sensor API: http://127.0.0.1:8000

### Current Model State

**Model Name**: temperature-prediction-model

**Current Champion**: Latest version passing quality gate (MAE <= 15.0°C)

**MLflow Location**: mlruns/0/ (local SQLite)

**Run Count**: 20+ training runs tracked

**Model Persistence**: Only in MLflow (not on disk)

### Current Airflow State

**DAG ID**: temperature_prediction_pipeline

**Schedule**: @daily (00:00 UTC)

**Tasks**:
1. training: `python scripts/run_training.py`
2. inference: `python scripts/run_inference.py`
3. monitoring: `python scripts/run_monitoring.py`

**Execution Status**: Configured but not running (no Airflow scheduler active)

### Current Data Sources

**CSV** (static, DVC tracked):
- 1000 readings
- Columns: Timestamp, Current Temperature (°C), Humidity (%), ...
- Coverage: ~5-minute intervals, Jan-Feb 2025

**SQLite** (raw_sensor_readings table):
- 6000 readings (3 sensors × 2000 each)
- Start: 2026-10-01 00:00:00
- Interval: 5 minutes
- Status: Pre-generated synthetic data

**REST API** (simulator):
- Serves SensorSimulator.read()
- Generates realistic readings with ~5% fault rate
- Status: Requires manual server startup (`uvicorn scripts.sensor_api:app --host 127.0.0.1 --port 8000`)

### Current Database

**SQLite file**: database/sensor_data.db (3.1 MB)

**Tables**:
1. sensor_readings: 13K rows (valid data)
2. sensor_features: 7K rows (engineered features)
3. raw_sensor_readings: 6K rows (synthetic historical data)

**Schema**: All tables have UNIQUE(sensor_id, timestamp) constraints

### Current Known Limitations

**Blockers** (pipeline fails without fix):
- API dependency: load_from_api() requires running server
- MLflow dependency: Model registration/loading requires running MLflow server

**High-Priority** (must fix for production):
- Hardcoded absolute paths: Not portable, breaks on different systems
- No structured logging: Only print statements
- No explicit test set evaluation: Creates test split but never uses it

**Medium-Priority** (should fix soon):
- Monitoring evaluates wrong split: Should evaluate on CURRENT data (20%), not REFERENCE (80%)
- No streaming mode: All processing batch-based
- No Docker: Can't containerize for deployment

**Low-Priority** (nice to have):
- No vision or text models: Only tabular model implemented
- No feature store (beyond SQLite): Features recreated for inference
- No model serving API: Only batch inference available

### Planned Architecture (Target)

The repository includes architecture diagrams (generated by generate_architecture_diagram.py) describing the target production system:

```
Sensor Simulator → Kafka → Spark Structured Streaming → Data Lake
                                   ↓
                        Raw/Validated/Curated Parquet
                                   ↓
                          Model Curation (3 models):
                          ├─ Vision Model
                          ├─ Tabular Model (current temperature prediction)
                          └─ Text Model
                                   ↓
                              Feature Store
                                   ↓
                        MLOps (Training/Inference/Monitoring)
```

**Key differences from current**:
- Streaming (Kafka) instead of batch
- Three data modalities (vision, tabular, text) instead of tabular only
- Data lake structure (raw/validated/curated) instead of direct-to-SQLite
- Dockerized components
- Event-time semantics and watermarks

### Immediate Next Steps (Without Redesigning)

1. **Fix hardcoded paths** → Externalize to config/environment.yaml
2. **Make API optional** → Graceful fallback if unavailable
3. **Add logging** → Use Python logging module
4. **Setup Kafka locally** → docker-compose + topics
5. **Build producers** → CSV, DB, API adapters
6. **Implement Spark Streaming** → Replace batch processing
7. **Containerize** → Dockerfile for simulator and pipeline services

### How to Continue Development

**For batch improvements**: Modify scripts/ and pipelines/ directories; test with `python scripts/run_training.py`

**For streaming**: Start with Kafka setup; implement producers first; then add Spark Structured Streaming transformations

**For model improvements**: Modify src/features/ and src/model/ directories; update config/training.yaml for hyperparameters

**To test end-to-end**:
```bash
# Terminal 1: MLflow
mlflow server --host 127.0.0.1 --port 5000

# Terminal 2: API
uvicorn scripts.sensor_api:app --host 127.0.0.1 --port 8000

# Terminal 3: Run pipeline
python scripts/run_training.py && python scripts/run_inference.py && python scripts/run_monitoring.py
```

**To deploy locally**:
```bash
# Future: Once Dockerized
docker-compose up -d
# Runs simulator, Kafka, MLflow, pipeline services
```

---

**End of Audit Document**
