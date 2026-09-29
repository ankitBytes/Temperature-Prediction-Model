# Complete Project Structure

## Directory Tree

```
temperature-prediction-pyspark/
├── config/                           # Configuration files
│   ├── __init__.py                  # Python package init
│   ├── config.yaml                  # Main config (empty)
│   ├── database.yaml                # Database configuration
│   └── logging_config.yaml          # Logging config (empty)
│
├── data/                             # Data directory (input/output)
│   ├── raw/                         # Raw input data
│   │   └── ttemperature_regulation_smart_manufacturing.csv
│   ├── processed/                   # Processed data output
│   │   ├── valid_data/
│   │   │   └── sensor_data.db      # SQLite database
│   │   └── invalid_data/
│   │       ├── part-00000-*.csv    # Invalid records (CSV)
│   │       └── _SUCCESS             # Spark completion marker
│   └── features/                    # Engineered features (empty)
│
├── docs/                             # Documentation (this folder)
│   ├── README.md                    # Documentation index
│   ├── 01-project-overview.md
│   ├── 02-technology-stack.md
│   ├── 03-architecture.md
│   ├── 04-project-structure.md
│   └── ... (more documentation)
│
├── logs/                             # Log files (empty, not implemented)
│
├── models/                           # Trained models (empty)
│
├── pipelines/                        # Pipeline orchestration
│   ├── __init__.py
│   ├── training_pipeline.py         # Training pipeline
│   ├── inference_pipeline.py        # Inference (not implemented)
│   └── monitoring_pipeline.py       # Monitoring (not implemented)
│
├── scripts/                          # Executable scripts
│   ├── __init__.py
│   ├── run_training.py              # Main training script
│   ├── run_inference.py             # Inference script (not implemented)
│   └── run_monitoring.py            # Monitoring script (not implemented)
│
├── src/                              # Source code (main application)
│   ├── __init__.py
│   ├── data/                        # Data loading & validation
│   │   ├── __init__.py
│   │   ├── data_loader.py           # Load CSV and transform
│   │   ├── data_cleaner.py          # Validate data
│   │   └── data_validator.py        # (empty, validation in cleaner)
│   │
│   ├── features/                    # Feature engineering
│   │   ├── __init__.py
│   │   ├── feature_engineer.py      # Create features
│   │   └── feature_store.py         # Store features in SQLite
│   │
│   ├── model/                       # Model training & evaluation
│   │   ├── __init__.py
│   │   ├── model_trainer.py         # Train LinearRegression
│   │   ├── model_evaluator.py       # Evaluate model performance
│   │   └── model_registry.py        # (empty, model persistence)
│   │
│   ├── inference/                   # Inference (not implemented)
│   │   ├── __init__.py
│   │   └── predictor.py             # (empty)
│   │
│   └── utils/                       # Utility functions
│       ├── __init__.py
│       ├── database.py              # Database utilities (empty)
│       ├── logger.py                # Logging setup (empty)
│       └── metrics.py               # Metrics utilities (empty)
│
├── tests/                            # Unit tests (not implemented)
│   ├── __init__.py
│   ├── test_data_loader.py          # (empty)
│   ├── test_data_validator.py       # (empty)
│   ├── test_data_cleaner.py         # (would test validation)
│   ├── test_feature_engineer.py     # (empty)
│   ├── test_model_trainer.py        # (empty)
│   └── test_predictor.py            # (empty)
│
├── .vscode/                          # VS Code settings
│   └── settings.json
│
├── temperature_prediction_pyspark.egg-info/  # Package metadata
│   ├── SOURCES.txt
│   ├── requires.txt
│   └── ...
│
├── setup.py                          # Package setup & installation
├── requirements.txt                  # Python dependencies
└── README.md                         # Project README (empty)

```

## Directory Descriptions

### `config/` - Configuration Files

**Purpose**: Centralized configuration storage

**Contents**:
- `database.yaml` - Database connection details (SQLite path, table names)
- `config.yaml` - Main configuration (empty, ready for expansion)
- `logging_config.yaml` - Logging setup (empty)

**Read by**: `run_training.py` uses `database.yaml` to determine database location

**Important**: Paths are absolute, not configurable per environment

---

### `data/` - Input and Output Data

**Purpose**: Store raw data, processed results, and intermediate outputs

**Subdirectories**:

#### `data/raw/`
- **Contains**: `ttemperature_regulation_smart_manufacturing.csv`
- **Purpose**: Original sensor data from manufacturing facility
- **Format**: CSV with headers and 1000+ readings
- **Frequency**: Static for this project (would be updated regularly in production)

#### `data/processed/valid_data/`
- **Contains**: `sensor_data.db`
- **Purpose**: SQLite database with validated sensor readings and engineered features
- **Tables**:
  - `sensor_readings`: Valid sensor data (5 columns)
  - `sensor_features`: Engineered features (7 columns)
- **Access**: Python sqlite3 module

#### `data/processed/invalid_data/`
- **Contains**: Invalid records as CSV files
- **Files**:
  - `part-00000-*.csv`: Actual data (Spark creates multiple partition files)
  - `_SUCCESS`: Marker file indicating Spark job completed
  - `._*.crc`: CRC checksum files (Spark internal)
- **Purpose**: Records that failed validation (for debugging and data quality monitoring)
- **Columns**: sensor_id, timestamp, temperature, humidity, reasons (rejection reasons)

#### `data/features/`
- **Purpose**: Placeholder for extracted features (currently empty)
- **Would contain**: CSV dumps of engineered features if export functionality added

---

### `docs/` - Documentation

**Purpose**: Comprehensive project documentation

**Contents**: Markdown files explaining:
- Project purpose and overview
- Technology stack
- Architecture and data flow
- Installation and setup
- Running the pipeline
- Troubleshooting
- Development guidelines

**For new developers**: Start here to understand the project

---

### `logs/` - Log Files

**Purpose**: Store application logs and audit trails

**Status**: Currently empty (logging not implemented)

**Would contain**: 
- `run_training.log`: Training pipeline logs
- `run_inference.log`: Inference logs
- `run_monitoring.log`: Monitoring logs
- Debug information and errors

---

### `models/` - Trained Models

**Purpose**: Store trained machine learning models

**Status**: Currently empty

**Would contain**:
- `model_v1.pkl`: Serialized LinearRegression model
- `model_metadata.json`: Model info (training date, metrics, etc.)

**Current limitation**: Model trained in memory, not saved to disk

---

### `pipelines/` - Pipeline Orchestration

**Purpose**: Define processing pipelines and workflows

**Files**:

#### `training_pipeline.py`
```python
def run_training(train_df, validation_df):
    # 1. Train model on training data
    model = train_model(train_df)
    
    # 2. Make predictions on validation data
    predictions = predict(model, validation_df)
    
    # 3. Evaluate performance
    mse, mae = evaluate_model(predictions)
    
    # 4. Print metrics
    print(f"MSE: {mse}, MAE: {mae}")
    
    return model, predictions
```

**Role**: Coordinates model training workflow

**Called by**: `run_training.py`

#### `inference_pipeline.py`
**Status**: Not implemented (empty file)

**Would do**: Make predictions on new sensor data using trained model

#### `monitoring_pipeline.py`
**Status**: Not implemented (empty file)

**Would do**: Track model performance metrics over time, detect drift

---

### `scripts/` - Executable Entry Points

**Purpose**: Scripts users run directly to execute pipelines

**Files**:

#### `run_training.py` - MAIN SCRIPT
**Lines of code**: 215

**Responsibilities**:
1. Load database configuration from YAML
2. Establish SQLite connection
3. Load raw CSV data into Spark DataFrame
4. Validate sensor readings (separate valid/invalid)
5. Write invalid data to CSV
6. Write valid data to database
7. Engineer features (temperature change, rolling avg, target)
8. Store engineered features in database
9. Split data chronologically (70/15/15)
10. Assemble features into vectors
11. Train LinearRegression model
12. Evaluate model on validation set
13. Print metrics
14. Close database connection
15. Stop Spark session

**Key sections**:
- Lines 22-30: Configuration loading
- Lines 33-121: data_split() function
- Lines 123-130: Spark initialization
- Lines 134-140: Data loading & validation
- Lines 143-166: Database operations
- Lines 169-193: Feature engineering
- Lines 195-204: Model training & evaluation

**Status**: ✓ Fully implemented and functional

#### `run_inference.py`
**Status**: Empty (not implemented)

**Would do**:
- Load trained model
- Load new sensor data
- Make predictions
- Write predictions to database/file

#### `run_monitoring.py`
**Status**: Empty (not implemented)

**Would do**:
- Compare recent predictions to actual values
- Calculate performance metrics
- Detect model drift
- Alert if metrics degrade

---

### `src/` - Source Code (Core Application Logic)

**Purpose**: Main application code organized by functionality

#### `src/data/` - Data Loading & Validation

##### `data_loader.py`
**Lines of code**: 36
**Key function**: `load_sensor_data(spark)`

**Steps**:
1. Read CSV with schema inference
2. Add sensor_id column (hardcoded value)
3. Cast Timestamp to TIMESTAMP type
4. Cast temperature to double
5. Cast humidity to double
6. Select only required columns

**Input**: CSV file path (hardcoded)

**Output**: Spark DataFrame with columns: sensor_id, timestamp, temperature, humidity

**Status**: ✓ Fully implemented

##### `data_cleaner.py`
**Lines of code**: 78
**Key function**: `validate_reading(df)`

**Validation rules**:
- sensor_id not null and not empty
- timestamp not null
- temperature not null
- temperature in range [300, 450]
- humidity not null
- humidity in range [30, 80]

**Output**: 
- `valid_df`: Records with no violations (reasons column dropped)
- `invalid_df`: Records with violations (reasons column included)

**Status**: ✓ Fully implemented

##### `data_validator.py`
**Status**: Empty (validation is in data_cleaner.py)

---

#### `src/features/` - Feature Engineering

##### `feature_engineer.py`
**Lines of code**: 53
**Key functions**:
- `create_temperature_change(df)` - Add temperature_change column
- `create_rolling_average(df)` - Add rolling_avg_temperature column
- `create_target(df)` - Add target_temperature column

**Window functions used**:
- `partitionBy("sensor_id")` - Separate data per sensor
- `orderBy("timestamp")` - Process in time order
- `lag("temperature", 1)` - Get previous value
- `avg("temperature")` - Calculate average
- `lead("temperature", 12)` - Get future value (12 readings ahead)
- `rangeBetween(-30*60, 0)` - Look back 30 minutes

**Status**: ✓ Fully implemented

##### `feature_store.py`
**Lines of code**: 74
**Key functions**:
- `create_feature_table(connection)` - Create SQLite table
- `store_features(connection, feature_df)` - Insert features
- `check_features(connection)` - Query and print features

**SQL table**:
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

**Status**: ✓ Fully implemented

---

#### `src/model/` - Model Training & Evaluation

##### `model_trainer.py`
**Lines of code**: 30
**Key functions**:
- `train_model(train_df)` - Fit LinearRegression model
- `predict(model, data_df)` - Make predictions

**Features used**:
- temperature
- humidity
- temperature_change
- rolling_avg_temperature

**Target variable**: target_temperature

**Model type**: LinearRegression (pyspark.ml.regression)

**Status**: ✓ Fully implemented

##### `model_evaluator.py`
**Lines of code**: 37 (incomplete)
**Key functions**:
- `evaluate_model(predictions_df)` - Calculate MSE and MAE
- `calculate_correlations(df)` - (incomplete)

**Metrics calculated**:
- MSE (Mean Squared Error)
- MAE (Mean Absolute Error)
- Average actual temperature
- Average predicted temperature

**Status**: Partial (correlations function incomplete)

##### `model_registry.py`
**Status**: Empty (not implemented)

**Would do**:
- Save trained model to disk
- Load saved models
- Track model versions
- Manage model metadata

---

#### `src/inference/` - Prediction Pipeline

##### `predictor.py`
**Status**: Empty (not implemented)

**Would do**:
- Load trained model
- Make predictions on new data
- Format predictions for output

---

#### `src/utils/` - Utility Functions

##### `database.py`
**Status**: Empty

**Could provide**:
- Database connection management
- Query builders
- Transaction management
- Connection pooling

**Currently**: Database operations inline in run_training.py

##### `logger.py`
**Status**: Empty

**Could provide**:
- Logging configuration
- Named loggers for each module
- Log level control
- Log file setup

##### `metrics.py`
**Status**: Empty

**Could provide**:
- Custom metric calculations
- Metric reporting
- Performance summaries

---

### `tests/` - Unit Tests

**Purpose**: Automated test suite for quality assurance

**Status**: All test files exist but are empty

**Test files**:
- `test_data_loader.py` - Tests for data loading
- `test_data_validator.py` - Tests for validation
- `test_feature_engineer.py` - Tests for feature creation
- `test_model_trainer.py` - Tests for model training
- `test_predictor.py` - Tests for predictions

**Would contain**: pytest test functions to verify each component works correctly

**Status**: ✗ Not implemented (no test code written)

---

### Root Level Files

#### `setup.py`
**Purpose**: Package installation and metadata

**Defines**:
- Package name: temperature-prediction-pyspark
- Version: 0.1.0
- Author: Data Science Team
- Python version: >=3.9
- Core dependencies (pyspark, pandas, etc.)
- Development dependencies (pytest, matplotlib, etc.)

**Usage**: `pip install -e .` to install package in development mode

#### `requirements.txt`
**Purpose**: List of Python packages needed

**Contains**:
- pyspark>=3.5.0
- numpy>=2.0.0
- pandas>=2.1.0
- scikit-learn>=1.3.0
- matplotlib>=3.8.0
- seaborn>=0.13.0
- pyyaml>=6.0.0
- python-dotenv>=1.0.0
- pytest>=7.4.0
- pytest-cov>=4.1.0

**Usage**: `pip install -r requirements.txt`

#### `README.md`
**Status**: Empty (you're reading the docs instead!)

---

## File Dependencies

### Import Graph
```
run_training.py
├── data_loader.py (load_sensor_data)
├── data_cleaner.py (validate_reading)
├── feature_engineer.py (create_*)
├── feature_store.py (create/store features)
├── training_pipeline.py
│   ├── model_trainer.py (train/predict)
│   └── model_evaluator.py (evaluate)
└── sqlite3 + yaml (external)
```

### Spark Session
- Created once in `run_training.py`
- Used by data_loader, data_cleaner, feature_engineer
- Stopped at end of run_training.py

### Database Connection
- Created once in `run_training.py`
- Used by feature_store
- Closed at end of run_training.py

---

## Data File Locations

| File | Path | Purpose | Format | Size |
|------|------|---------|--------|------|
| Raw data | `data/raw/ttemperature_regulation_smart_manufacturing.csv` | Input | CSV | ~500KB |
| Valid data DB | `data/processed/valid_data/sensor_data.db` | Output | SQLite3 | ~50KB |
| Invalid records | `data/processed/invalid_data/part-00000-*.csv` | Output | CSV | ~1KB |

---

## Configuration Flow

```
config/
├── database.yaml (SQLite path)
│   ↓
│   ↓ (yaml.safe_load)
│   ↓
run_training.py
│   ↓
│   ↓ (config["database"]["path"])
│   ↓
sqlite3.connect(path)
```

---

## Next Steps

- To understand what each file does, read [File-by-File Documentation](./05-data-ingestion.md)
- To see data movement, read [Data Flow](./08-data-flow.md)
- To set up and run, read [Setup Guide](./12-setup-guide.md)

---

**Key Takeaway**: The project is organized as: `scripts/` for entry points → `pipelines/` for orchestration → `src/` for implementation → `data/` for files → `config/` for settings → `tests/` for verification.
