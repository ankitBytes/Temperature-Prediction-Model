# Current Limitations

## Overview

This document identifies known gaps and limitations in the current implementation. Understanding these helps set realistic expectations and plan future improvements.

## Data Pipeline Limitations

### 1. Single Sensor Only

**Current**: Hardcoded sensor ID "MANUFACTURING_01"

**Location**: `src/data/data_loader.py` line 15
```python
.withColumn("sensor_id", lit("MANUFACTURING_01"))
```

**Impact**:
- Can't process data from multiple manufacturing facilities
- All records tagged with same sensor ID
- No way to distinguish between sources

**How to Fix**:
- Extract sensor_id from CSV
- Or make it a parameter
- Or read from configuration

**Workaround**: Process each sensor separately with different CSV files

### 2. Absolute File Paths

**Current**: Hardcoded absolute paths throughout codebase

**Locations**:
- `src/data/data_loader.py` line 8: CSV path
- `config/database.yaml`: Database path
- `scripts/run_training.py` line 131: Invalid data path

**Impact**:
- Fails when project moved to different directory
- Different paths needed per user
- Not portable across machines

**How to Fix**:
- Use relative paths with `os.path.join()`
- Move all paths to configuration file

**Example**:
```python
# ✗ Current (wrong)
csv_path = "/home/user/project/data/raw/sensors.csv"

# ✓ Better (relative)
import os
csv_path = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "sensors.csv")

# ✓ Best (configurable)
config = yaml.safe_load(open("config/config.yaml"))
csv_path = config["data"]["raw_csv_path"]
```

### 3. Hardcoded Configuration Values

**Current**: Many settings hardcoded in Python code

**Locations**:
- Temperature range: `src/data/data_cleaner.py` line 10
- Humidity range: `src/data/data_cleaner.py` line 11
- Rolling window: `src/features/feature_engineer.py` line 30
- Look-ahead: `src/features/feature_engineer.py` line 50
- Split ratios: `scripts/run_training.py` lines 44-45

**Impact**:
- Must edit code to change behavior
- No way to A/B test different configurations
- Configuration not version controlled separately

**How to Fix**:
- Move all to `config/config.yaml`
- Load configuration at startup
- Document all available options

## Model Limitations

### 4. Model Not Persisted

**Current**: Trained model only in memory during execution

**Issue**: Model is lost after pipeline completes

**Impact**:
- Can't reuse model for inference
- Must retrain for every prediction
- No model versioning
- Can't compare models over time

**Example of Problem**:
```python
# Run 1
model = train_model(train_df)  # Model created

# Program ends
# Model is gone!

# Next day, need predictions
# Must retrain entire model from scratch
```

**How to Fix**:
```python
# Save model
model.save("models/temperature_model_v1")

# Load later
from pyspark.ml.regression import LinearRegressionModel
model = LinearRegressionModel.load("models/temperature_model_v1")

# Use for predictions
predictions = model.transform(new_data)
```

### 5. No Model Versioning

**Current**: No tracking of different model versions

**Impact**:
- Can't compare performance across versions
- No way to rollback to previous model
- No metadata about when model was trained
- Can't track model improvements

**How to Fix**:
- Implement model registry
- Save model with timestamp and metrics
- Track which version is production
- Version control model files

### 6. No Hyperparameter Tuning

**Current**: Model uses default hyperparameters

**Linear Regression hyperparameters** currently unused:
```python
maxIter = 100       # Number of iterations
regParam = 0.0      # Regularization parameter
elasticNetParam = 0.0  # L1/L2 mix
```

**Impact**:
- Model performance not optimized
- May underfit or overfit
- No grid search for best parameters

**How to Fix**:
```python
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

paramGrid = ParamGridBuilder() \
    .addGrid(lr.maxIter, [10, 100, 1000]) \
    .addGrid(lr.regParam, [0.0, 0.1, 0.01]) \
    .build()

cv = CrossValidator(
    estimator=lr,
    estimatorParamMaps=paramGrid,
    evaluator=RegressionEvaluator()
)
```

### 7. No Cross-Validation

**Current**: Single train/validation split (no rotation)

**Impact**:
- Results may be affected by specific data split
- No assessment of model stability
- May overfit to this particular split

**How to Fix**:
```python
# Use k-fold cross-validation
# Multiple train/test splits
# Average results across folds
```

### 8. Linear Model Only

**Current**: LinearRegression always used

**Limitations**:
- Assumes linear relationships
- May not capture non-linear patterns
- No option to try other algorithms

**How to Fix**:
- Parameterize algorithm choice
- Try multiple algorithms
- Compare metrics

```python
# config.yaml
model:
  algorithm: "LinearRegression"  # or "RandomForest", "GradientBoosting"
```

## Feature Engineering Limitations

### 9. Limited Features

**Current**: Only 4 features used

**Features**:
- temperature
- humidity
- temperature_change
- rolling_avg_temperature

**What's Missing**:
- Hour of day (captures daily cycles)
- Day of week (captures weekly patterns)
- Historical temperature lags (1h ago, 2h ago, etc.)
- Temperature acceleration (change of change)
- External features (ambient temperature, setpoint)

**How to Fix**:
```python
def create_time_features(df):
    df = df.withColumn("hour", hour("timestamp"))
    df = df.withColumn("day_of_week", dayofweek("timestamp"))
    return df

def create_lag_features(df, hours=[1, 2, 3]):
    window = Window.partitionBy("sensor_id").orderBy("timestamp")
    for hour in hours:
        df = df.withColumn(
            f"temperature_lag_{hour}h",
            lag("temperature", hour*12).over(window)  # 12 readings/hour
        )
    return df
```

### 10. No Feature Scaling

**Current**: Features used as-is without scaling

**Issue**: 
- Temperature range: 300-450 (large scale)
- Humidity range: 30-80 (small scale)
- Model weights biased by feature scale

**How to Fix**:
```python
from pyspark.ml.feature import StandardScaler

scaler = StandardScaler(
    inputCol="features",
    outputCol="scaledFeatures"
)

scaled_df = scaler.fit(train_df).transform(train_df)
```

## Testing Limitations

### 11. No Unit Tests

**Current**: All test files are empty

**Impact**:
- No automated verification of components
- Regressions not detected
- Difficult to refactor safely

**Files**: `tests/test_*.py` (all empty)

**How to Fix**:
```python
# tests/test_data_loader.py
import pytest
from src.data.data_loader import load_sensor_data

def test_load_sensor_data_shape(spark):
    df = load_sensor_data(spark)
    assert df.count() == 1000
    assert len(df.columns) == 4

def test_load_sensor_data_columns(spark):
    df = load_sensor_data(spark)
    expected = ["sensor_id", "timestamp", "temperature", "humidity"]
    assert df.columns == expected
```

### 12. No Integration Tests

**Current**: Only manual end-to-end testing

**Impact**:
- Changes may break pipeline unexpectedly
- No CI/CD pipeline
- Quality issues detected too late

### 13. No Test Coverage Metrics

**Current**: No measurement of how much code is tested

**How to Fix**:
```bash
pytest --cov=src tests/
# Shows % of code covered by tests
```

## Logging and Monitoring Limitations

### 14. No Logging System

**Current**: No logs created; output only to console

**Impact**:
- No audit trail of what happened
- Can't debug production issues
- No historical record of runs

**Files**: `src/utils/logger.py` (empty), `config/logging_config.yaml` (empty)

**How to Fix**:
```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

logger.info("Starting data loading")
logger.error("Failed to load data", exc_info=True)
```

### 15. No Error Handling

**Current**: No try/except blocks; any error stops pipeline

**Impact**:
- Pipeline fails completely on any error
- No graceful degradation
- No error recovery

**How to Fix**:
```python
try:
    df = load_sensor_data(spark)
except FileNotFoundError as e:
    logger.error(f"CSV file not found: {e}")
    raise
except Exception as e:
    logger.error(f"Unexpected error loading data: {e}")
    raise
```

### 16. No Monitoring

**Current**: No performance monitoring after deployment

**Impact**:
- Model drift undetected
- Performance degradation unknown
- Can't trigger retraining alerts

**How to Fix**:
- Track model metrics over time
- Compare current performance to baseline
- Alert if performance drops below threshold

## Data Quality Limitations

### 17. No Data Profiling

**Current**: No statistics about data quality

**Impact**:
- Don't know data characteristics
- Can't detect anomalies
- Quality trends invisible

**How to Add**:
```python
def profile_data(df):
    print(f"Record count: {df.count()}")
    print(f"Columns: {df.columns}")
    
    for col_name in ["temperature", "humidity"]:
        df.select(col_name).describe().show()
```

### 18. No Data Validation Report

**Current**: Invalid records exist but not analyzed

**Impact**:
- Don't know common rejection reasons
- Can't improve validation rules
- Quality metrics hidden

**How to Fix**:
```python
def analyze_invalid_records(invalid_df):
    # Count by reason
    invalid_df.groupBy("reasons").count().show()
```

## Infrastructure Limitations

### 19. No Distributed Execution

**Current**: All data processed on single machine

**Impact**:
- Limited by single machine resources
- Can't handle very large datasets
- No fault tolerance

**How to Fix**:
```python
# Deploy on Spark cluster
spark = SparkSession.builder \
    .master("spark://cluster-master:7077") \
    .appName("temperature-prediction") \
    .getOrCreate()
```

### 20. No Pipeline Scheduling

**Current**: Must run manually

**Impact**:
- Pipeline doesn't run on schedule
- Requires manual intervention
- No automated retraining

**How to Fix**:
- Use Apache Airflow for scheduling
- Cron jobs for simple cases
- Cloud scheduling services

### 21. No API for Predictions

**Current**: Only batch pipeline, no serving

**Impact**:
- Can't get predictions on demand
- Must batch process
- No real-time inference

**How to Fix**:
```python
# Create FastAPI endpoint
from fastapi import FastAPI
from pyspark.ml import PipelineModel

app = FastAPI()
model = PipelineModel.load("models/temperature_model_v1")

@app.post("/predict")
def predict(temperature: float, humidity: float):
    prediction = model.transform(data)
    return {"prediction": prediction}
```

## Documentation Limitations

### 22. Configuration Documentation Incomplete

**Current**: `config/config.yaml` and `logging_config.yaml` are empty

**Impact**:
- No centralized configuration
- Hard to understand options
- Example configurations missing

## Summary: Priority Fixes

### High Priority (Impacts Usage)
1. Move hardcoded paths to configuration
2. Add model persistence
3. Add error handling
4. Add basic logging

### Medium Priority (Improves Quality)
5. Add unit tests
6. Add feature scaling
7. Add hyperparameter tuning
8. Add data profiling

### Low Priority (Nice to Have)
9. Add monitoring
10. Add scheduling
11. Add REST API
12. Distributed execution

## Resources for Improvements

- **Model Persistence**: PySpark MLlib documentation
- **Logging**: Python `logging` module
- **Testing**: pytest, pytest-cov
- **Scheduling**: Apache Airflow
- **Serving**: FastAPI, Flask
- **Monitoring**: Prometheus, Grafana

---

**Key Takeaway**: Current implementation is functional but has significant gaps in production readiness: no model persistence, no logging, no error handling, limited features, no testing, and no scheduling. These should be addressed for production deployment.
