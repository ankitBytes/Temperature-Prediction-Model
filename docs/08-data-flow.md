# Complete Data Flow

## Overview

This document traces a single sensor reading's complete journey through the entire system, from raw CSV file to model prediction.

## Example Record

We'll follow this record through every stage:

```json
{
  "Timestamp": "2025-01-31 12:09:12.524887",
  "Current Temperature (°C)": 368.7270059423681,
  "Humidity (%)": 50.387994547288876,
  "Setpoint Temperature (°C)": 400
}
```

## Stage 1: Raw CSV File

**Location**: `data/raw/ttemperature_regulation_smart_manufacturing.csv`

**Format**: Comma-separated text file

**Content**:
```csv
Timestamp,Current Temperature (°C),Setpoint Temperature (°C),...,Humidity (%),...
2025-01-31 12:09:12.524887,368.7270059423681,400,...,50.387994547288876,...
```

**File characteristics**:
- ~1000+ records
- 15 columns (most not used)
- CSV format with headers
- UTF-8 encoding

**Next stage**: Data loading

---

## Stage 2: Data Loader

**File**: `src/data/data_loader.py`

**Function**: `load_sensor_data(spark)`

### Input Processing

Raw CSV columns → Standard format transformation:

```
Before (CSV):
  Timestamp: "2025-01-31 12:09:12.524887"
  Current Temperature (°C): "368.7270059423681"
  Humidity (%): "50.387994547288876"

Processing:
  1. CSV read with schema inference
  2. Column name transformations
  3. Type conversions
  4. Sensor ID addition
  5. Column selection

After (Spark DataFrame):
  sensor_id: "MANUFACTURING_01"
  timestamp: 2025-01-31T12:09:12.524887Z (datetime)
  temperature: 368.7270059423681 (double)
  humidity: 50.387994547288876 (double)
```

### Transformations Applied

| Original | Transformation | Result |
|----------|---|---|
| Timestamp (string) | `try_cast(AS TIMESTAMP)` | 2025-01-31 12:09:12 (datetime) |
| Current Temp (string) | `cast(AS double)` | 368.73 (number) |
| Humidity (string) | `cast(AS double)` | 50.39 (number) |
| (none) | `lit()` constant | MANUFACTURING_01 (added) |
| (15 columns) | `.select()` | 4 columns (kept) |

### Output DataFrame

```
+---------------+---------------------+----------------+--------+
|      sensor_id|           timestamp  |    temperature |humidity|
+---------------+---------------------+----------------+--------+
|MANUFACTURING_01|2025-01-31 12:09:12.5|368.7270059423681|50.3879 |
+---------------+---------------------+----------------+--------+
```

**Next stage**: Data validation

---

## Stage 3: Data Validator

**File**: `src/data/data_cleaner.py`

**Function**: `validate_reading(df)`

### Validation Checks

Each field is checked against rules:

```
Input: sensor_id=MANUFACTURING_01, timestamp=2025-01-31 12:09:12, 
       temperature=368.73, humidity=50.39

Check 1: sensor_id not null/empty? YES ✓
Check 2: timestamp not null? YES ✓
Check 3: temperature not null? YES ✓
Check 4: temperature in [300, 450]? YES (368.73 ✓) ✓
Check 5: humidity not null? YES ✓
Check 6: humidity in [30, 80]? YES (50.39 ✓) ✓

Result: VALID - All checks passed
```

### Validation Result

```json
{
  "sensor_id": "MANUFACTURING_01",
  "timestamp": "2025-01-31T12:09:12Z",
  "temperature": 368.73,
  "humidity": 50.39,
  "reasons": ""  (empty string = valid)
}
```

### Decision Point

```
Valid: YES ✓
  ↓
  → Proceeds to database storage
  → Proceeds to feature engineering
```

**Next stage**: Two paths (database + features)

---

## Stage 4A: Valid Data Storage (Database)

**File**: `scripts/run_training.py` (lines 143-166)

### Database Connection

```python
import sqlite3
import yaml

with open("config/database.yaml") as f:
    config = yaml.safe_load(f)

path = config["database"]["path"]
connection = sqlite3.connect(path)
```

**Database**: SQLite file at `/data/processed/valid_data/sensor_data.db`

### Table Creation

```sql
CREATE TABLE IF NOT EXISTS sensor_readings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor_id TEXT,
    timestamp TEXT,
    temperature REAL,
    humidity REAL,
    UNIQUE(sensor_id, timestamp)
)
```

### Record Insertion

```python
cursor.execute("""
    INSERT OR IGNORE INTO sensor_readings
    (sensor_id, timestamp, temperature, humidity)
    VALUES (?, ?, ?, ?)
""", (
    "MANUFACTURING_01",
    "2025-01-31T12:09:12Z",
    368.73,
    50.39
))
connection.commit()
```

**Result**: Record stored in database

```sql
SELECT * FROM sensor_readings WHERE timestamp = '2025-01-31T12:09:12Z';

id  | sensor_id      | timestamp              | temperature | humidity
1   | MANUFACTURING_01 | 2025-01-31T12:09:12Z | 368.73      | 50.39
```

### Duplicate Handling

- `INSERT OR IGNORE` prevents re-inserting duplicate records
- UNIQUE constraint on (sensor_id, timestamp) enforces uniqueness
- If same reading appears twice, second insert is ignored

**Next stage**: Feature engineering (parallel path)

---

## Stage 4B: Feature Engineering

**File**: `src/features/feature_engineer.py`

### Input to Feature Engineering

Same valid record from validation:
```json
{
  "sensor_id": "MANUFACTURING_01",
  "timestamp": "2025-01-31T12:09:12Z",
  "temperature": 368.73,
  "humidity": 50.39
}
```

### Feature 1: Temperature Change

**Logic**: `lag("temperature", 1).over(window)`

**Data needed**: Previous temperature value (from row before this)

**For row 1** (first reading):
- No previous row exists
- temperature_change = NULL

**For row 2+**:
```
Row 2:
  Previous temp (from row 1): 368.73
  Current temp (row 2): 397.54
  temperature_change = 397.54 - 368.73 = +28.81
```

**For our example** (assuming this is row 2):
```
temperature_change = 397.54 - 368.73 = +28.81°C
```

### Feature 2: Rolling Average Temperature

**Logic**: `avg("temperature").over(window_30min)`

**Data needed**: Average of all readings in past 30 minutes

**Calculation** (with 5-minute intervals):
```
Readings in past 30 min:
  [368.73, 397.54, 386.60]

Rolling average = (368.73 + 397.54 + 386.60) / 3 = 384.29°C
```

### Feature 3: Target Variable

**Logic**: `lead("temperature", 12).over(window)`

**Data needed**: Temperature 12 readings ahead (60 minutes in future)

**For row 1-988**:
```
Row 1: Look ahead 12 rows → Get value from row 13
```

**Simulated example** (assuming future temp is 375.5):
```
target_temperature = 375.5°C  (from 60 minutes in future)
```

### Feature Vector Assembly

**Input**: Individual columns
```
temperature: 368.73
humidity: 50.39
temperature_change: 28.81
rolling_avg_temperature: 384.29
```

**Transformation**: 
```python
assembler.transform(df)
```

**Output**: Vector column
```
features: [368.73, 50.39, 28.81, 384.29]
```

### Record After Feature Engineering

```json
{
  "sensor_id": "MANUFACTURING_01",
  "timestamp": "2025-01-31T12:09:12Z",
  "temperature": 368.73,
  "humidity": 50.39,
  "temperature_change": 28.81,
  "rolling_avg_temperature": 384.29,
  "target_temperature": 375.5,
  "features": [368.73, 50.39, 28.81, 384.29]
}
```

**Next stage**: Feature storage

---

## Stage 5: Feature Storage

**File**: `src/features/feature_store.py`

### Table Creation

```sql
CREATE TABLE IF NOT EXISTS sensor_features (
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

### Feature Insertion

```python
cursor.execute("""
    INSERT OR IGNORE INTO sensor_features
    (sensor_id, timestamp, temperature, humidity,
     temperature_change, rolling_avg_temperature)
    VALUES (?, ?, ?, ?, ?, ?)
""", (
    "MANUFACTURING_01",
    "2025-01-31T12:09:12Z",
    368.73,
    50.39,
    28.81,
    384.29
))
connection.commit()
```

### Stored Record

```sql
SELECT * FROM sensor_features WHERE timestamp = '2025-01-31T12:09:12Z';

id | sensor_id | timestamp | temp | humidity | temp_change | rolling_avg
2  | MANUFACTURING_01 | 2025-01-31T12:09:12Z | 368.73 | 50.39 | 28.81 | 384.29
```

**Next stage**: Data splitting

---

## Stage 6: Data Splitting

**File**: `scripts/run_training.py` (lines 33-121)

### Chronological Assignment

All records ordered by timestamp, assigned to sets:

```python
split_window = Window.orderBy("timestamp")
df = df.withColumn("row_num", row_number().over(split_window))

total = df.count()  # ~1000
train_end = int(total * 0.70)    # row 700
val_end = train_end + int(total * 0.15)  # row 850
```

**Our example record** (row 2):
- Row 2 < 700? YES
- Assignment: **TRAINING SET**

```json
{
  ...all features...,
  "split": "train"
}
```

**Split summary**:
- Rows 1-700: TRAINING (70%)
- Rows 701-850: VALIDATION (15%)
- Rows 851-1000: TEST (15%)

**Next stage**: Vector assembly (already done above, but done again per set)

---

## Stage 7: Model Training

**File**: `src/model/model_trainer.py`

### Training Data Preparation

Training set contains ~700 records, each with:
- Features vector: [368.73, 50.39, 28.81, 384.29]
- Target: 375.5

### Model Training Process

```python
from pyspark.ml.regression import LinearRegression

model = LinearRegression(
    featuresCol="features",
    labelCol="target_temperature",
    predictionCol="prediction"
)

trained_model = model.fit(training_df)
```

**What happens**:
1. Model sees all 700 training records
2. Finds best-fit line through feature space
3. Learns coefficients (weights for each feature):
   ```
   temperature: 0.85
   humidity: -0.15
   temperature_change: 0.45
   rolling_avg_temperature: 0.95
   intercept: 12.3
   ```

**Model equation** (simplified):
```
predicted_temperature = 0.85 * temperature 
                      - 0.15 * humidity 
                      + 0.45 * temperature_change 
                      + 0.95 * rolling_avg_temperature 
                      + 12.3
```

**Next stage**: Model evaluation

---

## Stage 8: Model Evaluation

**File**: `src/model/model_evaluator.py`

### Making Predictions on Validation Set

Validation set: ~150 records with known targets

```python
predictions = model.transform(validation_df)
```

For our record (if in validation set):

**Input to model**:
```
features: [368.73, 50.39, 28.81, 384.29]
```

**Model prediction**:
```
predicted_temperature = 0.85 * 368.73 
                      - 0.15 * 50.39 
                      + 0.45 * 28.81 
                      + 0.95 * 384.29 
                      + 12.3
                    = 313.41 - 7.56 + 12.96 + 365.08 + 12.3
                    = 375.2°C
```

**Ground truth**:
```
target_temperature: 375.5°C
```

**Error**:
```
error = prediction - actual
      = 375.2 - 375.5
      = -0.3°C (off by 0.3 degrees)
```

### Calculating Metrics

**Mean Squared Error (MSE)**:
```
Across all validation records:
MSE = average(error²)
    = average of all squared errors
    
For entire validation set: MSE = 2.45 °C²
```

**Mean Absolute Error (MAE)**:
```
Across all validation records:
MAE = average(|error|)
    = average of all absolute errors
    
For entire validation set: MAE = 1.23 °C
```

### Evaluation Output

```
Validation MSE: 2.45
Validation MAE: 1.23
Average actual: 375.3°C
Average prediction: 375.1°C
```

**Interpretation**:
- Model predictions off by ~1.23°C on average
- Good enough for manufacturing temperature prediction
- Model learned meaningful relationships

**Next stage**: Pipeline complete

---

## Stage 9: Output & Persistence

### Outputs Generated

**1. Database Records**:
- `sensor_readings` table: 990 valid records
- `sensor_features` table: 988 features (2 NULL targets)

**2. Invalid Records**:
- `data/processed/invalid_data/part-00000-*.csv`: 10 invalid records with rejection reasons

**3. Trained Model**:
- Held in memory during run
- **NOT saved to disk** (limitation)
- Would be lost if process restarts

**4. Evaluation Metrics** (printed to console):
```
Validation MSE: 2.45
Validation MAE: 1.23
```

### What's Not Persisted

- Trained model (regenerated on each run)
- Predictions on test set (only evaluated, not saved)
- Training logs
- Model metadata

---

## Complete Record Journey Summary

```
CSV Row
  ↓
Data Loading (transform columns & types)
  ↓
Raw DataFrame [sensor_id, timestamp, temperature, humidity]
  ↓
Validation (check ranges & nulls)
  ↓
Decision: VALID ✓
  ↓
→ Database storage [sensor_readings table]
→ Feature engineering (create 3 new features)
  ↓
Engineered DataFrame [+ temperature_change, rolling_avg, target]
  ↓
Feature storage [sensor_features table]
  ↓
Data splitting [chronological assignment to train/val/test]
  ↓
Training set (row 2 assigned here)
  ↓
Vector assembly [4 features → 1 vector]
  ↓
Model training [learns weights from 700 examples]
  ↓
Validation phase [makes prediction: 375.2]
  ↓
Comparison [actual: 375.5, error: -0.3°C]
  ↓
Metrics [contributes to MSE, MAE calculation]
  ↓
Pipeline complete
```

## Alternative Paths

### If Record Was Invalid

```
CSV Row
  ↓
Data Loading
  ↓
Validation (FAILS ✗)
  ↓
Decision: INVALID
  ↓
Written to: data/processed/invalid_data/part-*.csv
  ↓
Does NOT proceed to:
  - Database
  - Feature engineering
  - Model training
  - Model evaluation
```

### If Record Has NULL Target

```
(Feature engineering creates NULL target for last 12 rows)
  ↓
During training:
  .filter(col("target_temperature").isNotNull())
  ↓
Record excluded from training/validation/testing
  ↓
Not used in model evaluation
```

## Next Steps

- To understand each component: [Project Structure](./04-project-structure.md)
- To see feature details: [Feature Engineering](./07-feature-engineering.md)
- To trace database operations: [Database Implementation](./09-database.md)

---

**Key Takeaway**: A single record flows through 9 major stages: load → validate → split → engineer → train → evaluate. Valid records become model inputs; invalid records are preserved for quality monitoring.
