# Feature Engineering

## Overview

Feature engineering is the process of creating new data columns (features) that improve machine learning model performance. Raw sensor readings are transformed into features that capture temporal patterns and relationships.

## Why Feature Engineering Matters

Raw data alone provides limited information:
- Current temperature (single point)
- Current humidity (single point)

Engineered features provide context:
- How fast is temperature changing? (trend)
- What is average recent temperature? (smoothed trend)
- What temperature comes next? (target for prediction)

Better features → Better model predictions

## Features Created

### Feature 1: Temperature Change

**File**: `feature_engineer.py` (lines 5-23)

**Purpose**: Capture temperature momentum (how fast it's changing)

**Calculation**:
```python
previous_temperature = lag("temperature", 1).over(window)
temperature_change = current_temperature - previous_temperature
```

**Step 1: Get Previous Temperature**
```python
window = Window.partitionBy("sensor_id").orderBy("timestamp")
previous_temp = lag("temperature", 1).over(window)
```

**What `lag()` does**:
- Looks at previous row in ordered sequence
- Returns previous temperature value
- Parameter `1` means look back 1 row
- Can use `lag(temperature, 2)` to look back 2 rows

**Window function**:
- `partitionBy("sensor_id")`: Separate calculations per sensor
- `orderBy("timestamp")`: Process in time order
- Ensures lag only looks at previous reading from same sensor

**Example**:
```
Row | Timestamp | Temperature | Previous (lag) | Change
1   | 12:09     | 368.73      | NULL           | NULL
2   | 12:14     | 397.54      | 368.73         | +28.81
3   | 12:19     | 386.60      | 397.54         | -10.94
4   | 12:24     | 379.93      | 386.60         | -6.67
```

**First row**: Has NULL previous_temperature (no prior reading)

**Step 2: Calculate Change**
```python
temperature_change = col("temperature") - col("previous_temperature")
```

**Result**:
- Row 1: NULL (no previous value)
- Row 2: 397.54 - 368.73 = +28.81°C
- Row 3: 386.60 - 397.54 = -10.94°C

**Interpretation**:
- **Positive value**: Temperature increasing
- **Negative value**: Temperature decreasing
- **Large value**: Rapid change
- **Small value**: Stable temperature

**Why useful**:
- Model can learn: "When change is +20, future temp will be +5 higher"
- Captures momentum/inertia in temperature behavior
- Helps predict trend continuation or reversal

### Feature 2: Rolling Average Temperature

**File**: `feature_engineer.py` (lines 25-38)

**Purpose**: Capture temperature trend over 30-minute window

**Calculation**:
```python
window = (
    Window
    .partitionBy("sensor_id")
    .orderBy(col("timestamp").cast("long"))
    .rangeBetween(-30 * 60, 0)
)

rolling_avg = avg("temperature").over(window)
```

**Window Specification**:

1. **partitionBy("sensor_id")**: Separate calculation per sensor

2. **orderBy(col("timestamp").cast("long"))**: 
   - Convert timestamp to seconds (long integer)
   - Order rows chronologically
   - Enables range-based windowing

3. **rangeBetween(-30 * 60, 0)**:
   - `-30 * 60` = -1800 seconds = 30 minutes back
   - `0` = current row
   - Looks at 30-minute window ending at current row

**Example** (with 5-minute readings):

```
Time    | Temp  | Last 30 min readings | Rolling Avg
12:09   | 368.7 | [368.7]              | 368.7
12:14   | 397.5 | [368.7, 397.5]       | 383.1
12:19   | 386.6 | [368.7, 397.5, 386.6] | 384.3
12:24   | 379.9 | [397.5, 386.6, 379.9] | 388.0
12:29   | 389.9 | [386.6, 379.9, 389.9] | 385.5
12:34   | 390.2 | [379.9, 389.9, 390.2] | 386.7
```

**Why rangeBetween(-30*60, 0)**:
- Creates sliding window over time
- Each row's window includes all readings in past 30 minutes
- Different from row-based windowing (row_number())

**Interpretation**:
- **High value**: Recently been hot
- **Low value**: Recently been cold
- **Changing value**: Temperature trend shifting
- **Stable value**: Temperature plateau

**Why useful**:
- Smooths out noise and spikes
- Captures recent trend without individual spike
- Model learns: "When rolling avg is 390, future will be similar"

### Feature 3: Target Variable (What to Predict)

**File**: `feature_engineer.py` (lines 40-53)

**Purpose**: Define what we want to predict (future temperature)

**Calculation**:
```python
window = Window.partitionBy("sensor_id").orderBy("timestamp")

target_temperature = lead("temperature", 12).over(window)
```

**What `lead()` does**:
- Looks at future rows in ordered sequence
- Parameter `12` means look ahead 12 rows
- With 5-minute intervals: 12 rows × 5 min = 60 minutes ahead
- Predicts temperature 1 hour in the future

**Example** (with 5-minute readings):

```
Row | Time    | Temp   | Future (lead 12) | Target
1   | 12:09   | 368.73 | (from row 13)    | 375.5
2   | 12:14   | 397.54 | (from row 14)    | 374.8
3   | 12:19   | 386.60 | (from row 15)    | 376.1
...
12  | 13:09   | 373.21 | (from row 24)    | 378.3
13  | 13:14   | 375.53 | (from row 25)    | 379.1
...
988 | (row 988) | 385.2 | NULL             | NULL
989 | (row 989) | 382.9 | NULL             | NULL
...
1000| (row 1000)| 388.1 | NULL             | NULL
```

**Last 12 rows**: Have NULL target (no future data)

**Why look ahead 12 rows**:
- Prediction horizon of 1 hour
- Reasonable timeframe for manufacturing control
- Can be adjusted: `lead(temperature, 6)` for 30 min ahead

**Why useful**:
- Defines ground truth for model training
- Model learns: "Given current features, predict this future value"
- Enables supervised learning (input → output)

## Feature Vector Assembly

**File**: `run_training.py` (lines 96-103)

**Purpose**: Combine features into single vector for ML model

**Spark ML Requirement**: Models expect features as vector, not individual columns

**Code**:
```python
from pyspark.ml.feature import VectorAssembler

feature_columns = [
    "temperature",
    "humidity",
    "temperature_change",
    "rolling_avg_temperature"
]

assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol="features"
)

assembled_df = assembler.transform(df)
```

**What it does**:
- Takes 4 individual feature columns
- Combines into single DenseVector
- New "features" column contains vector

**Example transformation**:

```
Before:
temperature | humidity | temp_change | rolling_avg
20.5        | 50.3     | 1.2         | 19.8
30.2        | 45.1     | 9.7         | 22.3

After (new features column):
features
[20.5, 50.3, 1.2, 19.8]
[30.2, 45.1, 9.7, 22.3]
```

**Order matters**: Features are in same order as inputCols

## Complete Feature Set

### All Features for Model Training

**Input Features** (what model uses to predict):
1. `temperature`: Current temperature (°C)
2. `humidity`: Current humidity (%)
3. `temperature_change`: Change from previous reading (°C)
4. `rolling_avg_temperature`: 30-min rolling average (°C)

**Target Feature** (what model predicts):
- `target_temperature`: Future temperature in 1 hour (°C)

### Example Record With All Features

```json
{
  "sensor_id": "MANUFACTURING_01",
  "timestamp": "2025-01-31T12:09:12Z",
  "temperature": 368.73,
  "humidity": 50.39,
  "temperature_change": 1.23,
  "rolling_avg_temperature": 368.00,
  "target_temperature": 370.50,
  "features": [368.73, 50.39, 1.23, 368.00]
}
```

## Feature Generation Process

### Step 1: Start with Valid Data
```python
valid_df = validate_reading(df)
```
Contains: sensor_id, timestamp, temperature, humidity

### Step 2: Add Temperature Change
```python
feature_df = create_temperature_change(valid_df)
```
Adds: temperature_change column

### Step 3: Add Rolling Average
```python
feature_df = create_rolling_average(feature_df)
```
Adds: rolling_avg_temperature column

### Step 4: Add Target Variable
```python
feature_df = create_target(feature_df)
```
Adds: target_temperature column

### Step 5: Assemble Features
```python
assembler = VectorAssembler(
    inputCols=[...],
    outputCol="features"
)
feature_df = assembler.transform(feature_df)
```
Adds: features vector column

### Step 6: Store Features
```python
store_features(connection, feature_df)
```
Saves to sensor_features table in database

## Feature Statistics

### Typical Values

**Temperature**:
- Range: 300-450°C
- Mean: ~375°C
- Std Dev: ~15°C

**Humidity**:
- Range: 30-80%
- Mean: ~55%
- Std Dev: ~8%

**Temperature Change**:
- Range: -50 to +50°C per reading
- Mean: ~0°C (balanced up/down)
- Std Dev: ~5°C

**Rolling Average**:
- Range: 300-450°C (same as temperature)
- Mean: ~375°C
- Std Dev: ~10°C (smoother than raw temp)

**Target Temperature**:
- Range: 300-450°C
- Mean: ~375°C
- Has NULL values for last 12 rows

## Null Value Handling

### Where NULLs Appear

**temperature_change**:
- Row 1 (first reading) - NULL

**target_temperature**:
- Rows (n-11) to n (last 12 rows) - NULL

**rolling_avg_temperature**:
- Never NULL (average of at least 1 reading)

### Null Handling in Training

**Model training filters out NULLs**:
```python
training_df = feature_df.filter(
    col("target_temperature").isNotNull()
)
```

Results in ~988 rows for training (out of ~1000)

## Feature Importance in Model

**Which features matter most**:

Through model coefficients:
```
temperature: 0.85
humidity: -0.15
temperature_change: 0.45
rolling_avg_temperature: 0.95
```

Higher coefficients = stronger influence on predictions

## Limitations of Current Features

1. **No lag features**: Doesn't use temperature from 2+ hours ago
2. **No domain features**: Doesn't use time-of-day, day-of-week
3. **No external features**: Doesn't use ambient temperature trend
4. **No velocity features**: Doesn't capture acceleration (change of change)
5. **No seasonal features**: Doesn't account for time of year

## Enhancing Features (Future Work)

Possible new features:
```python
# Temperature acceleration
temp_acceleration = temp_change - lag(temp_change, 1)

# Hour of day (for cyclical patterns)
hour_of_day = hour(timestamp)

# Recent maximum temperature
rolling_max = max(temperature).over(window_past_1hr)

# Stability metric
rolling_std = stddev(temperature).over(window_past_1hr)
```

## Next Steps

- To see where features are stored: [Database Implementation](./09-database.md)
- To understand feature flow: [Data Flow](./08-data-flow.md)
- To see features in training: [Model Training](./11-model-training.md)

---

**Key Takeaway**: Features are engineered to capture temporal patterns: momentum (change), trend (rolling avg), and prediction target (future value). These enable the model to learn relationships between current conditions and future temperature.
