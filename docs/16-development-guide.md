# Development Guide

## Overview

This guide helps developers safely modify the project, add features, or fix bugs.

## Before Modifying Code

### 1. Understand the Current System

Read these before making changes:
- [Project Overview](./01-project-overview.md) - What the project does
- [Data Flow](./08-data-flow.md) - How data moves through system
- [Architecture](./03-architecture.md) - System design
- [Project Structure](./04-project-structure.md) - Where files are

### 2. Understand Dependencies

See which files depend on changes:
- **data_loader.py** → used by `run_training.py`
- **data_cleaner.py** → uses `data_loader.py` output
- **feature_engineer.py** → uses `data_cleaner.py` output
- **model_trainer.py** → uses `feature_engineer.py` output

**If you change**: `data_loader.py` columns
**Then update**: `data_cleaner.py`, `feature_engineer.py`, `model_trainer.py`

### 3. Create a Backup

```bash
# Backup database
cp data/processed/valid_data/sensor_data.db data/processed/valid_data/sensor_data.db.backup

# Backup code
git status  # See changes
git diff    # Review before committing
```

## Common Modifications

### Modify 1: Change Validation Ranges

**File**: `src/data/data_cleaner.py` (lines 10-11)

```python
# Current
temperature_range = [300, 450]
humidity_range = [30, 80]

# Change to
temperature_range = [200, 500]
humidity_range = [20, 90]
```

**Impact**: More/fewer records marked invalid

**Test**:
```bash
python3 scripts/run_training.py
# Check: Are invalid records count reasonable?
```

### Modify 2: Change Data Split Ratios

**File**: `scripts/run_training.py` (lines 44-45)

```python
# Current
train_end = int(total_rows * 0.70)      # 70%
validation_end = train_end + int(total_rows * 0.15)  # +15%
# Test = remaining 15%

# Change to 80/10/10
train_end = int(total_rows * 0.80)
validation_end = train_end + int(total_rows * 0.10)
```

**Impact**: Changes how much data trains vs validates

**Test**:
```bash
python3 scripts/run_training.py
# Check: Model metrics should change based on different training data
```

### Modify 3: Add New Feature

**File**: `src/features/feature_engineer.py`

Add function:
```python
def create_temperature_squared(df):
    """Square the temperature to capture non-linear effects"""
    df = df.withColumn(
        "temperature_squared",
        col("temperature") * col("temperature")
    )
    return df
```

Use in `run_training.py`:
```python
feature_df = create_temperature_change(valid_df)
feature_df = create_rolling_average(feature_df)
feature_df = create_temperature_squared(feature_df)  # NEW
feature_df = create_target(feature_df)
```

Update model in `model_trainer.py`:
```python
FEATURE_COLUMNS = [
    "temperature",
    "humidity",
    "temperature_change",
    "rolling_avg_temperature",
    "temperature_squared"  # NEW
]
```

Test:
```bash
python3 scripts/run_training.py
# Should complete without errors
# Model performance may improve or worsen
```

### Modify 4: Change Model Type

**File**: `src/model/model_trainer.py`

```python
# Current
from pyspark.ml.regression import LinearRegression

def train_model(train_df):
    lr = LinearRegression(...)
    model = lr.fit(train_df)
    return model
```

Change to:
```python
# New
from pyspark.ml.regression import RandomForestRegressor

def train_model(train_df):
    rf = RandomForestRegressor(
        numTrees=10,
        featuresCol="features",
        labelCol="target_temperature"
    )
    model = rf.fit(train_df)
    return model
```

Test:
```bash
python3 scripts/run_training.py
# Should work with new model type
# Metrics will be different
```

### Modify 5: Change Input Data Source

**File**: `src/data/data_loader.py` (line 8)

```python
# Current
.csv("/home/vvdn/Desktop/.../ttemperature_regulation_smart_manufacturing.csv")

# Change to
.csv("path/to/different/file.csv")
```

Or make it configurable:
```python
import yaml

def load_sensor_data(spark, config_path="config/config.yaml"):
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    csv_path = config["data"]["csv_path"]
    
    df = spark.read.option("header", True).csv(csv_path)
    # ... rest of code
```

Test:
```bash
python3 scripts/run_training.py
```

### Modify 6: Add Logging

**File**: `scripts/run_training.py`

Add at top:
```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/training.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
```

Use throughout:
```python
logger.info("Loading data...")
df = load_sensor_data(spark)
logger.info(f"Loaded {df.count()} records")

logger.info("Validating records...")
valid_df, invalid_df = validate_reading(df)
logger.info(f"Valid: {valid_df.count()}, Invalid: {invalid_df.count()}")
```

### Modify 7: Add Error Handling

**Before** (no error handling):
```python
df = load_sensor_data(spark)
valid_df, invalid_df = validate_reading(df)
```

**After** (with error handling):
```python
try:
    df = load_sensor_data(spark)
    logger.info(f"Loaded {df.count()} records")
except Exception as e:
    logger.error(f"Failed to load data: {e}")
    raise

try:
    valid_df, invalid_df = validate_reading(df)
    logger.info(f"Validated: {valid_df.count()} valid, {invalid_df.count()} invalid")
except Exception as e:
    logger.error(f"Validation failed: {e}")
    raise
```

## Testing Your Changes

### Manual Testing

```bash
# Run full pipeline
python3 scripts/run_training.py

# Check output
# - No errors
# - Database created
# - Metrics printed
```

### Unit Testing (Not Implemented)

Create `tests/test_data_loader.py`:
```python
import pytest
from pyspark.sql import SparkSession
from src.data.data_loader import load_sensor_data

@pytest.fixture
def spark():
    session = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    yield session
    session.stop()

def test_load_sensor_data(spark):
    df = load_sensor_data(spark)
    
    # Check record count
    assert df.count() > 0
    
    # Check columns exist
    assert "sensor_id" in df.columns
    assert "timestamp" in df.columns
    assert "temperature" in df.columns
    assert "humidity" in df.columns
    
    # Check data types
    assert df.schema["temperature"].dataType.typeName() == "double"
```

Run tests:
```bash
pytest tests/test_data_loader.py -v
```

## Code Style

### PySpark DataFrame Operations

**Good**:
```python
df = (
    df
    .withColumn("new_col", col("existing_col") + 1)
    .filter(col("value") > 0)
    .select("col1", "col2", "col3")
)
```

**Avoid**:
```python
df = df.withColumn("new_col", col("existing_col") + 1)
df = df.filter(col("value") > 0)
df = df.select("col1", "col2", "col3")
```

### Naming Conventions

```python
# Good
valid_sensor_readings = df.filter(...)
temperature_range = [300, 450]
create_temperature_change(df)

# Avoid
valid_data = df.filter(...)  # Too vague
tr = [300, 450]              # Too abbreviated
calc_temp(df)                # Unclear abbreviation
```

### Comments

Only add when "why" is non-obvious:

```python
# Good
# Use 30-minute window because sensor readings are 5 min intervals
# and we need to capture hourly trends
rolling_avg = avg("temperature").over(window_30min)

# Avoid
# Calculate rolling average
rolling_avg = avg("temperature").over(window_30min)
```

## Version Control (Git)

### Committing Changes

```bash
# See what changed
git status

# Review changes
git diff src/data/data_cleaner.py

# Stage specific files
git add src/data/data_cleaner.py

# Commit with clear message
git commit -m "Widen temperature validation range to 200-500°C"

# Push to repository
git push origin main
```

### Good Commit Messages

```
# Good
"Add temperature_squared feature for non-linear modeling"
"Fix null handling in rolling average calculation"
"Refactor data splitting into separate function"

# Avoid
"fixed stuff"
"updated"
"changes"
```

## Documentation

### Update Documentation When Code Changes

**If you change**: Validation ranges
**Update**: [Data Validation](./06-data-validation.md) - ranges section

**If you change**: File structure
**Update**: [Project Structure](./04-project-structure.md)

**If you change**: How to run pipeline
**Update**: [Running the Pipeline](./13-running-pipeline.md)

**Add docstrings** to new functions:
```python
def create_my_feature(df):
    """Calculate my_feature from input data.
    
    Args:
        df: Input Spark DataFrame
    
    Returns:
        DataFrame with my_feature column added
    
    Note:
        Uses 30-minute window for calculations.
    """
    # Implementation
```

## Code Review Checklist

Before submitting changes:

- [ ] Code runs without errors: `python3 scripts/run_training.py`
- [ ] Database created successfully
- [ ] Invalid data separated correctly
- [ ] Model trained and evaluated
- [ ] Metrics reasonable (not NaN or infinite)
- [ ] No hardcoded paths (except those in code currently)
- [ ] No commented-out debug code
- [ ] Documentation updated if needed
- [ ] Commit message is clear
- [ ] No unnecessary files added

## Reversing Changes

### If Something Breaks

```bash
# See what changed
git log --oneline | head

# Revert last commit
git revert HEAD

# Or go back to specific commit
git reset --hard <commit-hash>

# Or restore database
cp data/processed/valid_data/sensor_data.db.backup data/processed/valid_data/sensor_data.db
```

## Performance Optimization

### Before Optimizing

```bash
# Profile execution time
time python3 scripts/run_training.py
# Real: 0m45s (seconds)
```

### Identify Bottleneck

Add timing logs:
```python
import time

start = time.time()
df = load_sensor_data(spark)
print(f"Load time: {time.time() - start:.2f}s")

start = time.time()
valid_df, invalid_df = validate_reading(df)
print(f"Validation time: {time.time() - start:.2f}s")
```

### Optimize

Common optimizations:
1. **Cache DataFrames** if reused:
   ```python
   df.cache()
   df.count()  # Force caching
   ```

2. **Use fewer cores** if memory limited:
   ```python
   .master("local[2]")  # Instead of local[*]
   ```

3. **Partition data** for parallel processing

## Next Steps

- To deploy changes: Commit to Git
- To test thoroughly: Add unit tests
- To document: Update relevant docs
- To troubleshoot: [Troubleshooting Guide](./15-troubleshooting.md)

---

**Key Takeaway**: Before modifying code: understand dependencies, back up data, test changes, update documentation, and commit with clear messages. Follow patterns already in the codebase.
