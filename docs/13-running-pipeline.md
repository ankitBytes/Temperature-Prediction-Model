# Running the Pipeline

## Prerequisites

Before running the pipeline:
- ✓ Setup completed ([Setup Guide](./12-setup-guide.md))
- ✓ Virtual environment activated (see `(venv)` in terminal prompt)
- ✓ All dependencies installed
- ✓ Raw CSV data exists at `data/raw/ttemperature_regulation_smart_manufacturing.csv`
- ✓ `config/database.yaml` is configured

## Main Pipeline

### Command: Run Complete Training Pipeline

```bash
python3 scripts/run_training.py
```

**What it does** (in order):
1. Loads configuration from `config/database.yaml`
2. Connects to SQLite database (creates if doesn't exist)
3. Loads raw CSV data into Spark DataFrame
4. Validates each sensor reading
5. Separates valid and invalid records
6. Writes invalid data to CSV files
7. Writes valid data to database
8. Engineers features (temperature change, rolling average, target)
9. Stores engineered features in database
10. Splits data chronologically (70% train, 15% validation, 15% test)
11. Trains LinearRegression model
12. Makes predictions on validation set
13. Evaluates model (calculates MSE, MAE)
14. Prints results to console
15. Closes database and Spark session

### Expected Runtime

- **First run**: 1-2 minutes (Spark startup overhead)
- **Subsequent runs**: 30-45 seconds
- **Varies by**: CPU cores available, disk speed

### Expected Output

```
Loading configuration from config/database.yaml...
Database path: /path/to/data/processed/valid_data/sensor_data.db

Loading raw data from CSV...
Loaded 1000 records
Columns: sensor_id, timestamp, temperature, humidity

Validating records...
Valid: 990 records
Invalid: 10 records

Writing invalid data to: data/processed/invalid_data/
...

Writing valid data to database...
Inserted 990 records into sensor_readings table

Engineering features...
Creating temperature change feature...
Creating rolling average feature...
Creating target variable...
Storing features in database...
Stored 988 features in sensor_features table

Preparing data for training...
Total rows: 988
Train rows: 691 (70%)
Validation rows: 147 (15%)
Test rows: 150 (15%)

Training model...
Model trained successfully

Making predictions on validation data...
Predictions completed

Evaluating model...
Validation MSE: 2.45
Validation MAE: 1.23
Average actual temperature: 375.3°C
Average prediction: 375.1°C

Pipeline completed successfully!
```

## Step-by-Step Execution

### Step 1: Navigate to Project Directory

```bash
cd /path/to/temperature-prediction-pyspark
```

### Step 2: Activate Virtual Environment

```bash
source venv/bin/activate
# Prompt should show: (venv) $
```

### Step 3: Run Pipeline

```bash
python3 scripts/run_training.py
```

### Step 4: Monitor Output

Watch the console for progress. Each stage prints updates.

### Step 5: Check Results

**Results appear in**:
- Console output (metrics printed)
- `data/processed/valid_data/sensor_data.db` (database with records)
- `data/processed/invalid_data/` (invalid records as CSV)

## Inspecting Results

### Check Database

```bash
# List tables
sqlite3 data/processed/valid_data/sensor_data.db ".tables"
# Output: sensor_features  sensor_readings

# Count valid records
sqlite3 data/processed/valid_data/sensor_data.db "SELECT COUNT(*) FROM sensor_readings"
# Output: 990

# Count features
sqlite3 data/processed/valid_data/sensor_data.db "SELECT COUNT(*) FROM sensor_features"
# Output: 988

# Show sample records
sqlite3 data/processed/valid_data/sensor_data.db "SELECT * FROM sensor_readings LIMIT 3"
# Shows 3 sample records

# Show sample features
sqlite3 data/processed/valid_data/sensor_data.db "SELECT * FROM sensor_features LIMIT 3"
# Shows 3 sample features
```

### Check Invalid Data

```bash
# List files in invalid data directory
ls -la data/processed/invalid_data/

# View invalid records
cat data/processed/invalid_data/part-00000-*.csv | head -5
# Shows first 5 invalid records with rejection reasons

# Count invalid records
wc -l data/processed/invalid_data/part-00000-*.csv
```

### View Metrics

```bash
# Metrics printed to console during run
# Search output for:
# - "Validation MSE: X.XX"
# - "Validation MAE: X.XX"
# - "Average actual temperature: X.X°C"
# - "Average prediction: X.X°C"
```

## Debugging Execution

### Run with Verbose Output

```bash
python3 -u scripts/run_training.py  # -u for unbuffered output
```

### Run with Python Debugging

```bash
python3 -m pdb scripts/run_training.py  # Enter Python debugger
# At (Pdb) prompt, type "c" to continue or "s" to step
```

### Run Only Data Loading

Create `test_load.py`:
```python
from pyspark.sql import SparkSession
from src.data.data_loader import load_sensor_data

spark = SparkSession.builder.appName("Test").master("local[1]").getOrCreate()
df = load_sensor_data(spark)

print(f"Rows loaded: {df.count()}")
print("Schema:")
df.printSchema()
print("\nFirst 5 rows:")
df.show(5)

spark.stop()
```

Run:
```bash
python3 test_load.py
```

### Run Only Validation

Create `test_validate.py`:
```python
from pyspark.sql import SparkSession
from src.data.data_loader import load_sensor_data
from src.data.data_cleaner import validate_reading

spark = SparkSession.builder.appName("Test").master("local[1]").getOrCreate()
df = load_sensor_data(spark)
valid_df, invalid_df = validate_reading(df)

print(f"Valid records: {valid_df.count()}")
print(f"Invalid records: {invalid_df.count()}")
print("\nInvalid records with reasons:")
invalid_df.show()

spark.stop()
```

## Modifying Configuration

### Change Database Location

Edit `config/database.yaml`:
```yaml
database:
  type: sqlite
  path: "/new/path/to/database.db"
  table: "sensor_readings"
```

### Change Validation Ranges

Edit `src/data/data_cleaner.py`:
```python
temperature_range = [200, 500]  # Changed from [300, 450]
humidity_range = [20, 90]       # Changed from [30, 80]
```

### Change Train/Validation Split

Edit `scripts/run_training.py` (lines 44-45):
```python
train_end = int(total_rows * 0.80)  # 80% training
validation_end = train_end + int(total_rows * 0.10)  # 10% validation
# Test becomes 10%
```

After any change, run pipeline again:
```bash
python3 scripts/run_training.py
```

## Restarting from Scratch

### Option 1: Keep Database

```bash
python3 scripts/run_training.py
# Database is updated with new data, old data overwritten
```

### Option 2: Fresh Database

```bash
# Delete database and invalid data
rm data/processed/valid_data/sensor_data.db
rm -rf data/processed/invalid_data/*

# Run pipeline
python3 scripts/run_training.py
```

### Option 3: Reset Everything

```bash
# Delete all processed data
rm -rf data/processed/valid_data/*
rm -rf data/processed/invalid_data/*

# Run pipeline
python3 scripts/run_training.py
```

**Note**: Raw CSV data (`data/raw/`) is never deleted

## Batch Processing

### Process Multiple Times

```bash
# Run pipeline 3 times
for i in 1 2 3; do
    echo "=== Run $i ==="
    python3 scripts/run_training.py
done
```

### Process with Different Settings

```bash
# Run with original settings
python3 scripts/run_training.py

# Modify config.yaml
# Change validation ranges, splits, etc.

# Run with new settings
python3 scripts/run_training.py
```

## Performance Tuning

### Run Faster (Single Core)

```bash
# Edit scripts/run_training.py, change:
# .master("local[1]")  # Only 1 core
```

### Run Slower but Use Less Memory

```bash
# Edit scripts/run_training.py, change:
# .master("local[1]")  # Instead of local[*]
```

### Run Faster (Use All Cores)

```bash
# Edit scripts/run_training.py, ensure:
# .master("local[*]")  # Use all cores
```

## Troubleshooting Execution

### Problem: "File Not Found"

```
Error: CSV file /path/to/file.csv not found
```

**Solution**: Check file exists:
```bash
ls -la data/raw/ttemperature_regulation_smart_manufacturing.csv
```

### Problem: Database Locked

```
Error: database is locked
```

**Solution**: Make sure no other process using database:
```bash
# Kill any running Python processes
pkill -f "python3 scripts/run_training.py"

# Delete lock file if exists
rm data/processed/valid_data/sensor_data.db-journal

# Try again
python3 scripts/run_training.py
```

### Problem: Out of Memory

```
Error: OutOfMemoryError
```

**Solution**: Use fewer cores:
```bash
# Edit scripts/run_training.py
.master("local[2]")  # Use 2 cores instead of all
```

### Problem: No Output for Long Time

**Cause**: First run takes time for Spark startup

**Solution**: Wait 1-2 minutes or check if process is still running:
```bash
# In another terminal
ps aux | grep run_training.py
```

## Verifying Success

### Checklist

After running pipeline successfully:

```bash
# 1. Database created
test -f data/processed/valid_data/sensor_data.db && echo "✓ Database created"

# 2. Valid records stored
sqlite3 data/processed/valid_data/sensor_data.db "SELECT COUNT(*) FROM sensor_readings" | grep -q "[0-9]" && echo "✓ Valid records stored"

# 3. Features stored
sqlite3 data/processed/valid_data/sensor_data.db "SELECT COUNT(*) FROM sensor_features" | grep -q "[0-9]" && echo "✓ Features stored"

# 4. Invalid data directory created
test -d data/processed/invalid_data && echo "✓ Invalid data directory exists"

# 5. Invalid records saved
test -f data/processed/invalid_data/part-*.csv && echo "✓ Invalid records saved"
```

All should show checkmarks (✓).

## Next Steps

- After running successfully: [Understanding Results](./08-data-flow.md)
- To troubleshoot problems: [Troubleshooting Guide](./15-troubleshooting.md)
- To modify pipeline: [Development Guide](./16-development-guide.md)
- To understand what happened: [Data Flow](./08-data-flow.md)

---

**Key Takeaway**: The complete pipeline runs with a single command `python3 scripts/run_training.py`. It loads data, validates it, engineers features, trains a model, and prints metrics. Results are stored in SQLite database and CSV files.
