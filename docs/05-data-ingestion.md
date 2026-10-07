# Data Ingestion Guide

## Overview

Data ingestion is the first step in the pipeline. It loads raw sensor data from a CSV file into a Spark DataFrame and transforms it into a standardized format that downstream components can rely on.

## Source Data

### File Location
```
data/raw/ttemperature_regulation_smart_manufacturing.csv
```

### File Format
- **Format**: Comma-separated values (CSV)
- **Encoding**: UTF-8
- **Headers**: Present in first row
- **Separator**: Comma (,)

### Data Characteristics
- **Rows**: 1000+ sensor readings
- **Columns**: 15 columns (only 2-3 used by pipeline)
- **Time Range**: Multiple readings over several days
- **Frequency**: Approximately 5-minute intervals
- **Source**: Smart manufacturing facility temperature control system

### Sample Data
```
Timestamp,Current Temperature (°C),Setpoint Temperature (°C),...,Humidity (%),...
2025-01-31 12:09:12.524887,368.7270059423681,400,31.272994057631877,...,50.387994547288876,...
2025-01-31 12:14:12.524887,397.5357153204958,400,2.4642846795042033,...,51.86772528231129,...
2025-01-31 12:19:12.524887,386.59969709057026,400,13.40030290942974,...,58.31245805089942,...
```

## Data Loading Process

### File: `src/data/data_loader.py`

### Function: `load_sensor_data(spark)`

**Purpose**: Load CSV file and transform into standardized DataFrame schema

**Parameters**:
- `spark`: SparkSession object (active Spark environment)

**Returns**:
- Spark DataFrame with columns: sensor_id, timestamp, temperature, humidity

**How it works**:

#### Step 1: Read CSV File
```python
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/path/to/ttemperature_regulation_smart_manufacturing.csv")
)
```

**Options explained**:
- `header=True`: First row contains column names
- `inferSchema=True`: Spark automatically detects data types
  - Numbers become IntegerType or DoubleType
  - Strings become StringType
  - Timestamps are detected as StringType (need manual conversion)

**Result**: DataFrame with 15 columns, column names from CSV header, inferred types

#### Step 2: Add Sensor Identifier
```python
df = df.withColumn(
    "sensor_id",
    lit("MANUFACTURING_01")
)
```

**Purpose**: Add sensor identifier to each row

**Why**: Distinguishes data from different manufacturing facilities

**Current limitation**: Hardcoded value - same for all rows

**What it does**:
- Creates new column named "sensor_id"
- Sets value to "MANUFACTURING_01" for every row
- `lit()` means "literal" - a constant value

**Example**:
```
Before: [Timestamp, Current Temperature, Humidity, ...]
After:  [Timestamp, Current Temperature, Humidity, ..., MANUFACTURING_01]
                                                      (new sensor_id column)
```

#### Step 3: Convert Timestamp String to Datetime
```python
df = df.withColumn(
    "timestamp",
    expr("try_cast(`Timestamp` AS TIMESTAMP)")
)
```

**Purpose**: Convert timestamp string to proper datetime type

**Why**: Enables time-based operations (ordering, windowing, calculations)

**How it works**:
- `try_cast()` attempts to convert value to TIMESTAMP type
- Returns NULL if conversion fails (instead of erroring)
- Backticks (`) used because column name has space: `Timestamp`

**Example transformation**:
```
"2025-01-31 12:09:12.524887" (string)
         ↓
2025-01-31T12:09:12.524887Z (datetime)
```

**Important**: If timestamp cannot be parsed, value becomes NULL (caught in validation later)

#### Step 4: Convert Temperature to Numeric
```python
df = df.withColumn(
    "temperature",
    col("Current Temperature (°C)").cast("double")
)
```

**Purpose**: Extract temperature and convert to numeric type

**Original column**: "Current Temperature (°C)" (contains degree symbol and parentheses)

**New column**: "temperature" (simple name)

**Type conversion**: Any value → double (floating point)

**Example**:
```
"368.7270059423681" (string in CSV)
         ↓
368.7270059423681 (double in memory)
```

**Type matching**: Uses `double` not `int` because temperature has decimal places

#### Step 5: Convert Humidity to Numeric
```python
df = df.withColumn(
    "humidity",
    col("Humidity (%)").cast("double")
)
```

**Purpose**: Extract humidity and convert to numeric type

**Original column**: "Humidity (%)" (contains % symbol and parentheses)

**New column**: "humidity" (simple name)

**Type conversion**: String → double

**Example**:
```
"50.387994547288876" (string in CSV)
         ↓
50.387994547288876 (double in memory)
```

#### Step 6: Select Required Columns
```python
return df.select(
    "sensor_id",
    "timestamp",
    "temperature",
    "humidity"
)
```

**Purpose**: Keep only columns needed for pipeline

**Discarded columns** (from original 15):
- Current Temperature (°C) - replaced by temperature
- Humidity (%) - replaced by humidity
- Timestamp - replaced by timestamp
- All PID control parameters
- All setpoint and error columns
- All other metadata

**Result**: DataFrame with exactly 4 columns

### DataFrame Schema After Loading

```python
root
 |-- sensor_id: string
 |-- timestamp: timestamp
 |-- temperature: double
 |-- humidity: double
```

**Column details**:

| Column | Type | Meaning | Example |
|--------|------|---------|---------|
| sensor_id | string | Manufacturing facility identifier | "MANUFACTURING_01" |
| timestamp | timestamp | When reading was taken | 2025-01-31 12:09:12 |
| temperature | double | Current temperature in Celsius | 368.73 |
| humidity | double | Humidity level in percent | 50.39 |

## Data Quality at Ingestion

### What is Validated Here
Nothing is validated during loading. All data is accepted as-is.

### What Happens to Bad Data
- **NULL values**: Passed through (caught in validation stage)
- **Type conversion errors**: Become NULL (e.g., non-numeric temperature)
- **Missing columns**: Error during load
- **Out-of-range values**: Passed through (caught in validation stage)

### What Happens to Extra Columns
Silently dropped by `.select()` call

## Performance Considerations

### Schema Inference Cost
`inferSchema=True` requires two passes through CSV:
1. First pass: Determine column types
2. Second pass: Read data with types

**Alternative**: Provide explicit schema to avoid second pass
```python
schema = StructType([
    StructField("Timestamp", StringType()),
    StructField("Current Temperature (°C)", DoubleType()),
    StructField("Humidity (%)", DoubleType()),
    # ... etc
])
df = spark.read.schema(schema).csv(path)
```

**When to use**: When reading same CSV multiple times

### Memory Usage
- **CSV file**: ~500 KB on disk
- **Spark DataFrame**: ~2-3 MB in memory (plus overhead)
- **Partition count**: 1 partition (single file)

## Typical Workflow

### Starting Spark Session
```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Temperature prediction pipeline")
    .master("local[*]")  # Use all cores
    .getOrCreate()
)
```

### Loading Data
```python
from src.data.data_loader import load_sensor_data

df = load_sensor_data(spark)
print(df.count())  # Number of rows
df.show(5)         # Show first 5 rows
```

### Output Example
```
+---------------+---------------------+--------------------+--------+
|      sensor_id|           timestamp  |        temperature |humidity|
+---------------+---------------------+--------------------+--------+
|MANUFACTURING_01|2025-01-31 12:09:12.5|368.7270059423681  |50.3879 |
|MANUFACTURING_01|2025-01-31 12:14:12.5|397.5357153204958  |51.8677 |
|MANUFACTURING_01|2025-01-31 12:19:12.5|386.59969709057026 |58.3124 |
|MANUFACTURING_01|2025-01-31 12:24:12.5|379.9329242098518  |57.6676 |
|MANUFACTURING_01|2025-01-31 12:29:12.5|389.9820635245678  |52.1234 |
+---------------+---------------------+--------------------+--------+
```

## Troubleshooting Data Loading

### Problem: File Not Found
**Error**: `java.io.FileNotFoundException: /path/to/file.csv`

**Cause**: CSV file path is incorrect or file doesn't exist

**Solution**: Verify file path is correct and file exists

### Problem: Wrong Number of Columns
**Error**: Data loaded but missing temperature or humidity column

**Cause**: CSV has different column names than expected

**Solution**: Check CSV header names match code expectations

### Problem: Null Values in Numeric Columns
**Cause**: Non-numeric values in temperature/humidity columns

**Solution**: These are handled in validation stage

### Problem: Memory Errors
**Error**: `OutOfMemoryError` when loading very large files

**Solution**: Process in batches or use external tools like Spark in cluster mode

## Connection to Next Stage

The DataFrame output from data loading is passed to the validation stage:

```python
# Load data
df = load_sensor_data(spark)

# Validate it
valid_df, invalid_df = validate_reading(df)
```

Validation checks the values that data loading parsed.

## Key Implementation Details

**Hardcoded values**:
```python
sensor_id = "MANUFACTURING_01"  # Same for all rows
csv_path = "/home/vvdn/Desktop/Projects/Python/.../ttemperature_regulation_smart_manufacturing.csv"
```

**Limitations**:
- Only handles single manufacturer
- CSV path hardcoded in code (not configurable)
- No partitioning by date
- No incremental loading
- All data loaded into memory

## Next Steps

- After loading, data is validated: [Data Validation Guide](./06-data-validation.md)
- To understand column transformations better: [Data Flow](./08-data-flow.md)
- To run the pipeline: [Running the Pipeline](./13-running-pipeline.md)

---

**Key Takeaway**: Data ingestion reads CSV, standardizes column names, converts types, and produces a clean DataFrame ready for validation. No data is rejected at this stage; all issues are caught later.
