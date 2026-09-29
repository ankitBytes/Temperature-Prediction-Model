# Data Validation and Invalid Data Handling

## Overview

Data validation is the quality control step. It checks each sensor reading against rules to ensure data quality. Valid records proceed to storage; invalid records are separated for investigation.

## Validation Architecture

### Two-Part Process
1. **Detection**: Identify which records violate which rules
2. **Separation**: Split into valid and invalid DataFrames

### File: `src/data/data_cleaner.py`

### Function: `validate_reading(df)`

**Input**: Spark DataFrame from data_loader (sensor_id, timestamp, temperature, humidity)

**Output**: 
- `valid_df`: Records passing all checks (4 columns)
- `invalid_df`: Records failing checks (5 columns including rejection reasons)

## Validation Rules

### Rule 1: Sensor ID Not Null or Empty

**Condition**:
```python
col("sensor_id").isNull() | (col("sensor_id") == " ")
```

**Rejects if**:
- sensor_id value is NULL/None
- sensor_id is exactly a single space " "

**Reason**: "Sensor ID is missing"

**Example**:
```
✓ MANUFACTURING_01 - VALID
✗ NULL            - INVALID
✗ " "             - INVALID
```

### Rule 2: Timestamp Not Null

**Condition**:
```python
col("timestamp").isNull()
```

**Rejects if**:
- timestamp is NULL/None

**Reason**: "Timestamp is missing"

**Why important**: Cannot order data chronologically without timestamps

**Example**:
```
✓ 2025-01-31 12:09:12 - VALID
✗ NULL                - INVALID
```

### Rule 3: Temperature Not Null

**Condition**:
```python
col("temperature").isNull()
```

**Rejects if**:
- temperature is NULL/None

**Reason**: "Temperature is missing"

**Why important**: Cannot use data without the primary measurement

**Example**:
```
✓ 368.73  - VALID
✗ NULL    - INVALID
```

### Rule 4: Temperature In Range

**Condition**:
```python
col("temperature").isNotNull()
& (
    (col("temperature") < 300) | (col("temperature") > 450)
)
```

**Range**: 300°C to 450°C (inclusive)

**Rejects if**:
- temperature < 300 OR temperature > 450
- Only checked if temperature is not NULL

**Reason**: "Temperature is out of range"

**Why this range**:
- Manufacturing equipment operating range
- Temperatures outside indicate sensor malfunction
- Prevents unrealistic predictions

**Examples**:
```
✓ 368.73   - VALID (in range)
✓ 300.0    - VALID (boundary)
✓ 450.0    - VALID (boundary)
✗ 299.99   - INVALID (below)
✗ 450.01   - INVALID (above)
✗ 100.0    - INVALID (impossible)
✗ 600.0    - INVALID (impossible)
```

### Rule 5: Humidity Not Null

**Condition**:
```python
col("humidity").isNull()
```

**Rejects if**:
- humidity is NULL/None

**Reason**: "Humidity is missing"

**Example**:
```
✓ 50.39  - VALID
✗ NULL   - INVALID
```

### Rule 6: Humidity In Range

**Condition**:
```python
col("humidity").isNotNull()
& (
    (col("humidity") < 30) | (col("humidity") > 80)
)
```

**Range**: 30% to 80% (inclusive)

**Rejects if**:
- humidity < 30 OR humidity > 80
- Only checked if humidity is not NULL

**Reason**: "Humidity is out of range"

**Why this range**:
- Typical manufacturing environment humidity
- Values outside indicate sensor error
- Prevents unrealistic environment assumptions

**Examples**:
```
✓ 50.39   - VALID (in range)
✓ 30.0    - VALID (boundary)
✓ 80.0    - VALID (boundary)
✗ 29.99   - INVALID (too dry)
✗ 80.01   - INVALID (too humid)
✗ 10.0    - INVALID (impossible)
✗ 100.0   - INVALID (impossible)
```

## Validation Constants

```python
temperature_range = [300, 450]  # Celsius
humidity_range = [30, 80]       # Percentage
```

**Location in code**: Lines 10-11 of data_cleaner.py

**How to modify**: Change these values to adjust acceptable ranges

**Example**: To allow colder temperatures:
```python
temperature_range = [200, 450]
```

## Validation Implementation Details

### Step 1: Build Reasons Array

```python
reasons = array(
    when(condition_1, lit("Reason 1")),
    when(condition_2, lit("Reason 2")),
    # ... more conditions
)
```

**What it does**:
- Creates array column with NULL elements
- For each failed condition, puts reason string in array
- If condition passes, array element is NULL

**Example**:
```
Input: temperature=299 (out of range)
       humidity=50 (valid)

Reasons array: [NULL, NULL, NULL, "Temperature is out of range", NULL, NULL]
```

### Step 2: Add Reasons Column

```python
df_with_reasons = df.withColumn("reasons", reasons)
```

**Adds new column** "reasons" containing array of violation messages

### Step 3: Filter Out Null Reasons

```python
.withColumn(
    "reasons",
    spark_filter(col("reasons"), lambda reason: reason.isNotNull())
)
```

**Removes NULL elements** from array, keeping only actual reasons

**Example**:
```
Before: [NULL, NULL, NULL, "Temperature is out of range", NULL, NULL]
After:  ["Temperature is out of range"]
```

### Step 4: Concatenate Reasons

```python
.withColumn(
    "reasons",
    concat_ws(", ", col("reasons"))
)
```

**Joins array elements** into single string with ", " separator

**Example**:
```
Before: ["Temperature is out of range", "Humidity is out of range"]
After:  "Temperature is out of range, Humidity is out of range"
```

### Step 5: Split Valid vs Invalid

```python
invalid_df = df_with_reasons.filter(col("reasons") != "")
valid_df = df_with_reasons.filter(col("reasons") == "").drop("reasons")
```

**Splits data**:
- **invalid_df**: reasons string is not empty (had violations)
- **valid_df**: reasons string is empty (no violations)

**Important**: valid_df drops the reasons column (not needed)

## Valid Records

### Definition
Records that pass ALL six validation rules

### Columns
```
sensor_id, timestamp, temperature, humidity
```

### Example Valid Record
```json
{
  "sensor_id": "MANUFACTURING_01",
  "timestamp": "2025-01-31T12:09:12Z",
  "temperature": 368.73,
  "humidity": 50.39
}
```

### Processing
- Stored in sensor_readings table
- Used for feature engineering
- Used for model training

## Invalid Records

### Definition
Records that fail at least one validation rule

### Columns
```
sensor_id, timestamp, temperature, humidity, reasons
```

### Example Invalid Records

**Record 1: Temperature out of range**
```json
{
  "sensor_id": "MANUFACTURING_01",
  "timestamp": "2025-02-03T23:39:12Z",
  "temperature": 299.5,
  "humidity": 50.2,
  "reasons": "Temperature is out of range"
}
```

**Record 2: Multiple violations**
```json
{
  "sensor_id": "MANUFACTURING_01",
  "timestamp": "2025-02-04T10:15:00Z",
  "temperature": 500.0,
  "humidity": 90.0,
  "reasons": "Temperature is out of range, Humidity is out of range"
}
```

**Record 3: Missing value**
```json
{
  "sensor_id": "MANUFACTURING_01",
  "timestamp": "2025-02-04T10:20:00Z",
  "temperature": NULL,
  "humidity": 52.5,
  "reasons": "Temperature is missing"
}
```

## Invalid Data Storage

### Location
```
data/processed/invalid_data/
```

### File Format
- **Format**: CSV
- **Multiple files**: `part-00000-*.csv`, `part-00001-*.csv`, etc.
- **Spark marker**: `_SUCCESS` file (indicates job completed)
- **Checksums**: `._*.crc` files (Spark integrity checks)

### Why Multiple Files
Spark writes one partition per CPU core used. For single machine with 4 cores:
- May create 4 CSV files
- Each contains subset of invalid records
- Can be combined by reading directory

### Reading Invalid Data Example
```python
# Read single file
invalid_df = spark.read.csv("data/processed/invalid_data/part-00000-*.csv", header=True)

# Or read entire directory
invalid_df = spark.read.csv("data/processed/invalid_data", header=True)
```

### CSV Example Content
```csv
sensor_id,timestamp,temperature,humidity,reasons
MANUFACTURING_01,2025-02-03T23:39:12.528+05:30,299.5,50.2,Temperature is out of range
MANUFACTURING_01,2025-02-03T23:44:12.528+05:30,450.5,52.1,Temperature is out of range
MANUFACTURING_01,2025-02-04T10:15:00.000+05:30,NULL,52.5,Temperature is missing
```

## Validation Statistics

### Counting Results
```python
valid_count = valid_df.count()
invalid_count = invalid_df.count()
total_count = valid_count + invalid_count
valid_pct = (valid_count / total_count) * 100

print(f"Valid: {valid_count} ({valid_pct:.1f}%)")
print(f"Invalid: {invalid_count} ({100-valid_pct:.1f}%)")
```

### Typical Results (This Project)
- **Total records**: ~1000
- **Valid records**: ~990 (99%)
- **Invalid records**: ~10 (1%)

## Validation Limitations

1. **No contextual validation**: Doesn't check if current reading makes sense given previous readings
2. **No duplicate detection**: Same (sensor_id, timestamp) can appear twice
3. **No precision validation**: Doesn't verify realistic temperature changes
4. **Hardcoded ranges**: Cannot adjust ranges per sensor or time period
5. **No timestamp format validation**: Accepts any timestamp format after parsing

## Troubleshooting Validation

### Problem: Too Many Invalid Records
**Cause**: Ranges may be too strict for your data

**Solution**: Adjust ranges in data_cleaner.py
```python
temperature_range = [200, 500]  # Wider range
```

### Problem: Records Marked Invalid But Look Valid
**Cause**: Check rejection reasons

**Solution**: 
```python
# Examine invalid records
invalid_df.select("temperature", "humidity", "reasons").show()
```

### Problem: Valid Records Missing Expected Data
**Cause**: Records with NULL values are rejected

**Solution**: Investigate data source quality

## Quality Metrics

### Data Quality Score
```
Quality = (Valid Records / Total Records) * 100
```

### Acceptable Thresholds
- **90%+**: Excellent - data is reliable
- **70-90%**: Good - acceptable for analysis
- **50-70%**: Poor - investigate data source
- **<50%**: Critical - major data quality issues

## Next Steps

- To see how invalid data flows: [Data Flow](./08-data-flow.md)
- To store valid data: [Database Implementation](./09-database.md)
- To understand the entire pipeline: [Running the Pipeline](./13-running-pipeline.md)

---

**Key Takeaway**: Validation separates reliable data from problematic data, ensuring downstream analysis is based on quality inputs. Valid data proceeds to storage and modeling; invalid data is preserved for quality monitoring.
