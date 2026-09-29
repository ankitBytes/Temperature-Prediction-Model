# Common Mistakes Junior Developers Make

This guide highlights mistakes that new developers commonly make and how to avoid them.

## 1. Modifying Raw Data

### The Mistake

```bash
# ✗ WRONG
cd data/raw
rm ttemperature_regulation_smart_manufacturing.csv
# Oops! Original data gone
```

### Why It's Wrong

- Raw data is the source of truth
- Can't regenerate without backup
- Reproducibility lost
- Other team members affected

### The Right Way

```bash
# ✓ CORRECT
# Never modify raw/ directory
# Create processed/ outputs instead

# If need to clean data:
# 1. Leave raw data untouched
# 2. Process in code (data_loader.py, data_cleaner.py)
# 3. Store results in data/processed/

# Backup before any changes:
cp -r data/raw data/raw.backup
```

## 2. Hardcoding File Paths

### The Mistake

```python
# ✗ WRONG
csv_path = "/home/alice/temperature-project/data/raw/sensors.csv"
db_path = "/home/alice/data/database.db"
```

**Problems**:
- Fails on Bob's computer (different home directory)
- Fails if project moves
- Different paths per machine required

### The Right Way

```python
# ✓ CORRECT - Use relative paths
import os

base_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(base_dir, "..", "..", "data", "raw", "sensors.csv")

# Or use configuration file
import yaml
config = yaml.safe_load(open("config/database.yaml"))
db_path = config["database"]["path"]
```

## 3. Ignoring Invalid Records

### The Mistake

```python
# ✗ WRONG
valid_df, invalid_df = validate_reading(df)
# Oops! Where did invalid_df go?
# No tracking, lost data quality visibility
```

### Why It's Wrong

- Don't know data quality
- Can't debug validation failures
- Metrics are incomplete
- Quality issues hidden

### The Right Way

```python
# ✓ CORRECT
valid_df, invalid_df = validate_reading(df)

# Store invalid records
invalid_df.write.mode("overwrite").option("header", True).csv("data/processed/invalid_data")

# Log counts
valid_count = valid_df.count()
invalid_count = invalid_df.count()
print(f"Valid: {valid_count}, Invalid: {invalid_count}")

# Analyze why
invalid_df.select("reasons").distinct().show()
```

## 4. Not Checking Data Before Using It

### The Mistake

```python
# ✗ WRONG
model = train_model(train_df)
print("Model trained!")
# Oops! train_df was empty or corrupted
# Model trained on nothing
```

### Why It's Wrong

- Silent failures
- Meaningless model
- Wasted time
- Results unreliable

### The Right Way

```python
# ✓ CORRECT
print(f"Training data: {train_df.count()} records")

# Check schema
train_df.printSchema()

# Check for nulls
null_counts = train_df.select([count(when(col(c).isNull(), 1)).alias(c) 
                               for c in train_df.columns])
null_counts.show()

# Sample data
train_df.show(5)

# Only then train
model = train_model(train_df)
```

## 5. Changing Column Names Without Updating All Code

### The Mistake

```python
# In data_loader.py
df = df.withColumn("temp", col("temperature"))  # Renamed

# In feature_engineer.py (still uses old name)
temperature_change = col("temperature") - col("previous")
# ✗ Error: column 'temperature' does not exist!
```

### Why It's Wrong

- Breaks downstream code
- Hard to debug
- Only works if you run full pipeline
- Others' code breaks

### The Right Way

```python
# 1. If renaming column, UPDATE EVERYWHERE:

# data_loader.py
.withColumn("temp", col("temperature"))

# feature_engineer.py - also update
temperature_change = col("temp") - col("previous")

# model_trainer.py
FEATURE_COLUMNS = ["temp", "humidity", ...]

# 2. Or better: Use consistent column names everywhere
# Don't rename unless necessary

# 3. If must rename: Search codebase
# grep -r "temperature" src/  # Find all uses
# Update all occurrences
```

## 6. Breaking Database Schema Silently

### The Mistake

```python
# ✗ WRONG
# Manually editing database
sqlite3 data/processed/valid_data/sensor_data.db

# Deleting columns
ALTER TABLE sensor_readings DROP COLUMN humidity;

# Oops! Code expects humidity column
# Pipeline breaks unpredictably
```

### Why It's Wrong

- Code assumes schema
- Silent failures
- Others don't know schema changed
- Hard to recover

### The Right Way

```python
# ✓ CORRECT
# Schema changes in code only:

# src/features/feature_store.py
cursor.execute("""
    CREATE TABLE IF NOT EXISTS sensor_features (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sensor_id TEXT,
        timestamp TEXT,
        temperature REAL,
        humidity REAL,
        new_column REAL  # Add here, not in database
    )
""")

# Document the change
# Update version/changelog
# Test thoroughly
# Update this code for future runs
```

## 7. Not Understanding Spark Lazy Evaluation

### The Mistake

```python
# ✗ WRONG
df = load_sensor_data(spark)
df2 = df.filter(col("temperature") > 0)
df3 = df2.select("temperature", "humidity")

print("Filtered successfully!")  # Not actually run yet!

# Computation only happens when:
df3.show()  # Here
# or
df3.collect()  # Or here
```

### Why It's Wrong

- Errors occur later than where code is
- Hard to debug
- Thinking about execution is confusing

### The Right Way

```python
# ✓ CORRECT
df = load_sensor_data(spark)
df2 = df.filter(col("temperature") > 0)
df3 = df2.select("temperature", "humidity")

# Force execution immediately to catch errors
df3.show()  # Or df3.count()

print("Filtered successfully!")  # Now truly done
```

## 8. Hardcoding Configuration Values

### The Mistake

```python
# ✗ WRONG - In data_cleaner.py
temperature_range = [300, 450]
humidity_range = [30, 80]

# Want to try different ranges? Edit code
# No way to change without editing source
# Changes not tracked in config
```

### Why It's Wrong

- Need to edit code to change behavior
- Different team members have different hardcoded values
- No history of configuration changes
- Not reproducible across machines

### The Right Way

```python
# ✓ CORRECT - In config/config.yaml
validation:
  temperature:
    min: 300
    max: 450
  humidity:
    min: 30
    max: 80

# In src/data/data_cleaner.py
import yaml

config = yaml.safe_load(open("config/config.yaml"))
temperature_range = [
    config["validation"]["temperature"]["min"],
    config["validation"]["temperature"]["max"]
]
```

## 9. Not Handling Null/Missing Values

### The Mistake

```python
# ✗ WRONG
model = train_model(train_df)
# train_df has NULL values in some rows
# Model silently ignores them or crashes
```

### Why It's Wrong

- Silent data loss
- Model trains on incomplete data
- Results unreliable
- May crash unpredictably

### The Right Way

```python
# ✓ CORRECT
# Option 1: Reject NULLs in validation
if col("temperature").isNull():
    mark_as_invalid()

# Option 2: Explicitly remove NULLs
training_df = train_df.filter(col("temperature").isNotNull())

# Option 3: Fill NULLs with default
filled_df = train_df.fillna({"temperature": 0})

# Then train
model = train_model(training_df)
```

## 10. Running Without Understanding What Will Happen

### The Mistake

```bash
# ✗ WRONG
# Seeing a script someone wrote
python3 scripts/run_training.py
# What just happened?
# Where did my data go?
# Did it work?
```

### Why It's Wrong

- Don't know if results are correct
- Can't debug problems
- Can't explain to others
- Repeat mistakes

### The Right Way

```bash
# ✓ CORRECT
# Read documentation first
# Understand what each stage does
# Check code to see exact operations
# Run a test version first

# Then run full pipeline
python3 scripts/run_training.py

# Inspect results
sqlite3 data/processed/valid_data/sensor_data.db "SELECT COUNT(*) FROM sensor_readings"
head data/processed/invalid_data/part-*.csv

# Understand what you got
```

## 11. Assuming Data is Correct

### The Mistake

```bash
# ✗ WRONG
# CSV file provided by team
# Load and train immediately
python3 scripts/run_training.py
# Model performs terribly
# "Why is it so bad?"
```

### Why It's Wrong

- Data quality often unknown
- Hidden errors in source data
- Validation catches real problems
- Model only as good as training data

### The Right Way

```python
# ✓ CORRECT
# Always validate data first

valid_count = valid_df.count()
invalid_count = invalid_df.count()
quality_score = valid_count / (valid_count + invalid_count)

print(f"Data quality: {quality_score:.1%}")

if quality_score < 0.5:
    print("WARNING: Less than 50% valid data!")
    print("Investigate data source before proceeding")
    
    # Show rejection reasons
    invalid_df.select("reasons").show()
```

## 12. Not Testing Before Deploying

### The Mistake

```bash
# ✗ WRONG
# Make changes to production code
git push
# Wait for complaints from users
```

### Why It's Wrong

- Breaks production
- Users affected immediately
- Hard to fix under pressure
- Reputation damaged

### The Right Way

```bash
# ✓ CORRECT
# Test thoroughly before pushing

# 1. Test locally
python3 scripts/run_training.py

# 2. Check results
sqlite3 data/processed/valid_data/sensor_data.db ".schema"
sqlite3 data/processed/valid_data/sensor_data.db "SELECT COUNT(*) FROM sensor_readings"

# 3. Review code changes
git diff

# 4. Commit with clear message
git commit -m "Fix temperature validation range"

# 5. Push only when ready
git push

# 6. Monitor for issues
# (in production, use logging/monitoring)
```

## Quick Reference: Do's and Don'ts

| Don't | Do |
|------|-----|
| ✗ Modify raw data | ✓ Process in code |
| ✗ Hardcode paths | ✓ Use config files |
| ✗ Ignore errors | ✓ Handle and log errors |
| ✗ Change schema | ✓ Update code instead |
| ✗ Skip validation | ✓ Always validate |
| ✗ Assume data quality | ✓ Check data first |
| ✗ Deploy untested | ✓ Test thoroughly |
| ✗ Rename columns randomly | ✓ Update everywhere |
| ✗ Hardcode ranges | ✓ Move to config |
| ✗ Ignore nulls | ✓ Handle explicitly |

## Getting Help

When you make a mistake:

1. **Don't panic** - Mistakes happen
2. **Don't hide it** - Tell your team
3. **Find solution** - Check [Troubleshooting](./15-troubleshooting.md)
4. **Learn from it** - Prevent next time
5. **Document** - Help others avoid it

---

**Key Takeaway**: Most junior developer mistakes stem from: not understanding the code, assuming things work, ignoring data quality, or hardcoding values. Read documentation, validate assumptions, and test thoroughly.
