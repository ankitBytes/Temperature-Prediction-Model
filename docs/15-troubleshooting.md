# Troubleshooting Guide

## Common Issues and Solutions

### Installation & Setup Issues

#### Issue: Python 3.9+ Not Found

**Error Message**:
```
python: No module named venv
Command 'python3' not found
```

**Cause**: Python version < 3.9 or not installed

**Solution**:
```bash
# Check Python version
python3 --version

# If < 3.9, install Python 3.9+
# Ubuntu: sudo apt-get install python3.9
# Mac: brew install python@3.9
# Windows: Download from python.org

# Use specific version
python3.9 -m venv venv
```

#### Issue: PySpark/Java Not Found

**Error Message**:
```
Error running exec: 'java': executable file not found in $PATH
```

**Cause**: Java not installed (required for Spark)

**Solution**:
```bash
# Check Java
java -version

# If not found, install:
# Ubuntu:
sudo apt-get install openjdk-11-jdk

# Mac:
brew install openjdk@11

# Windows: Download from oracle.com
```

#### Issue: Module Import Error

**Error Message**:
```
ModuleNotFoundError: No module named 'pyspark'
```

**Cause**: Dependencies not installed or wrong Python environment

**Solution**:
```bash
# Verify in virtual environment
which python
# Should show: /path/to/venv/bin/python

# If not, activate:
source venv/bin/activate

# Reinstall:
pip install -r requirements.txt

# Verify:
python -c "import pyspark; print('OK')"
```

---

### Data Loading Issues

#### Issue: CSV File Not Found

**Error Message**:
```
java.io.FileNotFoundException: /path/to/file.csv
```

**Cause**: Wrong file path or file missing

**Solution**:
```bash
# Verify file exists
ls -la data/raw/ttemperature_regulation_smart_manufacturing.csv

# If missing, check correct location
find . -name "*.csv" -type f

# If path is hardcoded, edit:
# src/data/data_loader.py line 8
# data/raw/  → update path
```

#### Issue: CSV Header Not Recognized

**Error**: Transformed data missing expected columns

**Cause**: Column names in CSV don't match code expectations

**Solution**:
```bash
# Check actual CSV headers
head -1 data/raw/ttemperature_regulation_smart_manufacturing.csv

# Expected:
# Timestamp, Current Temperature (°C), Humidity (%)

# If different, update data_loader.py:
# col("Actual Column Name").cast("double")
```

#### Issue: Invalid CSV Format

**Error**: `ValueError: invalid literal for int() with base 10: '...'`

**Cause**: Non-numeric values in numeric columns

**Solution**:
```bash
# Inspect problematic rows
grep -n "[^0-9.-]" data/raw/ttemperature_regulation_smart_manufacturing.csv | head

# Clean CSV:
# 1. Open in spreadsheet (LibreOffice, Excel)
# 2. Remove/fix non-numeric rows
# 3. Save as CSV
# 4. Verify: head data/raw/*.csv
```

---

### Database Issues

#### Issue: Database Locked

**Error Message**:
```
sqlite3.OperationalError: database is locked
```

**Cause**: Another process accessing database or unclean shutdown

**Solution**:
```bash
# Kill any running processes
ps aux | grep run_training.py
kill -9 <PID>

# Remove lock file
rm data/processed/valid_data/sensor_data.db-journal

# Try again
python3 scripts/run_training.py
```

#### Issue: Database Corrupted

**Error**: Random sqlite3 errors or incorrect data

**Cause**: Incomplete write or file corruption

**Solution**:
```bash
# Backup current database
cp data/processed/valid_data/sensor_data.db data/processed/valid_data/sensor_data.db.backup

# Delete and recreate
rm data/processed/valid_data/sensor_data.db
rm data/processed/invalid_data/*

# Run pipeline again
python3 scripts/run_training.py

# If still failing, check disk space:
df -h data/processed/
```

#### Issue: Cannot Query Database

**Error**: `no such table: sensor_readings`

**Cause**: Database hasn't been created yet or tables not created

**Solution**:
```bash
# Run pipeline to create tables
python3 scripts/run_training.py

# Verify tables created
sqlite3 data/processed/valid_data/sensor_data.db ".tables"
# Should show: sensor_features  sensor_readings
```

---

### Validation Issues

#### Issue: All Records Marked Invalid

**Error**: Valid data is 0, invalid is 1000+

**Cause**: Validation ranges too strict for data

**Solution**:
```bash
# Check actual temperature range
sqlite3 data/processed/valid_data/sensor_data.db \
  "SELECT MIN(temperature), MAX(temperature) FROM sensor_readings"

# If none recorded, check CSV:
head -20 data/raw/*.csv | grep -v "^="

# Edit validation ranges in src/data/data_cleaner.py:
temperature_range = [min_value, max_value]

# Rerun:
python3 scripts/run_training.py
```

#### Issue: Too Many Records Invalid

**Error**: Valid records < 50%

**Cause**: Sensor malfunction, wrong ranges, or data quality issues

**Solution**:
1. **Inspect invalid records**:
```bash
head data/processed/invalid_data/part-*.csv
```

2. **Analyze rejection reasons**:
```bash
cut -d',' -f5 data/processed/invalid_data/part-*.csv | sort | uniq -c
```

3. **Adjust ranges if appropriate**:
```python
# src/data/data_cleaner.py
temperature_range = [200, 500]  # Wider
```

4. **Or investigate data quality**

---

### Memory & Performance Issues

#### Issue: Out of Memory Error

**Error Message**:
```
java.lang.OutOfMemoryError: Java heap space
```

**Cause**: Spark using too much memory or too many cores

**Solution**:
```bash
# Edit scripts/run_training.py, line 127:
.master("local[2]")  # Use 2 cores instead of local[*]

# Or set memory limit:
.config("spark.driver.memory", "2g")

# Rerun:
python3 scripts/run_training.py
```

#### Issue: Pipeline Runs Very Slowly

**Cause**: Using too few cores or disk I/O bottleneck

**Solution**:
```bash
# Edit scripts/run_training.py, line 127:
.master("local[*]")  # Use all cores

# Check disk space:
df -h data/

# Run pipeline:
python3 scripts/run_training.py
```

#### Issue: Spark Startup Takes Long Time

**Expected**: 1-2 minutes first run, 30-45 sec after

**Solution**: Patient waiting, this is normal Spark overhead

---

### Model Training Issues

#### Issue: Model Training Fails

**Error**: Various Spark ML errors

**Cause**: Invalid feature vectors or missing target values

**Solution**:
```python
# Create test_train.py
from pyspark.sql import SparkSession
from src.data.data_loader import load_sensor_data
from src.data.data_cleaner import validate_reading
from src.features.feature_engineer import *

spark = SparkSession.builder.appName("Test").master("local[1]").getOrCreate()
df = load_sensor_data(spark)
df = validate_reading(df)[0]  # Get valid_df
df = create_temperature_change(df)
df = create_rolling_average(df)
df = create_target(df)

# Check for nulls
df.select("target_temperature").describe().show()

# Filter nulls
training_df = df.filter(df.target_temperature.isNotNull())
print(f"Training records: {training_df.count()}")

spark.stop()
```

#### Issue: Poor Model Performance (High MSE/MAE)

**Expected**: MSE ~2-5, MAE ~1-2

**Cause**: 
- Insufficient features
- Wrong feature selection
- Data quality issues

**Solution**:
1. Check training data quality
2. Engineer better features
3. Try different model (Random Forest)
4. Review feature correlations

---

### Configuration Issues

#### Issue: Configuration File Not Found

**Error**: `FileNotFoundError: [Errno 2] No such file or directory: 'config/database.yaml'`

**Cause**: Wrong working directory or config file missing

**Solution**:
```bash
# Verify working directory
pwd
# Should end with: temperature-prediction-pyspark

# Verify config file
ls -la config/database.yaml

# If missing, create it:
cat > config/database.yaml << 'EOF'
database:
  type: sqlite
  path: "/full/path/to/data/processed/valid_data/sensor_data.db"
  table: "sensor_readings"
EOF
```

#### Issue: Invalid YAML Configuration

**Error**: `yaml.YAMLError` or similar

**Cause**: YAML syntax error (indentation, special chars)

**Solution**:
```bash
# Validate YAML
python3 -c "import yaml; yaml.safe_load(open('config/database.yaml'))"

# Fix common issues:
# - Check indentation (use spaces, not tabs)
# - Quote paths with spaces
# - No trailing colons
# - Proper list syntax (- item1, - item2)
```

---

### Execution Issues

#### Issue: No Output for Long Time

**Error**: Pipeline appears hung

**Cause**: 
- First run Spark startup
- Large data processing
- Disk I/O bottleneck

**Solution**:
```bash
# Check if process running (in new terminal)
ps aux | grep run_training

# Monitor logs
tail -f logs/pipeline.log  # If logging implemented

# Wait up to 5 minutes
# If truly hung, Ctrl+C and check error messages
```

#### Issue: Pipeline Stops Unexpectedly

**Error**: Process exits without error message

**Cause**: Out of memory, killed by OS, or dependency issue

**Solution**:
```bash
# Run with error output
python3 -u scripts/run_training.py 2>&1 | tee output.log

# Check logs
cat output.log | tail -50

# Check system resources
free -h
df -h
```

#### Issue: Keyboard Interrupt (Ctrl+C)

**Expected behavior**: Safe to interrupt at any time

**Recovery**:
```bash
# Check database status
sqlite3 data/processed/valid_data/sensor_data.db ".tables"

# If locked, remove lock file
rm data/processed/valid_data/sensor_data.db-journal

# Restart pipeline
python3 scripts/run_training.py
```

---

### IDE/Development Issues

#### Issue: VS Code Doesn't Recognize Modules

**Error**: "Cannot find implementation or library stub"

**Cause**: VS Code not using virtual environment

**Solution**:
```bash
# 1. Open VS Code settings (Ctrl+,)
# 2. Search: "Python Interpreter"
# 3. Set to: /path/to/venv/bin/python

# Or create .vscode/settings.json:
{
  "python.defaultInterpreterPath": "${workspaceFolder}/venv/bin/python"
}
```

#### Issue: Pylint Warnings for PySpark

**Cause**: Pylint doesn't understand Spark dynamically

**Solution**:
```python
# Add to top of file
# pylint: disable=no-member
from pyspark.sql import SparkSession
```

---

### Getting Help

#### Where to Look

1. **This guide**: Troubleshooting issues above
2. **[Setup Guide](./12-setup-guide.md)**: Installation problems
3. **[Running Pipeline](./13-running-pipeline.md)**: Execution help
4. **[Architecture](./03-architecture.md)**: Understanding flow
5. **Error message**: Search online for exact error text

#### What to Include When Asking for Help

```
1. Error message (full text)
2. Command you ran
3. Output (all lines shown)
4. Environment:
   - OS (Linux/Mac/Windows)
   - Python version
   - PySpark version
5. File paths and directory structure
6. What you tried to fix it
```

---

**Key Takeaway**: Most issues fall into categories: setup, data, database, configuration, or resources. Start by identifying which category and follow the corresponding troubleshooting steps.
