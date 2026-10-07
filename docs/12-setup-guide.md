# Complete Setup and Installation Guide

## Prerequisites

Before starting, ensure you have:
- Linux/Mac/Windows with terminal access
- Python 3.9 or higher (check: `python3 --version`)
- Git (if cloning repository)
- ~2 GB disk space
- Internet connection (for package downloads)

## System Setup

### Check Python Version

```bash
python3 --version
# Output should be: Python 3.9.x or higher
```

If `python3` doesn't work, try `python`:
```bash
python --version
```

### Check pip

```bash
python3 -m pip --version
# Output: pip X.X.X from ...
```

## Step 1: Obtain the Project

### Option A: Clone from Git (if available)

```bash
git clone https://github.com/your-org/temperature-prediction-pyspark.git
cd temperature-prediction-pyspark
```

### Option B: Extract from Archive

```bash
unzip temperature-prediction-pyspark.zip
cd temperature-prediction-pyspark
```

### Option C: Already Have the Project

```bash
cd /path/to/temperature-prediction-pyspark
```

### Verify Project Structure

```bash
ls -la
# Should show: config  data  docs  logs  models  pipelines  scripts  setup.py  requirements.txt  src  tests
```

## Step 2: Create Virtual Environment

Virtual environments isolate project dependencies from system Python.

### Create Virtual Environment

```bash
python3 -m venv venv
```

**Creates**: `venv/` directory with isolated Python

### Activate Virtual Environment

**On Linux/Mac**:
```bash
source venv/bin/activate
```

**On Windows**:
```bash
venv\Scripts\activate
```

**Verify activation**: Prompt should show `(venv)` prefix

**Example**:
```
(venv) $ python --version
Python 3.9.5
```

## Step 3: Upgrade pip

Ensure pip is latest version before installing packages:

```bash
pip install --upgrade pip
```

## Step 4: Install Dependencies

### Option A: Install from requirements.txt (Recommended)

```bash
pip install -r requirements.txt
```

**Installs**:
- pyspark
- numpy, pandas
- scikit-learn
- matplotlib, seaborn
- pyyaml, python-dotenv
- pytest, pytest-cov

**Time**: ~5-10 minutes (depends on internet speed)

**What to expect**:
```
Collecting pyspark==3.5.0
Downloading pyspark-3.5.0.tar.gz (...
Collecting numpy>=2.0.0
...
Successfully installed pyspark-3.5.0 numpy-2.0.0 ...
```

### Option B: Install as Development Package

Installs project + dev dependencies:

```bash
pip install -e ".[dev]"
```

**Installs**:
- All core dependencies (from setup.py)
- All dev dependencies (pytest, matplotlib, seaborn)

### Verify Installation

```bash
pip list
# Should show: pyspark, pandas, numpy, scikit-learn, pyyaml, etc.
```

## Step 5: Verify PySpark Installation

```bash
python3 -c "from pyspark.sql import SparkSession; print('PySpark OK')"
# Output: PySpark OK
```

**If error**:
```bash
# Might need Java installed
java -version

# Or install Java:
# Ubuntu: sudo apt-get install openjdk-11-jdk
# Mac: brew install openjdk@11
```

## Step 6: Verify Project Structure

```bash
# Check key directories exist
test -d config && echo "✓ config/" || echo "✗ Missing config/"
test -d data && echo "✓ data/" || echo "✗ Missing data/"
test -d src && echo "✓ src/" || echo "✗ Missing src/"
test -d scripts && echo "✓ scripts/" || echo "✗ Missing scripts/"

# Check key files exist
test -f requirements.txt && echo "✓ requirements.txt" || echo "✗ Missing requirements.txt"
test -f setup.py && echo "✓ setup.py" || echo "✗ Missing setup.py"
test -f scripts/run_training.py && echo "✓ scripts/run_training.py" || echo "✗ Missing scripts/run_training.py"
test -f config/database.yaml && echo "✓ config/database.yaml" || echo "✗ Missing config/database.yaml"
```

## Step 7: Verify Data Files

```bash
# Check raw data exists
test -f data/raw/ttemperature_regulation_smart_manufacturing.csv && echo "✓ Raw data exists" || echo "✗ Raw data missing"

# Check it has content
wc -l data/raw/ttemperature_regulation_smart_manufacturing.csv
# Should show: 1000+ lines
```

## Step 8: Test Imports

Create test file `test_imports.py`:

```python
#!/usr/bin/env python3

print("Testing imports...")

try:
    import pyspark
    print("✓ pyspark")
except ImportError as e:
    print("✗ pyspark:", e)

try:
    import pandas
    print("✓ pandas")
except ImportError as e:
    print("✗ pandas:", e)

try:
    import numpy
    print("✓ numpy")
except ImportError as e:
    print("✗ numpy:", e)

try:
    import yaml
    print("✓ yaml")
except ImportError as e:
    print("✗ yaml:", e)

try:
    from src.data.data_loader import load_sensor_data
    print("✓ project modules")
except ImportError as e:
    print("✗ project modules:", e)

print("\nAll imports OK!")
```

Run test:

```bash
python3 test_imports.py
# Output should be all checkmarks
```

## Step 9: Verify Configuration

Check that configuration files exist and are readable:

```bash
cat config/database.yaml
# Should show database configuration

cat config/config.yaml
# Should show (empty, that's OK)

cat config/logging_config.yaml
# Should show (empty, that's OK)
```

## Step 10: Run Quick Test

Try running a small data load:

```bash
python3 -c "
from pyspark.sql import SparkSession
from src.data.data_loader import load_sensor_data

spark = SparkSession.builder.appName('Test').master('local[1]').getOrCreate()
df = load_sensor_data(spark)
print(f'Loaded {df.count()} records')
print('Columns:', df.columns)
spark.stop()
"
```

**Expected output**:
```
Loaded 1000+ records
Columns: ['sensor_id', 'timestamp', 'temperature', 'humidity']
```

## Complete Setup Checklist

```
[ ] Python 3.9+ installed
[ ] Virtual environment created
[ ] Virtual environment activated (see (venv) in prompt)
[ ] pip upgraded
[ ] Dependencies installed (pip install -r requirements.txt)
[ ] PySpark installed (python3 -c "from pyspark.sql import SparkSession")
[ ] Project directories exist (config/, data/, src/, etc.)
[ ] Raw data file exists (data/raw/*.csv)
[ ] Configuration files exist (config/database.yaml, etc.)
[ ] Project modules importable (from src.data import data_loader)
[ ] Test run successful (python3 test_imports.py)
```

## Troubleshooting Setup

### Problem: Command Not Found (python3)

**Solution**:
```bash
# Try python instead
python --version
python -m venv venv
```

### Problem: Permission Denied (venv activation)

**Solution** (Linux/Mac):
```bash
chmod +x venv/bin/activate
source venv/bin/activate
```

### Problem: Java Not Found (PySpark)

**Error**: `Error running exec: 'java': executable file not found in $PATH`

**Solution**:
- Install Java: `apt-get install openjdk-11-jdk` (Linux) or `brew install openjdk@11` (Mac)
- Or set JAVA_HOME: `export JAVA_HOME=/path/to/java`

### Problem: pip install fails

**Cause**: Network issue or wrong Python version

**Solution**:
```bash
# Verify Python version
python --version

# Try upgrading pip first
pip install --upgrade pip

# Try installing one package at a time
pip install pyspark
pip install pandas
# ... etc
```

### Problem: Import Error After Installation

**Error**: `ModuleNotFoundError: No module named 'pyspark'`

**Solution**:
```bash
# Check virtual environment is activated
which python
# Should show: /path/to/venv/bin/python

# If not, activate it
source venv/bin/activate

# Reinstall
pip install pyspark
```

## Cleanup

### Deactivate Virtual Environment

```bash
deactivate
# Prompt returns to normal (no (venv) prefix)
```

### Remove Virtual Environment (if needed)

```bash
rm -rf venv
```

### Remove Installed Packages

```bash
pip freeze > requirements_backup.txt
pip uninstall -r requirements_backup.txt
```

## Next Steps

After successful setup:

1. **Run the pipeline**: [Running the Pipeline](./13-running-pipeline.md)
2. **Understand the project**: [Project Overview](./01-project-overview.md)
3. **Explore the code**: [Project Structure](./04-project-structure.md)

## Getting Help

If setup fails:

1. Check [Troubleshooting Guide](./15-troubleshooting.md)
2. Verify all commands match your OS
3. Check Python version (must be 3.9+)
4. Ensure you're in virtual environment
5. Check error messages carefully

---

**Key Takeaway**: Setup involves creating a virtual environment, installing dependencies, and verifying the installation works. Virtual environments keep this project isolated from your system Python.
