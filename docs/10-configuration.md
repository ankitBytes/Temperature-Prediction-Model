# Configuration Management

## Overview

Configuration centralizes project settings so they can be changed without modifying code.

## Configuration Files

### 1. database.yaml - Database Configuration

**Location**: `config/database.yaml`

**Content**:
```yaml
database:
  type: sqlite
  path: "/home/vvdn/Desktop/Projects/Python/Temperature-prediction-model-using-pyspark/data/processed/valid_data/sensor_data.db"
  table: "sensor_readings"
```

**How it's used** (`scripts/run_training.py` lines 22-25):
```python
import yaml

with open("config/database.yaml", "r") as file:
    config = yaml.safe_load(file)

db_path = config["database"]["path"]
connection = sqlite3.connect(db_path)
```

**Changing database location**:
```yaml
database:
  type: sqlite
  path: "/tmp/my_database.db"  # Changed path
  table: "sensor_readings"
```

### 2. config.yaml - Main Configuration (Empty)

**Location**: `config/config.yaml`

**Status**: Empty - ready for expansion

**Could contain**:
```yaml
# Data pipeline settings
data:
  raw_csv_path: "data/raw/ttemperature_regulation_smart_manufacturing.csv"
  sensor_id: "MANUFACTURING_01"
  batch_size: 1000

# Validation ranges
validation:
  temperature:
    min: 300
    max: 450
  humidity:
    min: 30
    max: 80

# Model settings
model:
  algorithm: "LinearRegression"
  train_ratio: 0.70
  validation_ratio: 0.15
  test_ratio: 0.15

# Features
features:
  look_ahead_hours: 1
  rolling_window_minutes: 30
```

### 3. logging_config.yaml - Logging Configuration (Empty)

**Location**: `config/logging_config.yaml`

**Status**: Empty - logging not implemented

**Could contain**:
```yaml
version: 1
disable_existing_loggers: false

formatters:
  simple:
    format: '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

handlers:
  console:
    class: logging.StreamHandler
    level: DEBUG
    formatter: simple
    
  file:
    class: logging.FileHandler
    level: INFO
    formatter: simple
    filename: 'logs/pipeline.log'

root:
  level: DEBUG
  handlers: [console, file]
```

## Hardcoded Configuration (Not in YAML)

### Location 1: data_loader.py

**Sensor ID** (line 15):
```python
.withColumn("sensor_id", lit("MANUFACTURING_01"))
```

**CSV path** (line 8):
```python
.csv("/home/vvdn/Desktop/Projects/Python/.../ttemperature_regulation_smart_manufacturing.csv")
```

**Should be**: In config.yaml
**Current**: Hardcoded

### Location 2: data_cleaner.py

**Temperature range** (line 10):
```python
temperature_range = [300, 450]
```

**Humidity range** (line 11):
```python
humidity_range = [30, 80]
```

**Should be**: In config.yaml
**Current**: Hardcoded

### Location 3: feature_engineer.py

**Rolling window duration** (line 30):
```python
.rangeBetween(-30 * 60, 0)  # 30 minutes in seconds
```

**Look-ahead rows** (line 50):
```python
lead("temperature", 12)  # 12 readings ahead
```

**Should be**: In config.yaml
**Current**: Hardcoded

### Location 4: run_training.py

**Invalid data path** (line 131):
```python
processed_data_path = "data/processed/invalid_data"
```

**Train/val/test split** (lines 44-45):
```python
train_end = int(total_rows * 0.70)
validation_end = train_end + int(total_rows * 0.15)
```

**Should be**: In config.yaml
**Current**: Hardcoded

## Configuration Flow

```
config/database.yaml
        ↓
    yaml.safe_load(file)
        ↓
    Python dict: {"database": {"type": "sqlite", "path": "..."}}
        ↓
    config["database"]["path"]
        ↓
    sqlite3.connect(path)
        ↓
    Database connection
```

## Environment-Specific Configuration

### Development Configuration

```yaml
# config/dev.yaml
database:
  type: sqlite
  path: "./data/dev/sensor_data.db"

logging:
  level: DEBUG
```

### Production Configuration

```yaml
# config/prod.yaml
database:
  type: sqlite
  path: "/var/data/sensor_data.db"

logging:
  level: WARNING
```

### Loading environment-specific config

```python
import os
import yaml

env = os.getenv("ENV", "dev")
config_file = f"config/{env}.yaml"

with open(config_file) as f:
    config = yaml.safe_load(f)
```

**Usage**:
```bash
ENV=dev python scripts/run_training.py    # Uses dev config
ENV=prod python scripts/run_training.py   # Uses prod config
```

## Environment Variables

### python-dotenv Integration (Not Currently Used)

Create `.env` file:
```
DATABASE_PATH=/home/user/data/sensor_data.db
SENSOR_ID=MANUFACTURING_01
CSV_PATH=/home/user/data/sensors.csv
```

Load in Python:
```python
from dotenv import load_dotenv
import os

load_dotenv()

db_path = os.getenv("DATABASE_PATH")
sensor_id = os.getenv("SENSOR_ID")
csv_path = os.getenv("CSV_PATH")
```

**Advantages**:
- Keep secrets out of code
- Different per machine
- .env not committed to Git

## Configuration Best Practices

### ✓ Good: Externalized Configuration

```python
# config.yaml
model:
  algorithm: "LinearRegression"
  max_iter: 100

# Load and use
config = yaml.safe_load(open("config.yaml"))
model_type = config["model"]["algorithm"]
```

### ✗ Bad: Hardcoded Configuration

```python
model_type = "LinearRegression"
max_iter = 100
```

### ✓ Good: Environment-Specific

```bash
# development
ENV=dev python pipeline.py

# production
ENV=prod python pipeline.py
```

### ✗ Bad: Machine-Specific Paths

```python
if os.path.exists("/home/alice/data"):  # Alice's machine only
    load_data("/home/alice/data")
elif os.path.exists("/home/bob/data"):  # Bob's machine only
    load_data("/home/bob/data")
```

## Required vs Optional Configuration

### Required
- Database path (must exist, or database created)
- CSV input path (must exist)

### Optional (Have Defaults)
- Validation ranges (defaults: 300-450°C, 30-80%)
- Model algorithm (default: LinearRegression)
- Split ratios (default: 70/15/15)

## Modifying Configuration

### Example: Change Temperature Range

**Current** (in data_cleaner.py):
```python
temperature_range = [300, 450]
```

**To accept wider range**:
```python
temperature_range = [200, 500]
```

**Better**: Move to config.yaml:
```yaml
# config.yaml
validation:
  temperature:
    min: 200
    max: 500
```

**In data_cleaner.py**:
```python
import yaml

config = yaml.safe_load(open("config.yaml"))
temp_min = config["validation"]["temperature"]["min"]
temp_max = config["validation"]["temperature"]["max"]
temperature_range = [temp_min, temp_max]
```

### Example: Change Database Location

**Edit** `config/database.yaml`:
```yaml
database:
  type: sqlite
  path: "/new/path/to/database.db"  # Changed
  table: "sensor_readings"
```

**No code changes needed** - configuration separated from code

## Configuration Validation

Ensure required config exists:

```python
import yaml

def load_config(path):
    with open(path) as f:
        config = yaml.safe_load(f)
    
    # Validate required keys
    required = ["database", "validation"]
    for key in required:
        if key not in config:
            raise ValueError(f"Missing required config: {key}")
    
    return config
```

## Secrets Management

### Current Practice (Not Recommended)

Database path in config file (no secrets currently):
```yaml
database:
  path: "/data/sensor_data.db"  # Not sensitive
```

### Better Practice

Use environment variables for secrets:
```python
import os

db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASSWORD")

# Load from environment, not config file
```

**.env example** (not in Git):
```
DB_USER=admin
DB_PASSWORD=secret123
```

## Configuration as Code Alternative

Instead of YAML, use Python dataclass (not implemented):

```python
from dataclasses import dataclass

@dataclass
class DatabaseConfig:
    type: str = "sqlite"
    path: str = "data/processed/valid_data/sensor_data.db"
    table: str = "sensor_readings"

@dataclass
class AppConfig:
    database: DatabaseConfig
    debug: bool = False
```

**Advantages**: Type checking, IDE autocompletion
**Disadvantages**: Requires code changes for config changes

## Configuration Documentation

Every configuration option should be documented:

```yaml
# config.yaml
database:
  # SQLite database connection
  # path: Location of .db file
  # Must be writable directory
  type: sqlite
  path: "/data/sensor_data.db"
  table: "sensor_readings"  # Table name for sensor data

validation:
  # Data validation rules
  # temperature: Acceptable temperature range in Celsius
  # humidity: Acceptable humidity range in percent
  temperature:
    min: 300   # Minimum °C
    max: 450   # Maximum °C
  humidity:
    min: 30    # Minimum %
    max: 80    # Maximum %
```

## Next Steps

- To use configuration in setup: [Setup Guide](./12-setup-guide.md)
- To run with configuration: [Running the Pipeline](./13-running-pipeline.md)
- To modify configuration: [Development Guide](./16-development-guide.md)

---

**Key Takeaway**: Configuration should be externalized to YAML files, separate from code. Currently, many settings are hardcoded and should be moved to `config/config.yaml` for flexibility.
