# Technology Stack

## Overview

This project uses a carefully selected set of technologies optimized for data pipeline development, validation, and machine learning model training.

## Technology Stack Table

| Technology | Version | Purpose | Where Used | Why Chosen |
|------------|---------|---------|-----------|-----------|
| **PySpark** | >=3.5.0 | Distributed data processing framework | Data loading, validation, feature engineering, model training | Handles large datasets efficiently, provides unified API for data processing and ML |
| **Python** | >=3.9 | Programming language | Entire project | Data science standard, rich ecosystem |
| **SQLite** | Built-in | Lightweight SQL database | Valid data storage, feature storage | Simple setup, no server, sufficient for project scope |
| **NumPy** | >=2.0.0 | Numerical computing library | Available for analysis (not directly used in pipeline) | Efficient array operations, foundation for other libraries |
| **Pandas** | >=2.1.0 | Data manipulation library | Available for analysis (not directly used in pipeline) | Convenient DataFrames and operations |
| **Scikit-Learn** | >=1.3.0 | Machine learning library | Model evaluation metrics (MSE, MAE) | Standard ML evaluation tool |
| **PyYAML** | >=6.0.0 | YAML configuration parser | Configuration file parsing | Human-readable configuration format |
| **Python-dotenv** | >=1.0.0 | Environment variable manager | Available for configuration (not currently used) | Load configuration from .env files |
| **Matplotlib** | >=3.8.0 | Plotting library | Available for visualization (dev dependencies) | Create charts and plots |
| **Seaborn** | >=0.13.0 | Statistical visualization | Available for visualization (dev dependencies) | Enhanced matplotlib plots |
| **pytest** | >=7.4.0 | Testing framework | Testing infrastructure (not implemented yet) | Standard Python testing framework |
| **pytest-cov** | >=4.1.0 | Test coverage measurement | Testing infrastructure (not implemented yet) | Measure test coverage |

## Core Technologies Deep Dive

### PySpark (>=3.5.0)

**What it is**: A distributed data processing framework that runs on Hadoop or standalone

**How it works**:
- Divides data into partitions across multiple nodes (or cores on single machine)
- Processes each partition in parallel
- Combines results efficiently

**Used in this project for**:
1. **CSV Loading** (`data_loader.py`)
   - `spark.read.csv()` with automatic schema inference
   - Transforms column names and types
   
2. **Data Validation** (`data_cleaner.py`)
   - Row-by-row validation with complex conditions
   - Array operations for rejection reasons
   
3. **Feature Engineering** (`feature_engineer.py`)
   - Window functions for rolling calculations
   - Lag/lead functions for time-series features
   
4. **Model Training** (`model_trainer.py`)
   - PySpark ML for Linear Regression
   - VectorAssembler for feature vectors

**Why chosen**:
- Handles large datasets efficiently
- Provides both SQL and DataFrame APIs
- Built-in ML library (pyspark.ml)
- Scales from single machine to cluster

### SQLite (Built-in with Python)

**What it is**: A lightweight, serverless SQL database engine

**How it works**:
- Stores database as single file on disk
- Executes SQL queries through Python sqlite3 module
- Maintains ACID properties (Atomicity, Consistency, Isolation, Durability)

**Used in this project for**:
1. **sensor_readings table**
   - Stores validated sensor data
   - `(id, sensor_id, timestamp, temperature, humidity)`
   - Unique constraint on (sensor_id, timestamp) prevents duplicates

2. **sensor_features table**
   - Stores engineered features
   - `(id, sensor_id, timestamp, temperature, humidity, temperature_change, rolling_avg_temperature)`
   - Unique constraint on (sensor_id, timestamp) prevents duplicate features

**Connection method**:
```python
import sqlite3
connection = sqlite3.connect(db_path)
cursor = connection.cursor()
```

**Why chosen**:
- No server installation needed
- Perfect for data volumes in this project
- Python standard library support
- Easy to inspect database (can open file directly)
- Sufficient for development and small-scale deployment

### Python (>=3.9)

**Why 3.9+**:
- Modern language features (type hints, f-strings)
- Good async support (for future monitoring)
- Security patches
- Good NumPy/Pandas compatibility

### Scikit-Learn (>=1.3.0)

**Why it's here**: PySpark ML uses scikit-learn style APIs for model evaluation

**Used for**:
- `RegressionEvaluator` - calculates MSE and MAE metrics
- Not used for model training (PySpark ML's LinearRegression is used instead)

**Typical pattern**:
```python
from pyspark.ml.evaluation import RegressionEvaluator

evaluator = RegressionEvaluator(
    labelCol="target_temperature",
    predictionCol="prediction", 
    metricName="mse"
)
mse = evaluator.evaluate(predictions_df)
```

### NumPy & Pandas (Available but not directly used)

**Why included**:
- Standard tools for data analysis
- Available if analysis is needed
- Foundation for other scientific libraries

**Could be used for**:
- Post-pipeline analysis
- Data exploration before training
- Generating reports

### PyYAML (>=6.0.0)

**What it does**: Parses YAML configuration files into Python dictionaries

**Used in this project**:
- Reading `config/database.yaml` in `run_training.py`

**Pattern used**:
```python
import yaml
with open("config/database.yaml", "r") as file:
    config = yaml.safe_load(file)
db_path = config["database"]["path"]
```

**Why YAML**:
- Human-readable format
- Better for configuration than JSON or XML
- Standard for infrastructure/ops tools (Kubernetes, Ansible, etc.)

### Python-dotenv (>=1.0.0)

**Status**: Included in requirements but not currently used

**Purpose**: Load environment variables from `.env` file

**Could be used for**:
- Database credentials
- API keys
- Environment-specific paths
- Sensitive configuration not in version control

**Example usage**:
```python
from dotenv import load_dotenv
load_dotenv()
db_path = os.getenv("DATABASE_PATH")
```

### Visualization Libraries (Matplotlib, Seaborn)

**Status**: Included in dev dependencies

**Purpose**: Create charts and plots

**Could be used for**:
- Distribution plots of temperature readings
- Correlation heatmaps
- Model prediction vs actual plots
- Performance over time plots

**Example**:
```python
import matplotlib.pyplot as plt
import seaborn as sns

sns.heatmap(correlation_matrix, annot=True)
plt.show()
```

### pytest & pytest-cov (Testing)

**Status**: Included but no tests implemented

**Purpose**: Automated testing framework

**File locations**:
- Test files: `tests/test_*.py`
- Currently empty placeholder files exist

**Example test pattern** (not yet implemented):
```python
import pytest
from src.data.data_loader import load_sensor_data

def test_load_sensor_data(spark):
    df = load_sensor_data(spark)
    assert df.count() > 0
    assert "sensor_id" in df.columns
```

## Dependency Installation Flow

```
requirements.txt
    ↓
pip install -r requirements.txt
    ↓
Installs all packages
    ↓
setup.py specifies core vs dev dependencies
```

**Core dependencies** (always installed):
- pyspark
- numpy
- pandas
- scikit-learn
- pyyaml
- python-dotenv

**Development dependencies** (installed with `pip install -e .[dev]`):
- pytest
- pytest-cov
- matplotlib
- seaborn

## Why These Technologies?

### Distributed Processing (PySpark)
Manufacturing data can be large. PySpark allows processing on single machine now, scaling to clusters later.

### Data Validation (Built-in)
Python and PySpark provide all needed validation logic. No external validation frameworks used.

### Database (SQLite)
Perfect for this project size. Easy to inspect, no setup, sufficient performance.

### Configuration (YAML)
Human-readable, standard in DevOps, easier to maintain than code configuration.

### Machine Learning (PySpark ML)
Integrated with data processing pipeline. Can be replaced with other ML libraries if needed.

## Technology Decision Points

| Decision | Alternative | Why Chosen |
|----------|-------------|-----------|
| **PySpark for processing** | Pandas, Polars | Better for large data, integrated ML |
| **SQLite for database** | PostgreSQL, MongoDB | Simpler setup, sufficient scale |
| **Linear Regression** | Neural networks, forests | Simpler, interpretable, good baseline |
| **YAML config** | JSON, environment vars | Human-readable, standard |
| **Python 3.9+** | Python 3.8 | Modern features, security patches |

## Missing Technologies (Opportunities for Enhancement)

These technologies would improve the project but aren't currently used:

- **Logging Framework** (Python `logging`): Better observability
- **Apache Airflow**: Pipeline orchestration and scheduling
- **Docker**: Containerization for consistent environments
- **REST Framework** (FastAPI): For inference API
- **Database ORM** (SQLAlchemy): Better database abstraction
- **ML Pipeline** (MLflow): Model versioning and tracking
- **Monitoring** (Prometheus): Real-time metrics collection
- **CI/CD** (GitHub Actions): Automated testing and deployment

## Next Steps

- To see how these technologies are used together, read [Project Architecture](./03-architecture.md)
- To understand configuration, read [Configuration Management](./10-configuration.md)
- To see actual usage, read [Running the Pipeline](./13-running-pipeline.md)

---

**Key Takeaway**: This project uses PySpark for distributed data processing, SQLite for persistence, and Python for orchestration - a solid foundation for data pipeline work.
