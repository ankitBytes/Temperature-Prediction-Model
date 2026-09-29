# Temperature Prediction Model Using PySpark

A complete data pipeline for predicting temperature in smart manufacturing environments using PySpark, SQLite, and machine learning.

## Quick Start

```bash
# 1. Setup (one time)
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Run the pipeline
python3 scripts/run_training.py
```

## What This Project Does

This is a **data pipeline and ML project** that:

1. **Loads** sensor data from CSV (~1000 readings)
2. **Validates** each reading against quality rules
3. **Separates** valid and invalid data
4. **Engineers** features (temperature change, rolling average, target)
5. **Stores** validated data and features in SQLite database
6. **Trains** a LinearRegression model to predict future temperature
7. **Evaluates** model performance with MSE and MAE metrics

**Technology**: PySpark (distributed data processing) + SQLite (persistence) + Python

---

## 📚 Documentation

**All documentation is in the `docs/` directory.**

### Getting Started
- **[Setup Guide](docs/12-setup-guide.md)** - Install and configure
- **[Running the Pipeline](docs/13-running-pipeline.md)** - Execute and inspect results
- **[Project Overview](docs/01-project-overview.md)** - What this does and why

### Understanding the Project
- **[Architecture](docs/03-architecture.md)** - High-level system design with Mermaid diagram
- **[Data Flow](docs/08-data-flow.md)** - Complete journey of one record through the system
- **[Project Structure](docs/04-project-structure.md)** - Directory layout and file purposes
- **[Technology Stack](docs/02-technology-stack.md)** - Technologies used and why

### How Things Work
- **[Data Ingestion](docs/05-data-ingestion.md)** - How CSV is loaded and transformed
- **[Data Validation](docs/06-data-validation.md)** - Validation rules and invalid data handling
- **[Feature Engineering](docs/07-feature-engineering.md)** - Creating ML features
- **[Database Implementation](docs/09-database.md)** - SQLite schema and operations
- **[Configuration Management](docs/10-configuration.md)** - How settings are managed
- **[Model Training](docs/11-model-training.md)** - ML model implementation

### Troubleshooting & Development
- **[Troubleshooting Guide](docs/15-troubleshooting.md)** - Common problems and solutions
- **[Development Guide](docs/16-development-guide.md)** - How to modify the project
- **[Common Mistakes](docs/18-common-mistakes.md)** - What junior developers do wrong
- **[Current Limitations](docs/19-limitations.md)** - Known gaps and future improvements

### Advanced
- **[Rebuild from Scratch](docs/17-rebuild-from-scratch.md)** - Complete step-by-step reconstruction
- **[MLOps Concepts](docs/20-mlops-concepts.md)** - How this relates to data engineering

**[📖 View Full Documentation Index](docs/README.md)**

---

## Project Structure

```
temperature-prediction-pyspark/
├── config/                          # Configuration files
│   ├── database.yaml               # Database settings
│   ├── config.yaml                 # Main config (empty)
│   └── logging_config.yaml         # Logging config (empty)
├── data/
│   ├── raw/                        # Raw input data
│   │   └── ttemperature_*.csv     # 1000+ sensor readings
│   └── processed/
│       ├── valid_data/
│       │   └── sensor_data.db      # SQLite with validated records
│       └── invalid_data/           # Invalid records as CSV
├── docs/                            # 📚 Complete documentation
├── pipelines/                       # Pipeline orchestration
│   └── training_pipeline.py        # Model training workflow
├── scripts/                         # Executable entry points
│   └── run_training.py             # Main script (run this!)
├── src/                             # Source code
│   ├── data/
│   │   ├── data_loader.py          # Load CSV
│   │   └── data_cleaner.py         # Validate data
│   ├── features/
│   │   ├── feature_engineer.py     # Create ML features
│   │   └── feature_store.py        # Store features in DB
│   ├── model/
│   │   ├── model_trainer.py        # Train LinearRegression
│   │   └── model_evaluator.py      # Evaluate performance
│   ├── inference/                  # Inference (not implemented)
│   └── utils/                      # Utilities
└── tests/                           # Unit tests (not implemented)
```

---

## Quick Examples

### Run the Complete Pipeline

```bash
python3 scripts/run_training.py
```

**Output**: 
- SQLite database with ~990 validated records
- CSV file with 10 invalid records (with rejection reasons)
- Model trained on 70% of data
- Metrics printed (MSE, MAE)

### Inspect Results

```bash
# View valid records
sqlite3 data/processed/valid_data/sensor_data.db "SELECT COUNT(*) FROM sensor_readings"

# View engineered features
sqlite3 data/processed/valid_data/sensor_data.db "SELECT * FROM sensor_features LIMIT 5"

# View invalid records and rejection reasons
head data/processed/invalid_data/part-00000-*.csv
```

### Modify Validation Ranges

```python
# Edit src/data/data_cleaner.py
temperature_range = [200, 500]  # Changed from [300, 450]
humidity_range = [20, 90]       # Changed from [30, 80]

# Rerun pipeline
python3 scripts/run_training.py
```

---

## Key Concepts

### ETL (Extract, Transform, Load)
- **Extract**: CSV data → Spark DataFrame
- **Transform**: Validate, clean, engineer features
- **Load**: Store in SQLite database

### Data Validation
Checks each record for:
- Required fields not null
- Values within expected ranges
- Invalid records separated and preserved

### Features
- `temperature_change`: How fast temperature is changing
- `rolling_avg_temperature`: 30-minute trend
- `target_temperature`: Future temperature (what model predicts)

### Machine Learning
- **Model**: LinearRegression
- **Input Features**: temperature, humidity, temperature_change, rolling_avg
- **Target**: target_temperature (1 hour in future)
- **Metrics**: MSE (Mean Squared Error), MAE (Mean Absolute Error)

---

## Requirements

- **Python**: 3.9 or higher
- **PySpark**: 3.5.0+
- **RAM**: ~2 GB
- **Disk**: ~500 MB

## Installation

```bash
# Create virtual environment
python3 -m venv venv

# Activate it
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
```

**[Full setup guide →](docs/12-setup-guide.md)**

## Running

```bash
# Activate virtual environment first
source venv/bin/activate

# Run pipeline (processes CSV → database → model)
python3 scripts/run_training.py

# Expected output
# Loaded 1000 records
# Valid: 990 records
# Invalid: 10 records
# Training model...
# Validation MSE: 2.45
# Validation MAE: 1.23
```

**[Running guide with debugging →](docs/13-running-pipeline.md)**

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `python3: command not found` | Install Python 3.9+ |
| `ModuleNotFoundError: No module named 'pyspark'` | Activate venv, install requirements |
| `database is locked` | Kill process, delete `.db-journal`, restart |
| CSV not found | Check path in `src/data/data_loader.py` |
| Pipeline takes forever | First run is slow (Spark startup) |

**[Full troubleshooting guide →](docs/15-troubleshooting.md)**

## What's Implemented ✓

- ✓ CSV data loading with automatic schema inference
- ✓ Data validation with range checks
- ✓ Invalid data separation and storage
- ✓ SQLite database with unique constraint handling
- ✓ Feature engineering (3 features created)
- ✓ Linear regression model training
- ✓ Model evaluation (MSE, MAE metrics)
- ✓ Chronological train/validation/test split

## What's NOT Implemented ✗

- ✗ Model persistence (model not saved)
- ✗ Logging system (logs not written)
- ✗ Error handling (any error stops pipeline)
- ✗ Unit tests (no automated tests)
- ✗ Inference pipeline (can't predict on new data)
- ✗ Monitoring (no drift detection)
- ✗ Scheduling (must run manually)

**[Full limitations list →](docs/19-limitations.md)**

## Development

### Adding a New Feature

1. Create function in `src/features/feature_engineer.py`
2. Use it in `scripts/run_training.py`
3. Update model columns in `src/model/model_trainer.py`
4. Test: `python3 scripts/run_training.py`

### Changing Validation Rules

Edit `src/data/data_cleaner.py`:
```python
temperature_range = [300, 450]  # Adjust here
```

### Modifying Database

Edit `src/features/feature_store.py`:
```python
cursor.execute("""CREATE TABLE IF NOT EXISTS sensor_features (
    ... column definitions ...
)""")
```

**[Development guide →](docs/16-development-guide.md)**

## Performance

- **Dataset**: 1000 records
- **Runtime**: 30-45 seconds (after first run)
- **Database size**: ~50 KB
- **Valid records**: ~990 (99%)
- **Model MSE**: ~2.45

---

## Contributing

Before modifying:
1. Read [Development Guide](docs/16-development-guide.md)
2. Backup database
3. Test your changes
4. Check [Common Mistakes](docs/18-common-mistakes.md)
5. Update documentation

## Architecture Overview

```
Raw CSV Data
    ↓
Data Loading (PySpark)
    ↓
Data Validation
    ↓
Split: Valid → Database | Invalid → CSV
    ↓ (valid data)
Feature Engineering
    ↓
Feature Storage (Database)
    ↓
Train/Validation/Test Split (70/15/15)
    ↓
Model Training (LinearRegression)
    ↓
Model Evaluation (MSE, MAE)
    ↓
Metrics (printed to console)
```

**[Full architecture diagram →](docs/03-architecture.md)**

## Database Schema

### sensor_readings table
```sql
id (PK) | sensor_id | timestamp | temperature | humidity
```

### sensor_features table
```sql
id (PK) | sensor_id | timestamp | temperature | humidity | temperature_change | rolling_avg_temperature
```

**[Database docs →](docs/09-database.md)**

## Configuration

**File**: `config/database.yaml`

```yaml
database:
  type: sqlite
  path: "/path/to/sensor_data.db"
  table: "sensor_readings"
```

**[Configuration docs →](docs/10-configuration.md)**

## Next Steps

1. **Just starting?** → [Setup Guide](docs/12-setup-guide.md)
2. **Want to run it?** → [Running the Pipeline](docs/13-running-pipeline.md)
3. **Want to understand it?** → [Project Overview](docs/01-project-overview.md)
4. **Want to modify it?** → [Development Guide](docs/16-development-guide.md)
5. **Something broken?** → [Troubleshooting](docs/15-troubleshooting.md)

---

## Project Information

- **Type**: Data Pipeline + Machine Learning
- **Language**: Python 3.9+
- **Framework**: PySpark 3.5.0+
- **Database**: SQLite
- **Status**: Functional (not production-ready)

## Known Issues

- Model not saved after training (must retrain each run)
- No logging implemented
- No error recovery
- All paths are absolute (not portable)
- Only handles single sensor

**[Full limitations →](docs/19-limitations.md)**

## License

MIT License (assumed)

## Support

- **Documentation**: See `docs/` directory
- **Troubleshooting**: [Troubleshooting Guide](docs/15-troubleshooting.md)
- **Questions**: Check documentation before asking

---

**Last Updated**: September 2024  
**Documentation**: Complete and comprehensive  
**Ready for**: Development, learning, testing
