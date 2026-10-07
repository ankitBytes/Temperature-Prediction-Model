# Project Overview

## What Is This Project?

This is a **data pipeline and machine learning project** that predicts future temperature values in a smart manufacturing environment. The project processes sensor readings from manufacturing equipment, validates the data quality, engineers features, and trains a machine learning model to forecast temperature behavior.

## Problem Being Solved

In smart manufacturing facilities, temperature control is critical. The project solves two problems:

1. **Data Quality**: Raw sensor data contains errors, missing values, and out-of-range readings that would corrupt analysis
2. **Temperature Prediction**: Predicting future temperature enables proactive control adjustments and prevents equipment failures

## Project Purpose

The main purpose is to build a **production-ready data pipeline** that:
- Ingests sensor data reliably
- Validates and cleans data automatically
- Stores clean data in a database
- Engineers meaningful features from raw measurements
- Trains and evaluates predictive models
- Provides a foundation for real-time monitoring and inference

## Overall Workflow

The project follows a classic data engineering workflow:

```
Raw CSV Data
    ↓
Data Loading (PySpark)
    ↓
Data Validation
    ↓
Separate Valid/Invalid
    ↓
Store in SQLite Database
    ↓
Feature Engineering (create new columns)
    ↓
Train/Validation/Test Split
    ↓
Model Training (Linear Regression)
    ↓
Model Evaluation
    ↓
Metrics & Reports
```

## Main Technologies

### PySpark
- **Purpose**: Distributed data processing framework
- **Why Used**: Handles large datasets efficiently, provides SQL and DataFrame APIs
- **Role in Project**: Loads CSV, validates data, engineers features, trains models

### SQLite
- **Purpose**: Lightweight relational database
- **Why Used**: Simple to set up, no server needed, sufficient for this scope
- **Role in Project**: Stores valid sensor readings and engineered features

### Python
- **Purpose**: Programming language
- **Why Used**: Data science standard, extensive libraries
- **Role in Project**: Orchestrates entire pipeline

### NumPy & Pandas
- **Purpose**: Data manipulation libraries
- **Why Used**: Convenient data structures and operations
- **Role in Project**: Not directly used in main pipeline, available for analysis

### Scikit-Learn
- **Purpose**: Machine learning library
- **Why Used**: Provides evaluation metrics (MSE, MAE)
- **Role in Project**: Used by PySpark ML for model evaluation

## Main Components

### 1. Data Ingestion (`data_loader.py`)
Loads the raw CSV file and transforms column names to a standard format:
- Reads CSV with automatic schema inference
- Adds `sensor_id` identifier
- Converts timestamp strings to datetime
- Converts temperature and humidity to numeric types
- Selects only required columns

**Input**: CSV file  
**Output**: PySpark DataFrame with standardized columns

### 2. Data Validation (`data_cleaner.py`)
Checks each record against validation rules:
- Detects missing values
- Checks numeric ranges (300-450°C for temperature, 30-80% for humidity)
- Separates valid records from invalid ones
- Captures rejection reasons

**Input**: Raw DataFrame  
**Output**: Valid DataFrame + Invalid DataFrame with rejection reasons

### 3. Feature Engineering (`feature_engineer.py`)
Creates new columns that improve model performance:
- `temperature_change`: Change from previous reading
- `rolling_avg_temperature`: 30-minute moving average
- `target_temperature`: Future temperature (what to predict)

**Input**: Valid sensor readings  
**Output**: DataFrame with engineered features

### 4. Feature Storage (`feature_store.py`)
Persists engineered features in SQLite:
- Creates `sensor_features` table
- Inserts features with duplicate detection
- Allows feature retrieval for analysis

**Input**: Engineered features DataFrame  
**Output**: SQLite database with persistent storage

### 5. Model Training (`model_trainer.py`)
Trains a linear regression model:
- Assembles features into feature vectors
- Fits LinearRegression model
- Makes predictions on new data

**Input**: Training data with features and target  
**Output**: Trained model object

### 6. Model Evaluation (`model_evaluator.py`)
Measures model performance:
- Calculates Mean Squared Error (MSE)
- Calculates Mean Absolute Error (MAE)
- Compares actual vs predicted values
- Computes feature correlations

**Input**: Predictions DataFrame  
**Output**: Performance metrics

### 7. Data Splitting (`run_training.py`)
Divides data chronologically:
- 70% Training
- 15% Validation
- 15% Test

Data is split by timestamp order (chronological), not randomly, because time-series data has temporal dependencies.

## Current Capabilities

✅ **Fully Implemented**:
- CSV data loading with automatic schema inference
- Data validation with multiple rules
- Invalid data separation and storage
- SQLite database storage with duplicate handling
- Feature engineering using Spark Window functions
- Machine learning model training (Linear Regression)
- Model evaluation (MSE, MAE metrics)
- Chronological data splitting

⚠️ **Partially Implemented**:
- Model evaluation (calculate_correlations function is incomplete)
- Configuration management (database.yaml only, missing main config)

❌ **Not Implemented**:
- Logging system
- Error handling and exception management
- Unit tests
- Inference pipeline (for prediction on new data)
- Monitoring pipeline (for tracking model performance)
- Model registry (saving trained models)
- REST API or web interface
- Production deployment configuration
- Scheduled pipeline execution

## Current Limitations

1. **Single Sensor Only**: Hardcoded sensor_id "MANUFACTURING_01" in data loader
2. **Limited Configuration**: Only database.yaml exists, main config.yaml is empty
3. **No Persistence of Models**: Trained model is not saved, must retrain each run
4. **No Error Recovery**: Pipeline fails completely on any error, no retry logic
5. **No Logging**: No audit trail or debug information captured
6. **No Tests**: No automated test suite to verify functionality
7. **Local Only**: Runs only on single machine, not distributed
8. **Hardcoded Paths**: File paths are absolute, not configurable
9. **Manual Execution**: No scheduling or orchestration tool
10. **Incomplete Evaluation**: Model evaluation function has empty correlation calculation

## Typical Use Case Flow

1. **Data Collection Phase**: Manufacturing sensors collect temperature and humidity readings
2. **Pipeline Execution**: Run `python scripts/run_training.py` to:
   - Load today's sensor data
   - Validate and clean it
   - Store valid data in database
   - Engineer features
   - Train model on historical data
   - Evaluate performance
3. **Model Output**: Get trained model and performance metrics
4. **Monitoring**: Analyze MSE/MAE to track model quality

## Data Used in Project

The project uses simulated smart manufacturing temperature control data:

**Source File**: `data/raw/ttemperature_regulation_smart_manufacturing.csv`

**Key Columns**:
- `Timestamp`: When reading was taken
- `Current Temperature (°C)`: Actual temperature (300-450°C range)
- `Humidity (%)`: Humidity level (30-80% range)
- Additional columns about PID control parameters (used for context, not in pipeline)

**Data Characteristics**:
- ~1000+ sensor readings over time
- 5-minute intervals between readings
- Some readings intentionally out of range for testing validation
- CSV format with headers

## Architecture Pattern

This project follows the **ETL (Extract, Transform, Load)** pattern:

1. **Extract**: Load data from CSV file
2. **Transform**: Validate, clean, and engineer features
3. **Load**: Store results in SQLite database

## Next Steps

- To understand the system architecture, read [Project Architecture](./03-architecture.md)
- To see how data moves through the system, read [Data Flow](./08-data-flow.md)
- To start running it, read [Setup and Installation](./12-setup-guide.md)

---

**Key Takeaway**: This is a data pipeline project that demonstrates ETL, data validation, feature engineering, and machine learning in PySpark, designed for processing manufacturing sensor data.
