# Temperature Prediction Model Using PySpark - Documentation

Welcome to the comprehensive documentation for the Temperature Prediction Model project. This documentation is designed to help you understand, run, modify, and troubleshoot this data pipeline application.

## Documentation Structure

This documentation is organized into the following sections:

### 📋 Core Documentation
- **[Project Overview](./01-project-overview.md)** - What is this project and why it exists
- **[Technology Stack](./02-technology-stack.md)** - Technologies used and their purposes
- **[Project Architecture](./03-architecture.md)** - System design and high-level flow
- **[Complete Project Structure](./04-project-structure.md)** - Directory layout and file purposes

### 🔄 Data Pipeline Documentation
- **[Data Ingestion Guide](./05-data-ingestion.md)** - How data is loaded from CSV
- **[Data Validation](./06-data-validation.md)** - Validation rules and invalid data handling
- **[Feature Engineering](./07-feature-engineering.md)** - How features are created
- **[Data Flow](./08-data-flow.md)** - Complete journey of a record through the system

### 🏗️ System Components
- **[Database Implementation](./09-database.md)** - SQLite schema and operations
- **[Configuration Management](./10-configuration.md)** - How configuration is handled
- **[Model Training](./11-model-training.md)** - ML model implementation and training pipeline

### ⚙️ Operations & Development
- **[Setup and Installation](./12-setup-guide.md)** - Complete setup instructions
- **[Running the Pipeline](./13-running-pipeline.md)** - Exact commands to execute
- **[Testing Guide](./14-testing.md)** - How to write and run tests
- **[Troubleshooting](./15-troubleshooting.md)** - Common problems and solutions
- **[Development Guide](./16-development-guide.md)** - How to modify the project

### 🏗️ Advanced Topics
- **[Rebuild from Scratch](./17-rebuild-from-scratch.md)** - Complete reconstruction guide
- **[Common Mistakes](./18-common-mistakes.md)** - Errors junior developers make
- **[Current Limitations](./19-limitations.md)** - Known issues and gaps
- **[MLOps Concepts](./20-mlops-concepts.md)** - How this project relates to MLOps

## Quick Start

### For First-Time Users
1. Read [Project Overview](./01-project-overview.md) to understand what this does
2. Follow [Setup and Installation](./12-setup-guide.md) to get it running
3. Read [Running the Pipeline](./13-running-pipeline.md) to execute the code

### For Developers
1. Review [Complete Project Structure](./04-project-structure.md)
2. Study [Data Flow](./08-data-flow.md)
3. Check [Development Guide](./16-development-guide.md) before making changes

### For Troubleshooting
1. Consult [Troubleshooting](./15-troubleshooting.md)
2. Check [Common Mistakes](./18-common-mistakes.md)
3. Review relevant component documentation

## Key Concepts to Understand

### Data Pipeline
A data pipeline is a series of automated steps that process raw data into useful outputs. This project's pipeline:
- **Loads** raw CSV data
- **Validates** each record
- **Separates** valid and invalid data
- **Transforms** valid data into features
- **Stores** data and features in a database
- **Trains** a machine learning model

### PySpark
PySpark is a framework for distributed data processing. It allows processing large datasets across multiple computers. This project uses PySpark for data loading, validation, transformation, and model training.

### Feature Engineering
Feature engineering is the process of creating new data columns (features) from raw data. This project creates:
- **temperature_change**: The change in temperature from the previous reading
- **rolling_avg_temperature**: A 30-minute rolling average of temperature
- **target_temperature**: The temperature 12 readings into the future (what we predict)

### Machine Learning Model
This project trains a Linear Regression model to predict future temperature based on current features. The model learns the relationship between:
- **Input features**: temperature, humidity, temperature_change, rolling_avg_temperature
- **Target variable**: target_temperature (future temperature)

## Important Notes

- **Configuration is centralized** in `config/database.yaml`
- **Main execution starts** in `scripts/run_training.py`
- **Data validation happens** in `src/data/data_cleaner.py`
- **Features are engineered** in `src/features/feature_engineer.py`
- **All data flows through** SQLite database for persistence

## File-by-File Quick Reference

| File | Purpose | Status |
|------|---------|--------|
| `data_loader.py` | Loads CSV and transforms columns | ✓ Implemented |
| `data_cleaner.py` | Validates data and separates valid/invalid | ✓ Implemented |
| `feature_engineer.py` | Creates ML features using Spark Window functions | ✓ Implemented |
| `model_trainer.py` | Trains Linear Regression model | ✓ Implemented |
| `model_evaluator.py` | Evaluates model performance | Partial |
| `feature_store.py` | SQLite interface for features | ✓ Implemented |
| `run_training.py` | Main pipeline orchestration | ✓ Implemented |
| `run_inference.py` | Inference pipeline | ✗ Not implemented |
| `run_monitoring.py` | Monitoring pipeline | ✗ Not implemented |

## Next Steps

Choose what you need to do:

- **Want to run it?** → [Setup and Installation](./12-setup-guide.md)
- **Want to understand it?** → [Project Overview](./01-project-overview.md)
- **Want to modify it?** → [Development Guide](./16-development-guide.md)
- **Something broken?** → [Troubleshooting](./15-troubleshooting.md)
- **Want to rebuild it?** → [Rebuild from Scratch](./17-rebuild-from-scratch.md)

---

**Last Updated**: September 2024  
**Project**: Temperature Prediction Model Using PySpark  
**Python Version**: 3.9+  
**PySpark Version**: 3.5.0+
