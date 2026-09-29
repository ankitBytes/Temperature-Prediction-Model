# Model Training

## Overview

The project trains a Linear Regression model to predict future temperature based on current sensor readings and engineered features.

## Model Type: Linear Regression

### What is Linear Regression?

Linear regression finds the best-fit line through data points to predict continuous values.

**Formula**:
```
predicted_value = (weight₁ × feature₁) + (weight₂ × feature₂) + ... + intercept
```

**Example**:
```
predicted_temperature = 0.85 × temperature 
                      - 0.15 × humidity 
                      + 0.45 × temperature_change 
                      + 0.95 × rolling_avg_temperature 
                      + 12.3
```

### Why Linear Regression?

**Advantages**:
- ✓ Simple and fast
- ✓ Interpretable (can see feature importance)
- ✓ Good baseline to start with
- ✓ Requires less data than complex models
- ✓ Easy to debug

**Disadvantages**:
- ✗ Assumes linear relationships
- ✗ Sensitive to outliers
- ✗ May underfit complex patterns

### Alternative Models (Not Implemented)

- Random Forest: Handles non-linear relationships
- Gradient Boosting: Better performance but harder to interpret
- Neural Networks: For very complex patterns
- ARIMA: Specifically for time-series

## File: src/model/model_trainer.py

### Class: LinearRegression (from PySpark ML)

**Import**:
```python
from pyspark.ml.regression import LinearRegression
```

### Function: train_model(train_df)

**Purpose**: Fit linear regression model on training data

**Input**: 
- Spark DataFrame with columns: `features` (vector), `target_temperature` (numeric)

**Output**: 
- Trained model object ready for predictions

**Code**:
```python
def train_model(train_df):
    lr = LinearRegression(
        featuresCol="features",      # Input feature vector
        labelCol="target_temperature", # Output to predict
        predictionCol="prediction"    # Name of prediction column
    )
    
    model = lr.fit(train_df)  # Fit model to training data
    return model
```

**What `fit()` does**:
1. Reads all training data
2. Finds weights for each feature
3. Finds intercept (constant term)
4. Returns trained model object

**Output model properties**:
```python
model.coefficients  # [0.85, -0.15, 0.45, 0.95] - weights for features
model.intercept     # 12.3 - constant term
```

### Function: predict(model, data_df)

**Purpose**: Make predictions using trained model

**Input**:
- Trained model
- DataFrame with `features` column

**Output**:
- DataFrame with original columns + `prediction` column

**Code**:
```python
def predict(model, data_df):
    predictions = model.transform(data_df)
    return predictions
```

**What `transform()` does**:
1. Takes each row's features vector
2. Applies model equation
3. Adds new `prediction` column
4. Returns DataFrame with prediction added

**Example**:

```
Input:
features: [368.73, 50.39, 28.81, 384.29]

Calculation:
prediction = 0.85 × 368.73 - 0.15 × 50.39 + 0.45 × 28.81 + 0.95 × 384.29 + 12.3
           = 313.42 - 7.56 + 12.96 + 365.08 + 12.3
           = 695.2

Output:
features: [368.73, 50.39, 28.81, 384.29]
prediction: 695.2
```

**Note**: This example calculation is simplified; actual values depend on trained coefficients.

## Training Process

### Step 1: Prepare Data

**File**: `scripts/run_training.py` (lines 64-69)

```python
feature_columns = [
    "temperature",
    "humidity",
    "temperature_change",
    "rolling_avg_temperature"
]
```

### Step 2: Assemble Features

**File**: `scripts/run_training.py` (lines 96-101)

```python
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol="features"
)

train_df = assembler.transform(train_df)
```

**Transforms**:
```
temperature, humidity, temperature_change, rolling_avg
         ↓
         features (vector of 4 numbers)
```

### Step 3: Train Model

**File**: `scripts/run_training.py` (lines 201-202)

```python
from src.model.model_trainer import train_model

model = train_model(train_df)
```

**Training data**: ~700 records (70% of total)

**What model learns**:
- Coefficient for each feature (weight)
- Intercept value
- Statistical properties (R², error metrics)

### Step 4: Make Predictions

**File**: `pipelines/training_pipeline.py` (lines 9-11)

```python
from src.model.model_trainer import predict

validation_predictions = predict(model, validation_df)
```

**Prediction data**: ~150 records (15% validation set)

**Produces**: DataFrame with `prediction` column added

### Step 5: Evaluate Performance

**File**: `src/model/model_evaluator.py` (lines 5-29)

```python
from pyspark.ml.evaluation import RegressionEvaluator

def evaluate_model(predictions_df):
    mse_evaluator = RegressionEvaluator(
        labelCol="target_temperature",
        predictionCol="prediction",
        metricName="mse"
    )
    
    mae_evaluator = RegressionEvaluator(
        labelCol="target_temperature",
        predictionCol="prediction",
        metricName="mae"
    )
    
    mse = mse_evaluator.evaluate(predictions_df)
    mae = mae_evaluator.evaluate(predictions_df)
    
    return mse, mae
```

## Model Evaluation Metrics

### Mean Squared Error (MSE)

**Formula**:
```
MSE = average((predicted - actual)²)
```

**Characteristics**:
- Measured in °C²
- Penalizes large errors more heavily
- Good for detecting outliers

**Interpretation**:
- MSE = 2.45 °C²
- Root mean squared error (RMSE) = √2.45 ≈ 1.57°C
- Average prediction error ≈ 1.57°C

**Lower is better** ✓

### Mean Absolute Error (MAE)

**Formula**:
```
MAE = average(|predicted - actual|)
```

**Characteristics**:
- Measured in °C
- Average magnitude of error
- Easier to interpret than MSE

**Interpretation**:
- MAE = 1.23°C
- Average prediction is off by 1.23°C

**Lower is better** ✓

### Example Calculation

**3 predictions**:
```
Record 1: Actual=375.0, Predicted=374.5, Error=-0.5
Record 2: Actual=380.0, Predicted=382.0, Error=+2.0
Record 3: Actual=370.0, Predicted=369.8, Error=-0.2

MSE = ((-0.5)² + (2.0)² + (-0.2)²) / 3
    = (0.25 + 4.0 + 0.04) / 3
    = 4.29 / 3
    = 1.43 °C²

MAE = (|-0.5| + |2.0| + |-0.2|) / 3
    = (0.5 + 2.0 + 0.2) / 3
    = 2.7 / 3
    = 0.9 °C
```

## Training vs Validation

### Training Set (70%)

**Purpose**: Teach model the pattern

**Data**: ~700 records

**Process**:
1. Model sees all records
2. Adjusts weights to fit data
3. Tries to make predictions match actual values

**Result**: Model learns the pattern

### Validation Set (15%)

**Purpose**: Evaluate model performance

**Data**: ~150 records

**Process**:
1. Model NOT trained on this data
2. Model makes predictions
3. Predictions compared to actual values
4. Metrics calculated

**Result**: Know how good model is

### Test Set (15%)

**Purpose**: Final holdout for true performance estimate

**Data**: ~150 records (not used in current pipeline)

**In future**: Should use to verify model on completely unseen data

### Why Separate?

If trained and evaluated on same data:
- Model could memorize instead of learn
- Metrics would be artificially optimistic
- Would not generalize to new data

## Feature Importance

### From Coefficients

Higher coefficient = Stronger influence

**Example coefficients** (not actual):
```
temperature: 0.85                    ← Strongest influence
rolling_avg_temperature: 0.95        ← Strong influence  
temperature_change: 0.45             ← Moderate influence
humidity: -0.15                      ← Weak influence
```

**Interpretation**:
- Temperature matters most
- Increasing temperature → increase prediction
- Humidity has negative effect (inverse relationship)

## Model Limitations

1. **Linear only**: Assumes straight-line relationship
2. **No interactions**: Doesn't capture feature combinations
3. **Assumes normality**: Assumes errors are normally distributed
4. **Sensitive to scale**: Different scales can affect importance
5. **Not saved**: Model lost after run (must retrain)

## Improving the Model

### Option 1: Better Features

```python
# Cyclical features (hour of day)
hour = extract("hour", "timestamp")

# Lagged features (temperature from past)
prev_1h = lag("temperature", 12)
prev_2h = lag("temperature", 24)

# Interaction features
temp_humidity_interaction = temperature * humidity
```

### Option 2: Feature Scaling

```python
from pyspark.ml.feature import StandardScaler

scaler = StandardScaler(
    inputCol="features",
    outputCol="scaledFeatures"
)

scaler.fit(train_df).transform(train_df)
```

### Option 3: Different Algorithm

```python
from pyspark.ml.regression import RandomForestRegressor

model = RandomForestRegressor(
    featuresCol="features",
    labelCol="target_temperature"
)
```

### Option 4: Hyperparameter Tuning

```python
lr = LinearRegression(
    maxIter=100,      # More iterations
    regParam=0.01,    # Regularization
    elasticNetParam=0.5  # L1/L2 mix
)
```

## Model Persistence (Not Implemented)

### Saving Model

```python
model.save("models/temperature_model_v1")
```

### Loading Model

```python
from pyspark.ml.regression import LinearRegressionModel

model = LinearRegressionModel.load("models/temperature_model_v1")
```

**Current project**: Model not saved (limitation)

## Next Steps

- To see model evaluation: [Model Evaluation](./06-data-validation.md)
- To see how model fits in pipeline: [Data Flow](./08-data-flow.md)
- To run training: [Running the Pipeline](./13-running-pipeline.md)

---

**Key Takeaway**: The project trains a Linear Regression model on 70% of data, evaluates on 15%, and reports MSE/MAE metrics. Model learns weight for each feature showing their importance for predicting temperature.
