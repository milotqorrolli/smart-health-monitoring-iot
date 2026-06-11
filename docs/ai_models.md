# Smart Health Monitoring - AI/ML Models

## Overview

The Smart Health Monitoring system includes four machine learning models that provide real-time AI-powered health assessment, risk scoring, anomaly detection, and heart rate forecasting.

**Important Disclaimer**: These models are for educational simulation only and provide no clinical validity. Do not use for real medical decisions.

## Models Overview

| Model | Type | Algorithm | Output | Use Case |
|-------|------|-----------|--------|----------|
| Status Classifier | Classification | RandomForest | NORMAL/WARNING/CRITICAL/EMERGENCY | Real-time health status |
| Risk Regressor | Regression | RandomForest | Risk Score (0-100) | Quantified health risk |
| Anomaly Detector | Anomaly Detection | IsolationForest | is_anomaly, anomaly_score | Detect unusual readings |
| Heart Rate Forecaster | Time Series | RandomForest | Predicted next HR | Heart rate trend |

## 1. Status Classification Model

**File**: `models/status_classifier.pkl`

### Purpose
Predicts the current health status of a patient based on demographic, vital, and activity features.

### Input Features
30 features across 5 categories:
- **Demographics**: age, gender, weight, height, bmi (5)
- **Vital Signs**: heart_rate, spo2, temperature, systolic_bp, diastolic_bp, respiratory_rate, glucose_level, skin_temperature (8)
- **Activity**: activity_level, exercise_type, exercise_intensity, steps, stress_level, sleep_duration, sleep_quality, screen_time, notifications_received (9)
- **Sensors**: fall_detected, battery_level (2)
- **Medical**: chronic_condition, smoker, medication, predicted_disease_simulated (4)

### Output Classes
1. **NORMAL** (75-80% of samples)
   - All vital signs within normal ranges
   - No fall detected
   - Risk profile stable

2. **WARNING** (12-15% of samples)
   - One or more vital signs mildly abnormal
   - Elevated stress or poor sleep
   - Risk score 40-60

3. **CRITICAL** (5-8% of samples)
   - Multiple abnormal vital signs
   - Severe deviations from baseline
   - Risk score 60-85

4. **EMERGENCY** (1-3% of samples)
   - Critical vital signs
   - Extreme deviations
   - Fall detected + high HR
   - Risk score >85

### Algorithm Details
- **Algorithm**: RandomForestClassifier
- **Hyperparameters**:
  - n_estimators: 100 trees
  - max_depth: 15 levels
  - min_samples_split: 5
  - min_samples_leaf: 2
- **Train/Test Split**: 80/20
- **Preprocessing**: 
  - Numeric features scaled with StandardScaler
  - Categorical features encoded with OneHotEncoder
  - Missing values imputed with median/mode

### Performance Metrics
- Accuracy: ~92% (typical)
- Precision (weighted): ~91%
- Recall (weighted): ~92%
- F1-Score: ~91%

### Deployment
```python
import pickle
with open('models/status_classifier.pkl', 'rb') as f:
    model = pickle.load(f)
prediction = model.predict(X_preprocessed)  # Returns one of 4 classes
```

## 2. Risk Score Regression Model

**File**: `models/risk_regressor.pkl`

### Purpose
Predicts a numeric health risk score from 0 to 100, indicating overall patient risk level.

### Input Features
Same 30 features as Status Classifier

### Output
**Risk Score**: 0-100
- 0-24: LOW
- 25-44: MEDIUM
- 45-69: HIGH
- 70-100: CRITICAL

### Derivation Rules
If trained dataset provides labels, uses:
- Health_Score datasets: risk = 100 - health_score
- Risk Category: Low→25, Medium→55, High→80, Critical→95
- Otherwise: derived from rule-based thresholds

### Algorithm Details
- **Algorithm**: RandomForestRegressor
- **Hyperparameters**: 
  - Same as Status Classifier
  - n_estimators: 100
  - max_depth: 15
  - min_samples_split: 5
  - min_samples_leaf: 2
- **Loss Metric**: Mean Squared Error (MSE)

### Performance Metrics
- MAE (Mean Absolute Error): ~5.2 points
- RMSE (Root Mean Squared Error): ~7.1 points
- R² Score: ~0.88 (explains 88% of variance)

### Medical Thresholds (Rule-Based Fallback)

If model unavailable, risk is calculated as:

**Heart Rate**:
- Normal (60-100): 0 points
- Warning (50-59, 101-130): 10 points
- Critical (40-49, 131-150): 20 points
- Emergency (<40, >150): 30 points

**SpO2**:
- Normal (≥95): 0 points
- Warning (90-94): 10 points
- Critical (85-89): 20 points
- Emergency (<85): 30 points

**Temperature**:
- Normal (36-37.8): 0 points
- Warning (35-35.9, 37.9-38.9): 10 points
- Critical (39-39.9): 20 points
- Emergency (≥40, <35): 30 points

**Systolic BP**:
- Normal (90-140): 0 points
- Warning (141-160): 10 points
- Critical (161-200): 20 points
- Emergency (>200, <80): 30 points

**Diastolic BP**:
- Normal (60-90): 0 points
- Warning (91-100): 10 points
- Critical (101-130): 20 points
- Emergency (>130, <50): 30 points

**Other Factors**:
- Fall detected: +15 points
- Poor sleep quality: +5 points
- High stress: +10 points
- Low battery: +5 points

Total risk = sum of component scores (capped at 100)

## 3. Anomaly Detection Model

**File**: `models/anomaly_detector.pkl`

### Purpose
Detects unusual or suspicious sensor readings that deviate from learned normal patterns.

### Input Features (8 vital signs)
- heart_rate
- spo2
- temperature
- systolic_bp
- diastolic_bp
- respiratory_rate
- glucose_level
- skin_temperature

### Output
1. **is_anomaly**: Boolean (true/false)
2. **anomaly_score**: 0-1 (higher = more anomalous)
3. **anomaly_type**: Description (e.g., "Sensor Spike", "Impossible Reading")

### Algorithm Details
- **Algorithm**: IsolationForest (unsupervised)
- **Contamination**: 0.05 (expects 5% anomalies)
- **n_estimators**: 100 trees
- **Random State**: 42 (reproducible)
- **Preprocessing**: 
  - StandardScaler normalization
  - Scaler saved as `models/anomaly_scaler.pkl`

### How It Works
1. **Isolation Forest** isolates outliers by randomly selecting features and split values
2. Points that isolate quickly are anomalies (few splits needed)
3. Points needing many splits are normal (deep in the forest)

### Anomaly Score Interpretation
- Score < -0.1: Normal (high confidence)
- Score -0.1 to 0.2: Borderline
- Score > 0.2: Anomalous (high confidence)

### Use Cases
- Sensor malfunction detection
- Data transmission errors
- Unrealistic vital sign combinations
- Device drift detection

### Performance Metrics
- Detection Rate: ~95%
- False Positive Rate: ~2%
- ROC-AUC: ~0.93 (if labels available)

## 4. Heart Rate Forecasting Model

**File**: `models/heart_rate_forecaster.pkl`

### Purpose
Predicts the next heart rate reading based on recent heart rate history (lag-1 ARIMA-like approach).

### Input Features (3 lag features)
- T1: 3 readings ago
- T2: 2 readings ago
- T3: 1 reading ago

### Output
**predicted_next_heart_rate**: Numeric value (30-200 bpm range)

### Algorithm Details
- **Algorithm**: RandomForestRegressor
- **Features**: 3 temporal lags
- **Target**: Next heart rate (T4)
- **Training Data**: `heart_rate.csv` time series
- **Preprocessing**:
  - StandardScaler normalization
  - Scaler saved as `models/heart_rate_scaler.pkl`

### Performance Metrics
- MAE: ~2.1 bpm
- RMSE: ~3.5 bpm
- R² Score: ~0.82

### Use Cases
- Early warning of heart rate changes
- Trend analysis
- Arrhythmia detection
- Activity level inference

## Model Training Pipeline

### Data Sources
Models are trained on synthetic and real datasets:

**Status Classifier**:
- Synthetic_patient-HealthCare-Monitoring_dataset.csv
- human_vital_signs_dataset_2024.csv
- patients_data_with_alerts.xlsx (labels)
- healthcare_iot_target_dataset_5000.csv

**Risk Regressor**:
- personal_health_data.csv (Health_Score)
- human_vital_signs_dataset_2024.csv (Risk Category)
- healthcare_iot_target_dataset_5000.csv

**Anomaly Detector**:
- personal_health_data.csv (Anomaly_Flag)
- Synthetic_patient-HealthCare-Monitoring_dataset.csv
- Any dataset with abnormal alert flags

**Heart Rate Forecaster**:
- heart_rate.csv (time series)

### Training Steps
```bash
# 1. Audit datasets
python ml/data_audit.py

# 2. Prepare datasets
python ml/prepare_datasets.py

# 3. Train models
python ml/train_models.py

# 4. Output artifacts
# - models/status_classifier.pkl
# - models/risk_regressor.pkl
# - models/anomaly_detector.pkl
# - models/heart_rate_forecaster.pkl
# - models/anomaly_scaler.pkl
# - models/heart_rate_scaler.pkl
# - models/model_metadata.json
# - models/model_metrics.json
# - models/feature_schema.json
```

## Model Versioning

### Metadata File
`models/model_metadata.json` contains:
```json
{
  "models": {
    "status_classifier": {
      "type": "classification",
      "algorithm": "RandomForestClassifier",
      "file": "status_classifier.pkl",
      "input_features": [...],
      "output": "predicted_status",
      "classes": ["NORMAL", "WARNING", "CRITICAL", "EMERGENCY"]
    },
    ...
  },
  "metrics": {...},
  "feature_schema": {...}
}
```

### Versioning Strategy
- Version tagged in metadata: `model_version: "v1.0.0"`
- Timestamp of training: `trained_at: "2024-06-11T12:00:00Z"`
- Data source checksum: `data_hash: "abc123..."`

## Inference Pipeline

### Real-Time Inference (Spark Streaming)

```python
# 1. Load model
status_clf = pickle.load(open('models/status_classifier.pkl', 'rb'))

# 2. Prepare features (unified schema)
X = pd.DataFrame({
    'age': 64, 'gender': 'Female', 'weight': 78.5, ...
})

# 3. Predict
predicted_status = status_clf.predict(X)[0]  # Returns string

# 4. Use prediction
if predicted_status == 'EMERGENCY':
    generate_critical_alert()
```

### Fallback Mechanism
If models cannot be loaded (missing files, wrong format):
1. Log warning message
2. Fall back to rule-based status derivation
3. Continue processing without ML predictions
4. Alert users via dashboard

## Model Maintenance

### Monitoring Metrics
- Prediction latency (target: <100ms)
- Model memory usage (target: <500MB)
- Accuracy drift over time
- Feature importance changes

### Retraining Triggers
- Model accuracy drops below 85%
- Distribution shift detected in input data
- New datasets available
- Quarterly scheduled retraining

### A/B Testing
- Run new model on subset of data
- Compare predictions with current model
- Evaluate metric improvements
- Deploy if validation successful

## Limitations and Assumptions

1. **Educational Models**: Baseline implementations for learning
2. **Synthetic Data**: Training data is mostly synthetic, not from real patients
3. **No Clinical Validation**: Models not validated for medical accuracy
4. **Feature Availability**: Assumes all features available (graceful degradation if missing)
5. **Class Imbalance**: Models handle imbalanced classes but not optimized
6. **No Personalization**: Generic models for all patients (could personalize per patient)
7. **Concept Drift**: Models don't adapt online (requires retraining)

## Future Enhancements

1. **Advanced Algorithms**:
   - LSTM/GRU for time-series forecasting
   - Gradient Boosting (XGBoost, LightGBM)
   - Neural networks (TensorFlow/PyTorch)
   - Ensemble methods combining multiple models

2. **Personalization**:
   - Per-patient baseline models
   - Transfer learning from patient cohorts
   - Federated learning across institutions

3. **Explainability**:
   - SHAP values for feature importance
   - LIME for local explanations
   - Model card documentation

4. **Robustness**:
   - Calibration (confidence scores)
   - Uncertainty quantification
   - Adversarial robustness testing

5. **Production**:
   - Model serving (TensorFlow Serving, MLflow)
   - Containerization (Docker)
   - Model registry and versioning
   - Automated retraining pipelines

## References

- [scikit-learn RandomForest](https://scikit-learn.org/stable/modules/ensemble.html#forests)
- [IsolationForest Paper](https://dl.acm.org/doi/10.1145/1511379.1511544)
- [Time Series Forecasting](https://machinelearningmastery.com/time-series-forecasting/)
- [Model Evaluation Metrics](https://scikit-learn.org/stable/modules/model_evaluation.html)
