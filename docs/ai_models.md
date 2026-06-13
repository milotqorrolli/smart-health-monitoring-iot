# AI Models

## Overview

The system uses 4 machine learning models trained offline on healthcare datasets, loaded at runtime by Spark Structured Streaming for real-time inference.

All models use scikit-learn and are serialized with joblib.

---

## 1. Status Classification Model

**File:** `models/status_classifier.pkl`

**Purpose:** Predict current patient health status.

**Output classes:** NORMAL, WARNING, CRITICAL, EMERGENCY

**Algorithm:** RandomForestClassifier vs GradientBoostingClassifier (best F1 selected)

**Input Features:**
- heart_rate, spo2, temperature, systolic_bp, diastolic_bp
- respiratory_rate, glucose_level, fall_detected

**Training Data:**
1. Synthetic_patient-HealthCare-Monitoring_dataset.csv (primary)
2. human_vital_signs_dataset_2024.csv
3. patients_data_with_alerts.xlsx
4. healthcare_iot_target_dataset_5000.csv
5. Health data.csv

**Target Engineering:** Uses `derive_status()` function with clinical thresholds to label training data.

**Metrics:** Accuracy, Precision (macro), Recall (macro), F1 (macro), Confusion Matrix

---

## 2. Risk Score Regression Model

**File:** `models/risk_regressor.pkl`

**Purpose:** Predict numeric health risk score.

**Output:** risk_score (0–100)

**Algorithm:** RandomForestRegressor vs GradientBoostingRegressor (best RMSE selected)

**Input Features:**
- heart_rate, spo2, temperature, systolic_bp, diastolic_bp
- respiratory_rate, glucose_level, skin_temperature, battery_level, fall_detected

**Training Data:**
1. personal_health_data.csv → risk_score = 100 - Health_Score
2. human_vital_signs_dataset_2024.csv → Risk Category mapped to numeric
3. healthcare_iot_target_dataset_5000.csv → Health Status mapped to score

**Risk Level Derivation:**
- 0–30 → LOW
- 31–55 → MEDIUM
- 56–75 → HIGH
- 76–100 → CRITICAL

**Metrics:** MAE, RMSE, R²

---

## 3. Anomaly Detection Model

**File:** `models/anomaly_detector.pkl`

**Purpose:** Detect unusual sensor reading combinations.

**Output:**
- is_anomaly (boolean)
- anomaly_score (float, negative = more anomalous)
- anomaly_type (text)

**Algorithm:** IsolationForest (contamination=0.1, random_state=42)

**Input Features:**
- heart_rate, spo2, temperature, systolic_bp, diastolic_bp
- respiratory_rate, glucose_level, skin_temperature, battery_level

**Training Data:**
1. personal_health_data.csv (Anomaly_Flag as supervised reference)
2. healthcare_iot_target_dataset_5000.csv
3. Oxygen Dataset Final.csv (with median imputation)

**Metrics:** Contamination ratio, anomalies detected, precision/recall (when labels available)

---

## 4. Heart Rate Forecasting Model

**File:** `models/heart_rate_forecaster.pkl`

**Purpose:** Predict the next heart rate value.

**Output:** predicted_next_heart_rate

**Algorithm:** RandomForestRegressor or GradientBoostingRegressor (best RMSE)

**Input Features:** hr_lag_1, hr_lag_2, hr_lag_3, hr_lag_4, hr_lag_5

**Training Data:** heart_rate.csv exclusively
- Columns T1, T2, T3, T4 stacked into supervised dataset
- 5 lag features created per time step
- Target: next value in sequence

**Metrics:** MAE, RMSE, R²

---

## Preprocessing Pipeline

**File:** `models/preprocessing_pipeline.pkl`

A scikit-learn ColumnTransformer that handles:
- Numeric features: SimpleImputer (median) + StandardScaler
- Categorical features: SimpleImputer (constant "Unknown") + OneHotEncoder
- Boolean features: SimpleImputer (0)

**Feature Schema:** `models/feature_schema.json` — ordered list of expected features.

---

## Model Fallback Strategy

If any model file is missing at Spark startup:
- A warning is logged
- Rule-based logic is used instead
- The system never crashes due to missing models

Rule-based fallback:
- Status: `derive_status()` using clinical thresholds
- Risk score: derived from status (NORMAL=20, WARNING=50, CRITICAL=75, EMERGENCY=92)
- Anomaly: not detected (is_anomaly=False)
- HR forecast: None

---

## Limitations

- Models are trained on synthetic/educational datasets
- Limited sample sizes for some datasets
- Heart rate forecaster uses simplified lag approach
- Not validated against clinical standards
- **This is NOT a certified medical device**
