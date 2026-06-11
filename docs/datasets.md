# Smart Health Monitoring - Datasets Guide

## Overview

This project uses 14 publicly available and synthetic health datasets for training machine learning models and validating the system. This document explains which datasets are used, how they're processed, and why others are excluded.

**Note**: All datasets are synthetic or educational and not from real patients. The project is for simulation and learning purposes only.

## Dataset Usage Summary

| Dataset | Used | Type | Records | Use Case |
|---------|------|------|---------|----------|
| Synthetic_patient-HealthCare-Monitoring_dataset.csv | ✅ YES | PRIMARY | ~1000 | Vital signs, alerts, diseases |
| patients_data_with_alerts.xlsx | ✅ YES | PRIMARY | ~500 | Alert labels, status training |
| human_vital_signs_dataset_2024.csv | ✅ YES | PRIMARY | ~1000 | Demographics, risk categories |
| personal_health_data.csv | ✅ YES | PRIMARY | ~2000 | Health scores, anomaly flags |
| activity_environment_data.csv | ✅ YES | PRIMARY | ~2000 | Activity, exercise, environment |
| digital_interaction_data.csv | ✅ YES | PRIMARY | ~2000 | Screen time, notifications |
| healthcare_iot_target_dataset_5000.csv | ✅ YES | SECONDARY | 5000 | IoT targets, battery, sensors |
| heart_rate.csv | ✅ YES | SECONDARY | 100+ | Heart rate time series |
| Health data.csv | ✅ YES | SECONDARY | 500+ | Basic vitals, status |
| Oxygen Dataset Final.csv | ✅ YES | SECONDARY | 500+ | SpO2, oxygen flow |
| diabetes_dataset.csv | ❌ NO | OPTIONAL | ~250 | Disease-specific (not used) |
| updated_version.csv | ❌ NO | OPTIONAL | ~500 | Cardiovascular risk (not used) |
| Synthetic-Infant-Health-Data.csv | ❌ NO | EXCLUDED | ~300 | Pediatric (not applicable) |
| healthcare_patient_journey.csv | ❌ NO | EXCLUDED | ~1000 | Hospital admin (not applicable) |

## Primary Datasets (Used)

### 1. Synthetic_patient-HealthCare-Monitoring_dataset.csv

**Description**: Synthetic health monitoring dataset with vital signs and alert labels.

**Rows**: ~1000 patient records

**Key Columns**:
- Patient Number
- Heart Rate (bpm)
- SpO2 Level (%)
- Systolic Blood Pressure (mmHg)
- Diastolic Blood Pressure (mmHg)
- Body Temperature (°C)
- Fall Detection (boolean)
- Predicted Disease
- Data Accuracy (%)
- Heart Rate Alert, SpO2 Level Alert, Blood Pressure Alert, Temperature Alert (boolean)

**Use Case**:
- Primary source for **Status Classification** training
- Provides alert labels for supervised learning
- Rich vital signs data with fall detection

**Processing**:
```python
# Column renaming
'Heart Rate (bpm)' → 'heart_rate'
'SpO2 Level (%)' → 'spo2'
# Status derivation from alert columns
if any([heart_rate_alert, spo2_alert, bp_alert, temp_alert]):
    status = derive_from_thresholds()
```

**Quality**: Good - well-balanced classes, no significant missing values

### 2. patients_data_with_alerts.xlsx

**Description**: Excel file with patient data and alert classifications.

**Rows**: ~500 patient records

**Key Columns**:
- Patient ID
- Vital signs (HR, BP, SpO2, etc.)
- Alert Type (categorical)
- Alert Severity (LOW, MEDIUM, HIGH, CRITICAL)
- Patient Status (NORMAL, WARNING, CRITICAL, EMERGENCY)

**Use Case**:
- **Status Classification** training data
- Alert validation and threshold testing
- Ground truth labels for supervised learning

**Processing**:
```python
# Excel to DataFrame
df = pd.read_excel('patients_data_with_alerts.xlsx')

# Status mapping
alert_severity → predicted_status (reverse mapping)
```

**Quality**: High - well-labeled, consistent format

### 3. human_vital_signs_dataset_2024.csv

**Description**: Recent synthetic dataset with demographics and derived health features.

**Rows**: ~1000 records

**Key Columns**:
- Age, Gender, Weight, Height
- Heart Rate, Respiratory Rate, Body Temperature
- Oxygen Saturation, Systolic/Diastolic BP
- Derived_HRV (Heart Rate Variability)
- Derived_Pulse_Pressure
- Derived_BMI
- Derived_MAP (Mean Arterial Pressure)
- Risk Category (Low/Medium/High/Critical Risk)

**Use Case**:
- **Status Classification** training
- **Risk Regressor** training (Risk Category → numeric score)
- Demographics enrichment

**Processing**:
```python
# Risk category mapping
'Low Risk' → 25
'Medium Risk' → 55
'High Risk' → 80
'Critical Risk' → 95

# BMI already computed
```

**Quality**: Excellent - includes derived features, complete demographics

### 4. personal_health_data.csv

**Description**: Wearable health profile data with derived metrics.

**Rows**: ~2000 records

**Key Columns**:
- User_ID, Timestamp
- Health_Score (0-100)
- Anomaly_Flag (boolean)
- Sleep (hours), Stress (level)
- ECG (signal), BloodOxygen (%), SkinTemperature (°C)
- Other activity metrics

**Use Case**:
- **Risk Regressor** training (Health_Score → risk_score = 100 - Health_Score)
- **Anomaly Detector** training (Anomaly_Flag as labels)
- Activity-based features

**Processing**:
```python
# Risk score derivation
risk_score = 100 - health_score

# Anomaly labels
is_anomaly = anomaly_flag == True

# Timestamp parsing
pd.to_datetime(timestamp)
```

**Quality**: Good - includes both features and anomaly labels

### 5. activity_environment_data.csv

**Description**: Environmental and activity tracking data.

**Rows**: ~2000 records

**Key Columns**:
- User_ID, Timestamp (join keys)
- Steps, Calories, Distance
- ExerciseType, ExerciseDuration, ExerciseIntensity
- Temperature, Altitude, UVExposure
- Battery Level

**Use Case**:
- Activity-level features for status classification
- Exercise patterns for risk assessment
- Battery level for anomaly detection
- Environmental context

**Processing**:
```python
# Join with personal_health_data
df = pd.merge(
    personal_health_data,
    activity_environment_data,
    on=['User_ID', 'Timestamp'],
    how='left'
)

# Map categorical
ExerciseIntensity → ['Low', 'Moderate', 'High']
```

**Quality**: Good - can be joined with personal_health_data

### 6. digital_interaction_data.csv

**Description**: Device interaction and notifications data.

**Rows**: ~2000 records

**Key Columns**:
- User_ID, Timestamp (join keys)
- NotificationsReceived
- ScreenTime (hours)
- App usage patterns

**Use Case**:
- Screen time for risk features
- Notification frequency for stress inference
- Digital behavior patterns

**Processing**:
```python
# Join with personal_health_data
df = pd.merge(
    personal_health_data,
    digital_interaction_data,
    on=['User_ID', 'Timestamp'],
    how='left'
)

# Fill missing with defaults
notifications_received.fillna(5)
screen_time.fillna(3.0)
```

**Quality**: Good - sparse features, joinable on User_ID + Timestamp

## Secondary Datasets (Used)

### 7. healthcare_iot_target_dataset_5000.csv

**Description**: IoT-specific dataset with sensor targets and metadata.

**Rows**: 5000 records (largest dataset)

**Key Columns**:
- patient_id
- Target health status
- Target BP (systolic/diastolic)
- Target heart rate
- Battery level
- Sensor type
- Data quality metrics

**Use Case**:
- **Status Classification** training
- **Risk Regressor** training
- Battery-level anomaly detection
- IoT sensor metadata

**Quality**: Large, comprehensive

### 8. heart_rate.csv

**Description**: Heart rate time series for forecasting.

**Rows**: 100+ time steps

**Columns**:
- T1, T2, T3, T4, ... (sequential heart rate values)

**Use Case**:
- **Heart Rate Forecaster** model training
- Creates lag-1 supervised learning (T1,T2,T3 → T4)

**Processing**:
```python
# Reshape into supervised learning
X = df[['T1', 'T2', 'T3']]
y = df['T4']
```

**Quality**: Adequate for ARIMA-like forecasting

### 9. Health data.csv

**Description**: Lightweight auxiliary vital signs data.

**Rows**: 500+ records

**Key Columns**:
- Pulse (heart rate)
- Body Temperature
- SpO2
- Status (NORMAL, ABNORMAL, etc.)

**Use Case**:
- Supplement status classification
- Vital signs validation
- Fallback data source

**Quality**: Simple, useful for cross-validation

### 10. Oxygen Dataset Final.csv

**Description**: Oxygen-related measurements and metrics.

**Rows**: 500+ records

**Key Columns**:
- SpO2 (oxygen saturation)
- Pulse Rate
- Oxygen Flow
- Oxygen Risk Category

**Use Case**:
- Oxygen saturation features
- Respiratory system metrics
- Risk category enrichment

**Quality**: Specialized but useful

## Datasets NOT Used (Why)

### diabetes_dataset.csv
- **Reason**: Disease-specific dataset focused on diabetic patients only
- **Not Used**: Main system is for general health monitoring, not disease-specific
- **Future**: Could be used for optional diabetes risk module
- **Status**: Optional enhancement

### updated_version.csv
- **Reason**: Cardiovascular risk dataset, narrow focus
- **Not Used**: Main system covers broader health dimensions
- **Future**: Could supplement cardiovascular risk scoring
- **Status**: Optional enhancement

### Synthetic-Infant-Health-Data.csv
- **Reason**: Pediatric dataset for infants and young children
- **Not Used**: System targets adult health monitoring (age 18+)
- **Vital Sign Ranges**: Different for infants (different HR, BP, RR baselines)
- **Alert Thresholds**: Not applicable to adult system
- **Status**: Excluded from scope

### healthcare_patient_journey.csv
- **Reason**: Hospital administrative and longitudinal journey data
- **Not Used**: Real-time IoT streaming system, not historical patient journey
- **Data Type**: Administrative (admissions, discharges, transfers)
- **Alignment**: Not aligned with sensor streaming pipeline
- **Future**: Could enable longitudinal outcome analysis post-streaming
- **Status**: Out of scope for real-time monitoring

## Data Processing Pipeline

### 1. Data Audit Phase
```bash
python ml/data_audit.py
```
- Validates all datasets
- Checks for missing values
- Verifies column naming
- Outputs audit report

### 2. Preparation Phase
```python
# Load datasets
loader = DatasetLoader()

# Prepare for different models
X_status, y_status = loader.prepare_status_classification_data()
X_risk, y_risk = loader.prepare_risk_regression_data()
X_anomaly = loader.prepare_anomaly_detection_data()
X_hr, y_hr = loader.prepare_heart_rate_forecast_data()
```

### 3. Unified Schema Mapping
All datasets mapped to 30-field unified schema:
```python
# Input feature list (30 total)
['age', 'gender', 'weight', 'height', 'bmi',
 'heart_rate', 'spo2', 'temperature', 'systolic_bp', 'diastolic_bp',
 'respiratory_rate', 'glucose_level', 'skin_temperature',
 'activity_level', 'exercise_type', 'exercise_intensity', 'steps',
 'stress_level', 'sleep_duration', 'sleep_quality', 'screen_time',
 'notifications_received', 'fall_detected', 'battery_level',
 'chronic_condition', 'smoker', 'medication', 'predicted_disease_simulated']
```

### 4. Feature Engineering
- Derived features (BMI, MAP, Pulse Pressure)
- Categorical encoding (OneHot)
- Numeric scaling (StandardScaler)
- Missing value imputation

### 5. Training Data Split
- 80% training
- 20% testing
- Stratified splits for classification

## Data Quality

### Missing Values Handling
| Handling Method | Used For |
|-----------------|----------|
| Mean/Median imputation | Numeric vitals |
| Mode imputation | Categorical demographics |
| Forward-fill | Time series (HR, activity) |
| Default values | Optional features |

### Outlier Detection
- IsolationForest identifies outliers
- Anomaly detection model trained to flag unusual readings
- Dashboard alerts users to anomalies

### Data Validation
- Schema validation (expected columns)
- Type validation (int, float, string)
- Range validation (vital signs in medical ranges)
- Timestamp validation (ISO8601 format)

## Data Accessibility

All datasets in public domain or educational use:
- Kaggle datasets (verified licenses)
- UCI Machine Learning Repository
- Generated synthetic data (reproducible)

**Licensing**: Educational use only, not for production healthcare

## Future Dataset Considerations

1. **Real-World Data**: Partner with healthcare institutions for real de-identified patient data
2. **Streaming Sources**: Direct integration with EHR APIs
3. **Wearable Integration**: Apple Watch, Fitbit, Garmin data
4. **Environmental Data**: Weather, air quality, pollen data
5. **Genetic Data**: GWAS data for genetic risk factors
6. **Drug Databases**: Medication interaction checks

## References

- Datasets sourced from: Kaggle, UCI ML Repository, Zenodo
- Medical thresholds: AHA/ACC Clinical Guidelines
- Feature engineering: Established health informatics practices
