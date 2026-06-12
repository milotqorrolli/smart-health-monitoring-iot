# Datasets

## Summary

The project contains 14 dataset files. 8 are used for ML training, 6 are excluded.

| # | Dataset | Rows | Status | Use Case |
|---|---------|------|--------|----------|
| 1 | Synthetic_patient-HealthCare-Monitoring_dataset.csv | ~1000 | USED | Primary — vitals, alerts, status |
| 2 | human_vital_signs_dataset_2024.csv | ~5000 | USED | Primary — risk classification |
| 3 | personal_health_data.csv | ~10000 | USED | Primary — anomaly detection |
| 4 | patients_data_with_alerts.xlsx | ~500 | USED | Secondary — alert labels |
| 5 | healthcare_iot_target_dataset_5000.csv | 5000 | USED | Secondary — IoT targets |
| 6 | heart_rate.csv | ~50 | USED | Primary — HR forecasting |
| 7 | Oxygen Dataset Final.csv | ~1000 | USED | Secondary — SpO2 anomaly |
| 8 | Health data.csv | ~500 | USED | Supplementary — basic vitals |
| 9 | digital_interaction_data.csv | - | EXCLUDED | Digital behavior, not health sensors |
| 10 | activity_environment_data.csv | - | EXCLUDED | Environmental data, not physiological |
| 11 | healthcare_patient_journey.csv | - | EXCLUDED | Hospital administrative data |
| 12 | Synthetic-Infant-Health-Data.csv | - | EXCLUDED | Pediatric cardiac, not adult IoT |
| 13 | diabetes_dataset.csv | - | EXCLUDED | Static disease classification |
| 14 | updated_version.csv | - | EXCLUDED | Cardiovascular risk/cholesterol |

## Column Name Mappings

### Synthetic_patient-HealthCare-Monitoring_dataset.csv
- "Heart Rate (bpm)" → heart_rate
- "SpO2 Level (%)" → spo2
- "Systolic Blood Pressure (mmHg)" → systolic_bp
- "Diastolic Blood Pressure (mmHg)" → diastolic_bp
- "Body Temperature (°C)" → temperature
- "Fall Detection" → fall_detected (Yes→True, No→False)

### human_vital_signs_dataset_2024.csv
- "Oxygen Saturation" → spo2
- "Body Temperature" → temperature
- "Weight (kg)" → weight
- "Height (m)" → height (already in meters)
- "Risk Category" → risk_score (Low=25, Medium=55, High=80, Critical=95)

### personal_health_data.csv
- "Heart_Rate" → heart_rate
- "Blood_Oxygen_Level" → spo2
- "Skin_Temperature" → skin_temperature
- "Health_Score" → risk_score = 100 - Health_Score
- "Anomaly_Flag" → supervised anomaly label
- **Height: IN CENTIMETERS — divided by 100 before use**

### patients_data_with_alerts.xlsx
- Same column names as Synthetic_patient dataset
- **Alert labels normalized to uppercase**: Normal→NORMAL, Abnormal→ABNORMAL

### healthcare_iot_target_dataset_5000.csv
- "Temperature (°C)" → temperature
- "Heart_Rate (bpm)" → heart_rate
- "Device_Battery_Level (%)" → battery_level
- "Target_Health_Status" → Healthy=NORMAL, Unhealthy=WARNING

### Oxygen Dataset Final.csv
- "spo2" → spo2
- "pr" → heart_rate (pulse rate as proxy)
- "oxy_flow" → supplemental oxygen feature
- **Critical: Apply SimpleImputer(median) — many null values**

### heart_rate.csv
- Columns T1, T2, T3, T4 — independent time series
- Stacked into supervised lag dataset

## Data Normalization Notes

1. **Height normalization:** personal_health_data.csv stores height in cm. If max(height) > 3.0, divide by 100.
2. **Alert label normalization:** patients_data_with_alerts.xlsx uses mixed case. Map all to uppercase.
3. **Missing value imputation:** Oxygen Dataset Final.csv has significant nulls — use median imputation.
4. **Heart rate dataset size:** May have few rows after lag creation. Still train model for demo.

## Statement

All datasets used in this project are synthetic or publicly available educational data. No real patient data is used. This project is for educational purposes only.
