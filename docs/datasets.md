# Datasets

Medical disclaimer: datasets in this project are used for an educational simulation. Most are synthetic or simplified and should not be interpreted as clinically valid.

Run the audit:

```bash
python ml/data_audit.py
```

The script prints each dataset name, shape, columns, missing values, selected use case, use/ignore decision, and reason. It also writes `models/dataset_audit.json`.

## Used Datasets

### Synthetic_patient-HealthCare-Monitoring_dataset.csv

Used for vital signs, fall detection, alert labels, predicted disease, and status target engineering. It maps directly to the simulated patient monitoring problem.

### patients_data_with_alerts.xlsx

Used for additional alert labels and status classification training. It follows the same structure as the synthetic patient monitoring dataset.

### human_vital_signs_dataset_2024.csv

Used for heart rate, respiratory rate, body temperature, oxygen saturation, blood pressure, age, gender, weight, height, derived BMI, and risk category classification.

### personal_health_data.csv

Used for wearable health context, `Health_Score`, `Anomaly_Flag`, sleep, stress, ECG-adjacent context, blood oxygen, and skin temperature. The risk target is derived as:

```text
risk_score = 100 - Health_Score
```

### activity_environment_data.csv

Used for steps, exercise type, exercise intensity, battery level, and environmental context. It is joined to `personal_health_data.csv` by `User_ID` and `Timestamp`.

### digital_interaction_data.csv

Used for notifications received and screen time. It is joined to `personal_health_data.csv` by `User_ID` and `Timestamp`.

### healthcare_iot_target_dataset_5000.csv

Used for IoT sensor target health status, target blood pressure, target heart rate, battery level, and sensor-related context.

### heart_rate.csv

Used for heart-rate forecasting. Each time-series column is converted into supervised lag rows with five lag features and a next-heart-rate target.

### Health data.csv

Used as lightweight auxiliary data for pulse, body temperature, SpO2, and status.

### Oxygen Dataset Final.csv

Used as optional oxygen and pulse enrichment for SpO2-related patterns.

### diabetes_dataset.csv

Used only for optional risk enrichment. It contributes glucose, BMI, blood pressure, smoking, sleep, screen time, and diabetes risk score context. It is not the main monitoring target.

### updated_version.csv

Used only for optional cardiovascular risk enrichment through age, sex, blood pressure, smoking, diabetes, and heart attack target fields.

## Ignored For Main Pipeline

### Synthetic-Infant-Health-Data.csv

Ignored in the main version because it is pediatric/infant disease data and does not align with the adult smart health monitoring simulator.

### healthcare_patient_journey.csv

Ignored in the real-time sensor pipeline because it is administrative hospital journey data, not streaming IoT telemetry. It can be described as future work for hospital operations analytics.

## Mapping Strategy

When a dataset does not match the unified schema exactly:

- map usable columns to the canonical field names
- fill missing fields with practical defaults
- derive labels from vital-sign thresholds
- document the decision in the audit and docs
- avoid forcing unrelated datasets into the main streaming target
