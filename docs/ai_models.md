# AI Models

Medical disclaimer: these models support an educational IoT simulation only. They are not clinically validated and must not be used for diagnosis, triage, or treatment.

## Shared Feature Schema

Models use the canonical feature order stored in `models/feature_schema.json`.

Numeric features:

- `age`
- `weight`
- `height`
- `bmi`
- `heart_rate`
- `spo2`
- `temperature`
- `systolic_bp`
- `diastolic_bp`
- `respiratory_rate`
- `glucose_level`
- `skin_temperature`
- `steps`
- `sleep_duration`
- `screen_time`
- `notifications_received`
- `battery_level`

Categorical features:

- `gender`
- `activity_level`
- `exercise_type`
- `exercise_intensity`
- `stress_level`
- `sleep_quality`
- `chronic_condition`
- `smoker`
- `medication`
- `fall_detected`

Preprocessing uses `SimpleImputer` for missing values and `OneHotEncoder(handle_unknown="ignore")` for categorical values.

## Status Classification Model

Artifact: `models/status_classifier.pkl`

Purpose: predict current patient status:

- `NORMAL`
- `WARNING`
- `CRITICAL`
- `EMERGENCY`

Algorithm: `RandomForestClassifier`

Training datasets:

- `Synthetic_patient-HealthCare-Monitoring_dataset.csv`
- `patients_data_with_alerts.xlsx`
- `human_vital_signs_dataset_2024.csv`
- `healthcare_iot_target_dataset_5000.csv`
- `Health data.csv`
- auxiliary mapped rows from oxygen, diabetes, personal health, and cardiovascular datasets where useful

Labels are taken directly when available, or derived with deterministic vital-sign thresholds in `ml/model_utils.py`.

Saved metrics include accuracy, macro precision, macro recall, macro F1, classification report, and confusion matrix in `models/model_metrics.json`.

## Risk Score Regression Model

Artifact: `models/risk_regressor.pkl`

Purpose: predict `risk_score` from 0 to 100.

Algorithm: `RandomForestRegressor`

Risk targets are derived from:

- `Health_Score`: `risk_score = 100 - Health_Score`
- `Risk Category`: mapped from low/medium/high/critical categories
- `Target_Health_Status`: mapped from healthy/unhealthy categories
- threshold status fallback when no direct target exists

Saved metrics include MAE, RMSE, and R2.

## Anomaly Detection Model

Artifact: `models/anomaly_detector.pkl`

Purpose: identify unusual sensor patterns and produce:

- `is_anomaly`
- `anomaly_score`

Algorithm: `IsolationForest`

Training uses the unified feature frame. `personal_health_data.csv` contributes `Anomaly_Flag` where available, and threshold-derived abnormal patterns provide fallback evaluation labels.

Saved metrics include precision, recall, F1, labeled row count, and predicted anomaly rate when labels are available.

## Heart Rate Forecasting Model

Artifact: `models/heart_rate_forecaster.pkl`

Purpose: predict `predicted_next_heart_rate`.

Algorithm: `RandomForestRegressor`

Training data:

- `heart_rate.csv`

Lag features:

- `hr_lag_1`
- `hr_lag_2`
- `hr_lag_3`
- `hr_lag_4`
- `hr_lag_5`

Target:

- `next_heart_rate`

Saved metrics include MAE, RMSE, and R2.

## Streaming Inference

Spark loads models from `/models` inside the `spark-streaming` container. Inference runs in `foreachBatch` by converting small micro-batches to pandas DataFrames.

Fallback behavior:

- Missing status classifier: use rule-based status.
- Missing risk regressor: map status to default risk scores.
- Missing anomaly detector: use severe rule status as anomaly fallback.
- Missing heart-rate forecaster: use current heart rate as next-heart-rate fallback.
- Failed individual model: log a warning and keep processing.

## Limitations

- Most datasets are synthetic or educational.
- Dataset distributions may not match real clinical populations.
- Thresholds are simplified for demonstration.
- Forecasting uses short lag windows only.
- The anomaly model is tuned for demo behavior, not clinical sensitivity.
- The dashboard presents simulated risk and alerts, not medical advice.
