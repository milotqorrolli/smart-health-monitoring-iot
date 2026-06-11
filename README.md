# Smart Health Monitoring IoT System

An educational real-time IoT simulation that streams patient wearable data through Kafka, processes it with Spark Structured Streaming, applies AI/ML inference, stores enriched records in Cassandra, and displays patient status in a Flask dashboard.

Medical disclaimer: this project is for university learning and IoT/data engineering demonstration only. It is not clinically validated and must not be used for diagnosis, treatment, or real patient monitoring.

## Architecture

```text
Producer Simulator
  -> Kafka topic smart-health-data
  -> Spark Structured Streaming
  -> AI/ML inference
  -> Cassandra tables
  -> Flask Dashboard
```

## Technologies

- Python producer simulator
- Apache Kafka and Zookeeper
- Apache Spark Structured Streaming
- scikit-learn, pandas, NumPy, joblib
- Apache Cassandra
- Flask dashboard
- Docker Compose

## Real-Time Message Schema

The producer sends JSON messages to `smart-health-data` with stable patient profile fields, vital signs, wearable context, activity/sleep/stress fields, fall and battery signals, and simulated disease context:

```json
{
  "patient_id": "patient-1",
  "timestamp": "2026-06-11T12:30:00.000Z",
  "age": 64,
  "gender": "Female",
  "weight": 78.5,
  "height": 1.69,
  "bmi": 27.5,
  "heart_rate": 118,
  "spo2": 91.4,
  "temperature": 38.2,
  "systolic_bp": 156,
  "diastolic_bp": 94,
  "respiratory_rate": 24,
  "glucose_level": 162,
  "skin_temperature": 35.1,
  "activity_level": "Resting",
  "exercise_type": "None",
  "exercise_intensity": "Low",
  "steps": 2300,
  "stress_level": "High",
  "sleep_duration": 5.6,
  "sleep_quality": "Poor",
  "screen_time": 4.1,
  "notifications_received": 42,
  "fall_detected": false,
  "battery_level": 67,
  "chronic_condition": "Hypertension",
  "smoker": "No",
  "medication": "Yes",
  "predicted_disease_simulated": "Hypertension"
}
```

Spark enriches each record with `rule_status`, `predicted_status`, `risk_score`, `risk_level`, `is_anomaly`, `anomaly_score`, `anomaly_type`, `predicted_next_heart_rate`, alert fields, `model_version`, and `processed_at`.

## Dataset Usage

Primary datasets:

- `Synthetic_patient-HealthCare-Monitoring_dataset.csv`: vitals, falls, disease labels, alert labels.
- `patients_data_with_alerts.xlsx`: additional alert labels for status classification.
- `human_vital_signs_dataset_2024.csv`: adult vitals, age/gender, BMI/MAP, risk category.
- `personal_health_data.csv`: wearable health profile, Health_Score, Anomaly_Flag, sleep/stress/SpO2/skin temperature.
- `activity_environment_data.csv`: steps, exercise, battery, environment; joined to personal health by `User_ID` and `Timestamp`.
- `digital_interaction_data.csv`: notifications and screen time; joined to personal health by `User_ID` and `Timestamp`.
- `healthcare_iot_target_dataset_5000.csv`: IoT targets, target health status, battery.
- `heart_rate.csv`: lagged heart-rate forecasting.
- `Health data.csv`: auxiliary pulse, temperature, SpO2, status.
- `Oxygen Dataset Final.csv`: optional oxygen/pulse enrichment.

Optional enrichment:

- `diabetes_dataset.csv`: risk regression enrichment only.
- `updated_version.csv`: cardiovascular risk enrichment only.

Ignored for the main real-time pipeline:

- `Synthetic-Infant-Health-Data.csv`: pediatric data does not align with the adult simulator.
- `healthcare_patient_journey.csv`: administrative hospital journey data, not IoT telemetry.

Run the dataset audit:

```bash
python ml/data_audit.py
```

The audit writes `models/dataset_audit.json`.

## AI Models

The ML pipeline trains and saves:

- `models/status_classifier.pkl`: RandomForestClassifier for `NORMAL`, `WARNING`, `CRITICAL`, `EMERGENCY`.
- `models/risk_regressor.pkl`: RandomForestRegressor for numeric `risk_score` from 0 to 100.
- `models/anomaly_detector.pkl`: IsolationForest for unusual sensor values.
- `models/heart_rate_forecaster.pkl`: RandomForestRegressor using `hr_lag_1` through `hr_lag_5`.
- `models/preprocessing_pipeline.pkl`: fitted preprocessing pipeline.
- `models/feature_schema.json`: canonical feature schema.
- `models/model_metadata.json`: model version and artifact metadata.
- `models/model_metrics.json`: evaluation metrics.

If the model artifacts are missing, Spark continues with deterministic rule-based fallback and logs:

```text
ML models not found. Using rule-based fallback.
```

If one individual model fails, Spark logs a model-specific warning and keeps processing with fallback for that output.

## Install Docker

Install Docker Desktop:

- Windows/macOS: https://www.docker.com/products/docker-desktop/
- Linux: install Docker Engine and the Docker Compose plugin from your distribution or Docker documentation.

Confirm Docker is available:

```bash
docker --version
docker compose version
```

## Train Models

Create or activate a local Python environment, then install training dependencies:

```bash
python -m pip install -r ml/requirements.txt
```

Train models. The default training cap is 5,000 rows per large dataset so the command is practical for a demo machine:

```bash
python ml/data_audit.py
python ml/train_models.py
```

For a larger training run, increase the row cap:

```bash
MAX_ROWS_PER_DATASET=20000 python ml/train_models.py
```

On PowerShell:

```powershell
$env:MAX_ROWS_PER_DATASET="20000"; python ml/train_models.py
```

After training, confirm these files exist:

```text
models/status_classifier.pkl
models/risk_regressor.pkl
models/anomaly_detector.pkl
models/heart_rate_forecaster.pkl
models/preprocessing_pipeline.pkl
models/feature_schema.json
models/model_metadata.json
models/model_metrics.json
```

## Final Demo Order

Run these commands in this order:

```bash
python ml/data_audit.py
python ml/train_models.py
docker compose up -d --build
docker ps
docker logs -f smart-health-producer
docker logs -f smart-health-spark-streaming
docker logs -f smart-health-dashboard
```

Open:

```text
http://localhost:5000
```

## Build And Start

Build and start the full system:

```bash
docker compose up -d --build
```

Check containers:

```bash
docker ps
```

Open the dashboard:

```text
http://localhost:5000
```

## Demo Commands

View producer logs:

```bash
docker logs -f smart-health-producer
```

View Spark logs:

```bash
docker logs -f smart-health-spark-streaming
```

View dashboard logs:

```bash
docker logs -f smart-health-dashboard
```

List Kafka topics:

```bash
docker exec -it smart-health-kafka kafka-topics --list --bootstrap-server localhost:9092
```

Consume Kafka messages:

```bash
docker exec -it smart-health-kafka kafka-console-consumer --topic smart-health-data --bootstrap-server localhost:9092 --from-beginning
```

Check Cassandra:

```bash
docker exec -it smart-health-cassandra cqlsh
```

Inside `cqlsh`:

```sql
USE smart_health;
SELECT * FROM sensor_readings LIMIT 5;
SELECT * FROM patient_alerts LIMIT 5;
SELECT * FROM patient_latest_status LIMIT 5;
```

Dashboard APIs:

```text
http://localhost:5000/api/latest
http://localhost:5000/api/alerts
http://localhost:5000/api/readings/patient-1
```

## Stop

```bash
docker compose down
```

Remove Cassandra data too:

```bash
docker compose down -v
```

## Validation Checklist

- `python ml/train_models.py` generates all required model files in `models/`.
- Docker starts all services successfully.
- Kafka topic `smart-health-data` exists.
- Producer sends JSON messages to Kafka.
- Kafka consumer can read messages.
- Spark reads Kafka messages.
- Spark mounts `./models:/models`.
- Spark logs loaded model artifacts when models are present.
- Spark uses ML models for `predicted_status`, `risk_score`, `is_anomaly`, `anomaly_score`, and `predicted_next_heart_rate`.
- Spark logs `ML models not found. Using rule-based fallback.` and keeps running if model files are absent.
- Spark also computes `rule_status` as fallback context.
- Spark generates `predicted_status`.
- Spark generates `risk_score`.
- Spark generates `is_anomaly` and `anomaly_score`.
- Spark generates `predicted_next_heart_rate` if possible.
- Spark generates `alert_type`, `alert_severity`, and `alert_message`.
- Cassandra stores AI outputs in `sensor_readings`.
- Cassandra stores important alerts in `patient_alerts`.
- Cassandra stores current AI status in `patient_latest_status`.
- Dashboard reads already-computed AI fields from Cassandra and does not call ML models.
- `/api/latest` exposes `predicted_status`, `risk_score`, `risk_level`, `is_anomaly`, `anomaly_score`, `predicted_next_heart_rate`, `alert_type`, `alert_severity`, and `alert_message`.
- `/api/alerts` exposes alert rows with AI status, risk, anomaly, and forecast fields.
- `/api/readings/<patient_id>` exposes enriched reading history with AI outputs.
- Dashboard shows predictions, risk scores, anomalies, forecasting output, and alerts.
- System keeps running after restart.

## Documentation

- [Architecture](docs/architecture.md)
- [AI Models](docs/ai_models.md)
- [Datasets](docs/datasets.md)
- [Demo Steps](docs/demo_steps.md)
- [Troubleshooting](docs/troubleshooting.md)
