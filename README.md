# Smart Health Monitoring IoT System

An AI-powered real-time health monitoring system that simulates IoT health sensors, processes streaming data with Apache Spark, applies machine learning for predictions and anomaly detection, and displays results in a live Flask dashboard.

> **⚠️ Medical Disclaimer:** This is an educational IoT simulation project only. It is NOT a certified medical device. Do not use for real clinical decisions.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    SMART HEALTH MONITORING IoT SYSTEM                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────┐    ┌──────────────┐    ┌───────────────────────┐  │
│  │ VitalsMonitor    │───→│health.vitals │─┐  │                       │  │
│  │ BloodPressure    │───→│health.bp     │─┤  │  Spark Structured     │  │
│  │ Glucose          │───→│health.glucose│─┼─→│  Streaming             │  │
│  │ ActivityTracker  │───→│health.activity│─┤ │  (Join + ML Inference)│  │
│  │ FallSafety       │───→│health.fall   │─┘  │                       │  │
│  └─────────────────┘    └──────────────┘    └───────────┬───────────┘  │
│       Sensors               Kafka Topics                 │              │
│                                                          ▼              │
│                                              ┌───────────────────────┐  │
│  ┌─────────────────┐                         │   Apache Cassandra    │  │
│  │  Flask Dashboard │◄───────────────────────│   (Enriched Data)     │  │
│  │  (Live Display)  │                        └───────────┬───────────┘  │
│  └─────────────────┘                                     │              │
│                                                          ▼              │
│                                              ┌───────────────────────┐  │
│                                              │   Email Alerts →       │  │
│                                              │   Doctor Notification  │  │
│                                              └───────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Sensor Simulation | Python (multi-threaded) |
| Message Broker | Apache Kafka |
| Stream Processing | Apache Spark Structured Streaming |
| AI/ML Models | scikit-learn (4 models) |
| Database | Apache Cassandra |
| Dashboard | Flask + HTML/CSS |
| Email Alerts | SMTP (Gmail/MailHog) |
| Containerization | Docker + Docker Compose |

## AI Models

| Model | Purpose | Output |
|-------|---------|--------|
| Status Classifier | Predict health status | NORMAL / WARNING / CRITICAL / EMERGENCY |
| Risk Regressor | Predict numeric risk score | 0–100 |
| Anomaly Detector | Detect unusual sensor patterns | is_anomaly + anomaly_score |
| HR Forecaster | Predict next heart rate | predicted_next_heart_rate |

## Kafka Sensor Topics

| Sensor Class | Kafka Topic | Fields |
|-------------|-------------|--------|
| VitalsMonitorSensor | health.vitals | heart_rate, spo2, temperature, respiratory_rate |
| BloodPressureSensor | health.blood_pressure | systolic_bp, diastolic_bp |
| GlucoseSensor | health.glucose | glucose_level |
| ActivityTrackerSensor | health.activity | steps, activity_level, exercise_type, exercise_intensity |
| FallSafetySensor | health.fall_safety | fall_detected, skin_temperature |

## Prerequisites

- Docker Desktop (with 6GB+ RAM allocated)
- Docker Compose v2
- Python 3.9+ (for local ML training)
- pip

## How to Run

### Step 1: Clone and Setup

```bash
git clone <repository-url>
cd smart-health-monitoring-iot
cp .env.example .env  # Edit with your SMTP credentials (optional)
```

### Step 2: Install ML Training Dependencies

```bash
pip install pandas numpy scikit-learn joblib openpyxl pyyaml
```

### Step 3: Run Dataset Audit

```bash
python ml/data_audit.py
```

### Step 4: Train ML Models

```bash
python ml/train_models.py
```

### Step 5: Build and Start All Services

```bash
docker compose up -d --build
```

### Step 6: Verify Services

```bash
docker ps
```

### Step 7: View Logs

```bash
docker logs -f smart-health-producer         # Sensor events
docker logs -f smart-health-spark-streaming   # ML processing
docker logs -f smart-health-dashboard         # Dashboard
```

### Step 8: Verify Kafka Topics

```bash
docker exec -it smart-health-kafka kafka-topics \
  --list --bootstrap-server localhost:9092
```

### Step 9: Consume Sensor Events

```bash
# Vitals
docker exec -it smart-health-kafka kafka-console-consumer \
  --topic health.vitals --bootstrap-server localhost:9092 --from-beginning

# Blood Pressure
docker exec -it smart-health-kafka kafka-console-consumer \
  --topic health.blood_pressure --bootstrap-server localhost:9092 --from-beginning

# Glucose
docker exec -it smart-health-kafka kafka-console-consumer \
  --topic health.glucose --bootstrap-server localhost:9092 --from-beginning
```

### Step 10: Open Dashboard

- **Dashboard:** http://localhost:5000
- **Email Settings:** http://localhost:5000/settings/alerts
- **Spark Master UI:** http://localhost:8080
- **MailHog (if dev profile):** http://localhost:8025

### Step 11: Inspect Cassandra

```bash
docker exec -it smart-health-cassandra cqlsh
```

```sql
USE smart_health;
SELECT patient_id, reading_time, predicted_status, risk_score, alert_severity
FROM sensor_readings LIMIT 10;
SELECT * FROM patient_alerts LIMIT 10;
SELECT * FROM patient_latest_status;
SELECT * FROM email_alert_log LIMIT 10;
```

### Step 12: Configure Email Alerts (Optional)

To send real alert emails to your own address, create `.env` from `.env.example` and set:

```bash
ALERT_SMTP_ENABLED=true
ALERT_SMTP_HOST=smtp.gmail.com
ALERT_SMTP_PORT=587
ALERT_SMTP_USER=your-gmail-address@gmail.com
ALERT_SMTP_PASSWORD=your-16-character-app-password
ALERT_DOCTOR_EMAIL=your-recipient-email@example.com
ALERT_DOCTOR_NAME=Your Name
```

For Gmail, `ALERT_SMTP_PASSWORD` must be a Google App Password, not your normal account password. After restarting the services, open http://localhost:5000/settings/alerts and use **Send Test Email**.

For local testing with MailHog:
```bash
docker compose --profile dev up -d
```
Set in `.env`: `ALERT_SMTP_ENABLED=true`, `ALERT_SMTP_HOST=mailhog`, `ALERT_SMTP_PORT=1025`, and leave `ALERT_SMTP_USER` / `ALERT_SMTP_PASSWORD` blank.

### Stop

```bash
docker compose down       # Stop containers
docker compose down -v    # Stop and remove volumes
```

## Project Structure

```
smart-health-monitoring-iot/
├── producer/              # Sensor simulator (5 sensor classes)
│   ├── producer.py
│   └── Dockerfile
├── spark/                 # Spark streaming + Dockerfile
│   ├── streaming_job.py
│   └── Dockerfile
├── ml/                    # ML training pipeline
│   ├── data_audit.py
│   ├── prepare_datasets.py
│   ├── train_models.py
│   ├── model_utils.py
│   └── evaluate_models.py
├── models/                # Trained model artifacts (generated)
├── alerts/                # Email notification system
│   ├── email_notifier.py
│   └── alert_config.yml
├── cassandra/             # Schema initialization
│   └── init.cql
├── dashboard/             # Flask web dashboard
│   ├── app.py
│   ├── templates/
│   └── static/
├── datasets/              # Training datasets (14 files)
├── docs/                  # Documentation
├── docker-compose.yml
├── .env.example
├── .gitignore
└── README.md
```

## Documentation

- [Architecture Details](docs/architecture.md)
- [AI Models](docs/ai_models.md)
- [Datasets](docs/datasets.md)
- [Demo Steps](docs/demo_steps.md)
- [Troubleshooting](docs/troubleshooting.md)

## Medical Disclaimer

⚠️ **This is an educational IoT simulation project developed for university purposes only.**

- This system does NOT provide real medical diagnoses.
- Sensor data is simulated, not from real patients.
- ML predictions are for demonstration only.
- Do NOT use this system for actual patient care.
- All datasets used are synthetic or publicly available educational data.
