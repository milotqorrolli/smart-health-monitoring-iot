# Architecture

## Overview

The Smart Health Monitoring IoT System follows an event-driven streaming architecture:

```
VitalsMonitorSensor    → health.vitals          ┐
BloodPressureSensor    → health.blood_pressure  │
GlucoseSensor          → health.glucose         ├→ Spark Join → ML → Cassandra → Dashboard
ActivityTrackerSensor  → health.activity        │                  ↓
FallSafetySensor       → health.fall_safety     ┘            Email → Doctor
```

## Components

### 1. Producer (Sensor Simulator)

Five independent sensor classes run as daemon threads, each publishing to its own Kafka topic:

- **VitalsMonitorSensor** → `health.vitals` (every 5s)
- **BloodPressureSensor** → `health.blood_pressure` (every 15s)
- **GlucoseSensor** → `health.glucose` (every 30s)
- **ActivityTrackerSensor** → `health.activity` (every 10s)
- **FallSafetySensor** → `health.fall_safety` (every 5s)

Each sensor simulates realistic data using weighted condition profiles (75-80% normal, 12-15% warning, 5-8% critical, 1-3% emergency).

### 2. Apache Kafka

Serves as the message broker between sensors and Spark. Each sensor type has its own topic, enabling independent scaling and processing.

### 3. Apache Spark Structured Streaming

- Reads from all 5 topics simultaneously
- Applies event-time watermark (30 seconds) on each stream
- Performs stream-stream joins using `health.vitals` as the primary stream
- Converts micro-batches to Pandas for ML inference via `foreachBatch`
- Applies 4 ML models in sequence
- Generates alerts based on predictions
- Writes enriched results to Cassandra
- Sends email alerts for critical events

### 4. ML Inference Pipeline

Models are trained offline and loaded at Spark startup:
- Status Classifier (RandomForest/GradientBoosting)
- Risk Score Regressor
- Anomaly Detector (IsolationForest)
- Heart Rate Forecaster

If models are missing, the system falls back to rule-based logic.

### 5. Apache Cassandra

Stores 4 tables:
- `sensor_readings` — all enriched readings
- `patient_alerts` — critical alerts only
- `patient_latest_status` — latest status per patient (for dashboard)
- `email_alert_log` — email delivery history

### 6. Flask Dashboard

Displays real-time patient data, auto-refreshing every 4 seconds:
- Patient status cards with color-coded health indicators
- Risk score bars and anomaly indicators
- Recent alerts table
- Readings history table
- Email alert configuration page

### 7. Email Alert System

Sends HTML emails to configured doctors when critical events occur:
- Rate-limited (configurable cooldown)
- Color-coded HTML with vital signs table
- Supports Gmail, Outlook, Yahoo, and local MailHog testing

## Data Flow

1. Sensor threads generate readings based on patient profiles
2. Events published to Kafka with patient_id as key
3. Spark reads all topics, applies watermark, joins by patient_id
4. After join, batch converted to Pandas for inference
5. ML models predict status, risk, anomalies, and next HR
6. Alert rules applied based on predictions
7. Enriched records written to Cassandra
8. Email sent for HIGH/CRITICAL alerts
9. Dashboard reads latest status from Cassandra

## Stream-Stream Join

The join uses a 30-second watermark window:
- Primary stream: `health.vitals` (most frequent)
- Left-outer join: other streams by patient_id within ±30s
- If a slower sensor hasn't published yet, defaults are used

## foreachBatch Processing

Each micro-batch (every 10 seconds):
1. Convert Spark DataFrame to Pandas
2. Apply ML inference per row
3. Generate alerts
4. Write to Cassandra (cassandra-driver)
5. Send email alerts (smtplib)
