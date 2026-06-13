# Architecture

## Overview

The Smart Health Monitoring IoT System follows an event-driven streaming architecture:

```
VitalsMonitorSensor    → health.vitals          ┐
BloodPressureSensor    → health.blood_pressure  │
GlucoseSensor          → health.glucose         ├→ Spark Enrichment → ML → Cassandra → Dashboard
ActivityTrackerSensor  → health.activity        │                  ↓
FallSafetySensor       → health.fall_safety     ┘            Email → Doctor
```

## Components

### 1. Producer (Sensor Simulator)

Five independent sensor classes run as daemon threads, each publishing to its own Kafka topic:

- **VitalsMonitorSensor** → `health.vitals` (every 15s)
- **BloodPressureSensor** → `health.blood_pressure` (every 45s)
- **GlucoseSensor** → `health.glucose` (every 60s)
- **ActivityTrackerSensor** → `health.activity` (every 30s)
- **FallSafetySensor** → `health.fall_safety` (every 15s)

Each sensor simulates realistic data using weighted condition profiles (75-80% normal, 12-15% warning, 5-8% critical, 1-3% emergency).

### 2. Apache Kafka

Serves as the message broker between sensors and Spark. Each sensor type has its own topic, enabling independent scaling and processing.

### 3. Apache Spark Structured Streaming

- Reads from all 5 topics simultaneously
- Parses sensor IDs, sensor types, timestamps, and battery metadata from all five topics
- Uses `health.vitals` as the primary stream and enriches it with time-windowed joins to the other sensor streams
- Persists source sensor IDs/types and latest sensor metadata to Cassandra
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

Stores 6 tables:
- `sensor_readings` — all enriched readings
- `patient_alerts` — critical alerts only
- `patient_latest_status` — latest status per patient (for dashboard)
- `email_alert_log` — email delivery history
- `sensor_metadata` — latest metadata and status for each simulated sensor
- `patient_minute_metrics` — one-minute aggregate metrics per patient

### 6. Flask Dashboard

Displays real-time patient data, auto-refreshing every 4 seconds:
- Patient status cards with color-coded health indicators
- Risk score bars and anomaly indicators
- Recent alerts table
- Readings history table
- Sensor metadata table
- Email alert configuration page

### 7. Email Alert System

Sends HTML emails to configured doctors when critical events occur:
- Rate-limited (configurable cooldown)
- Color-coded HTML with vital signs table
- Supports Gmail, Outlook, Yahoo, and local MailHog testing

## Data Flow

1. Sensor threads generate readings based on patient profiles
2. Events published to Kafka with patient_id as key
3. Spark reads all topics and joins them by patient and event-time window
4. Joined vitals rows carry source sensor IDs/types and battery metadata
5. Enriched rows are converted to Pandas for inference
6. ML models predict status, risk, anomalies, and next HR
7. Alert rules applied based on predictions
8. Enriched records written to Cassandra
9. Email sent for HIGH/CRITICAL alerts
10. Dashboard reads latest status from Cassandra

## Stream Join And Enrichment

The streaming job uses event-time joins tuned for the simulator cadence:
- Primary stream: `health.vitals` every 15 seconds
- Left-outer joins: blood pressure, glucose, activity, and fall-safety by `patient_id`
- Joined rows carry `sensor_id`, `sensor_type`, source lists, and worst available sensor battery
- If a slower sensor has not published inside the join window, medically neutral defaults are used

## foreachBatch Processing

Each micro-batch (every 10 seconds):
1. Convert joined enriched rows to Pandas
2. Apply ML inference per enriched row
3. Generate alerts
4. Write readings, latest status, sensor metadata, and one-minute metrics to Cassandra
5. Send email alerts (smtplib)
