# Demo Steps

End-to-end demonstration walkthrough for university presentation.

## Pre-Demo Setup (Before Presentation)

```bash
# 1. Train ML models
pip install pandas numpy scikit-learn joblib openpyxl pyyaml
python ml/train_models.py

# 2. Start services (do this 5 minutes before demo)
docker compose up -d --build

# 3. Verify everything is running
docker ps
```

## Live Demo Script

### Step 1: Show Architecture (2 minutes)

Open `docs/architecture.md` or draw the architecture:
- 5 sensors → 5 Kafka topics → Spark join → ML → Cassandra → Dashboard

### Step 2: Show ML Training (3 minutes)

```bash
python ml/data_audit.py
```
- Show dataset selection decisions
- Show which datasets are USED vs EXCLUDED

```bash
python ml/train_models.py
```
- Show model training output
- Show metrics: F1, RMSE, anomaly detection rate

### Step 3: Show Running Services (2 minutes)

```bash
docker ps
```

Show all containers running. Point out:
- Producer generating sensor data
- Spark processing in real-time
- Dashboard serving results

### Step 4: Show Producer Logs (2 minutes)

```bash
docker logs -f smart-health-producer
```

Point out:
- 5 different sensor types publishing
- Different intervals (5s, 15s, 30s)
- Occasional events (falls, SpO2 drops)
- Per-patient readings

### Step 5: Show Kafka Topics (1 minute)

```bash
docker exec -it smart-health-kafka kafka-topics \
  --list --bootstrap-server localhost:9092
```

Show all 5 topics exist.

### Step 6: Show Live Dashboard (3 minutes)

Open http://localhost:5000

Point out:
- Patient status cards with color coding
- Risk score progress bars
- Vital signs display
- Anomaly indicators
- Alert messages
- Auto-refresh every 4 seconds

### Step 7: Show Cassandra Data (2 minutes)

```bash
docker exec -it smart-health-cassandra cqlsh
```

```sql
USE smart_health;
SELECT patient_id, predicted_status, risk_score, alert_type
FROM patient_latest_status;

SELECT patient_id, alert_time, alert_type, alert_severity
FROM patient_alerts LIMIT 5;
```

### Step 8: Show Email Settings (1 minute)

Open http://localhost:5000/settings/alerts

Show the configuration options:
- SMTP settings
- Doctor email
- Alert thresholds
- Rate limiting

### Step 9: Show Spark Processing (2 minutes)

```bash
docker logs -f smart-health-spark-streaming
```

Point out:
- Model loading at startup
- Batch processing every 10 seconds
- ML predictions being applied
- Cassandra writes

### Step 10: Summary (1 minute)

- 5 sensor types, independent threads
- Real-time stream processing with Spark
- 4 ML models: classification, regression, anomaly detection, forecasting
- Rule-based fallback when models unavailable
- Email alerting for critical events
- Live dashboard with auto-refresh

## Expected Output

### Producer Output
```
2026-06-12 10:00:00 [INFO] Started VITALS_MONITOR for patient-1 -> health.vitals (every 5s)
2026-06-12 10:00:00 [INFO] Started BLOOD_PRESSURE_MONITOR for patient-1 -> health.blood_pressure (every 15s)
...
2026-06-12 10:00:30 [INFO] Heartbeat: 25/25 sensors active
2026-06-12 10:01:05 [INFO] EVENT: Fall detected for patient-3!
```

### Dashboard
- Green cards for normal patients
- Orange/Red cards for patients with warnings/critical status
- Risk bars filling proportionally
- Alerts appearing in real-time

### Cassandra
```
patient-1 | NORMAL   | 22.5 | NO_ALERT
patient-2 | WARNING  | 48.3 | WARNING_HEALTH_ALERT
patient-3 | CRITICAL | 78.1 | HIGH_RISK_ALERT
```

## Troubleshooting During Demo

If dashboard shows "Waiting for data...":
- Spark may still be starting (wait 30-60 seconds)
- Check: `docker logs smart-health-spark-streaming`

If producer logs show errors:
- Kafka may not be ready yet
- The producer auto-retries

If Cassandra is empty:
- Wait for Spark to process first batch
- Check: `docker logs smart-health-cassandra-init`
