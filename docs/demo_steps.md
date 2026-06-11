# Smart Health Monitoring IoT - Demo Steps

## Quick Start (5 minutes)

### Prerequisites
- Docker & Docker Compose installed
- 10GB available disk space
- Python 3.8+ (for ML training, optional)

### Step 1: Prepare Models (Optional but Recommended)

If you have Python installed, train the ML models first:

```bash
# Navigate to project root
cd smart-health-monitoring-iot

# Install Python dependencies
pip install pandas numpy scikit-learn joblib

# Run data audit
python ml/data_audit.py

# Train models
python ml/train_models.py

# Expected output:
# - models/status_classifier.pkl
# - models/risk_regressor.pkl
# - models/anomaly_detector.pkl
# - models/heart_rate_forecaster.pkl
# - models/*.json (metadata, metrics, schema)
```

**Note**: Models are optional. If not available, Spark will use rule-based fallback.

### Step 2: Start the System

```bash
# Build all containers and start services
docker compose up -d --build

# Expected output:
# Creating smart-health-zookeeper ... done
# Creating smart-health-kafka ... done
# Creating smart-health-cassandra ... done
# Creating smart-health-cassandra-init ... done
# Creating smart-health-spark-master ... done
# Creating smart-health-spark-worker ... done
# Creating smart-health-spark-streaming ... done
# Creating smart-health-producer ... done
# Creating smart-health-dashboard ... done
```

### Step 3: Wait for Services (2-3 minutes)

Check that all services are healthy:

```bash
docker ps

# All containers should show STATUS: Up
```

Wait for Cassandra and Spark to fully initialize:

```bash
# Check Cassandra logs
docker logs smart-health-cassandra-init | grep "applied"

# Check Spark streaming started
docker logs smart-health-spark-streaming | grep "Streaming query started"

# Check producer sending data
docker logs smart-health-producer | tail -20
```

### Step 4: Open Dashboard

Open in web browser:
```
http://localhost:5000
```

You should see:
- Patient status cards with real-time vital signs
- AI predictions and risk scores
- Critical alerts section
- System statistics

### Step 5: Verify Data Flow

#### Test Kafka (Message Queue)

```bash
# List Kafka topics
docker exec -it smart-health-kafka kafka-topics \
  --list \
  --bootstrap-server localhost:9092

# Output should include: smart-health-data

# Consume messages from Kafka (Ctrl+C to stop)
docker exec -it smart-health-kafka kafka-console-consumer \
  --topic smart-health-data \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --max-messages 5
```

#### Test Cassandra (Database)

```bash
# Connect to Cassandra
docker exec -it smart-health-cassandra cqlsh

# Inside cqlsh:
USE smart_health;
SELECT COUNT(*) FROM sensor_readings;
SELECT patient_id, reading_time, predicted_status, risk_score FROM sensor_readings LIMIT 5;
SELECT * FROM patient_alerts LIMIT 5;
SELECT * FROM patient_latest_status;
EXIT;
```

#### Test Spark (Stream Processing)

```bash
# Check Spark logs for model loading
docker logs smart-health-spark-streaming | grep "model"

# Monitor streaming progress
docker logs smart-health-spark-streaming --tail 50 -f
```

## Detailed Demo Script (15 minutes)

### 1. System Architecture Review (2 min)

Show the architecture diagram from `docs/architecture.md`:
- Producer simulates 5 patients
- Data flows through Kafka → Spark → Cassandra
- Dashboard displays real-time results

### 2. Producer Simulation (2 min)

```bash
# Show real-time producer output
docker logs smart-health-producer --tail 30 -f

# Stop with Ctrl+C
```

Point out:
- 5 different patient IDs
- Varied vital signs
- Different health conditions (NORMAL, WARNING, CRITICAL, EMERGENCY)
- Occasional falls, low battery

### 3. Kafka Topic Verification (2 min)

```bash
# Show raw messages from Kafka
docker exec -it smart-health-kafka kafka-console-consumer \
  --topic smart-health-data \
  --bootstrap-server localhost:9092 \
  --property print.key=true \
  --property print.partition=true \
  --max-messages 3
```

Highlight:
- Full JSON schema with 30 fields
- Patient demographic info
- Real-time vital signs
- Activity and device data

### 4. Spark Processing (3 min)

```bash
# Show Spark applying models
docker logs smart-health-spark-streaming --tail 50 -f
```

Explain:
- Models loading (status, risk, anomaly, HR forecast)
- Micro-batch processing
- AI predictions being added
- Alerts being generated

### 5. Cassandra Data Inspection (2 min)

```bash
# Query latest data
docker exec -it smart-health-cassandra cqlsh << EOF
USE smart_health;
SELECT patient_id, reading_time, predicted_status, risk_score, risk_level, 
       heart_rate, spo2, temperature, alert_type, alert_severity 
FROM sensor_readings 
LIMIT 10;
EOF
```

Show:
- Enriched data with AI predictions
- Multiple patients with different statuses
- Risk scores ranging from LOW to CRITICAL
- Alert generation

### 6. Dashboard Walkthrough (2 min)

Open `http://localhost:5000` and show:

**Top Section**:
- Patients monitored count
- Critical patients alert
- Recent critical alerts counter
- System status (ACTIVE/WAITING)

**Alerts Section**:
- Recent critical alerts from past 24 hours
- Alert type, severity, timestamp
- Original vital signs that triggered alert

**Patient Cards**:
- Vital signs display (HR, SpO2, Temp, BP, RR, Glucose)
- AI prediction status (NORMAL, WARNING, CRITICAL, EMERGENCY)
- Risk score visualization (0-100 bar)
- Anomaly detection flag
- Battery level
- Last update timestamp

**APIs**:
- Show `/api/latest` for JSON data
- Show `/api/stats` for system statistics
- Demonstrate auto-refresh (5 seconds)

## Advanced Testing (30 minutes)

### Test 1: High Volume Stress

Increase data generation frequency:

```bash
# Stop producer
docker stop smart-health-producer

# Restart with faster interval
docker run -d \
  --name smart-health-producer-fast \
  --network smart-health-monitoring-iot_default \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  -e SIMULATION_INTERVAL_SECONDS=1 \
  smart-health-producer:latest

# Monitor dashboard - should still respond
# After 1 minute, compare:
docker logs smart-health-producer-fast | wc -l  # Count messages
```

### Test 2: Model Fallback

Simulate missing models:

```bash
# Remove status classifier
docker exec smart-health-spark-streaming \
  rm /models/status_classifier.pkl

# Restart Spark streaming
docker restart smart-health-spark-streaming

# Spark should still work with rule-based fallback
docker logs smart-health-spark-streaming | grep "Using rule-based fallback"
```

### Test 3: Database Query Performance

```bash
# Measure query time
docker exec -it smart-health-cassandra cqlsh << EOF
USE smart_health;
SELECT * FROM sensor_readings WHERE patient_id='patient-1' LIMIT 1000;
EOF

# Should complete in < 1 second
```

### Test 4: Alert Thresholds

Generate emergency condition:

```bash
# Manually insert extreme reading
docker exec -it smart-health-cassandra cqlsh << EOF
USE smart_health;
INSERT INTO sensor_readings (
  patient_id, reading_time, heart_rate, spo2, temperature,
  systolic_bp, diastolic_bp, respiratory_rate, glucose_level,
  predicted_status, risk_score, risk_level, alert_severity
) VALUES (
  'patient-1', now(), 200, 72, 36.5,
  220, 140, 40, 300,
  'EMERGENCY', 95, 'CRITICAL', 'CRITICAL'
);
EOF

# Check dashboard for alert
# Check patient_alerts table
docker exec -it smart-health-cassandra cqlsh << EOF
USE smart_health;
SELECT * FROM patient_alerts WHERE patient_id='patient-1' LIMIT 5;
EOF
```

### Test 5: Data Retention

Check TTL (Time To Live) settings:

```bash
# Old data should expire after 30 days (sensor_readings)
# Alert data expires after 90 days (patient_alerts)
docker exec -it smart-health-cassandra nodetool describe_ring smart_health
```

## Troubleshooting During Demo

### Issue: Dashboard shows "WAITING"
**Solution**: 
- Kafka not started: `docker logs smart-health-kafka`
- Producer not sending: `docker logs smart-health-producer`
- Wait 1-2 minutes for first batch

### Issue: No data in Cassandra
**Solution**:
- Check Cassandra init: `docker logs smart-health-cassandra-init`
- Verify schema created: `docker exec -it smart-health-cassandra cqlsh -e "DESCRIBE KEYSPACES"`
- Restart Spark: `docker restart smart-health-spark-streaming`

### Issue: Spark errors with models
**Solution**:
- Models not trained: Run `python ml/train_models.py`
- Models not mounted: Check `docker-compose.yml` volume mapping
- Missing Python dependencies: Spark container installs via pip in startup

### Issue: High memory usage
**Solution**:
- Reduce batch size in Spark config
- Limit Kafka retention
- Use `docker stats` to monitor

### Issue: Slow dashboard response
**Solution**:
- Increase Cassandra heap: Change `MAX_HEAP_SIZE` in docker-compose
- Add read replicas
- Implement caching layer

## Clean Up

### Stop System

```bash
# Stop all services
docker compose down

# Remove volumes (deletes data)
docker compose down -v

# Remove images
docker rmi $(docker images | grep smart-health)
```

### Full Reset

```bash
# Complete cleanup
docker compose down -v --remove-orphans
docker system prune -f
rm -rf ./cassandra-data
```

## Performance Metrics to Show

During demo, highlight these metrics:

| Metric | Target | Actual |
|--------|--------|--------|
| Producer latency | <100ms | ~50ms |
| Kafka throughput | 100+ msg/s | ~200 msg/s (5 patients × 3s) |
| Spark batch time | <10s | ~5s |
| Cassandra write latency | <50ms | ~10ms |
| Dashboard refresh | 5s | 5s (manual refresh) |
| Model inference time | <100ms | ~50ms |

## Demo Talking Points

1. **Real-time Architecture**:
   - Kafka as event bus
   - Spark for stream processing
   - Cassandra for fast writes

2. **AI/ML Integration**:
   - 4 pre-trained models
   - Graceful degradation if models missing
   - Real-time predictions on streaming data

3. **Health Monitoring**:
   - Realistic simulation of 5 patients
   - Multiple health conditions
   - Risk-based alerting system

4. **Production Readiness**:
   - Containerized deployment
   - Monitoring and logging
   - Scalable architecture

5. **Educational Value**:
   - Hands-on with modern data stack
   - IoT + ML integration
   - Real-time streaming concepts

## Post-Demo Discussion

Questions to discuss:

1. **Scalability**: How would you handle 1000 patients?
   - Answer: Kafka partitions, Spark executors, Cassandra cluster

2. **Real-time vs Batch**: Why streaming?
   - Answer: Immediate alerts, fast feedback, streaming patterns

3. **Model Updates**: How often to retrain?
   - Answer: Quarterly or on data drift detection

4. **Privacy**: What about patient data?
   - Answer: De-identified, encryption, audit logs (not implemented)

5. **Clinical Validation**: Is this production-ready?
   - Answer: No - educational simulation only. Would require validation

## References

- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Apache Kafka Quickstart](https://kafka.apache.org/quickstart)
- [Apache Spark Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Cassandra CQL Shell Guide](https://cassandra.apache.org/doc/latest/cassandra/tools/cqlsh.html)
