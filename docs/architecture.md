# Smart Health Monitoring IoT System - Architecture

## Overview

The Smart Health Monitoring IoT System is an advanced real-time health monitoring platform that simulates patient sensor data, processes it through machine learning models, and provides real-time health status, risk assessment, and alert generation.

**Important Disclaimer**: This is an educational IoT simulation project only. It is not a clinical-grade system and should not be used for real patient monitoring or diagnosis.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  ┌──────────────────┐                                              │
│  │ Producer         │ Simulates 5+ patient health profiles        │
│  │ Simulator        │ Generates realistic vital signs             │
│  │ (producer.py)    │ Injects anomalies, falls, alerts           │
│  └────────┬─────────┘                                              │
│           │                                                        │
│           │ JSON Messages (Kafka topic: smart-health-data)        │
│           ▼                                                        │
│  ┌─────────────────────────┐                                      │
│  │  Apache Kafka           │ Message Queue                        │
│  │  - Broker               │ - Stores streaming data              │
│  │  - Zookeeper            │ - Enables replay                     │
│  │  - Topic: smart-health  │ - Supports scalability               │
│  └────────┬────────────────┘                                      │
│           │                                                        │
│           │ Kafka Connector (Internal)                            │
│           ▼                                                        │
│  ┌──────────────────────────────────────────┐                     │
│  │  Apache Spark Structured Streaming       │                    │
│  │  (spark/streaming_job.py)                │                    │
│  │                                          │                    │
│  │  1. Parse incoming JSON                  │                    │
│  │  2. Load ML models                       │                    │
│  │  3. Apply AI inference:                  │                    │
│  │     - Status Classification              │                    │
│  │     - Risk Score Regression              │                    │
│  │     - Anomaly Detection                  │                    │
│  │     - Heart Rate Forecasting             │                    │
│  │  4. Generate alerts based on rules       │                    │
│  │  5. Enrich data with predictions         │                    │
│  └────────┬──────────────────────────────────┘                    │
│           │                                                        │
│           │ Enriched Records + Alerts                             │
│           ▼                                                        │
│  ┌────────────────────────────────┐                              │
│  │  Apache Cassandra              │                              │
│  │  (cassandra/init.cql)          │                              │
│  │                                │                              │
│  │  Table 1: sensor_readings      │ All enriched data            │
│  │  Table 2: patient_alerts       │ Critical alerts              │
│  │  Table 3: patient_latest_status│ Quick dashboard lookup       │
│  └────────┬───────────────────────┘                              │
│           │                                                        │
│           │ REST APIs / CQL Queries                               │
│           ▼                                                        │
│  ┌────────────────────────────────┐                              │
│  │  Flask Dashboard               │                              │
│  │  (dashboard/app.py)            │                              │
│  │                                │                              │
│  │  - Real-time patient cards     │                              │
│  │  - Risk visualization          │                              │
│  │  - Alert notifications         │                              │
│  │  - API endpoints               │                              │
│  │  - 5-second auto-refresh       │                              │
│  └────────────────────────────────┘                              │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. Producer Simulator (`producer/producer.py`)

**Purpose**: Simulate realistic patient health sensor data

**Features**:
- 5 pre-defined patient profiles with stable demographics
- Realistic vital sign generation
- Weighted probability health conditions:
  - NORMAL: 76%
  - WARNING: 14%
  - CRITICAL: 7%
  - EMERGENCY: 3%
- Occasional anomalies:
  - Fall detection
  - Low battery events
  - Sensor spikes
- Complete schema with 30+ fields per reading

**Output**: JSON messages to Kafka topic `smart-health-data` every 3 seconds

### 2. Apache Kafka

**Purpose**: Event streaming and message brokering

**Configuration**:
- Broker: `kafka:29092` (internal Docker)
- External Port: `localhost:9092` (for local testing)
- Topic: `smart-health-data`
- Partition: 1 (simplified for educational use)
- Retention: 24 hours

**Message Format**: Full patient health record as JSON

### 3. Apache Spark Structured Streaming (`spark/streaming_job.py`)

**Purpose**: Real-time processing and ML inference

**Processing Pipeline**:
1. Read messages from Kafka
2. Parse JSON schema
3. Convert timestamp fields
4. Load pre-trained ML models:
   - Status Classifier (RandomForest)
   - Risk Regressor (RandomForest)
   - Anomaly Detector (IsolationForest)
   - Heart Rate Forecaster (RandomForest)
5. Apply inference using `foreachBatch` with pandas conversion
6. Generate alerts based on predictions
7. Write enriched data to Cassandra

**Key Features**:
- Graceful fallback to rule-based predictions if models missing
- Batch processing for efficiency
- Error handling per batch
- Adds 8 new AI-enriched fields

### 4. Machine Learning Pipeline (`ml/`)

**Components**:
- `data_audit.py`: Audit all datasets
- `prepare_datasets.py`: Load and prepare data
- `train_models.py`: Train 4 models
- `model_utils.py`: Utilities and schema definitions
- `models/`: Directory for trained artifacts

**Models**:
1. **Status Classifier**: Predicts NORMAL, WARNING, CRITICAL, EMERGENCY
2. **Risk Regressor**: Predicts risk score (0-100)
3. **Anomaly Detector**: Detects unusual sensor values
4. **Heart Rate Forecaster**: Predicts next heart rate

### 5. Apache Cassandra (`cassandra/init.cql`)

**Purpose**: Time-series data storage

**Tables**:
1. **sensor_readings**: Complete enriched records (40+ columns)
   - Partitioned by patient_id
   - Sorted by reading_time DESC
   - TTL: 30 days

2. **patient_alerts**: Critical alerts only
   - Partitioned by patient_id
   - Sorted by alert_time DESC
   - TTL: 90 days

3. **patient_latest_status**: Latest status per patient
   - Optimized for dashboard queries
   - TTL: 30 days

**Query Optimization**:
- Clustering orders for time queries
- Indexes on status, severity, risk_level
- Materialized views available (optional)

### 6. Flask Dashboard (`dashboard/app.py`)

**Purpose**: Real-time visualization and API

**Features**:
- Patient status cards with vital information
- Risk score visualization
- Alert notifications
- Auto-refresh every 5 seconds
- Health check endpoint
- System statistics

**API Endpoints**:
- `GET /` - Main dashboard
- `GET /api/latest` - All patient latest status (JSON)
- `GET /api/patient/<id>/latest` - Specific patient
- `GET /api/patient/<id>/readings` - Patient history
- `GET /api/alerts` - Recent alerts
- `GET /api/alerts/critical` - Critical alerts only
- `GET /api/health` - System health
- `GET /api/stats` - System statistics

## Data Flow

### End-to-End Message Flow

```
Producer generates reading
    ↓
JSON sent to Kafka (smart-health-data)
    ↓
Spark reads from Kafka
    ↓
Parse JSON → Load ML models → Apply inference
    ↓
Generate alerts → Enrich with AI fields
    ↓
Write to Cassandra (sensor_readings + patient_alerts)
    ↓
Dashboard queries Cassandra
    ↓
REST APIs return JSON
    ↓
Web interface updates (5 sec refresh)
    ↓
User sees real-time patient status
```

### Unified Data Schema

**Input Fields** (30 fields):
- Demographics: age, gender, weight, height, bmi
- Vital Signs: heart_rate, spo2, temperature, systolic_bp, diastolic_bp, respiratory_rate, glucose_level, skin_temperature
- Activity: activity_level, exercise_type, exercise_intensity, steps, stress_level, sleep_duration, sleep_quality, screen_time, notifications_received
- Sensors: fall_detected, battery_level
- Medical: chronic_condition, smoker, medication, predicted_disease_simulated

**AI Output Fields** (8 new fields):
- predicted_status: NORMAL, WARNING, CRITICAL, EMERGENCY
- risk_score: 0-100
- risk_level: LOW, MEDIUM, HIGH, CRITICAL
- is_anomaly: boolean
- anomaly_score: 0-1
- anomaly_type: description
- predicted_next_heart_rate: forecast
- alert_* fields: type, severity, message

## Alert Generation Logic

Alerts are generated based on predictions and rules:

1. **FALL_DETECTED**: If fall_detected == true
   - Severity: MEDIUM or HIGH

2. **EMERGENCY_HEALTH_ALERT**: If status == EMERGENCY or risk_score >= 85
   - Severity: CRITICAL

3. **HIGH_RISK_ALERT**: If status == CRITICAL or risk_score >= 70
   - Severity: HIGH

4. **WARNING_HEALTH_ALERT**: If status == WARNING or risk_score >= 45
   - Severity: MEDIUM

5. **ANOMALY_DETECTED**: If is_anomaly == true
   - Severity: MEDIUM

6. **LOW_SENSOR_BATTERY**: If battery_level < 15
   - Severity: LOW

7. **NO_ALERT**: Otherwise
   - Severity: NONE

## Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Message Queue | Apache Kafka | 7.6.1 |
| Streaming | Apache Spark | 3.5.1 |
| Database | Apache Cassandra | 4.1 |
| Backend | Flask | 2.x |
| ML Framework | scikit-learn | 1.x |
| Container | Docker & Docker Compose | Latest |

## Scalability Considerations

**Current Setup** (Educational):
- 5 patients
- 1 Kafka partition
- 1 Spark executor
- Single Cassandra node

**Future Enhancements**:
- Increase Kafka partitions for horizontal scaling
- Add Spark executors for parallel processing
- Cassandra cluster for high availability
- Load balancer for dashboard
- Caching layer (Redis) for frequently accessed data

## Deployment

All components run in Docker containers orchestrated by Docker Compose:

```
docker compose up -d --build
```

**Services**:
- zookeeper: Kafka coordination
- kafka: Message broker
- cassandra: Database
- cassandra-init: Schema initialization
- spark-master: Spark master node
- spark-worker: Spark worker node
- spark-streaming: Streaming job
- producer: Data generator
- dashboard: Web interface

## Monitoring and Logging

- Spark logs: `docker logs smart-health-spark-streaming`
- Producer logs: `docker logs smart-health-producer`
- Dashboard logs: `docker logs smart-health-dashboard`
- Cassandra logs: `docker logs smart-health-cassandra`

## Limitations and Assumptions

1. **Educational Simulation**: All data is synthetic and generated for demonstration
2. **Not for Clinical Use**: Do not use for real patient monitoring
3. **Single Node Cassandra**: Not production-ready for high-volume data
4. **Simple ML Models**: Baseline implementations; can be enhanced
5. **No Authentication**: Dashboard is open and unsecured
6. **No Data Persistence**: Kafka and Spark checkpoints reset on restart
7. **Limited History**: 30-day TTL on sensor data

## Future Enhancements

1. **Advanced ML**:
   - LSTM for time-series forecasting
   - Ensemble methods
   - Auto-tuning hyperparameters

2. **Architecture**:
   - Multi-region deployment
   - Event-driven alerting (SMS/Email)
   - GraphQL API

3. **Features**:
   - Patient trend analysis
   - Predictive hospitalization risk
   - Medication interaction checks
   - Integration with EHR systems

4. **Operations**:
   - Kubernetes deployment
   - Comprehensive monitoring (Prometheus/Grafana)
   - Audit logging
   - Data encryption

## References

- [Apache Kafka Documentation](https://kafka.apache.org/)
- [Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Apache Cassandra Documentation](https://cassandra.apache.org/)
- [scikit-learn Documentation](https://scikit-learn.org/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
