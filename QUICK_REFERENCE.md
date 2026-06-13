# Smart Health Monitoring IoT - QUICK REFERENCE GUIDE

## 🚀 Get Started in 5 Steps (10 minutes)

```bash
# 1. Train ML Models (creates models/*.pkl)
python ml/train_models.py

# 2. Start Docker Services
docker compose up -d --build

# 3. Wait for initialization (2-3 min)
docker ps  # All services should show "Up"

# 4. Validate everything
bash validate.sh

# 5. Open Dashboard
# Browser: http://localhost:5000
```

---

## 📋 Essential Commands

### Training & Setup
```bash
# Audit datasets
python ml/data_audit.py

# Train all 4 models
python ml/train_models.py

# View trained models
ls -lah models/

# Check model metadata
cat models/model_metadata.json | python -m json.tool

# Check model metrics
cat models/model_metrics.json | python -m json.tool
```

### Docker Operations
```bash
# Start all services
docker compose up -d --build

# Stop all services
docker compose down

# View running services
docker compose ps

# View all logs
docker compose logs

# Follow specific service logs
docker logs -f smart-health-producer
docker logs -f smart-health-spark-streaming
docker logs -f smart-health-dashboard

# Restart a service
docker compose restart smart-health-producer
```

### Verification
```bash
# Run automated validation
bash validate.sh

# Check if models loaded by Spark
docker logs smart-health-spark-streaming | grep -i "loaded\|fallback"

# Check Kafka connectivity
docker exec -it smart-health-kafka kafka-topics --list --bootstrap-server localhost:9092

# Check Cassandra
docker exec -it smart-health-cassandra cqlsh -e "DESCRIBE KEYSPACES"
```

### Database Queries
```bash
# Open Cassandra shell
docker exec -it smart-health-cassandra cqlsh

# Inside cqlsh:
USE smart_health;
SELECT COUNT(*) FROM sensor_readings;
SELECT * FROM sensor_readings LIMIT 5;
SELECT * FROM patient_alerts LIMIT 5;
SELECT * FROM patient_latest_status;
SELECT * FROM sensor_metadata LIMIT 10;
SELECT * FROM patient_minute_metrics WHERE patient_id = 'patient-1' LIMIT 10;

# Exit cqlsh
EXIT;
```

### Dashboard API Endpoints
```bash
# Latest patient data (ALL patients)
curl http://localhost:5000/api/latest

# Single patient latest
curl http://localhost:5000/api/patient/patient-1/latest

# Patient reading history
curl http://localhost:5000/api/patient/patient-1/readings?limit=10

# Recent alerts
curl http://localhost:5000/api/alerts?limit=10

# Critical alerts (last N hours)
curl http://localhost:5000/api/alerts/critical?hours=1

# Sensor metadata
curl http://localhost:5000/api/sensors

# One-minute patient aggregates
curl http://localhost:5000/api/metrics/patient-1?limit=10

# System statistics
curl http://localhost:5000/api/stats

# Health check
curl http://localhost:5000/api/health

# Format output as JSON
curl http://localhost:5000/api/latest | python -m json.tool
```

### Debugging
```bash
# View producer output
docker logs smart-health-producer | tail -20

# View Spark errors
docker logs smart-health-spark-streaming | grep -i error

# View dashboard errors
docker logs smart-health-dashboard | grep -i error

# Check container health
docker ps --format "table {{.Names}}\t{{.Status}}"

# Inspect container
docker inspect smart-health-cassandra | grep -A 5 "Health"

# Check network
docker network ls
docker network inspect smart-health-monitoring-iot_default

# View resource usage
docker stats
```

### Cleanup
```bash
# Stop and remove all containers
docker compose down

# Remove volumes (deletes data!)
docker compose down -v

# Remove images
docker rmi $(docker images | grep smart-health)

# Clean up everything
docker system prune
```

---

## 🎯 Common Tasks

### Regenerate Models
```bash
# Delete old models
rm models/*.pkl models/*.json

# Retrain
python ml/train_models.py

# Verify new models
ls -la models/
```

### Restart Spark (to reload models)
```bash
docker compose restart smart-health-spark-streaming

# Wait 30 seconds, then check logs
docker logs -f smart-health-spark-streaming
```

### Reset Everything
```bash
# Stop services
docker compose down -v

# Delete models
rm models/*.pkl models/*.json

# Retrain
python ml/train_models.py

# Restart
docker compose up -d --build

# Validate
bash validate.sh
```

### Monitor in Real-Time
```bash
# Terminal 1: Producer output
docker logs -f smart-health-producer

# Terminal 2: Spark processing
docker logs -f smart-health-spark-streaming

# Terminal 3: Dashboard requests
docker logs -f smart-health-dashboard

# Terminal 4: Cassandra queries
docker exec -it smart-health-cassandra cqlsh
# SELECT COUNT(*) FROM smart_health.sensor_readings;
```

### Check Model Usage
```bash
# Verify Spark using models
docker logs smart-health-spark-streaming 2>&1 | grep -E "Loaded|fallback" | head -10

# View how many predictions made
docker exec -it smart-health-cassandra cqlsh -e "USE smart_health; SELECT COUNT(*) FROM sensor_readings;"

# Check prediction values
docker exec -it smart-health-cassandra cqlsh << EOF
USE smart_health;
SELECT patient_id, predicted_status, risk_score, is_anomaly FROM sensor_readings LIMIT 10;
EOF
```

---

## 🐛 Quick Troubleshooting

### Problem: "Models not found"
```bash
# Solution: Train models first
python ml/train_models.py
docker compose restart smart-health-spark-streaming
```

### Problem: Cassandra connection refused
```bash
# Solution: Wait longer for startup (3 min) or restart
docker compose down
docker compose up -d --build
# Wait 3 minutes
docker logs smart-health-cassandra | tail -20
```

### Problem: No data in dashboard
```bash
# Check each component
docker logs smart-health-producer | grep -i patient
docker logs smart-health-spark-streaming | grep -i "error\|exception"
docker logs smart-health-cassandra | grep -i "error"
```

### Problem: Dashboard returns empty
```bash
# Check if Cassandra has data
docker exec -it smart-health-cassandra cqlsh -e "USE smart_health; SELECT COUNT(*) FROM sensor_readings;"

# Check if dashboard can connect
curl http://localhost:5000/api/latest
```

### Problem: Port already in use
```bash
# Find process using port 5000
lsof -i :5000

# Kill it
kill -9 <PID>

# Or change docker-compose port mapping and rebuild
docker compose down
docker compose up -d --build
```

---

## 📊 What To Expect

### Startup Timeline (after `docker compose up`)
- **T+0s**: Services starting
- **T+10s**: Kafka ready
- **T+30s**: Cassandra initializing
- **T+60s**: Cassandra ready, schema created
- **T+90s**: Spark loading models
- **T+120s**: Producer sending data
- **T+150s**: First data in Cassandra
- **T+180s**: Dashboard showing data

### Data Flow Rate
- **Producer**: About 1 Kafka message/second across 25 simulated sensors
- **Spark**: Batches every 10 seconds and enriches vitals-primary records
- **Cassandra**: Stores readings, latest status, alerts, sensor metadata, and minute aggregates
- **Dashboard**: Refreshes every 4 seconds

### Disk Usage
- **Total**: ~2-5 GB
- **Cassandra**: Grows over time (TTL: 30 days)
- **Logs**: ~100-500 MB

---

## 🎓 Project Structure

```
smart-health-monitoring-iot/
├── README.md                           # Main documentation
├── FINAL_COMPLETION_REPORT.md          # This project status
├── validate.sh                         # Automated validation script
├── docker-compose.yml                  # Docker orchestration
│
├── ml/                                 # ML Pipeline
│   ├── train_models.py                 # Train all 4 models
│   ├── prepare_datasets.py             # Dataset loading
│   ├── model_utils.py                  # Utilities & enums
│   ├── data_audit.py                   # Data validation
│   ├── __init__.py                     # Module exports
│   └── requirements.txt                # ML dependencies
│
├── spark/                              # Real-time Processing
│   └── streaming_job.py                # Spark + ML inference
│
├── producer/                           # Data Simulator
│   ├── producer.py                     # 5 patient simulator
│   ├── Dockerfile                      # Container image
│   └── requirements.txt                # Dependencies
│
├── dashboard/                          # Flask Web UI
│   ├── app.py                          # REST API endpoints
│   ├── Dockerfile                      # Container image
│   ├── requirements.txt                # Dependencies
│   ├── templates/
│   │   └── index.html                  # Web UI
│   └── static/
│       └── style.css                   # Styling
│
├── cassandra/                          # Database Schema
│   └── init.cql                        # 6 tables, enriched fields, metadata
│
├── models/                             # ML Artifacts (created by training)
│   ├── status_classifier.pkl           # Trained model
│   ├── risk_regressor.pkl              # Trained model
│   ├── anomaly_detector.pkl            # Trained model
│   ├── heart_rate_forecaster.pkl       # Trained model
│   ├── model_metadata.json             # Model info
│   ├── model_metrics.json              # Performance metrics
│   └── feature_schema.json             # Feature definitions
│
└── docs/                               # Documentation
    ├── architecture.md                 # System design
    ├── ai_models.md                    # ML models explained
    ├── datasets.md                     # Data sources
    ├── demo_steps.md                   # Demo walkthrough
    ├── troubleshooting.md              # 30+ issue solutions
    └── END_TO_END_VALIDATION.md        # Validation checklist
```

---

## 🔗 Useful Links

- **Dashboard**: http://localhost:5000
- **API Latest**: http://localhost:5000/api/latest
- **API Stats**: http://localhost:5000/api/stats
- **API Sensors**: http://localhost:5000/api/sensors
- **API Metrics**: http://localhost:5000/api/metrics/patient-1
- **Kafka UI** (if running): http://localhost:8080
- **Cassandra**: localhost:9042 (CQL)

---

## 📝 Environment Variables

### Docker Compose
```bash
KAFKA_BOOTSTRAP_SERVERS=kafka:29092         # Internal Kafka address
CASSANDRA_HOST=cassandra                    # Cassandra hostname
CASSANDRA_PORT=9042                         # Cassandra port
MODELS_PATH=/models                         # Mounted model directory
DASHBOARD_PORT=5000                         # Flask port
```

### Python (ml/train_models.py)
```bash
DATASETS_PATH=./datasets                    # Input datasets
MODELS_PATH=./models                        # Output models directory
```

---

## 🎯 Success Criteria

### You know it's working when:

✅ `docker compose ps` shows 9 services UP
✅ `bash validate.sh` shows "ALL TESTS PASSED"
✅ `curl http://localhost:5000/api/latest` returns JSON with AI fields
✅ `curl http://localhost:5000/api/sensors` returns sensor metadata
✅ Dashboard shows 5 patient cards with vitals & predictions
✅ Cassandra has sensor_readings with predicted_status values
✅ Logs show "Loaded model:"
✅ Data auto-refreshes every 4 seconds
✅ Alerts appear in feed

### If not working:
1. Check: `docker compose ps` (all UP?)
2. Wait: Services need 3 minutes to initialize
3. Check: `bash validate.sh` (which tests failed?)
4. Read: [docs/troubleshooting.md](docs/troubleshooting.md)
5. Ask: Check logs with `docker logs <service>`

---

## 📞 Support

### Documentation Files
- 🏗️ [Architecture](docs/architecture.md) - System design
- 🤖 [AI Models](docs/ai_models.md) - ML explained
- 📊 [Datasets](docs/datasets.md) - Data sources
- 🎬 [Demo Steps](docs/demo_steps.md) - Walkthrough
- 🐛 [Troubleshooting](docs/troubleshooting.md) - 30+ solutions
- ✅ [Validation](docs/END_TO_END_VALIDATION.md) - Test guide

### Quick Help
- Stuck? Run: `bash validate.sh`
- Errors? Check: `docker logs <service>`
- Reset? Run: `docker compose down && docker compose up -d --build`
- Learn? Read: [README.md](README.md)

---

**Smart Health Monitoring IoT - v1.0**
*Production-Ready | Fully Tested | End-to-End Validated*
