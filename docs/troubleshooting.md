# Smart Health Monitoring IoT - Troubleshooting Guide

## Common Issues and Solutions

This guide covers common problems you might encounter and how to resolve them.

## Docker & Container Issues

### Issue: Docker daemon not running
**Symptoms**:
- `Cannot connect to Docker daemon`
- `docker: command not found`

**Solutions**:
```bash
# On Windows/Mac, start Docker Desktop
# On Linux, start Docker service
sudo systemctl start docker

# Verify Docker is running
docker ps
docker version
```

### Issue: Port already in use
**Symptoms**:
- `Error starting container: port is already allocated`
- Dashboard won't load at http://localhost:5000

**Solutions**:
```bash
# Check what's using port 5000
netstat -tuln | grep 5000   # Linux/Mac
netstat -ano | findstr :5000  # Windows

# Kill process using port (e.g., port 5000)
kill <PID>  # Linux/Mac
taskkill /PID <PID> /F  # Windows

# Or use different ports in docker-compose.yml
services:
  dashboard:
    ports:
      - "5001:5000"  # Changed from 5000:5000
```

### Issue: Out of disk space
**Symptoms**:
- `no space left on device`
- Services crash unexpectedly
- Docker build fails

**Solutions**:
```bash
# Check disk usage
df -h

# Remove old volumes and images
docker compose down -v
docker system prune -f

# Remove dangling images
docker rmi $(docker images -f "dangling=true" -q)

# Free up 10GB+ space before retrying
```

### Issue: Insufficient memory
**Symptoms**:
- Cassandra/Spark containers killed
- Dashboard crashes intermittently
- High swap usage

**Solutions**:
```bash
# Monitor resource usage
docker stats

# Reduce memory in docker-compose.yml
services:
  cassandra:
    environment:
      MAX_HEAP_SIZE: "256M"  # Reduce from 512M
      HEAP_NEWSIZE: "100M"

  spark-streaming:
    environment:
      SPARK_DRIVER_MEMORY: "512m"  # Reduce from 1g
```

### Issue: Container keeps restarting
**Symptoms**:
- `docker compose logs` shows repeated restarts
- Container status shows `Restarting`

**Solutions**:
```bash
# Check logs for specific error
docker logs <container_name>

# Get full error details
docker logs --tail 100 <container_name>

# Verify container can start alone
docker run -it --name test <image_name> /bin/bash

# Common causes: network connectivity, missing dependencies
```

## Kafka Issues

### Issue: Kafka topic not found
**Symptoms**:
- `Topic 'smart-health-data' does not exist`
- Producer errors about missing topic

**Solutions**:
```bash
# Verify Kafka is running
docker compose ps kafka

# Create topic manually
docker exec -it smart-health-kafka kafka-topics \
  --create \
  --topic smart-health-data \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# List all topics
docker exec -it smart-health-kafka kafka-topics \
  --list \
  --bootstrap-server localhost:9092
```

### Issue: Producer can't connect to Kafka
**Symptoms**:
- Producer logs show `Connection refused`
- Messages not appearing in Kafka

**Solutions**:
```bash
# Check Kafka container is healthy
docker compose logs kafka

# Verify network connectivity
docker exec smart-health-producer ping kafka

# Check Kafka bootstrap server config
# Should be: kafka:29092 (internal)
# NOT: localhost:9092

# Wait for Kafka to be ready (can take 1-2 minutes)
sleep 60
docker compose up -d producer
```

### Issue: Consumer lag increasing
**Symptoms**:
- Messages backing up in Kafka
- Dashboard data stale

**Solutions**:
```bash
# Check consumer group status
docker exec -it smart-health-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list

# Check lag
docker exec -it smart-health-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group <group_name> \
  --describe

# Restart Spark streaming if stuck
docker restart smart-health-spark-streaming

# Check Spark processing logs
docker logs smart-health-spark-streaming --tail 100
```

## Cassandra Issues

### Issue: Cassandra won't start
**Symptoms**:
- `docker compose logs cassandra` shows errors
- Status checks failing

**Solutions**:
```bash
# Check Cassandra logs
docker logs smart-health-cassandra

# Verify storage isn't corrupted
docker compose down -v cassandra

# Restart with fresh volume
docker compose up -d cassandra

# Wait 30 seconds for startup
sleep 30
docker exec smart-health-cassandra cqlsh -e "DESCRIBE KEYSPACES"
```

### Issue: No data in Cassandra
**Symptoms**:
- `SELECT * FROM sensor_readings` returns 0 rows
- Dashboard shows "No data available"

**Solutions**:
```bash
# Check if init script ran
docker logs smart-health-cassandra-init | tail -20

# Manually apply schema if init didn't run
docker exec -it smart-health-cassandra cqlsh << EOF
CREATE KEYSPACE IF NOT EXISTS smart_health WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS smart_health.sensor_readings (
  patient_id TEXT,
  reading_time TIMESTAMP,
  heart_rate INT,
  PRIMARY KEY ((patient_id), reading_time)
) WITH CLUSTERING ORDER BY (reading_time DESC) AND default_time_to_live = 2592000;
EOF

# Verify tables exist
docker exec -it smart-health-cassandra cqlsh << EOF
USE smart_health;
SHOW TABLES;
EOF

# Check Spark logs for insert errors
docker logs smart-health-spark-streaming | grep -i "cassandra\|insert\|error"
```

### Issue: Cassandra connection timeout
**Symptoms**:
- `Unable to connect to cassandra:9042`
- Dashboard throws Cassandra errors

**Solutions**:
```bash
# Verify Cassandra is running and healthy
docker compose ps cassandra

# Check if port 9042 is open
docker exec smart-health-cassandra nc -zv localhost 9042

# Test connectivity from dashboard
docker exec smart-health-dashboard python -c \
  "from cassandra.cluster import Cluster; print(Cluster(['cassandra']).connect())"

# Restart both Cassandra and Dashboard
docker restart smart-health-cassandra
sleep 30
docker restart smart-health-dashboard
```

### Issue: Out of disk space (Cassandra data)
**Symptoms**:
- `No space left on device` in Cassandra logs
- Cassandra crashes repeatedly

**Solutions**:
```bash
# Check Cassandra data volume size
docker exec smart-health-cassandra df -h /var/lib/cassandra

# Clean old data (keep only last 30 days)
# Edit cassandra/init.cql to set lower TTL
TTL 2592000  # 30 days

# Or remove and recreate
docker compose down -v cassandra
docker compose up -d cassandra
```

## Spark Issues

### Issue: Spark container exits immediately
**Symptoms**:
- `docker ps` doesn't show spark-streaming
- `docker compose logs spark-streaming` shows error

**Solutions**:
```bash
# Check Spark logs for specific error
docker logs smart-health-spark-streaming | tail -50

# Common causes:
# 1. Kafka not ready
# 2. Models not found
# 3. Memory issues

# Ensure Kafka is ready
docker compose up -d kafka
sleep 30

# Ensure models directory exists
ls models/
mkdir -p models

# Try restarting with wait
docker compose up -d kafka cassandra
sleep 30
docker compose up -d spark-streaming
```

### Issue: Spark batch processing slow
**Symptoms**:
- Batch time increasing over time
- Data lagging behind real-time

**Solutions**:
```bash
# Monitor Spark metrics
docker logs smart-health-spark-streaming | grep "Batch\|Duration"

# Increase batch interval in streaming_job.py
spark.readStream.option("startingOffsets", "latest") \
  .option("maxOffsetsPerTrigger", 100) \  # Increase from default

# Add more Spark executors in docker-compose.yml
command: [
  "spark-submit",
  "--driver-memory", "2g",      # Increase
  "--executor-memory", "2g",    # Increase
  "--conf", "spark.executor.cores=4"  # Increase
]

# Reduce Cassandra write operations (batch them)
```

### Issue: Models not loading in Spark
**Symptoms**:
- `FileNotFoundError: models/status_classifier.pkl`
- Spark logs show "Using rule-based fallback"
- No ML predictions in dashboard

**Solutions**:
```bash
# Verify models exist locally
ls -la models/

# Train models if missing
python ml/data_audit.py
python ml/train_models.py

# Verify volume mount in docker-compose.yml
services:
  spark-streaming:
    volumes:
      - ./models:/models:ro  # Must exist

# Restart Spark with models available
docker compose down spark-streaming
docker compose up -d spark-streaming

# Check Spark logs for model loading
docker logs smart-health-spark-streaming | grep -i "model\|load"
```

### Issue: Python dependency not found in Spark
**Symptoms**:
- `ModuleNotFoundError: No module named 'pandas'`
- Spark container exits

**Solutions**:
```bash
# Check pip install ran in startup
docker logs smart-health-spark-streaming | grep -i "pip install"

# Verify docker-compose.yml has pip install command
services:
  spark-streaming:
    command: |
      bash -c "
      pip install pandas numpy scikit-learn joblib &&
      spark-submit ...
      "

# Try installing manually
docker exec smart-health-spark-streaming pip install pandas

# If stuck, rebuild
docker compose down spark-streaming
docker compose up -d --build spark-streaming
```

## Producer Issues

### Issue: Producer not sending data
**Symptoms**:
- No messages in Kafka topic
- Dashboard shows "waiting for data"

**Solutions**:
```bash
# Check producer is running
docker compose ps producer

# Check producer logs
docker logs smart-health-producer | tail -50

# Verify Kafka connection
docker exec smart-health-producer python -c \
  "from kafka import KafkaProducer; print(KafkaProducer(bootstrap_servers='kafka:29092'))"

# Restart producer
docker restart smart-health-producer

# Wait 10 seconds and check for messages
sleep 10
docker exec -it smart-health-kafka kafka-console-consumer \
  --topic smart-health-data \
  --bootstrap-server localhost:9092 \
  --max-messages 1
```

### Issue: Producer crashes with low memory
**Symptoms**:
- Producer killed unexpectedly
- `docker logs producer` shows OOM

**Solutions**:
```bash
# Reduce memory usage in producer.py
# Reduce NUM_PATIENTS or generation frequency

# Or increase container memory in docker-compose.yml
services:
  producer:
    mem_limit: 512m  # Increase if available

# Monitor memory while running
docker stats smart-health-producer
```

## Dashboard Issues

### Issue: Dashboard won't load
**Symptoms**:
- `Connection refused` when accessing http://localhost:5000
- Browser shows error

**Solutions**:
```bash
# Check dashboard container is running
docker compose ps dashboard

# Check if Flask app started
docker logs smart-health-dashboard | tail -20

# Verify port is accessible
curl -v http://localhost:5000

# Restart dashboard
docker restart smart-health-dashboard

# Check for Python errors
docker logs smart-health-dashboard | grep -i "error\|exception"
```

### Issue: Dashboard shows "WAITING" status
**Symptoms**:
- No patient data displayed
- Status bar shows "WAITING"

**Solutions**:
```bash
# Check if producer is sending data
docker exec -it smart-health-kafka kafka-console-consumer \
  --topic smart-health-data \
  --bootstrap-server localhost:9092 \
  --max-messages 1

# If no messages:
docker restart smart-health-producer
sleep 10

# Check if Spark is processing
docker logs smart-health-spark-streaming | grep -i "batch\|processed"

# Check Cassandra has data
docker exec -it smart-health-cassandra cqlsh << EOF
USE smart_health;
SELECT COUNT(*) FROM sensor_readings;
EOF

# If count is 0, check Spark insert logs
docker logs smart-health-spark-streaming | grep -i "insert\|write"
```

### Issue: Dashboard shows old data
**Symptoms**:
- Data not updating
- Timestamps are stale

**Solutions**:
```bash
# Check auto-refresh is working (every 5 seconds)
# Open browser console (F12) and check network tab
# Should see GET /api/latest every 5 seconds

# Check Cassandra query is returning latest
docker exec -it smart-health-cassandra cqlsh << EOF
USE smart_health;
SELECT patient_id, reading_time FROM sensor_readings 
LIMIT 5;
EOF

# Verify timestamps are recent
# Should be within last few minutes

# Restart dashboard to clear cache
docker restart smart-health-dashboard
```

### Issue: Dashboard API errors
**Symptoms**:
- API endpoints return 500 errors
- Dashboard console shows errors

**Solutions**:
```bash
# Check dashboard logs
docker logs smart-health-dashboard | grep -i "error\|exception"

# Test specific endpoint
curl -v http://localhost:5000/api/latest

# Check Cassandra connection from dashboard
docker exec smart-health-dashboard python << EOF
from cassandra.cluster import Cluster
cluster = Cluster(['cassandra'])
session = cluster.connect('smart_health')
print(session.execute('SELECT COUNT(*) FROM sensor_readings')[0][0])
EOF

# Check database connectivity
docker exec smart-health-dashboard python -c \
  "import socket; socket.create_connection(('cassandra', 9042), timeout=5)"
```

## Data & Schema Issues

### Issue: Cassandra schema mismatch
**Symptoms**:
- `InvalidRequest: Error from server: code=1200`
- Column not found errors

**Solutions**:
```bash
# Check existing schema
docker exec -it smart-health-cassandra cqlsh << EOF
USE smart_health;
DESCRIBE TABLE sensor_readings;
EOF

# If schema is wrong, backup and recreate
docker exec -it smart-health-cassandra cqlsh << EOF
USE smart_health;
DROP TABLE IF EXISTS sensor_readings;
DROP TABLE IF EXISTS patient_alerts;
DROP TABLE IF EXISTS patient_latest_status;
DROP KEYSPACE smart_health;
EOF

# Restart Cassandra init
docker compose restart cassandra-init
sleep 30

# Verify new schema
docker exec -it smart-health-cassandra cqlsh << EOF
USE smart_health;
DESCRIBE TABLES;
EOF
```

### Issue: Data gaps in readings
**Symptoms**:
- Missing readings for certain patients
- Dashboard shows incomplete history

**Solutions**:
```bash
# Check producer is generating all patients
docker logs smart-health-producer | grep "patient_id" | sort | uniq -c

# Should show all 5 patients equally

# Check Spark filtering
docker logs smart-health-spark-streaming | grep -i "filter\|drop"

# Query database for gaps
docker exec -it smart-health-cassandra cqlsh << EOF
USE smart_health;
SELECT patient_id, COUNT(*) FROM sensor_readings 
GROUP BY patient_id;
EOF

# Should show roughly equal counts per patient
```

## ML Model Issues

### Issue: Models not making predictions
**Symptoms**:
- `predicted_status` always null
- Risk scores all 0 or default values

**Solutions**:
```bash
# Check models exist and are valid
python -c "import pickle; pickle.load(open('models/status_classifier.pkl', 'rb'))"

# Check model artifacts
ls -la models/

# Verify model was trained on correct data
python ml/train_models.py

# Check Spark is loading models
docker logs smart-health-spark-streaming | grep -i "model\|predict"

# Test model inference locally
python << EOF
import pickle
import pandas as pd
clf = pickle.load(open('models/status_classifier.pkl', 'rb'))
print(clf.predict([[30, 75, 98, 120, 80, 16, 100, 0, 6, 0, 0, 0, 3, 50, 1, 3, 5000, 2, 7, 1, 2, 1, 95, 0, 0, 0, 0, 0, 0, 0]]))
EOF
```

### Issue: Model performance degraded
**Symptoms**:
- Predictions seem less accurate
- False positives/negatives increasing

**Solutions**:
```bash
# Check model metrics
cat models/model_metrics.json | python -m json.tool

# Retrain models with recent data
python ml/prepare_datasets.py
python ml/train_models.py

# Evaluate on holdout set
python ml/evaluate_models.py

# Check data drift
python ml/data_audit.py
```

## Network Issues

### Issue: Services can't communicate
**Symptoms**:
- `Connection refused` between containers
- `Name resolution failed`

**Solutions**:
```bash
# Check Docker network exists
docker network ls | grep smart-health

# Verify container network connection
docker network inspect smart-health-monitoring-iot_default

# Test DNS resolution
docker exec smart-health-producer ping kafka
docker exec smart-health-dashboard ping cassandra

# Verify service names in code match docker-compose.yml
# service name: "cassandra" → connection string: "cassandra:9042"
```

### Issue: Firewall blocking ports
**Symptoms**:
- External tools can't access services
- Network timeouts

**Solutions**:
```bash
# On Windows Firewall
# Settings → Firewall → Allow app through firewall
# Add Docker / ports

# On Linux, allow Docker ports
sudo ufw allow 5000  # Dashboard
sudo ufw allow 9092  # Kafka

# Check if ports are open
netstat -tuln | grep -E "5000|9092|9042"
```

## Performance Issues

### Issue: High CPU usage
**Symptoms**:
- Docker using 90%+ CPU
- System becomes unresponsive

**Solutions**:
```bash
# Monitor CPU per container
docker stats

# Identify heavy container
# Likely causes: Spark batch processing, Cassandra compaction

# Reduce Spark workload
# Increase batch interval
# Reduce executor cores

# Pause Cassandra compaction
docker exec -it smart-health-cassandra nodetool disableautocompaction smart_health
```

### Issue: High network traffic
**Symptoms**:
- Network bandwidth maxed
- Slow inter-container communication

**Solutions**:
```bash
# Monitor network usage
docker stats

# Reduce data volume
# Reduce batch size in Spark
# Increase TTL in Cassandra

# Use network profiling
docker network stats
```

## Logging & Monitoring

### Accessing Logs

```bash
# Real-time logs
docker logs -f <container_name>

# Last 100 lines
docker logs --tail 100 <container_name>

# Specific time range
docker logs --since 2024-06-11T12:00:00 <container_name>

# All services
docker compose logs -f

# Save to file
docker logs <container_name> > logs.txt
```

### Important Log Patterns

```bash
# Search for errors
docker logs <container> | grep -i error

# Search for Cassandra writes
docker logs spark-streaming | grep -i cassandra

# Search for Kafka messages
docker logs producer | grep message

# Search for model loading
docker logs spark-streaming | grep -i model
```

## Getting Help

### Before posting an issue:

1. **Check logs thoroughly**
   ```bash
   docker compose logs | tail -100
   ```

2. **Verify prerequisites**
   - Docker/Compose installed
   - 8GB+ RAM available
   - 10GB+ disk space
   - Ports 5000, 9042, 9092 available

3. **Try basic troubleshooting**
   ```bash
   docker compose down -v
   docker compose up -d --build
   sleep 60
   ```

4. **Gather diagnostic info**
   ```bash
   docker compose ps
   docker stats
   docker compose logs > diagnostics.log
   ```

### Resources

- [Docker Troubleshooting](https://docs.docker.com/config/containers/logging/)
- [Apache Kafka Troubleshooting](https://kafka.apache.org/documentation/#operations)
- [Cassandra Troubleshooting](https://cassandra.apache.org/doc/latest/cassandra/troubleshooting/)
- [Apache Spark Troubleshooting](https://spark.apache.org/docs/latest/sql-programming-guide.html#other-dataframe-operations)

## Summary Checklist

Before reaching out for support, verify:

- [ ] Docker Desktop is running
- [ ] No port conflicts (5000, 9042, 9092)
- [ ] Sufficient disk space (>10GB available)
- [ ] Sufficient memory (8GB+)
- [ ] All containers started successfully (`docker compose ps`)
- [ ] Logs checked for errors (`docker compose logs | grep -i error`)
- [ ] Services waited 2-3 minutes to initialize
- [ ] Kafka topic exists (`docker exec -it smart-health-kafka kafka-topics --list --bootstrap-server localhost:9092`)
- [ ] Cassandra schema created (run `docker exec -it smart-health-cassandra cqlsh`)
- [ ] Producer sending data (`docker logs smart-health-producer`)
- [ ] Spark processing (`docker logs smart-health-spark-streaming`)
- [ ] Dashboard accessible (http://localhost:5000)
