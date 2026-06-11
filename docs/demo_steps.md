# Demo Steps

## 1. Train Models

Install local ML dependencies:

```bash
python -m pip install -r ml/requirements.txt
```

Audit datasets:

```bash
python ml/data_audit.py
```

Train models:

```bash
python ml/train_models.py
```

For a quicker demo training run:

```bash
MAX_ROWS_PER_DATASET=5000 python ml/train_models.py
```

PowerShell:

```powershell
$env:MAX_ROWS_PER_DATASET="5000"; python ml/train_models.py
```

## 2. Start The System

```bash
docker compose up -d --build
```

## 3. Check Containers

```bash
docker ps
```

Expected containers include:

- `smart-health-zookeeper`
- `smart-health-kafka`
- `smart-health-cassandra`
- `smart-health-cassandra-init`
- `smart-health-spark-master`
- `smart-health-spark-worker`
- `smart-health-spark-streaming`
- `smart-health-producer`
- `smart-health-dashboard`

## 4. Check Logs

Producer:

```bash
docker logs -f smart-health-producer
```

Spark:

```bash
docker logs -f smart-health-spark-streaming
```

Dashboard:

```bash
docker logs -f smart-health-dashboard
```

## 5. Inspect Kafka

List topics:

```bash
docker exec -it smart-health-kafka kafka-topics --list --bootstrap-server localhost:9092
```

Consume messages:

```bash
docker exec -it smart-health-kafka kafka-console-consumer --topic smart-health-data --bootstrap-server localhost:9092 --from-beginning
```

## 6. Inspect Cassandra

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

## 7. Open Dashboard

```text
http://localhost:5000
```

API endpoints:

```text
http://localhost:5000/api/latest
http://localhost:5000/api/alerts
http://localhost:5000/api/readings/patient-1
```

## 8. Stop

```bash
docker compose down
```

Remove data volumes:

```bash
docker compose down -v
```
