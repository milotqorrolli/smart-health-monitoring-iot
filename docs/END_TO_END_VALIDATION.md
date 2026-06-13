# End-to-End Validation

Use this checklist before a presentation to prove the full IoT path works:

```bash
python ml/train_models.py
docker compose up -d --build
bash validate.sh
```

## What `validate.sh` Checks

- ML model artifacts exist in `models/`.
- Kafka, Cassandra, Spark, producer, and dashboard containers are running.
- Spark logs show model loading or rule-based fallback.
- Cassandra has the `smart_health` keyspace and required tables.
- `sensor_readings` contains enriched health predictions.
- `patient_latest_status` includes `predicted_next_heart_rate`.
- `sensor_metadata` stores simulated sensor IDs and types.
- Dashboard APIs respond:
  - `/api/health`
  - `/api/latest`
  - `/api/stats`
  - `/api/sensors`

## Manual Spot Checks

```bash
docker exec -it smart-health-cassandra cqlsh
```

```sql
USE smart_health;
SELECT COUNT(*) FROM sensor_readings;
SELECT * FROM patient_latest_status;
SELECT * FROM sensor_metadata LIMIT 10;
SELECT * FROM patient_minute_metrics WHERE patient_id = 'patient-1' LIMIT 5;
```

Open `http://localhost:5000` and verify that patient cards, recent alerts,
recent readings, predicted next heart rate, and sensor metadata are visible.
