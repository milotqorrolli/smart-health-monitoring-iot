# Smart Health Monitoring IoT

Docker-based local IoT pipeline:

Python sensor simulator -> Kafka -> Spark Structured Streaming -> Cassandra -> Flask dashboard

## Services

- `producer`: simulates 5 patients every 3 seconds and sends JSON readings to Kafka topic `smart-health-data`.
- `kafka` and `zookeeper`: message broker layer.
- `spark-master`, `spark-worker`, `spark-streaming`: Spark Structured Streaming job that reads Kafka, classifies readings, and writes to Cassandra.
- `cassandra`: stores processed sensor readings in keyspace `smart_health`.
- `dashboard`: Flask app that displays the latest reading for each patient.

## Message Format

Each Kafka message contains:

```json
{
  "patient_id": "patient-1",
  "timestamp": "2026-06-03T18:00:00.000Z",
  "heart_rate": 78,
  "spo2": 98.1,
  "temperature": 36.8,
  "systolic_bp": 120,
  "diastolic_bp": 78,
  "respiratory_rate": 16
}
```

Spark adds:

- `status`: `NORMAL`, `WARNING`, `CRITICAL`, or `EMERGENCY`
- `processed_at`: Spark processing timestamp

## Cassandra Schema

The startup init container creates:

- Keyspace: `smart_health`
- Table: `sensor_readings`

The schema is in `cassandra/init.cql`.

## Run Locally

Prerequisite: Docker Desktop with Docker Compose.

From this folder:

```bash
cd smart-health-iot
docker compose up --build
```

The first run can take several minutes because Docker pulls images and Spark downloads the Kafka and Cassandra connector packages.

Open the dashboard:

```text
http://localhost:5000
```

Useful service UIs and ports:

- Flask dashboard: `http://localhost:5000`
- Spark master UI: `http://localhost:8080`
- Kafka broker for local tools: `localhost:9092`
- Cassandra CQL port: `localhost:9042`

## Check Data

View producer logs:

```bash
docker compose logs -f producer
```

View Spark streaming logs:

```bash
docker compose logs -f spark-streaming
```

Query Cassandra:

```bash
docker compose exec cassandra cqlsh
```

Then run:

```sql
SELECT * FROM smart_health.sensor_readings LIMIT 20;
```

## Stop

Stop containers:

```bash
docker compose down
```

Stop containers and remove Cassandra/Spark volumes:

```bash
docker compose down -v
```

## Project Structure

```text
smart-health-iot/
  cassandra/
    init.cql
  dashboard/
    app.py
    Dockerfile
    requirements.txt
    static/style.css
    templates/index.html
  producer/
    producer.py
    Dockerfile
    requirements.txt
  spark/
    streaming_job.py
  docker-compose.yml
  README.md
```
