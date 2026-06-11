# Architecture

Medical disclaimer: this is an educational IoT simulation. It is not clinically validated and must not be used for diagnosis or real patient monitoring.

## Pipeline

```text
Producer Simulator
  -> Kafka topic smart-health-data
  -> Spark Structured Streaming
  -> AI models and rule fallback
  -> Cassandra
  -> Flask Dashboard
```

## Components

### Producer Simulator

`producer/producer.py` creates stable profiles for five simulated patients. Each profile has demographics, chronic condition context, medication/smoking flags, and baseline vitals. Every loop emits one JSON event per patient to Kafka topic `smart-health-data`.

The simulator uses weighted condition profiles:

- `NORMAL`: about 78 percent
- `WARNING`: about 14 percent
- `CRITICAL`: about 6 percent
- `EMERGENCY`: about 2 percent

It also injects occasional fall events, low battery, SpO2 drops, heart-rate spikes, blood-pressure spikes, and sensor anomalies.

### Kafka

Kafka is the decoupling layer between producers and stream processing. Docker internal services use `kafka:29092`; host tools use `localhost:9092`.

### Spark Structured Streaming

`spark/streaming_job.py` reads Kafka messages, parses the unified JSON schema, and processes each micro-batch with `foreachBatch`.

For each record, Spark:

- normalizes missing values
- computes `rule_status`
- loads scikit-learn artifacts from `/models`
- predicts `predicted_status`
- predicts `risk_score`
- detects anomalies
- forecasts next heart rate from recent per-patient history
- generates alert fields
- writes Cassandra tables

If models are unavailable, Spark keeps running with deterministic rule fallback.

### AI Models

Models are trained by `ml/train_models.py` and saved under `models/`. They are not trained inside Spark streaming.

### Cassandra

The init container applies `cassandra/init.cql` after Cassandra becomes healthy. It creates:

- `smart_health.sensor_readings`: full enriched event history by patient and reading time.
- `smart_health.patient_alerts`: alert history by patient and alert time.
- `smart_health.patient_latest_status`: latest status per patient for fast dashboard reads.

### Flask Dashboard

`dashboard/app.py` reads Cassandra and serves:

- `/`
- `/api/latest`
- `/api/alerts`
- `/api/readings/<patient_id>`

The UI refreshes every 5 seconds and shows waiting states if Cassandra is temporarily unavailable.
