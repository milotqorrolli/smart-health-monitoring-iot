# Implementation Summary

This project implements the required Project 2 IoT system in the Smart Health
Monitoring domain.

## Requirement Mapping

| Requirement | Implementation |
|---|---|
| IoT domain | Smart Health Monitoring for five simulated patients. |
| Sensor collection | `producer/producer.py` simulates vitals, blood pressure, glucose, activity, and fall-safety sensors at regular intervals. |
| Kafka transmission | `docker-compose.yml` creates five Kafka topics; the producer publishes JSON with `patient_id`, `sensor_id`, `sensor_type`, timestamp, values, and battery metadata. |
| Spark Streaming | `spark/streaming_job.py` reads Kafka streams, normalizes sensor events, enriches vitals with latest per-patient sensor context, applies rule and ML inference, prepares alerts, stores metadata, and writes aggregate windows. |
| Cassandra storage | `cassandra/init.cql` defines readings, alerts, latest status, email log, sensor metadata, and one-minute metric tables. |
| Visualization | `dashboard/app.py` and `dashboard/templates/index.html` show patient cards, AI predictions, alerts, readings, and sensor metadata from Cassandra. |
| Live demo | `docker-compose.yml`, `.env.example`, `README.md`, `docs/demo_steps.md`, and `validate.sh` provide startup and validation flow. |
| Advanced components | AI/ML models, alerting/email files/SMTP, sensor metadata, and one-minute aggregate metrics. |

## Main Runtime Flow

```text
Sensor simulator -> Kafka topics -> Spark Structured Streaming -> Cassandra -> Flask dashboard
```

The dashboard does not use static mock data; it queries Cassandra tables populated
by the Spark streaming job.
