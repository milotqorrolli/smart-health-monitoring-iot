# Troubleshooting

## Docker Build Is Slow

The first build downloads Kafka, Cassandra, Spark, and Python ML dependencies. This can take several minutes.

Check progress:

```bash
docker compose ps
docker compose logs -f
```

## Spark Cannot Find Models

Spark logs a warning and uses rule fallback if model files are missing from `/models`.

Train models locally:

```bash
python -m pip install -r ml/requirements.txt
python ml/train_models.py
```

Then restart Spark:

```bash
docker compose restart spark-streaming
```

## Dashboard Shows Waiting

Cassandra may still be starting, or Spark may not have written the first records yet.

Check:

```bash
docker logs -f smart-health-cassandra
docker logs -f smart-health-spark-streaming
docker logs -f smart-health-dashboard
```

## Kafka Has No Messages

Check producer logs:

```bash
docker logs -f smart-health-producer
```

Confirm Kafka is healthy:

```bash
docker exec -it smart-health-kafka kafka-topics --list --bootstrap-server localhost:9092
```

## Cassandra Schema Did Not Update

The init container runs once after Cassandra becomes healthy. If an old volume contains older schema state, restart with a clean volume:

```bash
docker compose down -v
docker compose up -d --build
```

## Spark Package Download Fails

Spark downloads the Kafka and Cassandra connector packages on startup. If the host has network restrictions, the streaming container may fail before processing.

Retry:

```bash
docker compose restart spark-streaming
```

Then inspect logs:

```bash
docker logs -f smart-health-spark-streaming
```

## Port Already In Use

The project uses:

- Flask dashboard: `5000`
- Kafka host listener: `9092`
- Cassandra: `9042`
- Spark master UI: `8080`
- Spark master: `7077`

Stop conflicting local services or edit `docker-compose.yml` port mappings for the host side.

## Reset Everything

```bash
docker compose down -v
docker compose up -d --build
```

## Model Training Is Too Slow

Use a row cap for a faster demo:

```bash
MAX_ROWS_PER_DATASET=5000 python ml/train_models.py
```

PowerShell:

```powershell
$env:MAX_ROWS_PER_DATASET="5000"; python ml/train_models.py
```
