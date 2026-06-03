import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "smart-health-data")
INTERVAL_SECONDS = int(os.getenv("SIMULATION_INTERVAL_SECONDS", "3"))
PATIENT_IDS = [f"patient-{index}" for index in range(1, 6)]


def connect_producer() -> KafkaProducer:
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                key_serializer=lambda value: value.encode("utf-8"),
                retries=5,
            )
        except NoBrokersAvailable:
            print("Kafka is not ready yet. Retrying in 5 seconds...", flush=True)
            time.sleep(5)


def vital_range(patient_index: int) -> dict:
    # Most readings are normal, with occasional warning/critical spikes.
    profile = random.choices(
        population=["normal", "warning", "critical", "emergency"],
        weights=[78, 14, 6, 2],
        k=1,
    )[0]

    if profile == "emergency":
        return {
            "heart_rate": random.choice([random.randint(30, 39), random.randint(151, 170)]),
            "spo2": random.uniform(78, 84),
            "temperature": random.uniform(39.8, 40.8),
            "systolic_bp": random.randint(195, 220),
            "diastolic_bp": random.randint(120, 140),
            "respiratory_rate": random.choice([random.randint(5, 7), random.randint(36, 42)]),
        }

    if profile == "critical":
        return {
            "heart_rate": random.choice([random.randint(42, 50), random.randint(130, 150)]),
            "spo2": random.uniform(85, 89),
            "temperature": random.uniform(38.8, 39.6),
            "systolic_bp": random.randint(170, 190),
            "diastolic_bp": random.randint(105, 122),
            "respiratory_rate": random.choice([random.randint(8, 10), random.randint(28, 34)]),
        }

    if profile == "warning":
        return {
            "heart_rate": random.choice([random.randint(52, 59), random.randint(101, 125)]),
            "spo2": random.uniform(90, 94),
            "temperature": random.choice([random.uniform(35.4, 35.9), random.uniform(37.8, 38.5)]),
            "systolic_bp": random.randint(135, 160),
            "diastolic_bp": random.randint(88, 100),
            "respiratory_rate": random.choice([random.randint(10, 12), random.randint(21, 27)]),
        }

    baseline_shift = patient_index - 3
    return {
        "heart_rate": random.randint(66, 88) + baseline_shift,
        "spo2": random.uniform(96, 99),
        "temperature": random.uniform(36.2, 37.3),
        "systolic_bp": random.randint(110, 128) + baseline_shift,
        "diastolic_bp": random.randint(68, 82) + baseline_shift,
        "respiratory_rate": random.randint(13, 18),
    }


def make_reading(patient_id: str, patient_index: int) -> dict:
    vitals = vital_range(patient_index)
    return {
        "patient_id": patient_id,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "heart_rate": int(vitals["heart_rate"]),
        "spo2": round(float(vitals["spo2"]), 1),
        "temperature": round(float(vitals["temperature"]), 1),
        "systolic_bp": int(vitals["systolic_bp"]),
        "diastolic_bp": int(vitals["diastolic_bp"]),
        "respiratory_rate": int(vitals["respiratory_rate"]),
    }


def main() -> None:
    producer = connect_producer()
    print(f"Producing simulated health data to Kafka topic '{TOPIC}'", flush=True)

    while True:
        for index, patient_id in enumerate(PATIENT_IDS, start=1):
            reading = make_reading(patient_id, index)
            producer.send(TOPIC, key=patient_id, value=reading)
            print(reading, flush=True)

        producer.flush()
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
