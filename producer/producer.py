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

CONDITION_WEIGHTS = {
    "NORMAL": 78,
    "WARNING": 14,
    "CRITICAL": 6,
    "EMERGENCY": 2,
}

PATIENT_PROFILES = [
    {
        "patient_id": "patient-1",
        "age": 64,
        "gender": "Female",
        "weight": 78.5,
        "height": 1.69,
        "chronic_condition": "Hypertension",
        "smoker": "No",
        "medication": "Yes",
        "baseline_heart_rate": 82,
        "baseline_systolic_bp": 138,
        "baseline_diastolic_bp": 86,
        "baseline_spo2": 96.5,
        "baseline_temperature": 36.8,
        "baseline_glucose": 142,
        "baseline_risk": "Medium",
        "predicted_disease_simulated": "Hypertension",
    },
    {
        "patient_id": "patient-2",
        "age": 38,
        "gender": "Male",
        "weight": 86.0,
        "height": 1.82,
        "chronic_condition": "None",
        "smoker": "No",
        "medication": "No",
        "baseline_heart_rate": 72,
        "baseline_systolic_bp": 118,
        "baseline_diastolic_bp": 76,
        "baseline_spo2": 98.0,
        "baseline_temperature": 36.7,
        "baseline_glucose": 96,
        "baseline_risk": "Low",
        "predicted_disease_simulated": "None",
    },
    {
        "patient_id": "patient-3",
        "age": 57,
        "gender": "Female",
        "weight": 92.4,
        "height": 1.66,
        "chronic_condition": "Diabetes",
        "smoker": "No",
        "medication": "Yes",
        "baseline_heart_rate": 88,
        "baseline_systolic_bp": 132,
        "baseline_diastolic_bp": 84,
        "baseline_spo2": 96.0,
        "baseline_temperature": 36.9,
        "baseline_glucose": 168,
        "baseline_risk": "Medium",
        "predicted_disease_simulated": "Diabetes",
    },
    {
        "patient_id": "patient-4",
        "age": 71,
        "gender": "Male",
        "weight": 74.2,
        "height": 1.74,
        "chronic_condition": "COPD",
        "smoker": "Former",
        "medication": "Yes",
        "baseline_heart_rate": 90,
        "baseline_systolic_bp": 145,
        "baseline_diastolic_bp": 88,
        "baseline_spo2": 94.0,
        "baseline_temperature": 36.6,
        "baseline_glucose": 118,
        "baseline_risk": "High",
        "predicted_disease_simulated": "Respiratory Risk",
    },
    {
        "patient_id": "patient-5",
        "age": 29,
        "gender": "Female",
        "weight": 61.7,
        "height": 1.64,
        "chronic_condition": "Asthma",
        "smoker": "No",
        "medication": "Yes",
        "baseline_heart_rate": 76,
        "baseline_systolic_bp": 112,
        "baseline_diastolic_bp": 72,
        "baseline_spo2": 97.5,
        "baseline_temperature": 36.7,
        "baseline_glucose": 92,
        "baseline_risk": "Low",
        "predicted_disease_simulated": "Asthma",
    },
]


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


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def choose_condition() -> str:
    return random.choices(
        population=list(CONDITION_WEIGHTS.keys()),
        weights=list(CONDITION_WEIGHTS.values()),
        k=1,
    )[0]


def jitter(value: float, spread: float, digits: int = 1) -> float:
    return round(random.uniform(value - spread, value + spread), digits)


def build_vitals(profile: dict, condition: str) -> dict:
    vitals = {
        "heart_rate": int(round(jitter(profile["baseline_heart_rate"], 7, 0))),
        "spo2": jitter(profile["baseline_spo2"], 1.1),
        "temperature": jitter(profile["baseline_temperature"], 0.25),
        "systolic_bp": int(round(jitter(profile["baseline_systolic_bp"], 7, 0))),
        "diastolic_bp": int(round(jitter(profile["baseline_diastolic_bp"], 5, 0))),
        "respiratory_rate": random.randint(13, 19),
        "glucose_level": jitter(profile["baseline_glucose"], 18),
    }

    if condition == "WARNING":
        vitals.update(
            {
                "heart_rate": random.choice([random.randint(52, 59), random.randint(101, 128)]),
                "spo2": random.uniform(90, 94.5),
                "temperature": random.choice([random.uniform(35.2, 35.9), random.uniform(37.9, 38.8)]),
                "systolic_bp": random.randint(141, 160),
                "diastolic_bp": random.randint(91, 100),
                "respiratory_rate": random.choice([random.randint(10, 11), random.randint(21, 30)]),
                "glucose_level": random.uniform(141, 180),
            }
        )
    elif condition == "CRITICAL":
        vitals.update(
            {
                "heart_rate": random.choice([random.randint(40, 49), random.randint(131, 150)]),
                "spo2": random.uniform(85, 89.5),
                "temperature": random.uniform(39.0, 39.8),
                "systolic_bp": random.randint(161, 200),
                "diastolic_bp": random.randint(101, 130),
                "respiratory_rate": random.choice([random.randint(8, 9), random.randint(31, 35)]),
                "glucose_level": random.uniform(181, 250),
            }
        )
    elif condition == "EMERGENCY":
        vitals.update(
            {
                "heart_rate": random.choice([random.randint(30, 39), random.randint(151, 175)]),
                "spo2": random.uniform(76, 84.5),
                "temperature": random.choice([random.uniform(34.0, 34.9), random.uniform(40.0, 41.0)]),
                "systolic_bp": random.choice([random.randint(65, 79), random.randint(201, 225)]),
                "diastolic_bp": random.choice([random.randint(40, 49), random.randint(131, 145)]),
                "respiratory_rate": random.choice([random.randint(5, 7), random.randint(36, 42)]),
                "glucose_level": random.choice([random.uniform(38, 49), random.uniform(251, 320)]),
            }
        )

    return {
        "heart_rate": int(vitals["heart_rate"]),
        "spo2": round(float(vitals["spo2"]), 1),
        "temperature": round(float(vitals["temperature"]), 1),
        "systolic_bp": int(vitals["systolic_bp"]),
        "diastolic_bp": int(vitals["diastolic_bp"]),
        "respiratory_rate": int(vitals["respiratory_rate"]),
        "glucose_level": round(float(vitals["glucose_level"]), 1),
    }


def activity_context(condition: str) -> dict:
    if condition in {"CRITICAL", "EMERGENCY"}:
        activity_level = "Resting"
        exercise_type = "None"
        exercise_intensity = "Low"
        steps = random.randint(0, 1800)
    else:
        exercise_type = random.choice(["None", "Walking", "Cycling", "Yoga"])
        exercise_intensity = "Low" if exercise_type == "None" else random.choice(["Low", "Medium", "High"])
        steps = random.randint(800, 12000)
        activity_level = "Active" if steps > 8000 else "Light" if steps > 2500 else "Resting"

    sleep_duration = round(random.uniform(4.5, 9.0), 1)
    sleep_quality = "Poor" if sleep_duration < 6 else "Good" if sleep_duration >= 7.5 else "Fair"
    stress_level = random.choices(["Low", "Medium", "High"], weights=[45, 40, 15], k=1)[0]
    if condition in {"WARNING", "CRITICAL", "EMERGENCY"}:
        stress_level = random.choices(["Medium", "High"], weights=[45, 55], k=1)[0]

    return {
        "activity_level": activity_level,
        "exercise_type": exercise_type,
        "exercise_intensity": exercise_intensity,
        "steps": steps,
        "stress_level": stress_level,
        "sleep_duration": sleep_duration,
        "sleep_quality": sleep_quality,
        "screen_time": round(random.uniform(0.5, 7.5), 1),
        "notifications_received": random.randint(0, 90),
    }


def inject_events(reading: dict, condition: str) -> list[str]:
    events: list[str] = []

    if random.random() < 0.015 or condition == "EMERGENCY" and random.random() < 0.08:
        reading["fall_detected"] = True
        events.append("fall_event")

    if random.random() < 0.025:
        reading["battery_level"] = round(random.uniform(3, 14), 1)
        events.append("low_battery")

    if random.random() < 0.025:
        reading["spo2"] = round(random.uniform(80, 89), 1)
        events.append("sudden_spo2_drop")

    if random.random() < 0.025:
        reading["heart_rate"] = random.randint(135, 170)
        events.append("heart_rate_spike")

    if random.random() < 0.02:
        reading["systolic_bp"] = random.randint(170, 215)
        reading["diastolic_bp"] = random.randint(105, 135)
        events.append("blood_pressure_spike")

    if random.random() < 0.015:
        sensor_field = random.choice(["heart_rate", "spo2", "temperature"])
        if sensor_field == "heart_rate":
            reading[sensor_field] = random.choice([25, 210])
        elif sensor_field == "spo2":
            reading[sensor_field] = random.choice([55.0, 100.0])
        else:
            reading[sensor_field] = random.choice([32.0, 42.0])
        events.append(f"sensor_anomaly_{sensor_field}")

    return events


def make_reading(profile: dict) -> dict:
    condition = choose_condition()
    vitals = build_vitals(profile, condition)
    context = activity_context(condition)
    bmi = round(profile["weight"] / (profile["height"] ** 2), 1)

    reading = {
        "patient_id": profile["patient_id"],
        "timestamp": utc_now(),
        "age": profile["age"],
        "gender": profile["gender"],
        "weight": profile["weight"],
        "height": profile["height"],
        "bmi": bmi,
        **vitals,
        "skin_temperature": round(vitals["temperature"] - random.uniform(1.4, 2.4), 1),
        **context,
        "fall_detected": False,
        "battery_level": round(random.uniform(18, 100), 1),
        "chronic_condition": profile["chronic_condition"],
        "smoker": profile["smoker"],
        "medication": profile["medication"],
        "predicted_disease_simulated": profile["predicted_disease_simulated"],
    }
    events = inject_events(reading, condition)
    if events:
        print(f"{profile['patient_id']} injected events: {', '.join(events)}", flush=True)
    return reading


def main() -> None:
    producer = connect_producer()
    print(f"Producing simulated health data to Kafka topic '{TOPIC}'", flush=True)
    print(f"Patients: {', '.join(profile['patient_id'] for profile in PATIENT_PROFILES)}", flush=True)

    while True:
        for profile in PATIENT_PROFILES:
            reading = make_reading(profile)
            producer.send(TOPIC, key=profile["patient_id"], value=reading)
            print(json.dumps(reading, sort_keys=True), flush=True)

        producer.flush()
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
