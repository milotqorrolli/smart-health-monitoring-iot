"""
Smart Health Monitoring IoT - Producer Simulator

Simulates realistic patient health sensor data and sends to Kafka.
Generates data for 5+ patients with stable profiles and varied health conditions.
"""

import json
import os
import random
import time
from datetime import datetime, timezone
from typing import Dict, Any

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "smart-health-data")
INTERVAL_SECONDS = int(os.getenv("SIMULATION_INTERVAL_SECONDS", "3"))
NUM_PATIENTS = int(os.getenv("NUM_PATIENTS", "5"))


# Patient profiles - stable demographics and medical history
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
        "baseline_hr": 68,
        "baseline_sbp": 135,
        "baseline_dbp": 82,
        "baseline_spo2": 97.5,
        "baseline_temp": 37.0,
        "baseline_glucose": 105,
    },
    {
        "patient_id": "patient-2",
        "age": 52,
        "gender": "Male",
        "weight": 92.0,
        "height": 1.80,
        "chronic_condition": "Type 2 Diabetes",
        "smoker": "Yes",
        "medication": "Yes",
        "baseline_hr": 75,
        "baseline_sbp": 142,
        "baseline_dbp": 88,
        "baseline_spo2": 96.8,
        "baseline_temp": 37.1,
        "baseline_glucose": 145,
    },
    {
        "patient_id": "patient-3",
        "age": 45,
        "gender": "Female",
        "weight": 65.0,
        "height": 1.62,
        "chronic_condition": "None",
        "smoker": "No",
        "medication": "No",
        "baseline_hr": 62,
        "baseline_sbp": 118,
        "baseline_dbp": 76,
        "baseline_spo2": 98.2,
        "baseline_temp": 36.9,
        "baseline_glucose": 92,
    },
    {
        "patient_id": "patient-4",
        "age": 73,
        "gender": "Male",
        "weight": 88.5,
        "height": 1.75,
        "chronic_condition": "Atrial Fibrillation",
        "smoker": "No",
        "medication": "Yes",
        "baseline_hr": 82,
        "baseline_sbp": 148,
        "baseline_dbp": 85,
        "baseline_spo2": 96.5,
        "baseline_temp": 37.0,
        "baseline_glucose": 115,
    },
    {
        "patient_id": "patient-5",
        "age": 38,
        "gender": "Male",
        "weight": 78.0,
        "height": 1.78,
        "chronic_condition": "Asthma",
        "smoker": "No",
        "medication": "Yes",
        "baseline_hr": 65,
        "baseline_sbp": 122,
        "baseline_dbp": 78,
        "baseline_spo2": 97.8,
        "baseline_temp": 36.9,
        "baseline_glucose": 100,
    },
]


def connect_producer() -> KafkaProducer:
    """Connect to Kafka with retry logic."""
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


def generate_health_condition() -> str:
    """Generate health condition state based on weighted probabilities."""
    return random.choices(
        population=["NORMAL", "WARNING", "CRITICAL", "EMERGENCY"],
        weights=[76, 14, 7, 3],  # 76% normal, 14% warning, 7% critical, 3% emergency
        k=1,
    )[0]


def generate_vitals(profile: Dict[str, Any], condition: str) -> Dict[str, float]:
    """Generate vital signs based on patient profile and health condition."""
    vitals = {}

    # Heart Rate
    if condition == "EMERGENCY":
        vitals["heart_rate"] = random.choice([
            random.randint(30, 39),
            random.randint(151, 170),
        ])
    elif condition == "CRITICAL":
        vitals["heart_rate"] = random.choice([
            random.randint(42, 50),
            random.randint(130, 150),
        ])
    elif condition == "WARNING":
        vitals["heart_rate"] = random.choice([
            random.randint(52, 59),
            random.randint(101, 125),
        ])
    else:  # NORMAL
        vitals["heart_rate"] = profile["baseline_hr"] + random.randint(-5, 5)

    # SpO2
    if condition == "EMERGENCY":
        vitals["spo2"] = random.uniform(78, 84)
    elif condition == "CRITICAL":
        vitals["spo2"] = random.uniform(85, 89)
    elif condition == "WARNING":
        vitals["spo2"] = random.uniform(90, 94)
    else:
        vitals["spo2"] = profile["baseline_spo2"] + random.uniform(-1, 0.5)

    # Temperature
    if condition == "EMERGENCY":
        vitals["temperature"] = random.uniform(39.8, 40.8)
    elif condition == "CRITICAL":
        vitals["temperature"] = random.uniform(38.8, 39.6)
    elif condition == "WARNING":
        vitals["temperature"] = random.choice([
            random.uniform(35.4, 35.9),
            random.uniform(37.8, 38.5),
        ])
    else:
        vitals["temperature"] = profile["baseline_temp"] + random.uniform(-0.3, 0.3)

    # Systolic BP
    if condition == "EMERGENCY":
        vitals["systolic_bp"] = random.randint(195, 220)
    elif condition == "CRITICAL":
        vitals["systolic_bp"] = random.randint(170, 190)
    elif condition == "WARNING":
        vitals["systolic_bp"] = random.randint(141, 160)
    else:
        vitals["systolic_bp"] = profile["baseline_sbp"] + random.randint(-8, 8)

    # Diastolic BP
    if condition == "EMERGENCY":
        vitals["diastolic_bp"] = random.randint(120, 140)
    elif condition == "CRITICAL":
        vitals["diastolic_bp"] = random.randint(105, 122)
    elif condition == "WARNING":
        vitals["diastolic_bp"] = random.randint(91, 100)
    else:
        vitals["diastolic_bp"] = profile["baseline_dbp"] + random.randint(-5, 5)

    # Respiratory Rate
    if condition == "EMERGENCY":
        vitals["respiratory_rate"] = random.choice([
            random.randint(5, 7),
            random.randint(36, 42),
        ])
    elif condition == "CRITICAL":
        vitals["respiratory_rate"] = random.choice([
            random.randint(8, 10),
            random.randint(28, 34),
        ])
    elif condition == "WARNING":
        vitals["respiratory_rate"] = random.choice([
            random.randint(10, 12),
            random.randint(21, 27),
        ])
    else:
        vitals["respiratory_rate"] = random.randint(13, 18)

    # Glucose
    if condition == "EMERGENCY":
        vitals["glucose_level"] = random.choice([
            random.randint(40, 50),
            random.randint(240, 280),
        ])
    elif condition == "CRITICAL":
        vitals["glucose_level"] = random.choice([
            random.randint(55, 70),
            random.randint(200, 240),
        ])
    elif condition == "WARNING":
        vitals["glucose_level"] = random.choice([
            random.randint(70, 85),
            random.randint(140, 180),
        ])
    else:
        vitals["glucose_level"] = profile["baseline_glucose"] + random.randint(-10, 10)

    # Skin Temperature
    vitals["skin_temperature"] = 34.0 + random.uniform(-1, 1.5)

    return vitals


def generate_activity_features() -> Dict[str, Any]:
    """Generate activity and lifestyle features."""
    return {
        "activity_level": random.choice(["Resting", "Light", "Moderate", "High"]),
        "exercise_type": random.choice(["None", "Walking", "Running", "Cycling", "Sports"]),
        "exercise_intensity": random.choice(["Low", "Moderate", "High"]),
        "steps": random.randint(1000, 15000),
        "stress_level": random.choice(["Low", "Normal", "High", "Very High"]),
        "sleep_duration": random.uniform(4.0, 10.0),
        "sleep_quality": random.choice(["Poor", "Fair", "Good", "Excellent"]),
        "screen_time": random.uniform(0.5, 8.0),
        "notifications_received": random.randint(0, 100),
    }


def generate_sensor_features(condition: str) -> Dict[str, Any]:
    """Generate sensor-related features."""
    # Occasional fall detection (more likely in CRITICAL/EMERGENCY)
    fall_probability = {"NORMAL": 0.001, "WARNING": 0.01, "CRITICAL": 0.05, "EMERGENCY": 0.1}
    fall_detected = random.random() < fall_probability.get(condition, 0.001)

    # Battery level (occasional low battery)
    battery_level = random.uniform(20, 100)
    if random.random() < 0.02:  # 2% chance of low battery
        battery_level = random.uniform(5, 15)

    return {
        "fall_detected": fall_detected,
        "battery_level": round(battery_level, 1),
    }


def make_reading(profile: Dict[str, Any]) -> Dict[str, Any]:
    """Generate a complete health reading for a patient."""
    condition = generate_health_condition()
    vitals = generate_vitals(profile, condition)
    activity = generate_activity_features()
    sensors = generate_sensor_features(condition)

    # Calculate BMI
    bmi = round(profile["weight"] / (profile["height"] ** 2), 1)

    # Determine predicted disease based on condition (educational simulation only)
    disease_mapping = {
        "NORMAL": "No Disease",
        "WARNING": random.choice(["Hypertension", "Pre-diabetes", "Sleep Apnea"]),
        "CRITICAL": random.choice(["Hypertensive Crisis", "Acute Coronary Syndrome", "Severe Sepsis"]),
        "EMERGENCY": "Critical State - Immediate Care Required",
    }

    reading = {
        "patient_id": profile["patient_id"],
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        # Demographics
        "age": profile["age"],
        "gender": profile["gender"],
        "weight": profile["weight"],
        "height": profile["height"],
        "bmi": bmi,
        # Vital Signs
        "heart_rate": int(vitals["heart_rate"]),
        "spo2": round(vitals["spo2"], 1),
        "temperature": round(vitals["temperature"], 1),
        "systolic_bp": int(vitals["systolic_bp"]),
        "diastolic_bp": int(vitals["diastolic_bp"]),
        "respiratory_rate": int(vitals["respiratory_rate"]),
        "glucose_level": int(vitals["glucose_level"]),
        "skin_temperature": round(vitals["skin_temperature"], 1),
        # Activity & Lifestyle
        "activity_level": activity["activity_level"],
        "exercise_type": activity["exercise_type"],
        "exercise_intensity": activity["exercise_intensity"],
        "steps": activity["steps"],
        "stress_level": activity["stress_level"],
        "sleep_duration": round(activity["sleep_duration"], 1),
        "sleep_quality": activity["sleep_quality"],
        "screen_time": round(activity["screen_time"], 1),
        "notifications_received": activity["notifications_received"],
        # Sensor Data
        "fall_detected": sensors["fall_detected"],
        "battery_level": sensors["battery_level"],
        # Medical History
        "chronic_condition": profile["chronic_condition"],
        "smoker": profile["smoker"],
        "medication": profile["medication"],
        "predicted_disease_simulated": disease_mapping[condition],
    }

    return reading


def log_reading(reading: Dict[str, Any]) -> None:
    """Log a reading in a human-readable format."""
    patient_id = reading["patient_id"]
    hr = reading["heart_rate"]
    spo2 = reading["spo2"]
    temp = reading["temperature"]
    status_indicator = "✓" if hr >= 60 and hr <= 100 and spo2 >= 95 else "⚠"
    print(
        f"{status_indicator} {patient_id} | HR:{hr} SpO2:{spo2}% T:{temp}°C "
        f"SBP:{reading['systolic_bp']}/{reading['diastolic_bp']} "
        f"Fall:{reading['fall_detected']} Battery:{reading['battery_level']}%",
        flush=True
    )


def main() -> None:
    """Main producer loop."""
    producer = connect_producer()
    print(f"\n{'='*100}", flush=True)
    print(f"Smart Health Monitoring IoT - Producer Simulator", flush=True)
    print(f"Sending data to Kafka topic: '{TOPIC}'", flush=True)
    print(f"Interval: {INTERVAL_SECONDS}s | Patients: {len(PATIENT_PROFILES)}", flush=True)
    print(f"{'='*100}\n", flush=True)

    message_count = 0
    try:
        while True:
            for profile in PATIENT_PROFILES:
                reading = make_reading(profile)
                producer.send(TOPIC, key=profile["patient_id"], value=reading)
                log_reading(reading)
                message_count += 1

            producer.flush()
            time.sleep(INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print(f"\n\nProducer stopped. Total messages sent: {message_count}", flush=True)
        producer.close()


if __name__ == "__main__":
    main()
