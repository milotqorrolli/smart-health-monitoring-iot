"""
Smart Health Monitoring IoT — Producer Simulator
Simulates 5 independent health sensor classes, each publishing to its own Kafka topic.
"""

import json
import logging
import os
import random
import signal
import sys
import threading
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# =============================================================================
# Configuration
# =============================================================================

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("HealthProducer")

# Global stop event for graceful shutdown
stop_event = threading.Event()

# =============================================================================
# Patient Profiles
# =============================================================================

PATIENT_PROFILES = {
    "patient-1": {
        "patient_id": "patient-1",
        "age": 64, "gender": "Female", "weight": 78.5, "height": 1.69,
        "bmi": round(78.5 / (1.69 ** 2), 1),
        "chronic_condition": "Hypertension", "smoker": "No", "medication": "Yes",
        "stress_level": "High", "sleep_duration": 5.6, "sleep_quality": "Poor",
        "predicted_disease_simulated": "Hypertension",
        "baseline_hr": 82, "baseline_spo2": 96, "baseline_systolic": 145,
        "baseline_diastolic": 92, "baseline_temp": 36.8, "baseline_glucose": 110,
        "baseline_rr": 17,
    },
    "patient-2": {
        "patient_id": "patient-2",
        "age": 45, "gender": "Male", "weight": 92.0, "height": 1.78,
        "bmi": round(92.0 / (1.78 ** 2), 1),
        "chronic_condition": "Diabetes", "smoker": "No", "medication": "Yes",
        "stress_level": "Moderate", "sleep_duration": 6.8, "sleep_quality": "Fair",
        "predicted_disease_simulated": "Diabetes Mellitus",
        "baseline_hr": 78, "baseline_spo2": 97, "baseline_systolic": 132,
        "baseline_diastolic": 85, "baseline_temp": 36.6, "baseline_glucose": 145,
        "baseline_rr": 16,
    },
    "patient-3": {
        "patient_id": "patient-3",
        "age": 72, "gender": "Female", "weight": 68.0, "height": 1.62,
        "bmi": round(68.0 / (1.62 ** 2), 1),
        "chronic_condition": "Heart Disease", "smoker": "No", "medication": "Yes",
        "stress_level": "High", "sleep_duration": 5.2, "sleep_quality": "Poor",
        "predicted_disease_simulated": "Heart Disease",
        "baseline_hr": 88, "baseline_spo2": 94, "baseline_systolic": 158,
        "baseline_diastolic": 96, "baseline_temp": 36.7, "baseline_glucose": 105,
        "baseline_rr": 19,
    },
    "patient-4": {
        "patient_id": "patient-4",
        "age": 38, "gender": "Male", "weight": 80.0, "height": 1.82,
        "bmi": round(80.0 / (1.82 ** 2), 1),
        "chronic_condition": "None", "smoker": "No", "medication": "No",
        "stress_level": "Low", "sleep_duration": 7.5, "sleep_quality": "Good",
        "predicted_disease_simulated": "None",
        "baseline_hr": 70, "baseline_spo2": 98, "baseline_systolic": 118,
        "baseline_diastolic": 76, "baseline_temp": 36.5, "baseline_glucose": 95,
        "baseline_rr": 15,
    },
    "patient-5": {
        "patient_id": "patient-5",
        "age": 58, "gender": "Female", "weight": 72.0, "height": 1.65,
        "bmi": round(72.0 / (1.65 ** 2), 1),
        "chronic_condition": "Asthma", "smoker": "Yes", "medication": "Yes",
        "stress_level": "Moderate", "sleep_duration": 6.2, "sleep_quality": "Fair",
        "predicted_disease_simulated": "Asthma",
        "baseline_hr": 76, "baseline_spo2": 93, "baseline_systolic": 128,
        "baseline_diastolic": 82, "baseline_temp": 36.7, "baseline_glucose": 100,
        "baseline_rr": 22,
    },
}


# =============================================================================
# Kafka Producer Connection
# =============================================================================

def connect_kafka():
    """Connect to Kafka with retries."""
    while not stop_event.is_set():
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                retries=5,
                acks="all",
            )
            logger.info(f"Connected to Kafka at {BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            logger.warning("Kafka not ready. Retrying in 5 seconds...")
            time.sleep(5)
    return None


# =============================================================================
# Condition Profile Generator
# =============================================================================

def choose_condition(patient_profile):
    """Choose a condition profile with weighted probabilities."""
    chronic = patient_profile.get("chronic_condition", "None")
    if chronic != "None":
        weights = [75, 15, 7, 3]
    else:
        weights = [80, 12, 6, 2]

    return random.choices(
        population=["NORMAL", "WARNING", "CRITICAL", "EMERGENCY"],
        weights=weights,
        k=1,
    )[0]


# =============================================================================
# Base Sensor Class
# =============================================================================

class BaseSensor:
    """Base class for all health sensors."""

    def __init__(self, patient_profile, kafka_producer, topic, sensor_type, sensor_id_prefix, interval):
        self.patient = patient_profile
        self.patient_id = patient_profile["patient_id"]
        self.kafka_producer = kafka_producer
        self.topic = topic
        self.sensor_type = sensor_type
        self.sensor_id = f"{sensor_id_prefix}-{self.patient_id}"
        self.interval = interval
        self.battery_level = random.uniform(85, 100)
        self._thread = None

    def _drain_battery(self):
        """Simulate slow battery drain with occasional spikes."""
        self.battery_level -= random.uniform(0.01, 0.05)
        if random.random() < 0.005:
            self.battery_level -= random.uniform(5, 15)
        if random.random() < 0.002:
            self.battery_level = random.uniform(80, 100)
        self.battery_level = max(5, min(100, self.battery_level))

    def _base_payload(self):
        """Build base payload with common fields."""
        self._drain_battery()
        return {
            "patient_id": self.patient_id,
            "sensor_id": self.sensor_id,
            "sensor_type": self.sensor_type,
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "battery_level": round(self.battery_level, 1),
        }

    def _embed_patient_profile(self, payload):
        """Embed patient profile fields into the payload."""
        payload["age"] = self.patient["age"]
        payload["gender"] = self.patient["gender"]
        payload["weight"] = self.patient["weight"]
        payload["height"] = self.patient["height"]
        payload["bmi"] = self.patient["bmi"]
        payload["chronic_condition"] = self.patient["chronic_condition"]
        payload["smoker"] = self.patient["smoker"]
        payload["medication"] = self.patient["medication"]
        payload["stress_level"] = self.patient["stress_level"]
        payload["sleep_duration"] = self.patient["sleep_duration"]
        payload["sleep_quality"] = self.patient["sleep_quality"]
        payload["predicted_disease_simulated"] = self.patient["predicted_disease_simulated"]
        return payload

    def generate_reading(self, condition):
        """Override in subclass."""
        raise NotImplementedError

    def publish(self, payload):
        """Publish payload to Kafka topic."""
        try:
            self.kafka_producer.send(self.topic, key=self.patient_id, value=payload)
            logger.debug(f"{self.patient_id} | {self.sensor_type} | {self.topic}")
        except Exception as e:
            logger.error(f"Publish failed [{self.sensor_type}] {self.patient_id}: {e}")

    def run(self):
        """Main sensor loop."""
        logger.info(f"Started {self.sensor_type} for {self.patient_id} -> {self.topic} (every {self.interval}s)")
        while not stop_event.is_set():
            try:
                condition = choose_condition(self.patient)
                payload = self._base_payload()
                reading = self.generate_reading(condition)
                payload.update(reading)
                payload = self._embed_patient_profile(payload)
                self.publish(payload)
            except Exception as e:
                logger.error(f"Error in {self.sensor_type} for {self.patient_id}: {e}")
            stop_event.wait(self.interval)

    def start(self):
        """Start sensor in a daemon thread."""
        self._thread = threading.Thread(target=self.run, daemon=True, name=self.sensor_id)
        self._thread.start()
        return self._thread


# =============================================================================
# Sensor Implementations
# =============================================================================

class VitalsMonitorSensor(BaseSensor):
    """Monitors heart rate, SpO2, temperature, respiratory rate."""

    def __init__(self, patient_profile, kafka_producer):
        super().__init__(patient_profile, kafka_producer, "health.vitals",
                         "VITALS_MONITOR", "vitals-monitor", 15)

    def generate_reading(self, condition):
        bl_hr = self.patient["baseline_hr"]
        bl_spo2 = self.patient["baseline_spo2"]
        bl_temp = self.patient["baseline_temp"]
        bl_rr = self.patient["baseline_rr"]

        if condition == "EMERGENCY":
            hr = random.choice([random.randint(30, 39), random.randint(155, 180)])
            spo2 = random.uniform(78, 84)
            temp = random.uniform(40.0, 41.2)
            rr = random.choice([random.randint(5, 7), random.randint(36, 42)])
        elif condition == "CRITICAL":
            hr = random.choice([random.randint(40, 49), random.randint(131, 150)])
            spo2 = random.uniform(85, 89)
            temp = random.uniform(39.0, 39.9)
            rr = random.choice([random.randint(8, 9), random.randint(31, 35)])
        elif condition == "WARNING":
            hr = random.choice([random.randint(50, 59), random.randint(101, 130)])
            spo2 = random.uniform(90, 94)
            temp = random.choice([random.uniform(35.0, 35.9), random.uniform(37.9, 38.9)])
            rr = random.choice([random.randint(10, 11), random.randint(21, 30)])
        else:
            hr = bl_hr + random.randint(-10, 10)
            hr = max(60, min(100, hr))
            spo2 = min(100, bl_spo2 + random.uniform(-1, 2))
            spo2 = max(95, spo2)
            temp = bl_temp + random.uniform(-0.3, 0.3)
            temp = max(36.0, min(37.8, temp))
            rr = bl_rr + random.randint(-2, 2)
            rr = max(12, min(20, rr))

        # Occasional sudden SpO2 drop
        if random.random() < 0.01:
            spo2 = random.uniform(80, 86)
            logger.info(f"EVENT: Sudden SpO2 drop for {self.patient_id}: {spo2:.1f}%")
        # Occasional HR spike
        if random.random() < 0.01:
            hr = random.randint(155, 180)
            logger.info(f"EVENT: Heart rate spike for {self.patient_id}: {hr} bpm")

        return {"heart_rate": int(hr), "spo2": round(float(spo2), 1),
                "temperature": round(float(temp), 1), "respiratory_rate": int(rr)}


class BloodPressureSensor(BaseSensor):
    """Monitors systolic and diastolic blood pressure."""

    def __init__(self, patient_profile, kafka_producer):
        super().__init__(patient_profile, kafka_producer, "health.blood_pressure",
                         "BLOOD_PRESSURE_MONITOR", "bp-monitor", 45)

    def generate_reading(self, condition):
        bl_sys = self.patient["baseline_systolic"]
        bl_dia = self.patient["baseline_diastolic"]

        if condition == "EMERGENCY":
            systolic = random.randint(200, 220)
            diastolic = random.randint(130, 145)
        elif condition == "CRITICAL":
            systolic = random.randint(161, 200)
            diastolic = random.randint(101, 130)
        elif condition == "WARNING":
            systolic = random.randint(141, 160)
            diastolic = random.randint(91, 100)
        else:
            systolic = bl_sys + random.randint(-10, 10)
            systolic = max(90, min(140, systolic))
            diastolic = bl_dia + random.randint(-5, 5)
            diastolic = max(60, min(90, diastolic))

        if random.random() < 0.008:
            systolic = random.randint(185, 210)
            diastolic = random.randint(115, 135)
            logger.info(f"EVENT: BP spike for {self.patient_id}: {systolic}/{diastolic}")

        return {"systolic_bp": int(systolic), "diastolic_bp": int(diastolic)}


class GlucoseSensor(BaseSensor):
    """Monitors blood glucose level."""

    def __init__(self, patient_profile, kafka_producer):
        super().__init__(patient_profile, kafka_producer, "health.glucose",
                         "GLUCOSE_SENSOR", "glucose-sensor", 60)

    def generate_reading(self, condition):
        bl_glucose = self.patient["baseline_glucose"]

        if condition == "EMERGENCY":
            glucose = random.choice([random.uniform(30, 49), random.uniform(251, 350)])
        elif condition == "CRITICAL":
            glucose = random.uniform(181, 250)
        elif condition == "WARNING":
            glucose = random.uniform(141, 180)
        else:
            glucose = bl_glucose + random.uniform(-15, 15)
            glucose = max(70, min(140, glucose))

        return {"glucose_level": round(float(glucose), 1)}


class ActivityTrackerSensor(BaseSensor):
    """Monitors steps, activity level, exercise type, and intensity."""

    def __init__(self, patient_profile, kafka_producer):
        super().__init__(patient_profile, kafka_producer, "health.activity",
                         "ACTIVITY_TRACKER", "activity-tracker", 30)

    def generate_reading(self, condition):
        choices = [
            ("Resting", "None", "Low", random.randint(0, 50)),
            ("Light", "Walking", "Low", random.randint(50, 200)),
            ("Moderate", "Walking", "Moderate", random.randint(200, 500)),
            ("Active", "Running", "High", random.randint(500, 1200)),
            ("Vigorous", "Cycling", "High", random.randint(800, 2000)),
        ]
        if condition in ("CRITICAL", "EMERGENCY"):
            choice = choices[0]
        elif condition == "WARNING":
            choice = random.choice(choices[:3])
        else:
            choice = random.choice(choices)

        return {"steps": choice[3], "activity_level": choice[0],
                "exercise_type": choice[1], "exercise_intensity": choice[2]}


class FallSafetySensor(BaseSensor):
    """Monitors fall detection and skin temperature."""

    def __init__(self, patient_profile, kafka_producer):
        super().__init__(patient_profile, kafka_producer, "health.fall_safety",
                         "FALL_SAFETY_SENSOR", "fall-sensor", 15)

    def generate_reading(self, condition):
        skin_temp = 35.0 + random.uniform(-0.5, 1.5)
        fall_detected = False

        if condition == "EMERGENCY":
            fall_detected = random.random() < 0.15
        elif condition == "CRITICAL":
            fall_detected = random.random() < 0.06
        elif condition == "WARNING":
            fall_detected = random.random() < 0.02
        else:
            fall_detected = random.random() < 0.005

        if fall_detected:
            logger.info(f"EVENT: Fall detected for {self.patient_id}!")

        return {"fall_detected": fall_detected, "skin_temperature": round(skin_temp, 1)}


# =============================================================================
# Main Entry Point
# =============================================================================

def main():
    logger.info("=" * 60)
    logger.info("  Smart Health Monitoring IoT - Sensor Simulator")
    logger.info("=" * 60)
    logger.info(f"  Kafka: {BOOTSTRAP_SERVERS}")
    logger.info(f"  Patients: {len(PATIENT_PROFILES)}")
    logger.info(f"  Sensors per patient: 5")
    logger.info(f"  Total sensors: {len(PATIENT_PROFILES) * 5}")
    logger.info("=" * 60)

    kafka_producer = connect_kafka()
    if kafka_producer is None:
        logger.error("Failed to connect to Kafka. Exiting.")
        sys.exit(1)

    all_threads = []
    sensor_classes = [VitalsMonitorSensor, BloodPressureSensor, GlucoseSensor,
                      ActivityTrackerSensor, FallSafetySensor]

    for patient_id, profile in PATIENT_PROFILES.items():
        for sensor_class in sensor_classes:
            sensor = sensor_class(profile, kafka_producer)
            thread = sensor.start()
            all_threads.append(thread)

    logger.info(f"All {len(all_threads)} sensor threads started.")

    def signal_handler(sig, frame):
        logger.info("Received shutdown signal. Stopping all sensors...")
        stop_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        while not stop_event.is_set():
            stop_event.wait(30)
            if not stop_event.is_set():
                alive = sum(1 for t in all_threads if t.is_alive())
                logger.info(f"Heartbeat: {alive}/{len(all_threads)} sensors active")
    except KeyboardInterrupt:
        stop_event.set()

    logger.info("Producer shutdown complete.")
    kafka_producer.close()


if __name__ == "__main__":
    main()
