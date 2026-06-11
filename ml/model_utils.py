"""
Model Utilities for Smart Health Monitoring IoT System

Contains:
- Unified feature schema definitions
- Status derivation functions
- Risk score calculation
- Feature preprocessing utilities
"""

import json
from pathlib import Path
from enum import Enum

# Feature schema definitions
DEMOGRAPHIC_FEATURES = [
    "age",
    "gender",
    "weight",
    "height",
    "bmi",
]

VITAL_FEATURES = [
    "heart_rate",
    "spo2",
    "temperature",
    "systolic_bp",
    "diastolic_bp",
    "respiratory_rate",
    "glucose_level",
    "skin_temperature",
]

ACTIVITY_FEATURES = [
    "activity_level",
    "exercise_type",
    "exercise_intensity",
    "steps",
    "stress_level",
    "sleep_duration",
    "sleep_quality",
    "screen_time",
    "notifications_received",
]

SENSOR_FEATURES = [
    "fall_detected",
    "battery_level",
]

MEDICAL_FEATURES = [
    "chronic_condition",
    "smoker",
    "medication",
    "predicted_disease_simulated",
]

ALL_INPUT_FEATURES = (
    DEMOGRAPHIC_FEATURES
    + VITAL_FEATURES
    + ACTIVITY_FEATURES
    + SENSOR_FEATURES
    + MEDICAL_FEATURES
)

AI_OUTPUT_FEATURES = [
    "predicted_status",
    "risk_score",
    "risk_level",
    "is_anomaly",
    "anomaly_score",
    "anomaly_type",
    "predicted_next_heart_rate",
    "alert_type",
    "alert_severity",
    "alert_message",
    "model_version",
]

CATEGORICAL_FEATURES = [
    "gender",
    "activity_level",
    "exercise_type",
    "exercise_intensity",
    "stress_level",
    "sleep_quality",
    "chronic_condition",
    "smoker",
    "medication",
    "predicted_disease_simulated",
]

NUMERIC_FEATURES = [f for f in ALL_INPUT_FEATURES if f not in CATEGORICAL_FEATURES and f != "fall_detected"]

BOOLEAN_FEATURES = [
    "fall_detected",
]


class HealthStatus(str, Enum):
    """Health status levels."""
    NORMAL = "NORMAL"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    EMERGENCY = "EMERGENCY"


class AlertType(str, Enum):
    """Alert types."""
    NO_ALERT = "NO_ALERT"
    WARNING_HEALTH_ALERT = "WARNING_HEALTH_ALERT"
    HIGH_RISK_ALERT = "HIGH_RISK_ALERT"
    EMERGENCY_HEALTH_ALERT = "EMERGENCY_HEALTH_ALERT"
    FALL_DETECTED = "FALL_DETECTED"
    ANOMALY_DETECTED = "ANOMALY_DETECTED"
    LOW_SENSOR_BATTERY = "LOW_SENSOR_BATTERY"


class AlertSeverity(str, Enum):
    """Alert severity levels."""
    NONE = "NONE"
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


def derive_status(row: dict) -> str:
    """
    Derive health status from vital signs using medical thresholds.

    Returns: NORMAL, WARNING, CRITICAL, or EMERGENCY
    """
    # Extract values with defaults
    hr = row.get("heart_rate", 72)
    spo2 = row.get("spo2", 97)
    temp = row.get("temperature", 37.0)
    sys_bp = row.get("systolic_bp", 120)
    dia_bp = row.get("diastolic_bp", 80)
    rr = row.get("respiratory_rate", 16)
    glucose = row.get("glucose_level", 100)
    fall = row.get("fall_detected", False)

    # Emergency conditions
    emergency_conditions = (
        hr < 40
        or hr > 150
        or spo2 < 85
        or temp >= 40.0
        or sys_bp > 200
        or dia_bp > 130
        or rr < 8
        or rr > 35
        or glucose < 50
        or glucose > 250
    )

    if emergency_conditions:
        return HealthStatus.EMERGENCY.value

    # Critical conditions
    critical_conditions = (
        hr < 50
        or hr > 130
        or spo2 < 90
        or temp >= 39.0
        or sys_bp > 180
        or dia_bp > 120
        or rr < 10
        or rr > 30
        or glucose < 70
        or glucose > 200
    )

    if critical_conditions or (fall and hr > 100):
        return HealthStatus.CRITICAL.value

    # Warning conditions
    warning_conditions = (
        hr < 60
        or hr > 100
        or spo2 < 95
        or temp < 36.0
        or temp > 37.8
        or sys_bp > 140
        or dia_bp > 90
        or rr < 12
        or rr > 20
        or glucose < 80
        or glucose > 140
    )

    if warning_conditions or fall:
        return HealthStatus.WARNING.value

    return HealthStatus.NORMAL.value


def derive_risk_level(risk_score: float) -> str:
    """Map risk score to risk level."""
    if risk_score < 25:
        return "LOW"
    elif risk_score < 45:
        return "MEDIUM"
    elif risk_score < 70:
        return "HIGH"
    else:
        return "CRITICAL"


def get_feature_schema() -> dict:
    """Return unified feature schema."""
    return {
        "input_features": ALL_INPUT_FEATURES,
        "demographic_features": DEMOGRAPHIC_FEATURES,
        "vital_features": VITAL_FEATURES,
        "activity_features": ACTIVITY_FEATURES,
        "sensor_features": SENSOR_FEATURES,
        "medical_features": MEDICAL_FEATURES,
        "categorical_features": CATEGORICAL_FEATURES,
        "numeric_features": NUMERIC_FEATURES,
        "boolean_features": BOOLEAN_FEATURES,
        "output_features": AI_OUTPUT_FEATURES,
        "total_input_features": len(ALL_INPUT_FEATURES),
        "total_output_features": len(AI_OUTPUT_FEATURES),
    }


def save_feature_schema(output_path: Path):
    """Save feature schema to JSON."""
    schema = get_feature_schema()
    with open(output_path, "w") as f:
        json.dump(schema, f, indent=2)


def get_default_values() -> dict:
    """Get default values for all features."""
    return {
        # Demographics
        "age": 50,
        "gender": "Unknown",
        "weight": 75.0,
        "height": 1.70,
        "bmi": 25.9,
        # Vitals
        "heart_rate": 72,
        "spo2": 98.0,
        "temperature": 37.0,
        "systolic_bp": 120,
        "diastolic_bp": 80,
        "respiratory_rate": 16,
        "glucose_level": 100,
        "skin_temperature": 34.0,
        # Activity
        "activity_level": "Resting",
        "exercise_type": "None",
        "exercise_intensity": "Low",
        "steps": 2000,
        "stress_level": "Normal",
        "sleep_duration": 7.0,
        "sleep_quality": "Good",
        "screen_time": 3.0,
        "notifications_received": 5,
        # Sensors
        "fall_detected": False,
        "battery_level": 85.0,
        # Medical
        "chronic_condition": "None",
        "smoker": "No",
        "medication": "No",
        "predicted_disease_simulated": "No Disease",
    }


if __name__ == "__main__":
    schema = get_feature_schema()
    print(json.dumps(schema, indent=2))
