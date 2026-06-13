"""
Model utility functions for Smart Health Monitoring IoT System.
Contains derive_status(), generate_alert(), and shared preprocessing logic.
"""

import numpy as np


# =============================================================================
# STATUS DERIVATION (Rule-based)
# =============================================================================

def derive_vital_status(vital_name, value):
    """Derive status for a single vital sign based on clinical thresholds."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "NORMAL"

    if vital_name == "heart_rate":
        if 60 <= value <= 100:
            return "NORMAL"
        elif (50 <= value <= 59) or (101 <= value <= 130):
            return "WARNING"
        elif (40 <= value <= 49) or (131 <= value <= 150):
            return "CRITICAL"
        else:
            return "EMERGENCY"

    elif vital_name == "spo2":
        if value >= 95:
            return "NORMAL"
        elif 90 <= value <= 94:
            return "WARNING"
        elif 85 <= value <= 89:
            return "CRITICAL"
        else:
            return "EMERGENCY"

    elif vital_name == "temperature":
        if 36.0 <= value <= 37.8:
            return "NORMAL"
        elif (35.0 <= value <= 35.9) or (37.9 <= value <= 38.9):
            return "WARNING"
        elif 39.0 <= value <= 39.9:
            return "CRITICAL"
        else:
            return "EMERGENCY"

    elif vital_name == "systolic_bp":
        if 90 <= value <= 140:
            return "NORMAL"
        elif 141 <= value <= 160:
            return "WARNING"
        elif 161 <= value <= 200:
            return "CRITICAL"
        elif value > 200 or value < 80:
            return "EMERGENCY"
        else:
            return "WARNING"

    elif vital_name == "diastolic_bp":
        if 60 <= value <= 90:
            return "NORMAL"
        elif 91 <= value <= 100:
            return "WARNING"
        elif 101 <= value <= 130:
            return "CRITICAL"
        elif value > 130 or value < 50:
            return "EMERGENCY"
        else:
            return "WARNING"

    elif vital_name == "respiratory_rate":
        if 12 <= value <= 20:
            return "NORMAL"
        elif (10 <= value <= 11) or (21 <= value <= 30):
            return "WARNING"
        elif (8 <= value <= 9) or (31 <= value <= 35):
            return "CRITICAL"
        else:
            return "EMERGENCY"

    elif vital_name == "glucose_level":
        if 70 <= value <= 140:
            return "NORMAL"
        elif 141 <= value <= 180:
            return "WARNING"
        elif 181 <= value <= 250:
            return "CRITICAL"
        elif value < 50 or value > 250:
            return "EMERGENCY"
        else:
            return "WARNING"

    return "NORMAL"


SEVERITY_ORDER = {"NORMAL": 0, "WARNING": 1, "CRITICAL": 2, "EMERGENCY": 3}
SEVERITY_FROM_INT = {0: "NORMAL", 1: "WARNING", 2: "CRITICAL", 3: "EMERGENCY"}


def derive_status(row):
    """
    Derive patient health status from vital signs using rule-based thresholds.
    Returns the maximum severity across all vital sign checks.
    """
    vitals_to_check = [
        ("heart_rate", row.get("heart_rate")),
        ("spo2", row.get("spo2")),
        ("temperature", row.get("temperature")),
        ("systolic_bp", row.get("systolic_bp")),
        ("diastolic_bp", row.get("diastolic_bp")),
        ("respiratory_rate", row.get("respiratory_rate")),
        ("glucose_level", row.get("glucose_level")),
    ]

    max_severity = 0
    for vital_name, value in vitals_to_check:
        if value is not None:
            try:
                value = float(value)
                status = derive_vital_status(vital_name, value)
                severity = SEVERITY_ORDER.get(status, 0)
                max_severity = max(max_severity, severity)
            except (ValueError, TypeError):
                pass

    # Fall upgrade rule
    fall_detected = row.get("fall_detected", False)
    if fall_detected is True or str(fall_detected).lower() in ("true", "yes", "1"):
        if max_severity <= 1:  # NORMAL or WARNING → upgrade to CRITICAL
            max_severity = 2
        elif max_severity == 2:  # CRITICAL → upgrade to EMERGENCY
            max_severity = 3

    return SEVERITY_FROM_INT.get(max_severity, "NORMAL")


def derive_risk_score_from_status(status):
    """Map status to a numeric risk score."""
    mapping = {"NORMAL": 20, "WARNING": 50, "CRITICAL": 75, "EMERGENCY": 92}
    return mapping.get(status, 20)


def derive_risk_level(risk_score):
    """Map numeric risk score to risk level category."""
    if risk_score is None:
        return "LOW"
    if risk_score <= 30:
        return "LOW"
    elif risk_score <= 55:
        return "MEDIUM"
    elif risk_score <= 75:
        return "HIGH"
    else:
        return "CRITICAL"


# =============================================================================
# ALERT GENERATION
# =============================================================================

def generate_alert(row):
    """
    Generate alert based on AI inference results and vital signs.
    Returns dict with alert_type, alert_severity, alert_message.
    """
    fall_detected = row.get("fall_detected", False)
    if isinstance(fall_detected, str):
        fall_detected = fall_detected.lower() in ("true", "yes", "1")

    predicted_status = row.get("predicted_status", "NORMAL")
    risk_score = row.get("risk_score", 0) or 0
    is_anomaly = row.get("is_anomaly", False)
    battery_level = row.get("battery_level", 100) or 100

    hr = row.get("heart_rate", "N/A")
    spo2 = row.get("spo2", "N/A")
    temp = row.get("temperature", "N/A")
    sys_bp = row.get("systolic_bp", "N/A")
    dia_bp = row.get("diastolic_bp", "N/A")
    rr = row.get("respiratory_rate", "N/A")

    alert_type = "NO_ALERT"
    alert_severity = "NONE"
    alert_message = "No alert."

    # Priority 1: Fall detected
    if fall_detected:
        alert_type = "FALL_DETECTED"
        alert_severity = "CRITICAL" if risk_score >= 70 else "HIGH"
        alert_message = "Fall detected: patient may require immediate assistance."

    # Priority 2: Emergency
    elif predicted_status == "EMERGENCY" or risk_score >= 85:
        alert_type = "EMERGENCY_HEALTH_ALERT"
        alert_severity = "CRITICAL"
        alert_message = f"Emergency alert: SpO2 at {spo2}% and heart rate at {hr} bpm detected."

    # Priority 3: Critical / High risk
    elif predicted_status == "CRITICAL" or risk_score >= 70:
        alert_type = "HIGH_RISK_ALERT"
        alert_severity = "HIGH"
        alert_message = f"High risk alert: systolic BP at {sys_bp} mmHg and respiratory rate at {rr} breaths/min."

    # Priority 4: Warning
    elif predicted_status == "WARNING" or risk_score >= 45:
        alert_type = "WARNING_HEALTH_ALERT"
        alert_severity = "MEDIUM"
        alert_message = f"Warning: heart rate elevated at {hr} bpm. Monitor closely."

    # Priority 5: Anomaly
    elif is_anomaly:
        alert_type = "ANOMALY_DETECTED"
        alert_severity = "MEDIUM"
        alert_message = "Anomaly detected: sensor values are unusual compared to learned patterns."

    # Priority 6: Low battery
    elif battery_level < 15:
        alert_type = "LOW_SENSOR_BATTERY"
        alert_severity = "LOW"
        alert_message = "Low sensor battery: device battery is below 15%."

    # Append rules
    if alert_type != "NO_ALERT":
        if is_anomaly and alert_type != "ANOMALY_DETECTED":
            alert_message += " Anomaly also detected in sensor readings."
        if battery_level < 15 and alert_type != "LOW_SENSOR_BATTERY":
            alert_message += " Sensor battery is critically low."

    return {
        "alert_type": alert_type,
        "alert_severity": alert_severity,
        "alert_message": alert_message,
    }


# =============================================================================
# FEATURE SCHEMA
# =============================================================================

NUMERIC_FEATURES = [
    "age", "weight", "height", "bmi",
    "heart_rate", "spo2", "temperature", "respiratory_rate",
    "systolic_bp", "diastolic_bp", "glucose_level",
    "steps", "skin_temperature", "sleep_duration", "battery_level",
]

CATEGORICAL_FEATURES = [
    "gender", "activity_level", "exercise_type", "exercise_intensity",
    "stress_level", "sleep_quality", "chronic_condition", "smoker", "medication",
]

BOOLEAN_FEATURES = ["fall_detected"]

ALL_INPUT_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES + BOOLEAN_FEATURES
