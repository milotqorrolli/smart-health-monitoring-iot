from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping


MODEL_VERSION = "smart-health-ai-v1"

INPUT_FIELDS = [
    "patient_id",
    "timestamp",
    "age",
    "gender",
    "weight",
    "height",
    "bmi",
    "heart_rate",
    "spo2",
    "temperature",
    "systolic_bp",
    "diastolic_bp",
    "respiratory_rate",
    "glucose_level",
    "skin_temperature",
    "activity_level",
    "exercise_type",
    "exercise_intensity",
    "steps",
    "stress_level",
    "sleep_duration",
    "sleep_quality",
    "screen_time",
    "notifications_received",
    "fall_detected",
    "battery_level",
    "chronic_condition",
    "smoker",
    "medication",
    "predicted_disease_simulated",
]

ENRICHED_FIELDS = [
    "patient_id",
    "reading_time",
    "age",
    "gender",
    "weight",
    "height",
    "bmi",
    "heart_rate",
    "spo2",
    "temperature",
    "systolic_bp",
    "diastolic_bp",
    "respiratory_rate",
    "glucose_level",
    "skin_temperature",
    "activity_level",
    "exercise_type",
    "exercise_intensity",
    "steps",
    "stress_level",
    "sleep_duration",
    "sleep_quality",
    "screen_time",
    "notifications_received",
    "fall_detected",
    "battery_level",
    "chronic_condition",
    "smoker",
    "medication",
    "predicted_disease_simulated",
    "rule_status",
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
    "processed_at",
]

NUMERIC_FEATURES = [
    "age",
    "weight",
    "height",
    "bmi",
    "heart_rate",
    "spo2",
    "temperature",
    "systolic_bp",
    "diastolic_bp",
    "respiratory_rate",
    "glucose_level",
    "skin_temperature",
    "steps",
    "sleep_duration",
    "screen_time",
    "notifications_received",
    "battery_level",
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
    "fall_detected",
]

MODEL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES

STATUS_LABELS = ["NORMAL", "WARNING", "CRITICAL", "EMERGENCY"]
STATUS_TO_SEVERITY = {label: index for index, label in enumerate(STATUS_LABELS)}
SEVERITY_TO_STATUS = {index: label for label, index in STATUS_TO_SEVERITY.items()}

FIELD_DEFAULTS: dict[str, Any] = {
    "patient_id": "unknown",
    "timestamp": "",
    "age": 45,
    "gender": "Unknown",
    "weight": 75.0,
    "height": 1.72,
    "bmi": 25.0,
    "heart_rate": 78,
    "spo2": 97.0,
    "temperature": 36.8,
    "systolic_bp": 120,
    "diastolic_bp": 78,
    "respiratory_rate": 16,
    "glucose_level": 105.0,
    "skin_temperature": 34.0,
    "activity_level": "Resting",
    "exercise_type": "None",
    "exercise_intensity": "Low",
    "steps": 2500,
    "stress_level": "Medium",
    "sleep_duration": 7.0,
    "sleep_quality": "Fair",
    "screen_time": 3.0,
    "notifications_received": 20,
    "fall_detected": False,
    "battery_level": 80.0,
    "chronic_condition": "None",
    "smoker": "No",
    "medication": "No",
    "predicted_disease_simulated": "None",
}

TRUE_VALUES = {"1", "true", "yes", "y", "t", "fall", "detected", "abnormal"}
FALSE_VALUES = {"0", "false", "no", "n", "f", "none", "normal", "not detected"}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    if text in TRUE_VALUES:
        return True
    if text in FALSE_VALUES:
        return False
    try:
        return float(text) > 0
    except (TypeError, ValueError):
        return False


def safe_float(value: Any, default: float) -> float:
    try:
        if value is None or value == "":
            return float(default)
        result = float(value)
        if result != result:
            return float(default)
        return result
    except (TypeError, ValueError):
        return float(default)


def safe_int(value: Any, default: int) -> int:
    return int(round(safe_float(value, float(default))))


def normalize_gender(value: Any) -> str:
    text = str(value or "Unknown").strip()
    if text.lower() in {"m", "male", "man"}:
        return "Male"
    if text.lower() in {"f", "female", "woman"}:
        return "Female"
    return text.title() if text else "Unknown"


def fill_record_defaults(record: Mapping[str, Any]) -> dict[str, Any]:
    filled = dict(FIELD_DEFAULTS)
    filled.update({key: value for key, value in dict(record).items() if value is not None})

    for field in NUMERIC_FEATURES:
        default = FIELD_DEFAULTS[field]
        if field in {"age", "heart_rate", "systolic_bp", "diastolic_bp", "respiratory_rate", "steps", "notifications_received"}:
            filled[field] = safe_int(filled.get(field), int(default))
        else:
            filled[field] = safe_float(filled.get(field), float(default))

    height = filled["height"]
    if height > 3.0:
        height = height / 100.0
        filled["height"] = height

    if not filled.get("bmi") or filled["bmi"] <= 0:
        filled["bmi"] = round(filled["weight"] / max(height * height, 0.1), 1)

    filled["gender"] = normalize_gender(filled.get("gender"))
    filled["fall_detected"] = coerce_bool(filled.get("fall_detected"))
    for field in CATEGORICAL_FEATURES:
        if field == "fall_detected":
            continue
        value = filled.get(field, FIELD_DEFAULTS[field])
        filled[field] = str(value).strip() if str(value).strip() else FIELD_DEFAULTS[field]

    return filled


def _vital_severity(name: str, value: Any) -> int:
    v = safe_float(value, float(FIELD_DEFAULTS.get(name, 0)))

    if name == "heart_rate":
        if v < 40 or v > 150:
            return 3
        if v < 50 or v > 130:
            return 2
        if v < 60 or v > 100:
            return 1
        return 0

    if name == "spo2":
        if v < 85:
            return 3
        if v < 90:
            return 2
        if v < 95:
            return 1
        return 0

    if name == "temperature":
        if v < 35 or v >= 40:
            return 3
        if v >= 39:
            return 2
        if v < 36 or v > 37.8:
            return 1
        return 0

    if name == "systolic_bp":
        if v < 80 or v > 200:
            return 3
        if v < 90 or v > 160:
            return 2
        if v > 140:
            return 1
        return 0

    if name == "diastolic_bp":
        if v < 50 or v > 130:
            return 3
        if v < 60 or v > 100:
            return 2
        if v > 90:
            return 1
        return 0

    if name == "respiratory_rate":
        if v < 8 or v > 35:
            return 3
        if v < 10 or v > 30:
            return 2
        if v < 12 or v > 20:
            return 1
        return 0

    if name == "glucose_level":
        if v < 50 or v > 250:
            return 3
        if v > 180:
            return 2
        if v > 140:
            return 1
        return 0

    return 0


def derive_status(record: Mapping[str, Any]) -> str:
    row = fill_record_defaults(record)
    severities = [
        _vital_severity("heart_rate", row["heart_rate"]),
        _vital_severity("spo2", row["spo2"]),
        _vital_severity("temperature", row["temperature"]),
        _vital_severity("systolic_bp", row["systolic_bp"]),
        _vital_severity("diastolic_bp", row["diastolic_bp"]),
        _vital_severity("respiratory_rate", row["respiratory_rate"]),
        _vital_severity("glucose_level", row["glucose_level"]),
    ]
    severity = max(severities)

    abnormal_count = sum(1 for item in severities if item >= 1)
    critical_count = sum(1 for item in severities if item >= 2)
    if abnormal_count >= 3 and severity < 2:
        severity = 2
    if critical_count >= 2 and severity < 3:
        severity = 3

    if row["fall_detected"]:
        severity = max(1, min(3, severity + 1))

    return SEVERITY_TO_STATUS[severity]


def derive_status_from_alert_columns(record: Mapping[str, Any]) -> str:
    row = dict(record)
    alert_columns = [
        "Heart Rate Alert",
        "SpO2 Level Alert",
        "Blood Pressure Alert",
        "Temperature Alert",
        "heart_rate_alert",
        "spo2_level_alert",
        "blood_pressure_alert",
        "temperature_alert",
    ]
    present_alerts = [str(row[col]).strip().lower() for col in alert_columns if col in row and str(row[col]).strip()]
    abnormal_alerts = [
        value
        for value in present_alerts
        if value not in {"normal", "nan", "none", "no alert", "false", "0"}
    ]
    threshold_status = derive_status(row)
    threshold_severity = STATUS_TO_SEVERITY[threshold_status]

    if not abnormal_alerts:
        return threshold_status
    if len(abnormal_alerts) >= 3:
        return SEVERITY_TO_STATUS[max(threshold_severity, 2)]
    if len(abnormal_alerts) >= 2:
        return SEVERITY_TO_STATUS[max(threshold_severity, 2)]
    return SEVERITY_TO_STATUS[max(threshold_severity, 1)]


def risk_from_status(status: str) -> float:
    return {
        "NORMAL": 25.0,
        "WARNING": 55.0,
        "CRITICAL": 78.0,
        "EMERGENCY": 94.0,
    }.get(str(status).upper(), 50.0)


def risk_level(score: Any) -> str:
    value = safe_float(score, 50.0)
    if value >= 85:
        return "CRITICAL"
    if value >= 70:
        return "HIGH"
    if value >= 45:
        return "MODERATE"
    return "LOW"


def detect_anomaly_type(record: Mapping[str, Any]) -> str:
    row = fill_record_defaults(record)
    parts = []
    for field in ["heart_rate", "spo2", "temperature", "systolic_bp", "diastolic_bp", "respiratory_rate", "glucose_level"]:
        if _vital_severity(field, row[field]) >= 2:
            parts.append(field)
    if row["battery_level"] < 15:
        parts.append("battery_level")
    if row["fall_detected"]:
        parts.append("fall_detected")
    return ", ".join(parts) if parts else "none"


def generate_alert(record: Mapping[str, Any]) -> dict[str, str]:
    row = fill_record_defaults(record)
    predicted_status = str(record.get("predicted_status") or record.get("rule_status") or derive_status(row)).upper()
    score = safe_float(record.get("risk_score"), risk_from_status(predicted_status))
    is_anomaly = coerce_bool(record.get("is_anomaly"))
    anomaly_type = str(record.get("anomaly_type") or detect_anomaly_type(row))

    alert_type = "NO_ALERT"
    alert_severity = "NONE"
    messages: list[str] = []

    if row["fall_detected"]:
        alert_type = "FALL_DETECTED"
        alert_severity = "CRITICAL" if score >= 85 or predicted_status == "EMERGENCY" else "HIGH"
        messages.append("Fall detected: patient may require immediate assistance.")

    if predicted_status == "EMERGENCY" or score >= 85:
        alert_type = "EMERGENCY_HEALTH_ALERT"
        alert_severity = "CRITICAL"
        messages.append("Emergency alert: severe vital sign pattern detected.")
    elif predicted_status == "CRITICAL" or score >= 70:
        if alert_type == "NO_ALERT":
            alert_type = "HIGH_RISK_ALERT"
            alert_severity = "HIGH"
        messages.append("High risk alert: critical health indicators require review.")
    elif predicted_status == "WARNING" or score >= 45:
        if alert_type == "NO_ALERT":
            alert_type = "WARNING_HEALTH_ALERT"
            alert_severity = "MEDIUM"
        messages.append("Warning alert: patient readings are outside normal simulation limits.")

    if is_anomaly:
        if alert_type == "NO_ALERT":
            alert_type = "ANOMALY_DETECTED"
            alert_severity = "MEDIUM"
        messages.append(f"Anomaly detected: unusual sensor pattern ({anomaly_type}).")

    if row["battery_level"] < 15:
        if alert_type == "NO_ALERT":
            alert_type = "LOW_SENSOR_BATTERY"
            alert_severity = "LOW"
        messages.append("Low sensor battery: device battery is below 15%.")

    if not messages:
        messages.append("No alert.")

    return {
        "alert_type": alert_type,
        "alert_severity": alert_severity,
        "alert_message": " ".join(messages),
    }


def feature_schema_payload() -> dict[str, Any]:
    return {
        "input_fields": INPUT_FIELDS,
        "enriched_fields": ENRICHED_FIELDS,
        "numeric_features": NUMERIC_FEATURES,
        "categorical_features": CATEGORICAL_FEATURES,
        "model_features": MODEL_FEATURES,
        "status_labels": STATUS_LABELS,
        "model_version": MODEL_VERSION,
        "medical_disclaimer": "Educational IoT simulation only. Not for diagnosis or clinical use.",
    }
