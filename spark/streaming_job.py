"""
Smart Health Monitoring IoT — Spark Structured Streaming Job
Reads from 5 Kafka sensor topics, joins streams, applies ML inference, writes to Cassandra.
"""

import json
import logging
import os
import sys
from datetime import datetime

import numpy as np

# Add paths for imports
sys.path.insert(0, "/app")
sys.path.insert(0, "/app/alerts")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    concat_ws,
    expr,
    from_json,
    least,
    lit,
    to_timestamp,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SparkStreaming")

# =============================================================================
# Configuration
# =============================================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "smart_health")
CHECKPOINT_LOCATION = os.getenv("SPARK_CHECKPOINT_LOCATION")
if not CHECKPOINT_LOCATION:
    CHECKPOINT_LOCATION = f"/tmp/smart-health-checkpoint-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

# Kafka topics
TOPICS = {
    "vitals": "health.vitals",
    "blood_pressure": "health.blood_pressure",
    "glucose": "health.glucose",
    "activity": "health.activity",
    "fall_safety": "health.fall_safety",
}

# =============================================================================
# Stream Schemas
# =============================================================================

vitals_schema = StructType([
    StructField("patient_id", StringType(), False),
    StructField("sensor_id", StringType()),
    StructField("sensor_type", StringType()),
    StructField("timestamp", StringType(), False),
    StructField("heart_rate", IntegerType()),
    StructField("spo2", DoubleType()),
    StructField("temperature", DoubleType()),
    StructField("respiratory_rate", IntegerType()),
    StructField("battery_level", DoubleType()),
    StructField("age", IntegerType()),
    StructField("gender", StringType()),
    StructField("weight", DoubleType()),
    StructField("height", DoubleType()),
    StructField("bmi", DoubleType()),
    StructField("chronic_condition", StringType()),
    StructField("smoker", StringType()),
    StructField("medication", StringType()),
    StructField("stress_level", StringType()),
    StructField("sleep_duration", DoubleType()),
    StructField("sleep_quality", StringType()),
    StructField("predicted_disease_simulated", StringType()),
])

bp_schema = StructType([
    StructField("patient_id", StringType(), False),
    StructField("sensor_id", StringType()),
    StructField("sensor_type", StringType()),
    StructField("timestamp", StringType(), False),
    StructField("systolic_bp", IntegerType()),
    StructField("diastolic_bp", IntegerType()),
    StructField("battery_level", DoubleType()),
])

glucose_schema = StructType([
    StructField("patient_id", StringType(), False),
    StructField("sensor_id", StringType()),
    StructField("sensor_type", StringType()),
    StructField("timestamp", StringType(), False),
    StructField("glucose_level", DoubleType()),
    StructField("battery_level", DoubleType()),
])

activity_schema = StructType([
    StructField("patient_id", StringType(), False),
    StructField("sensor_id", StringType()),
    StructField("sensor_type", StringType()),
    StructField("timestamp", StringType(), False),
    StructField("steps", IntegerType()),
    StructField("activity_level", StringType()),
    StructField("exercise_type", StringType()),
    StructField("exercise_intensity", StringType()),
    StructField("battery_level", DoubleType()),
])

fall_schema = StructType([
    StructField("patient_id", StringType(), False),
    StructField("sensor_id", StringType()),
    StructField("sensor_type", StringType()),
    StructField("timestamp", StringType(), False),
    StructField("fall_detected", BooleanType()),
    StructField("skin_temperature", DoubleType()),
    StructField("battery_level", DoubleType()),
])

# =============================================================================
# ML Model Loading
# =============================================================================

MODELS = {
    "status_classifier": None,
    "risk_regressor": None,
    "anomaly_detector": None,
    "heart_rate_forecaster": None,
    "preprocessing_pipeline": None,
}
FEATURE_SCHEMA = None
MODEL_VERSION = "1.0.0"


def load_models():
    """Load ML models from /models directory. Gracefully handle missing models."""
    global FEATURE_SCHEMA, MODEL_VERSION

    try:
        import joblib
    except ImportError:
        logger.warning("joblib not available. ML models will not be loaded.")
        return

    models_dir = "/models"

    model_files = {
        "status_classifier": "status_classifier.pkl",
        "risk_regressor": "risk_regressor.pkl",
        "anomaly_detector": "anomaly_detector.pkl",
        "heart_rate_forecaster": "heart_rate_forecaster.pkl",
        "preprocessing_pipeline": "preprocessing_pipeline.pkl",
    }

    for key, filename in model_files.items():
        path = os.path.join(models_dir, filename)
        if os.path.exists(path):
            try:
                MODELS[key] = joblib.load(path)
                logger.info(f"Loaded model: {filename}")
            except Exception as e:
                logger.warning(f"Failed to load {filename}: {e}")
        else:
            logger.warning(f"Model file not found: {path} — using rule-based fallback.")

    # Load feature schema
    schema_path = os.path.join(models_dir, "feature_schema.json")
    if os.path.exists(schema_path):
        try:
            with open(schema_path) as f:
                FEATURE_SCHEMA = json.load(f)
            logger.info("Loaded feature_schema.json")
        except Exception as e:
            logger.warning(f"Failed to load feature_schema.json: {e}")

    # Load model metadata
    meta_path = os.path.join(models_dir, "model_metadata.json")
    if os.path.exists(meta_path):
        try:
            with open(meta_path) as f:
                meta = json.load(f)
            MODEL_VERSION = meta.get("model_version", "1.0.0")
        except Exception:
            pass


# =============================================================================
# Rule-Based Status Derivation
# =============================================================================

def derive_status(row):
    """Derive patient status from vital signs using rule-based thresholds."""
    severity_order = {"NORMAL": 0, "WARNING": 1, "CRITICAL": 2, "EMERGENCY": 3}
    severity_names = {0: "NORMAL", 1: "WARNING", 2: "CRITICAL", 3: "EMERGENCY"}
    max_sev = 0

    hr = row.get("heart_rate")
    if hr is not None:
        if hr < 40 or hr > 150:
            max_sev = max(max_sev, 3)
        elif hr < 50 or hr > 130:
            max_sev = max(max_sev, 2)
        elif hr < 60 or hr > 100:
            max_sev = max(max_sev, 1)

    spo2 = row.get("spo2")
    if spo2 is not None:
        if spo2 < 85:
            max_sev = max(max_sev, 3)
        elif spo2 < 90:
            max_sev = max(max_sev, 2)
        elif spo2 < 95:
            max_sev = max(max_sev, 1)

    temp = row.get("temperature")
    if temp is not None:
        if temp >= 40.0 or temp < 35.0:
            max_sev = max(max_sev, 3)
        elif temp >= 39.0:
            max_sev = max(max_sev, 2)
        elif temp < 36.0 or temp > 37.8:
            max_sev = max(max_sev, 1)

    sys_bp = row.get("systolic_bp")
    if sys_bp is not None:
        if sys_bp > 200 or sys_bp < 80:
            max_sev = max(max_sev, 3)
        elif sys_bp > 160:
            max_sev = max(max_sev, 2)
        elif sys_bp > 140:
            max_sev = max(max_sev, 1)

    dia_bp = row.get("diastolic_bp")
    if dia_bp is not None:
        if dia_bp > 130 or dia_bp < 50:
            max_sev = max(max_sev, 3)
        elif dia_bp > 100:
            max_sev = max(max_sev, 2)
        elif dia_bp > 90:
            max_sev = max(max_sev, 1)

    rr = row.get("respiratory_rate")
    if rr is not None:
        if rr < 8 or rr > 35:
            max_sev = max(max_sev, 3)
        elif rr < 10 or rr > 30:
            max_sev = max(max_sev, 2)
        elif rr < 12 or rr > 20:
            max_sev = max(max_sev, 1)

    glucose = row.get("glucose_level")
    if glucose is not None:
        if glucose < 50 or glucose > 250:
            max_sev = max(max_sev, 3)
        elif glucose > 180:
            max_sev = max(max_sev, 2)
        elif glucose > 140:
            max_sev = max(max_sev, 1)

    # Fall upgrade rule
    fall = row.get("fall_detected", False)
    if fall:
        if max_sev <= 1:
            max_sev = 2
        elif max_sev == 2:
            max_sev = 3

    return severity_names.get(max_sev, "NORMAL")


def derive_risk_level(score):
    """Map risk score to level."""
    if score is None:
        return "LOW"
    if score <= 30:
        return "LOW"
    elif score <= 55:
        return "MEDIUM"
    elif score <= 75:
        return "HIGH"
    return "CRITICAL"


def generate_alert(row):
    """Generate alert based on inference results."""
    fall_detected = row.get("fall_detected", False)
    predicted_status = row.get("predicted_status", "NORMAL")
    risk_score = row.get("risk_score", 0) or 0
    is_anomaly = row.get("is_anomaly", False)
    battery_level = row.get("battery_level", 100) or 100

    hr = row.get("heart_rate", "N/A")
    spo2 = row.get("spo2", "N/A")
    sys_bp = row.get("systolic_bp", "N/A")
    rr = row.get("respiratory_rate", "N/A")

    alert_type = "NO_ALERT"
    alert_severity = "NONE"
    alert_message = "No alert."

    if fall_detected:
        alert_type = "FALL_DETECTED"
        alert_severity = "CRITICAL" if risk_score >= 70 else "HIGH"
        alert_message = "Fall detected: patient may require immediate assistance."
    elif predicted_status == "EMERGENCY" or risk_score >= 85:
        alert_type = "EMERGENCY_HEALTH_ALERT"
        alert_severity = "CRITICAL"
        alert_message = f"Emergency alert: SpO2 at {spo2}% and heart rate at {hr} bpm detected."
    elif predicted_status == "CRITICAL" or risk_score >= 70:
        alert_type = "HIGH_RISK_ALERT"
        alert_severity = "HIGH"
        alert_message = f"High risk alert: systolic BP at {sys_bp} mmHg and respiratory rate at {rr} breaths/min."
    elif predicted_status == "WARNING" or risk_score >= 45:
        alert_type = "WARNING_HEALTH_ALERT"
        alert_severity = "MEDIUM"
        alert_message = f"Warning: heart rate elevated at {hr} bpm. Monitor closely."
    elif is_anomaly:
        alert_type = "ANOMALY_DETECTED"
        alert_severity = "MEDIUM"
        alert_message = "Anomaly detected: sensor values are unusual compared to learned patterns."
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

    return alert_type, alert_severity, alert_message


# =============================================================================
# ML Inference
# =============================================================================

def _safe_float(value):
    """Convert a scalar to float, returning None for missing/non-numeric values."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def fallback_heart_rate_forecast(row_dict):
    """Deterministic one-step HR forecast used when the trained model is unavailable."""
    hr = _safe_float(row_dict.get("heart_rate"))
    if hr is None:
        return None

    spo2 = _safe_float(row_dict.get("spo2"))
    temp = _safe_float(row_dict.get("temperature"))
    rr = _safe_float(row_dict.get("respiratory_rate"))

    delta = 0.0
    if spo2 is not None:
        if spo2 < 90:
            delta += 3.0
        elif spo2 < 95:
            delta += 1.5

    if temp is not None:
        if temp >= 39:
            delta += 2.0
        elif temp >= 38:
            delta += 1.0
        elif temp < 35.5:
            delta += 1.0

    if rr is not None:
        if rr >= 30:
            delta += 1.5
        elif rr <= 10:
            delta -= 1.0

    return float(round(max(30.0, min(190.0, hr + delta)), 1))


def apply_ml_inference(row_dict):
    """Apply ML models to a single row. Falls back to rules if models unavailable."""
    import pandas as pd

    # Rule-based status (always computed as fallback)
    rule_status = derive_status(row_dict)
    row_dict["rule_status"] = rule_status

    # Status Classification
    if MODELS["status_classifier"] is not None:
        try:
            features = _prepare_features_for_model(row_dict, "status_classifier")
            if features is not None:
                pred = MODELS["status_classifier"].predict(features)
                row_dict["predicted_status"] = pred[0]
            else:
                row_dict["predicted_status"] = rule_status
        except Exception as e:
            logger.debug(f"Status classifier failed: {e}")
            row_dict["predicted_status"] = rule_status
    else:
        row_dict["predicted_status"] = rule_status

    # Risk Score Regression
    if MODELS["risk_regressor"] is not None:
        try:
            features = _prepare_features_for_model(row_dict, "risk_regressor")
            if features is not None:
                pred = MODELS["risk_regressor"].predict(features)
                row_dict["risk_score"] = float(np.clip(pred[0], 0, 100))
            else:
                row_dict["risk_score"] = _risk_from_status(rule_status)
        except Exception as e:
            logger.debug(f"Risk regressor failed: {e}")
            row_dict["risk_score"] = _risk_from_status(rule_status)
    else:
        row_dict["risk_score"] = _risk_from_status(rule_status)

    row_dict["risk_level"] = derive_risk_level(row_dict["risk_score"])

    # Anomaly Detection
    if MODELS["anomaly_detector"] is not None:
        try:
            features = _prepare_features_for_model(row_dict, "anomaly_detector")
            if features is not None:
                pred = MODELS["anomaly_detector"].predict(features)
                score = MODELS["anomaly_detector"].decision_function(features)
                row_dict["is_anomaly"] = bool(pred[0] == -1)
                row_dict["anomaly_score"] = float(score[0])
                row_dict["anomaly_type"] = "SENSOR_ANOMALY" if row_dict["is_anomaly"] else "NONE"
            else:
                row_dict["is_anomaly"] = False
                row_dict["anomaly_score"] = 0.0
                row_dict["anomaly_type"] = "NONE"
        except Exception as e:
            logger.debug(f"Anomaly detector failed: {e}")
            row_dict["is_anomaly"] = False
            row_dict["anomaly_score"] = 0.0
            row_dict["anomaly_type"] = "NONE"
    else:
        row_dict["is_anomaly"] = False
        row_dict["anomaly_score"] = 0.0
        row_dict["anomaly_type"] = "NONE"

    # Heart Rate Forecasting
    if MODELS["heart_rate_forecaster"] is not None:
        try:
            hr = row_dict.get("heart_rate", 75)
            # Use current HR as all lags (simplified for streaming)
            lag_features = np.array([[hr, hr, hr, hr, hr]])
            pred = MODELS["heart_rate_forecaster"].predict(lag_features)
            row_dict["predicted_next_heart_rate"] = float(round(pred[0], 1))
            row_dict["heart_rate_forecast_method"] = "model"
        except Exception as e:
            logger.debug(f"HR forecaster failed: {e}")
            row_dict["predicted_next_heart_rate"] = fallback_heart_rate_forecast(row_dict)
            row_dict["heart_rate_forecast_method"] = "fallback"
    else:
        row_dict["predicted_next_heart_rate"] = fallback_heart_rate_forecast(row_dict)
        row_dict["heart_rate_forecast_method"] = "fallback"

    # Generate alert
    alert_type, alert_severity, alert_message = generate_alert(row_dict)
    row_dict["alert_type"] = alert_type
    row_dict["alert_severity"] = alert_severity
    row_dict["alert_message"] = alert_message

    row_dict["model_version"] = MODEL_VERSION
    row_dict["processed_at"] = to_cassandra_datetime(datetime.utcnow())

    return row_dict


def _prepare_features_for_model(row_dict, model_name):
    """Prepare feature array for a specific model."""
    import pandas as pd

    model = MODELS[model_name]
    if model is None:
        return None

    # Get expected features from model
    try:
        if hasattr(model, "feature_names_in_"):
            feature_names = list(model.feature_names_in_)
        elif hasattr(model, "n_features_in_"):
            # Use standard feature set
            feature_names = _get_standard_features(model_name)
        else:
            feature_names = _get_standard_features(model_name)
    except Exception:
        feature_names = _get_standard_features(model_name)

    # Build feature row
    row_data = {}
    for feat in feature_names:
        val = row_dict.get(feat)
        if val is None:
            row_data[feat] = 0
        elif isinstance(val, bool):
            row_data[feat] = int(val)
        else:
            try:
                row_data[feat] = float(val)
            except (ValueError, TypeError):
                row_data[feat] = 0

    df = pd.DataFrame([row_data])
    return df


def _get_standard_features(model_name):
    """Get standard feature list for a model."""
    if model_name == "anomaly_detector":
        return ["heart_rate", "spo2", "temperature", "systolic_bp", "diastolic_bp",
                "respiratory_rate", "glucose_level", "skin_temperature", "battery_level"]
    elif model_name == "heart_rate_forecaster":
        return ["hr_lag_1", "hr_lag_2", "hr_lag_3", "hr_lag_4", "hr_lag_5"]
    else:
        return ["heart_rate", "spo2", "temperature", "systolic_bp", "diastolic_bp",
                "respiratory_rate", "glucose_level", "fall_detected"]


def _risk_from_status(status):
    """Derive risk score from rule-based status."""
    mapping = {"NORMAL": 20, "WARNING": 50, "CRITICAL": 75, "EMERGENCY": 92}
    return mapping.get(status, 20)


SENSOR_SOURCES = [
    {
        "id_field": "vitals_sensor_id",
        "type_field": "vitals_sensor_type",
        "battery_field": "vitals_battery",
        "topic": TOPICS["vitals"],
    },
    {
        "id_field": "bp_sensor_id",
        "type_field": "bp_sensor_type",
        "battery_field": "bp_battery",
        "topic": TOPICS["blood_pressure"],
    },
    {
        "id_field": "glucose_sensor_id",
        "type_field": "glucose_sensor_type",
        "battery_field": "glucose_battery",
        "topic": TOPICS["glucose"],
    },
    {
        "id_field": "activity_sensor_id",
        "type_field": "activity_sensor_type",
        "battery_field": "activity_battery",
        "topic": TOPICS["activity"],
    },
    {
        "id_field": "fall_sensor_id",
        "type_field": "fall_sensor_type",
        "battery_field": "fall_battery",
        "topic": TOPICS["fall_safety"],
    },
]

INT_RECORD_FIELDS = [
    "age", "heart_rate", "respiratory_rate", "systolic_bp", "diastolic_bp", "steps"
]

FLOAT_RECORD_FIELDS = [
    "weight", "height", "bmi", "spo2", "temperature", "glucose_level",
    "skin_temperature", "battery_level", "vitals_battery", "bp_battery",
    "glucose_battery", "activity_battery", "fall_battery", "sleep_duration",
    "risk_score", "anomaly_score", "predicted_next_heart_rate",
]

BOOL_RECORD_FIELDS = ["fall_detected", "is_anomaly"]


def _present(value):
    """Return True when a scalar contains a useful sensor value."""
    if value is None:
        return False
    if isinstance(value, float) and np.isnan(value):
        return False
    return str(value).strip() != ""


def coerce_record_types(record):
    """Convert Pandas/numpy scalar types into Cassandra-friendly Python types."""
    for field in INT_RECORD_FIELDS:
        value = record.get(field)
        if value is not None:
            record[field] = int(value)

    for field in FLOAT_RECORD_FIELDS:
        value = record.get(field)
        if value is not None:
            record[field] = float(value)

    for field in BOOL_RECORD_FIELDS:
        value = record.get(field)
        if value is not None:
            record[field] = bool(value)

    return record


# =============================================================================
# Cassandra Writer
# =============================================================================

cassandra_session = None


def get_cassandra_session():
    """Get or create Cassandra session."""
    global cassandra_session
    if cassandra_session is not None:
        return cassandra_session

    try:
        from cassandra.cluster import Cluster
        cluster = Cluster([CASSANDRA_HOST])
        cassandra_session = cluster.connect(CASSANDRA_KEYSPACE)
        logger.info(f"Connected to Cassandra at {CASSANDRA_HOST}/{CASSANDRA_KEYSPACE}")
        return cassandra_session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {e}")
        return None


def execute_insert(session, table, columns, values):
    """Execute an INSERT with generated placeholders for fixed local table names."""
    placeholders = ",".join(["%s"] * len(columns))
    session.execute(
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
        tuple(values),
    )


def write_sensor_metadata(session, record):
    """Upsert latest metadata for each source sensor present in an enriched row."""
    for source in SENSOR_SOURCES:
        sensor_id = record.get(source["id_field"])
        if not _present(sensor_id):
            continue

        try:
            execute_insert(
                session,
                "sensor_metadata",
                [
                    "sensor_id",
                    "patient_id",
                    "sensor_type",
                    "kafka_topic",
                    "last_reading_time",
                    "battery_level",
                    "last_status",
                    "last_seen_at",
                ],
                [
                    sensor_id,
                    record.get("patient_id"),
                    record.get(source["type_field"]),
                    source["topic"],
                    record.get("reading_time"),
                    record.get(source["battery_field"]),
                    record.get("predicted_status"),
                    record.get("processed_at"),
                ],
            )
        except Exception as e:
            logger.warning(f"Sensor metadata write failed for {sensor_id}: {e}")


def write_minute_metrics(session, records):
    """Write one-minute patient aggregates for the demo analytics table."""
    groups = {}
    for record in records:
        reading_time = record.get("reading_time")
        if not isinstance(reading_time, datetime):
            reading_time = to_cassandra_datetime(reading_time)
        window_start = reading_time.replace(second=0, microsecond=0)
        key = (record.get("patient_id"), window_start)

        if key not in groups:
            groups[key] = {
                "count": 0,
                "heart_rates": [],
                "spo2_values": [],
                "temperatures": [],
                "systolic_values": [],
                "risk_scores": [],
                "high_alerts": 0,
            }

        group = groups[key]
        group["count"] += 1
        if record.get("heart_rate") is not None:
            group["heart_rates"].append(record.get("heart_rate"))
        if record.get("spo2") is not None:
            group["spo2_values"].append(record.get("spo2"))
        if record.get("temperature") is not None:
            group["temperatures"].append(record.get("temperature"))
        if record.get("systolic_bp") is not None:
            group["systolic_values"].append(record.get("systolic_bp"))
        if record.get("risk_score") is not None:
            group["risk_scores"].append(record.get("risk_score"))
        if record.get("alert_severity") in ("HIGH", "CRITICAL"):
            group["high_alerts"] += 1

    for (patient_id, window_start), group in groups.items():
        try:
            execute_insert(
                session,
                "patient_minute_metrics",
                [
                    "patient_id",
                    "window_start",
                    "reading_count",
                    "avg_heart_rate",
                    "min_spo2",
                    "max_temperature",
                    "max_systolic_bp",
                    "max_risk_score",
                    "high_or_critical_alerts",
                ],
                [
                    patient_id,
                    window_start,
                    int(group["count"]),
                    float(round(np.mean(group["heart_rates"]), 2)) if group["heart_rates"] else None,
                    float(round(min(group["spo2_values"]), 2)) if group["spo2_values"] else None,
                    float(round(max(group["temperatures"]), 2)) if group["temperatures"] else None,
                    int(max(group["systolic_values"])) if group["systolic_values"] else None,
                    float(round(max(group["risk_scores"]), 2)) if group["risk_scores"] else None,
                    int(group["high_alerts"]),
                ],
            )
        except Exception as e:
            logger.warning(f"Minute metric write failed for {patient_id}: {e}")


def should_update_latest_status(session, record):
    """Return True when this record is fresh enough for the latest-status table."""
    patient_id = record.get("patient_id")
    candidate_time = record.get("reading_time")
    if not patient_id or candidate_time is None:
        return True

    candidate_time = to_comparable_datetime(candidate_time)
    try:
        current = session.execute(
            "SELECT reading_time FROM patient_latest_status WHERE patient_id = %s",
            (patient_id,),
        ).one()
    except Exception as e:
        logger.warning(f"Latest-status freshness check failed for {patient_id}: {e}")
        return True

    if current is None or current.reading_time is None:
        return True

    current_time = to_comparable_datetime(current.reading_time)
    if candidate_time < current_time:
        logger.info(
            "Skipping older latest-status update for %s: candidate=%s current=%s",
            patient_id,
            candidate_time,
            current_time,
        )
        return False
    return True


def write_to_cassandra(records):
    """Write enriched records to Cassandra tables."""
    logger.info(f"write_to_cassandra called with {len(records)} records")
    session = get_cassandra_session()
    if session is None:
        logger.warning("Cassandra unavailable. Skipping writes.")
        return

    for record in records:
        try:
            logger.debug(f"Writing record for {record.get('patient_id')}: {record.get('predicted_status')}")
            # Write to sensor_readings
            execute_insert(
                session,
                "sensor_readings",
                [
                    "patient_id", "reading_time",
                    "vitals_sensor_id", "bp_sensor_id", "glucose_sensor_id",
                    "activity_sensor_id", "fall_sensor_id",
                    "source_sensor_ids", "source_sensor_types",
                    "age", "gender", "weight", "height", "bmi",
                    "heart_rate", "spo2", "temperature", "systolic_bp", "diastolic_bp",
                    "respiratory_rate", "glucose_level", "skin_temperature",
                    "activity_level", "exercise_type", "exercise_intensity", "steps",
                    "stress_level", "sleep_duration", "sleep_quality", "fall_detected",
                    "battery_level", "chronic_condition", "smoker", "medication",
                    "predicted_disease_simulated", "rule_status", "predicted_status",
                    "risk_score", "risk_level", "is_anomaly", "anomaly_score", "anomaly_type",
                    "predicted_next_heart_rate", "alert_type", "alert_severity",
                    "alert_message", "model_version", "processed_at",
                ],
                [
                    record.get("patient_id"),
                    record.get("reading_time"),
                    record.get("vitals_sensor_id"),
                    record.get("bp_sensor_id"),
                    record.get("glucose_sensor_id"),
                    record.get("activity_sensor_id"),
                    record.get("fall_sensor_id"),
                    record.get("source_sensor_ids"),
                    record.get("source_sensor_types"),
                    record.get("age"),
                    record.get("gender"),
                    record.get("weight"),
                    record.get("height"),
                    record.get("bmi"),
                    record.get("heart_rate"),
                    record.get("spo2"),
                    record.get("temperature"),
                    record.get("systolic_bp"),
                    record.get("diastolic_bp"),
                    record.get("respiratory_rate"),
                    record.get("glucose_level"),
                    record.get("skin_temperature"),
                    record.get("activity_level"),
                    record.get("exercise_type"),
                    record.get("exercise_intensity"),
                    record.get("steps"),
                    record.get("stress_level"),
                    record.get("sleep_duration"),
                    record.get("sleep_quality"),
                    record.get("fall_detected"),
                    record.get("battery_level"),
                    record.get("chronic_condition"),
                    record.get("smoker"),
                    record.get("medication"),
                    record.get("predicted_disease_simulated"),
                    record.get("rule_status"),
                    record.get("predicted_status"),
                    record.get("risk_score"),
                    record.get("risk_level"),
                    record.get("is_anomaly"),
                    record.get("anomaly_score"),
                    record.get("anomaly_type"),
                    record.get("predicted_next_heart_rate"),
                    record.get("alert_type"),
                    record.get("alert_severity"),
                    record.get("alert_message"),
                    record.get("model_version"),
                    record.get("processed_at"),
                ],
            )

            write_sensor_metadata(session, record)

            # Write alerts to patient_alerts
            if record.get("alert_severity") in ("HIGH", "CRITICAL"):
                session.execute(
                    """INSERT INTO patient_alerts (
                        patient_id, alert_time, alert_type, alert_severity, alert_message,
                        predicted_status, risk_score, is_anomaly, fall_detected,
                        heart_rate, spo2, temperature, systolic_bp, diastolic_bp,
                        respiratory_rate, processed_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (
                        record.get("patient_id"),
                        record.get("reading_time"),
                        record.get("alert_type"),
                        record.get("alert_severity"),
                        record.get("alert_message"),
                        record.get("predicted_status"),
                        record.get("risk_score"),
                        record.get("is_anomaly"),
                        record.get("fall_detected"),
                        record.get("heart_rate"),
                        record.get("spo2"),
                        record.get("temperature"),
                        record.get("systolic_bp"),
                        record.get("diastolic_bp"),
                        record.get("respiratory_rate"),
                        record.get("processed_at"),
                    ),
                )

            # Upsert patient_latest_status only when the event-time reading is current.
            if should_update_latest_status(session, record):
                execute_insert(
                    session,
                    "patient_latest_status",
                    [
                        "patient_id", "reading_time", "source_sensor_ids", "source_sensor_types",
                        "predicted_status", "risk_score", "risk_level",
                        "is_anomaly", "anomaly_score", "alert_type", "alert_severity", "alert_message",
                        "heart_rate", "spo2", "temperature", "systolic_bp", "diastolic_bp",
                        "respiratory_rate", "glucose_level", "battery_level",
                        "predicted_next_heart_rate", "processed_at",
                    ],
                    [
                        record.get("patient_id"),
                        record.get("reading_time"),
                        record.get("source_sensor_ids"),
                        record.get("source_sensor_types"),
                        record.get("predicted_status"),
                        record.get("risk_score"),
                        record.get("risk_level"),
                        record.get("is_anomaly"),
                        record.get("anomaly_score"),
                        record.get("alert_type"),
                        record.get("alert_severity"),
                        record.get("alert_message"),
                        record.get("heart_rate"),
                        record.get("spo2"),
                        record.get("temperature"),
                        record.get("systolic_bp"),
                        record.get("diastolic_bp"),
                        record.get("respiratory_rate"),
                        record.get("glucose_level"),
                        record.get("battery_level"),
                        record.get("predicted_next_heart_rate"),
                        record.get("processed_at"),
                    ],
                )

        except Exception as e:
            logger.error(f"Cassandra write error for {record.get('patient_id')}: {e}")

    write_minute_metrics(session, records)


# =============================================================================
# Email Alert Integration
# =============================================================================

email_notifier = None


def init_email_notifier():
    """Initialize email notifier. Non-fatal if unavailable."""
    global email_notifier
    try:
        from alerts.email_notifier import AlertEmailNotifier
        config_path = "/app/alerts/alert_config.yml"
        if not os.path.exists(config_path):
            config_path = None
        email_notifier = AlertEmailNotifier(
            config_path=config_path,
            cassandra_session=get_cassandra_session(),
        )
        email_notifier.test_connection()
    except Exception as e:
        logger.warning(f"Email notifier initialization failed: {e}. Email alerts disabled.")
        email_notifier = None

def to_cassandra_datetime(value):
    """Convert Spark/Pandas timestamp to Python datetime for Cassandra."""
    if value is None:
        return datetime.utcnow()

    # pandas.Timestamp
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()

    # already Python datetime
    if isinstance(value, datetime):
        return value

    # string timestamp fallback
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            return datetime.utcnow()

    return datetime.utcnow()


def to_comparable_datetime(value):
    """Normalize a timestamp for ordering checks before latest-status upserts."""
    dt = to_cassandra_datetime(value)
    if getattr(dt, "tzinfo", None) is not None:
        dt = dt.replace(tzinfo=None)
    return dt


# =============================================================================
# Batch Processing
# =============================================================================

def process_batch(batch_df, batch_id):
    """Process a micro-batch: apply ML inference, write to Cassandra, send alerts."""
    logger.info(f"process_batch START batch_id={batch_id}")
    try:
        logger.info("Converting batch to Pandas...")
        import pandas as pd
        pdf = batch_df.toPandas()
        logger.info(f"SUCCESS: Got {len(pdf)} records")
        
        if len(pdf) == 0:
            logger.info(f"Batch {batch_id} is empty, skipping")
            return

        logger.info(f"Processing {len(pdf)} records from batch {batch_id}")
        enriched_records = []
        
        for idx, (_, row) in enumerate(pdf.iterrows()):
            if idx % 10 == 0:
                logger.debug(f"Processing row {idx}/{len(pdf)}")
            row_dict = row.to_dict()

            # Convert NaN/None
            for k, v in row_dict.items():
                if isinstance(v, float) and np.isnan(v):
                    row_dict[k] = None

            # Set reading_time
            row_dict["reading_time"] = to_cassandra_datetime(row_dict.get("vitals_time"))

            # Apply ML inference
            enriched = coerce_record_types(apply_ml_inference(row_dict))
            enriched_records.append(enriched)

        # Write to Cassandra
        logger.info(f"Writing {len(enriched_records)} records to Cassandra")
        write_to_cassandra(enriched_records)
        logger.info(f"Cassandra write complete")

        # Send email alerts for HIGH and CRITICAL
        if email_notifier is not None:
            alert_count = 0
            for record in enriched_records:
                if record.get("alert_severity") in ("CRITICAL", "HIGH"):
                    try:
                        if email_notifier.send_alert_email(record):
                            alert_count += 1
                    except Exception as e:
                        logger.error(f"Email send failed for {record.get('patient_id')}: {e}")
            if alert_count > 0:
                logger.info(f"Sent {alert_count} alerts")

        logger.info(f"process_batch COMPLETE batch_id={batch_id}")
    except Exception as e:
        logger.error(f"ERROR in process_batch {batch_id}: {type(e).__name__}: {str(e)[:200]}")
        import traceback
        logger.error(traceback.format_exc())


# =============================================================================
# Main Streaming Job
# =============================================================================

def main():
    logger.info("=" * 60)
    logger.info("  Smart Health Monitoring IoT — Spark Streaming")
    logger.info("=" * 60)

    # Load ML models
    load_models()

    # Initialize email notifier
    init_email_notifier()

    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("SmartHealthStreaming")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Read from all 5 Kafka topics
    def read_stream(topic, schema, prefix):
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("maxOffsetsPerTrigger", "50")
            .option("failOnDataLoss", "false")
            .load()
        )
        parsed = (
            raw.select(from_json(col("value").cast("string"), schema).alias("data"))
            .select("data.*")
            .withColumn(f"{prefix}_time",
                        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
            .withWatermark(f"{prefix}_time", "30 seconds")
        )
        return parsed

    # Read all streams
    logger.info("Creating Kafka streams from all topics...")
    vitals_df = read_stream(TOPICS["vitals"], vitals_schema, "vitals")
    logger.info(f"Vitals stream created: {vitals_df}")
    bp_df = read_stream(TOPICS["blood_pressure"], bp_schema, "bp")
    logger.info(f"BP stream created: {bp_df}")
    glucose_df = read_stream(TOPICS["glucose"], glucose_schema, "glucose")
    logger.info(f"Glucose stream created: {glucose_df}")
    activity_df = read_stream(TOPICS["activity"], activity_schema, "activity")
    logger.info(f"Activity stream created: {activity_df}")
    fall_df = read_stream(TOPICS["fall_safety"], fall_schema, "fall")
    logger.info(f"Fall stream created: {fall_df}")

    # Rename columns to avoid conflicts during joins
    bp_df = (bp_df
             .withColumnRenamed("sensor_id", "bp_sensor_id")
             .withColumnRenamed("sensor_type", "bp_sensor_type")
             .withColumnRenamed("battery_level", "bp_battery")
             .withColumnRenamed("timestamp", "bp_timestamp"))

    glucose_df = (glucose_df
                  .withColumnRenamed("sensor_id", "glucose_sensor_id")
                  .withColumnRenamed("sensor_type", "glucose_sensor_type")
                  .withColumnRenamed("battery_level", "glucose_battery")
                  .withColumnRenamed("timestamp", "glucose_timestamp"))

    activity_df = (activity_df
                   .withColumnRenamed("sensor_id", "activity_sensor_id")
                   .withColumnRenamed("sensor_type", "activity_sensor_type")
                   .withColumnRenamed("battery_level", "activity_battery")
                   .withColumnRenamed("timestamp", "activity_timestamp"))

    fall_df = (fall_df
               .withColumnRenamed("sensor_id", "fall_sensor_id")
               .withColumnRenamed("sensor_type", "fall_sensor_type")
               .withColumnRenamed("battery_level", "fall_battery")
               .withColumnRenamed("timestamp", "fall_timestamp"))

    # Join streams on patient_id within watermark window
    # Vitals is the primary stream; left join others
    joined = (
        vitals_df.alias("v")
        .join(
            bp_df.alias("bp"),
            expr("""
                v.patient_id = bp.patient_id AND
                bp.bp_time BETWEEN v.vitals_time - interval 10 seconds AND v.vitals_time + interval 10 seconds
            """),
            "leftOuter",
        )
        .join(
            glucose_df.alias("g"),
            expr("""
                v.patient_id = g.patient_id AND
                g.glucose_time BETWEEN v.vitals_time - interval 10 seconds AND v.vitals_time + interval 10 seconds
            """),
            "leftOuter",
        )
        .join(
            activity_df.alias("a"),
            expr("""
                v.patient_id = a.patient_id AND
                a.activity_time BETWEEN v.vitals_time - interval 10 seconds AND v.vitals_time + interval 10 seconds
            """),
            "leftOuter",
        )
        .join(
            fall_df.alias("f"),
            expr("""
                v.patient_id = f.patient_id AND
                f.fall_time BETWEEN v.vitals_time - interval 10 seconds AND v.vitals_time + interval 10 seconds
            """),
            "leftOuter",
        )
    )

    # Select and apply defaults for missing values
    logger.info("Building result dataframe from joined streams...")
    result = joined.select(
        col("v.patient_id"),
        col("v.vitals_time"),
        col("v.sensor_id").alias("vitals_sensor_id"),
        col("v.sensor_type").alias("vitals_sensor_type"),
        col("bp.bp_sensor_id"),
        col("bp.bp_sensor_type"),
        col("g.glucose_sensor_id"),
        col("g.glucose_sensor_type"),
        col("a.activity_sensor_id"),
        col("a.activity_sensor_type"),
        col("f.fall_sensor_id"),
        col("f.fall_sensor_type"),
        concat_ws(
            ",",
            col("v.sensor_id"),
            col("bp.bp_sensor_id"),
            col("g.glucose_sensor_id"),
            col("a.activity_sensor_id"),
            col("f.fall_sensor_id"),
        ).alias("source_sensor_ids"),
        concat_ws(
            ",",
            col("v.sensor_type"),
            col("bp.bp_sensor_type"),
            col("g.glucose_sensor_type"),
            col("a.activity_sensor_type"),
            col("f.fall_sensor_type"),
        ).alias("source_sensor_types"),
        col("v.battery_level").alias("vitals_battery"),
        col("bp.bp_battery"),
        col("g.glucose_battery"),
        col("a.activity_battery"),
        col("f.fall_battery"),
        col("v.heart_rate"),
        col("v.spo2"),
        col("v.temperature"),
        col("v.respiratory_rate"),
        col("v.age"),
        col("v.gender"),
        col("v.weight"),
        col("v.height"),
        col("v.bmi"),
        col("v.chronic_condition"),
        col("v.smoker"),
        col("v.medication"),
        col("v.stress_level"),
        col("v.sleep_duration"),
        col("v.sleep_quality"),
        col("v.predicted_disease_simulated"),
        coalesce(col("bp.systolic_bp"), lit(120)).alias("systolic_bp"),
        coalesce(col("bp.diastolic_bp"), lit(80)).alias("diastolic_bp"),
        coalesce(col("g.glucose_level"), lit(100.0)).alias("glucose_level"),
        coalesce(col("a.steps"), lit(0)).alias("steps"),
        coalesce(col("a.activity_level"), lit("Unknown")).alias("activity_level"),
        coalesce(col("a.exercise_type"), lit("None")).alias("exercise_type"),
        coalesce(col("a.exercise_intensity"), lit("Low")).alias("exercise_intensity"),
        coalesce(col("f.fall_detected"), lit(False)).alias("fall_detected"),
        coalesce(col("f.skin_temperature"), lit(36.0)).alias("skin_temperature"),
        # Dashboard battery health: worst available source battery in the joined record.
        least(
            coalesce(col("v.battery_level"), lit(100.0)),
            coalesce(col("bp.bp_battery"), lit(100.0)),
            coalesce(col("g.glucose_battery"), lit(100.0)),
            coalesce(col("a.activity_battery"), lit(100.0)),
            coalesce(col("f.fall_battery"), lit(100.0)),
        ).alias("battery_level"),
    )

    # Start streaming query with foreachBatch
    logger.info(f"Result dataframe schema:\n{result.printSchema()}")
    logger.info(f"Using checkpoint location: {CHECKPOINT_LOCATION}")
    logger.info("Starting streaming query with foreachBatch...")
    query = (
        result.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    logger.info("Streaming query started. Waiting for data...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
