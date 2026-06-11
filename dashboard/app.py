import os
import time
from datetime import datetime, timezone
from typing import Any

from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from flask import Flask, jsonify, render_template


CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "smart_health")
PATIENT_IDS = [
    patient.strip()
    for patient in os.getenv("PATIENT_IDS", "patient-1,patient-2,patient-3,patient-4,patient-5").split(",")
    if patient.strip()
]

app = Flask(__name__)
cluster = None
session = None
statements: dict[str, Any] = {}
last_connection_error = ""


def serialize_value(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    return value


def serialize_row(row: dict) -> dict:
    return {key: serialize_value(value) for key, value in row.items()}


def waiting_latest() -> list[dict]:
    return [
        {
            "patient_id": patient_id,
            "predicted_status": "WAITING",
            "risk_score": None,
            "risk_level": "WAITING",
            "is_anomaly": None,
            "anomaly_score": None,
            "predicted_next_heart_rate": None,
            "alert_type": "NO_ALERT",
            "alert_severity": "NONE",
            "alert_message": "Waiting for Cassandra and streaming data.",
        }
        for patient_id in PATIENT_IDS
    ]


def connect_to_cassandra() -> bool:
    global cluster, session, statements, last_connection_error

    if session is not None:
        return True

    try:
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(KEYSPACE)
        session.row_factory = dict_factory
        statements = {
            "latest": session.prepare(
                """
                SELECT patient_id, reading_time, predicted_status, risk_score, risk_level,
                       is_anomaly, anomaly_score, predicted_next_heart_rate,
                       alert_type, alert_severity, alert_message,
                       heart_rate, spo2, temperature, systolic_bp, diastolic_bp,
                       respiratory_rate, glucose_level, battery_level, processed_at
                FROM patient_latest_status
                WHERE patient_id = ?
                """
            ),
            "alerts": session.prepare(
                """
                SELECT patient_id, alert_time, alert_type, alert_severity, alert_message,
                       predicted_status, risk_score, risk_level, is_anomaly, anomaly_score,
                       predicted_next_heart_rate, fall_detected,
                       heart_rate, spo2, temperature, systolic_bp, diastolic_bp,
                       respiratory_rate, processed_at
                FROM patient_alerts
                WHERE patient_id = ?
                LIMIT 10
                """
            ),
            "readings": session.prepare(
                """
                SELECT patient_id, reading_time, predicted_status, risk_score, risk_level,
                       is_anomaly, anomaly_score, anomaly_type, predicted_next_heart_rate,
                       alert_type, alert_severity, alert_message, heart_rate, spo2,
                       temperature, systolic_bp, diastolic_bp, respiratory_rate,
                       glucose_level, battery_level, processed_at
                FROM sensor_readings
                WHERE patient_id = ?
                LIMIT 25
                """
            ),
        }
        last_connection_error = ""
        return True
    except Exception as exc:
        session = None
        last_connection_error = str(exc)
        print(f"Cassandra unavailable: {exc}", flush=True)
        time.sleep(0.2)
        return False


def safe_execute(statement_name: str, values: list[Any]) -> list[dict]:
    global session
    if not connect_to_cassandra():
        return []
    try:
        return [serialize_row(row) for row in session.execute(statements[statement_name], values)]
    except Exception as exc:
        print(f"Cassandra query failed: {exc}", flush=True)
        session = None
        return []


def get_latest_status() -> list[dict]:
    readings = []
    for patient_id in PATIENT_IDS:
        rows = safe_execute("latest", [patient_id])
        if rows:
            readings.append(rows[0])
        else:
            readings.append(
                {
                    "patient_id": patient_id,
                    "predicted_status": "WAITING",
                    "risk_score": None,
                    "risk_level": "WAITING",
                    "is_anomaly": None,
                    "anomaly_score": None,
                    "predicted_next_heart_rate": None,
                    "alert_type": "NO_ALERT",
                    "alert_severity": "NONE",
                    "alert_message": "Waiting for first reading.",
                }
            )
    return readings


def get_recent_alerts(limit: int = 20) -> list[dict]:
    alerts: list[dict] = []
    for patient_id in PATIENT_IDS:
        alerts.extend(safe_execute("alerts", [patient_id]))
    alerts.sort(key=lambda row: row.get("alert_time") or "", reverse=True)
    return alerts[:limit]


def get_recent_readings(patient_id: str | None = None, limit: int = 25) -> list[dict]:
    patient_ids = [patient_id] if patient_id else PATIENT_IDS
    readings: list[dict] = []
    for current_patient_id in patient_ids:
        readings.extend(safe_execute("readings", [current_patient_id]))
    readings.sort(key=lambda row: row.get("reading_time") or "", reverse=True)
    return readings[:limit]


@app.route("/")
def index():
    latest = get_latest_status()
    if not latest and last_connection_error:
        latest = waiting_latest()
    return render_template(
        "index.html",
        latest=latest,
        alerts=get_recent_alerts(),
        readings=get_recent_readings(limit=30),
        cassandra_error=last_connection_error,
    )


@app.route("/api/latest")
def api_latest():
    return jsonify(get_latest_status())


@app.route("/api/alerts")
def api_alerts():
    return jsonify(get_recent_alerts())


@app.route("/api/readings/<patient_id>")
def api_readings(patient_id):
    return jsonify(get_recent_readings(patient_id=patient_id, limit=50))


if __name__ == "__main__":
    connect_to_cassandra()
    app.run(host="0.0.0.0", port=5000)
