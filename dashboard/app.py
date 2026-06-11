"""
Smart Health Monitoring IoT - Flask Dashboard

Real-time dashboard for displaying patient health status, alerts, and AI predictions.
Connects to Cassandra database and provides REST APIs for data visualization.
"""

import os
import time
import logging
from datetime import datetime, timezone, timedelta

from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from flask import Flask, jsonify, render_template, request

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "smart_health")
PATIENT_IDS = [
    patient.strip()
    for patient in os.getenv(
        "PATIENT_IDS", "patient-1,patient-2,patient-3,patient-4,patient-5"
    ).split(",")
]

app = Flask(__name__)
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0
session = None


def connect_to_cassandra():
    """Connect to Cassandra cluster with retry logic."""
    global session

    while True:
        try:
            cluster = Cluster([CASSANDRA_HOST])
            session = cluster.connect(KEYSPACE)
            session.row_factory = dict_factory
            logger.info(f"Connected to Cassandra at {CASSANDRA_HOST}")
            return
        except Exception as exc:
            logger.warning(f"Cassandra not ready: {exc}. Retrying in 5 seconds...")
            time.sleep(5)


def serialize_datetime(value):
    """Convert Cassandra datetime to readable string."""
    if value is None:
        return ""
    return value.replace(tzinfo=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def get_patient_latest_status(patient_id: str) -> dict:
    """Get the latest status for a patient."""
    try:
        query = """
            SELECT patient_id, reading_time, age, gender, predicted_status, risk_score, 
                   risk_level, is_anomaly, anomaly_score, alert_type, alert_severity, 
                   alert_message, heart_rate, spo2, temperature, systolic_bp, diastolic_bp,
                   respiratory_rate, glucose_level, battery_level, chronic_condition, smoker,
                   medication, predicted_next_heart_rate, processed_at
            FROM sensor_readings
            WHERE patient_id = %s
            LIMIT 1
        """
        rows = list(session.execute(query, [patient_id]))
        if rows:
            reading = rows[0]
            reading["reading_time"] = serialize_datetime(reading.get("reading_time"))
            reading["processed_at"] = serialize_datetime(reading.get("processed_at"))
            return reading
        return {"patient_id": patient_id, "status": "NO_DATA"}
    except Exception as e:
        logger.error(f"Error fetching latest status for {patient_id}: {e}")
        return {"patient_id": patient_id, "status": "ERROR"}


def get_all_latest_statuses() -> list:
    """Get latest status for all patients."""
    readings = []
    for patient_id in PATIENT_IDS:
        reading = get_patient_latest_status(patient_id)
        if reading:
            readings.append(reading)
    return readings


def get_recent_alerts(limit: int = 50) -> list:
    """Get recent critical alerts."""
    try:
        query = """
            SELECT patient_id, alert_time, alert_type, alert_severity, alert_message,
                   predicted_status, risk_score, is_anomaly, fall_detected, heart_rate,
                   spo2, temperature, systolic_bp, diastolic_bp, respiratory_rate, glucose_level,
                   battery_level, processed_at
            FROM patient_alerts
            LIMIT %s
        """
        rows = list(session.execute(query, [limit]))
        for row in rows:
            row["alert_time"] = serialize_datetime(row.get("alert_time"))
            row["processed_at"] = serialize_datetime(row.get("processed_at"))
        return sorted(rows, key=lambda x: x.get("alert_time", ""), reverse=True)
    except Exception as e:
        logger.error(f"Error fetching alerts: {e}")
        return []


def get_patient_recent_readings(patient_id: str, limit: int = 20) -> list:
    """Get recent readings for a patient."""
    try:
        query = """
            SELECT patient_id, reading_time, heart_rate, spo2, temperature, systolic_bp,
                   diastolic_bp, respiratory_rate, glucose_level, predicted_status, risk_score,
                   is_anomaly, alert_type, alert_severity, battery_level, processed_at
            FROM sensor_readings
            WHERE patient_id = %s
            LIMIT %s
        """
        rows = list(session.execute(query, [patient_id, limit]))
        for row in rows:
            row["reading_time"] = serialize_datetime(row.get("reading_time"))
            row["processed_at"] = serialize_datetime(row.get("processed_at"))
        return sorted(rows, key=lambda x: x.get("reading_time", ""), reverse=True)
    except Exception as e:
        logger.error(f"Error fetching readings for {patient_id}: {e}")
        return []


def get_critical_alerts(hours: int = 1) -> list:
    """Get critical alerts from the last N hours."""
    try:
        query = """
            SELECT patient_id, alert_time, alert_type, alert_severity, alert_message
            FROM patient_alerts
            WHERE alert_severity IN ('HIGH', 'CRITICAL')
            LIMIT 100
        """
        rows = list(session.execute(query))
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        filtered = []
        for row in rows:
            alert_time = row.get("alert_time")
            if alert_time and alert_time.replace(tzinfo=timezone.utc) > cutoff_time:
                row["alert_time"] = serialize_datetime(alert_time)
                filtered.append(row)
        return sorted(filtered, key=lambda x: x.get("alert_time", ""), reverse=True)
    except Exception as e:
        logger.error(f"Error fetching critical alerts: {e}")
        return []


@app.route("/")
def index():
    """Main dashboard page."""
    readings = get_all_latest_statuses()
    alerts = get_recent_alerts(10)
    return render_template("index.html", readings=readings, alerts=alerts)


@app.route("/api/latest")
def api_latest():
    """Get latest status for all patients."""
    return jsonify(get_all_latest_statuses())


@app.route("/api/patient/<patient_id>/latest")
def api_patient_latest(patient_id):
    """Get latest status for a specific patient."""
    reading = get_patient_latest_status(patient_id)
    if reading:
        return jsonify(reading)
    return jsonify({"error": "Patient not found"}), 404


@app.route("/api/patient/<patient_id>/readings")
def api_patient_readings(patient_id):
    """Get recent readings for a patient."""
    limit = request.args.get("limit", 20, type=int)
    readings = get_patient_recent_readings(patient_id, min(limit, 100))
    return jsonify(readings)


@app.route("/api/alerts")
def api_alerts():
    """Get recent alerts."""
    limit = request.args.get("limit", 50, type=int)
    alerts = get_recent_alerts(min(limit, 200))
    return jsonify(alerts)


@app.route("/api/alerts/critical")
def api_critical_alerts():
    """Get critical alerts from the last hour."""
    hours = request.args.get("hours", 1, type=int)
    alerts = get_critical_alerts(min(hours, 24))
    return jsonify(alerts)


@app.route("/api/health")
def api_health():
    """Health check endpoint."""
    try:
        session.execute("SELECT * FROM sensor_readings LIMIT 1")
        return jsonify({"status": "healthy", "database": "connected"}), 200
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({"status": "unhealthy", "database": "disconnected", "error": str(e)}), 503


@app.route("/api/stats")
def api_stats():
    """Get system statistics."""
    try:
        # Count total patients with data
        patients_with_data = 0
        total_readings = 0
        critical_patients = 0

        for patient_id in PATIENT_IDS:
            latest = get_patient_latest_status(patient_id)
            if latest and latest.get("status") != "NO_DATA" and latest.get("status") != "ERROR":
                patients_with_data += 1
                if latest.get("predicted_status") == "EMERGENCY":
                    critical_patients += 1

        # Get alert counts
        critical_alerts = get_critical_alerts(1)

        return jsonify({
            "patients_monitored": len(PATIENT_IDS),
            "patients_with_data": patients_with_data,
            "critical_patients": critical_patients,
            "recent_critical_alerts": len(critical_alerts),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    connect_to_cassandra()
    logger.info("Dashboard started on http://0.0.0.0:5000")
    app.run(host="0.0.0.0", port=5000, debug=False)
    return jsonify(get_latest_readings())


if __name__ == "__main__":
    connect_to_cassandra()
    app.run(host="0.0.0.0", port=5000)
