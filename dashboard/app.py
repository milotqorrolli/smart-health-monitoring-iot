import os
import time
from datetime import timezone

from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from flask import Flask, jsonify, render_template


CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "smart_health")
PATIENT_IDS = [patient.strip() for patient in os.getenv("PATIENT_IDS", "patient-1,patient-2,patient-3,patient-4,patient-5").split(",")]

app = Flask(__name__)
session = None
latest_statement = None
recent_statement = None


def connect_to_cassandra():
    global session, latest_statement, recent_statement

    while True:
        try:
            cluster = Cluster([CASSANDRA_HOST])
            session = cluster.connect(KEYSPACE)
            session.row_factory = dict_factory
            latest_statement = session.prepare(
                """
                SELECT patient_id, reading_time, heart_rate, spo2, temperature,
                       systolic_bp, diastolic_bp, respiratory_rate, status, processed_at
                FROM sensor_readings
                WHERE patient_id = ?
                LIMIT 1
                """
            )
            recent_statement = session.prepare(
                """
                SELECT patient_id, reading_time, heart_rate, spo2, temperature,
                       systolic_bp, diastolic_bp, respiratory_rate, status, processed_at
                FROM sensor_readings
                LIMIT 50
                """
            )
            return
        except Exception as exc:
            print(f"Cassandra is not ready yet: {exc}. Retrying in 5 seconds...", flush=True)
            time.sleep(5)


def serialize_datetime(value):
    if value is None:
        return ""

    return value.replace(tzinfo=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def get_latest_readings():
    readings = []
    for patient_id in PATIENT_IDS:
        rows = list(session.execute(latest_statement, [patient_id]))
        if not rows:
            continue

        reading = rows[0]
        reading["reading_time"] = serialize_datetime(reading.get("reading_time"))
        reading["processed_at"] = serialize_datetime(reading.get("processed_at"))
        readings.append(reading)

    if readings:
        return readings

    recent_rows = list(session.execute(recent_statement))
    latest_by_patient = {}
    for row in recent_rows:
        patient_id = row["patient_id"]
        existing = latest_by_patient.get(patient_id)
        if existing is None or row["reading_time"] > existing["reading_time"]:
            latest_by_patient[patient_id] = row

    for patient_id in PATIENT_IDS:
        reading = latest_by_patient.get(patient_id)
        if reading is None:
            readings.append({"patient_id": patient_id, "status": "WAITING"})
            continue

        reading["reading_time"] = serialize_datetime(reading.get("reading_time"))
        reading["processed_at"] = serialize_datetime(reading.get("processed_at"))
        readings.append(reading)

    return readings


@app.route("/")
def index():
    return render_template("index.html", readings=get_latest_readings())


@app.route("/api/latest")
def api_latest():
    return jsonify(get_latest_readings())


if __name__ == "__main__":
    connect_to_cassandra()
    app.run(host="0.0.0.0", port=5000)
