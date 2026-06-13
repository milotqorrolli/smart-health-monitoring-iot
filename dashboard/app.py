"""
Smart Health Monitoring IoT — Flask Dashboard
Displays live patient status, risk scores, anomalies, alerts, and email settings.
"""

import logging
import os
import time
from datetime import datetime, timezone

import yaml
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from flask import Flask, flash, jsonify, redirect, render_template, request, url_for

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "smart_health")
PATIENT_IDS = [p.strip() for p in os.getenv("PATIENT_IDS", "patient-1,patient-2,patient-3,patient-4,patient-5").split(",")]
ALERT_CONFIG_PATH = os.getenv("ALERT_CONFIG_PATH", "/app/alerts/alert_config.yml")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Dashboard")

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "smart-health-secret-key-2024")

session = None


def connect_to_cassandra():
    """Connect to Cassandra with retries."""
    global session
    while True:
        try:
            cluster = Cluster([CASSANDRA_HOST])
            session = cluster.connect(KEYSPACE)
            session.row_factory = dict_factory
            logger.info(f"Connected to Cassandra at {CASSANDRA_HOST}/{KEYSPACE}")
            return
        except Exception as exc:
            logger.warning(f"Cassandra not ready: {exc}. Retrying in 5s...")
            time.sleep(5)


def safe_query(query, params=None):
    """Execute Cassandra query safely. Returns empty list on failure."""
    global session
    try:
        if session is None:
            return []
        if params:
            return list(session.execute(query, params))
        return list(session.execute(query))
    except Exception as e:
        logger.warning(f"Query failed: {e}")
        return []


def serialize_datetime(value):
    """Format datetime for display."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return value.replace(tzinfo=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def serialize_row_datetimes(row, fields):
    """Format selected datetime fields in a Cassandra row dict."""
    for field in fields:
        if field in row:
            row[field] = serialize_datetime(row.get(field))
    return row


def safe_count(table):
    """Return a small demo-friendly table count, or 0 when unavailable."""
    rows = safe_query(f"SELECT COUNT(*) AS count FROM {table}")
    if not rows:
        return 0
    return rows[0].get("count", 0)


def latest_for_patient(patient_id):
    """Fetch one latest-status row for a patient."""
    rows = safe_query(
        "SELECT * FROM patient_latest_status WHERE patient_id = %s", [patient_id]
    )
    if not rows:
        return None
    return serialize_row_datetimes(rows[0], ["reading_time", "processed_at"])


def recent_partitioned_rows(table, time_field, limit=20):
    """Read recent rows from patient-partitioned tables and sort globally."""
    rows = []
    per_patient_limit = max(1, limit)
    for pid in PATIENT_IDS:
        rows.extend(
            safe_query(
                f"SELECT * FROM {table} WHERE patient_id = %s LIMIT {per_patient_limit}",
                [pid],
            )
        )
    rows.sort(key=lambda row: row.get(time_field) or datetime.min, reverse=True)
    return rows[:limit]


# =============================================================================
# Dashboard Routes
# =============================================================================

@app.route("/")
def index():
    """Main dashboard page."""
    # Get latest status for all patients
    patients = []
    for pid in PATIENT_IDS:
        row = latest_for_patient(pid)
        if row:
            patients.append(row)
        else:
            patients.append({"patient_id": pid, "predicted_status": "WAITING"})

    # Get recent alerts
    alerts = recent_partitioned_rows("patient_alerts", "alert_time", limit=10)
    for a in alerts:
        a["alert_time"] = serialize_datetime(a.get("alert_time"))

    # Get recent readings
    readings = recent_partitioned_rows("sensor_readings", "reading_time", limit=20)
    for r in readings:
        r["reading_time"] = serialize_datetime(r.get("reading_time"))
        r["processed_at"] = serialize_datetime(r.get("processed_at"))

    sensor_metadata = safe_query("SELECT * FROM sensor_metadata LIMIT 25")
    for sensor in sensor_metadata:
        serialize_row_datetimes(sensor, ["last_reading_time", "last_seen_at"])

    return render_template(
        "index.html",
        patients=patients,
        alerts=alerts,
        readings=readings,
        sensor_metadata=sensor_metadata,
    )


@app.route("/api/latest")
def api_latest():
    """API: Get latest status for all patients."""
    patients = []
    for pid in PATIENT_IDS:
        row = latest_for_patient(pid)
        if row:
            patients.append(row)
    return jsonify(patients)


@app.route("/api/health")
def api_health():
    """API: Lightweight service health check for validation and demos."""
    cassandra_ready = session is not None
    return jsonify({
        "status": "ok" if cassandra_ready else "degraded",
        "service": "smart-health-dashboard",
        "cassandra_connected": cassandra_ready,
        "cassandra_host": CASSANDRA_HOST,
        "keyspace": KEYSPACE,
        "patients_configured": PATIENT_IDS,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


@app.route("/api/stats")
def api_stats():
    """API: Demo statistics for data, alerts, sensors, and latest statuses."""
    latest = [row for row in (latest_for_patient(pid) for pid in PATIENT_IDS) if row]
    high_risk = [
        row for row in latest
        if row.get("alert_severity") in ("HIGH", "CRITICAL")
        or row.get("risk_level") in ("HIGH", "CRITICAL")
    ]
    return jsonify({
        "patients_configured": len(PATIENT_IDS),
        "patients_with_latest_status": len(latest),
        "high_risk_patients": len(high_risk),
        "sensor_readings_count": safe_count("sensor_readings"),
        "patient_alerts_count": safe_count("patient_alerts"),
        "sensor_metadata_count": safe_count("sensor_metadata"),
        "email_alert_log_count": safe_count("email_alert_log"),
    })


@app.route("/api/sensors")
def api_sensors():
    """API: Get latest metadata for simulated sensors."""
    rows = safe_query("SELECT * FROM sensor_metadata LIMIT 100")
    for row in rows:
        serialize_row_datetimes(row, ["last_reading_time", "last_seen_at"])
    return jsonify(rows)


@app.route("/api/metrics/<patient_id>")
def api_metrics(patient_id):
    """API: Get one-minute aggregate metrics for a patient."""
    limit = int(request.args.get("limit", 20))
    rows = safe_query(
        "SELECT * FROM patient_minute_metrics WHERE patient_id = %s LIMIT %s",
        [patient_id, limit],
    )
    for row in rows:
        serialize_row_datetimes(row, ["window_start"])
    return jsonify(rows)


@app.route("/api/patient/<patient_id>/latest")
def api_patient_latest(patient_id):
    """API compatibility route: latest status for one patient."""
    row = latest_for_patient(patient_id)
    return jsonify(row or {})


@app.route("/api/patient/<patient_id>/readings")
def api_patient_readings(patient_id):
    """API compatibility route: reading history for one patient."""
    return api_readings(patient_id)


@app.route("/api/alerts")
def api_alerts():
    """API: Get recent alerts with optional severity filter."""
    severity = request.args.get("severity")
    limit = int(request.args.get("limit", 20))
    rows = recent_partitioned_rows("patient_alerts", "alert_time", limit=limit)
    for r in rows:
        r["alert_time"] = serialize_datetime(r.get("alert_time"))
    if severity:
        rows = [r for r in rows if r.get("alert_severity") == severity.upper()]
    return jsonify(rows)


@app.route("/api/alerts/critical")
def api_critical_alerts():
    """API compatibility route: recent high and critical alerts."""
    limit = int(request.args.get("limit", 20))
    rows = recent_partitioned_rows("patient_alerts", "alert_time", limit=limit)
    rows = [r for r in rows if r.get("alert_severity") in ("HIGH", "CRITICAL")]
    for r in rows:
        r["alert_time"] = serialize_datetime(r.get("alert_time"))
    return jsonify(rows)


@app.route("/api/readings/<patient_id>")
def api_readings(patient_id):
    """API: Get reading history for a specific patient."""
    limit = int(request.args.get("limit", 50))
    rows = safe_query(
        "SELECT * FROM sensor_readings WHERE patient_id = %s LIMIT %s", [patient_id, limit]
    )
    for r in rows:
        r["reading_time"] = serialize_datetime(r.get("reading_time"))
        r["processed_at"] = serialize_datetime(r.get("processed_at"))
    return jsonify(rows)


@app.route("/api/alerts/email-log")
def api_email_log():
    """API: Get email alert log entries."""
    rows = safe_query("SELECT * FROM email_alert_log LIMIT 20")
    for r in rows:
        r["sent_at"] = serialize_datetime(r.get("sent_at"))
    return jsonify(rows)


@app.route("/api/alerts/test-email", methods=["POST"])
def api_test_email():
    """API: Send test email."""
    try:
        from alerts.email_notifier import AlertEmailNotifier
        notifier = AlertEmailNotifier(config_path=ALERT_CONFIG_PATH, cassandra_session=session)
        recipients = notifier.send_test_email()
        return jsonify({"status": "sent", "recipients": recipients})
    except Exception as e:
        return jsonify({"status": "failed", "error": str(e)})


@app.route("/settings/alerts", methods=["GET", "POST"])
def settings_alerts():
    """Email alert settings page."""
    if request.method == "POST":
        # Save settings
        try:
            config = _load_alert_config()
            config.setdefault("email", {})
            config["email"]["smtp_host"] = request.form.get("smtp_host", "smtp.gmail.com")
            config["email"]["smtp_port"] = int(request.form.get("smtp_port", 587))
            config["email"]["smtp_user"] = request.form.get("smtp_user", "")
            smtp_pass = request.form.get("smtp_password", "")
            if smtp_pass:  # Only update password if provided
                config["email"]["smtp_password"] = smtp_pass

            config.setdefault("doctors", [{}])
            if config["doctors"]:
                config["doctors"][0]["name"] = request.form.get("doctor_name", "Doctor")
                config["doctors"][0]["email"] = request.form.get("doctor_email", "")
                min_sev = request.form.get("min_severity", "HIGH")
                config["doctors"][0]["receives_severity"] = [s.strip() for s in min_sev.split(",")]

            config.setdefault("rate_limiting", {})
            config["rate_limiting"]["cooldown_minutes"] = int(request.form.get("cooldown_minutes", 5))

            config.setdefault("thresholds", {})
            config["thresholds"]["send_on_fall_detected"] = "send_on_fall" in request.form
            config["thresholds"]["send_on_anomaly"] = "send_on_anomaly" in request.form
            config["thresholds"]["minimum_risk_score"] = int(request.form.get("min_risk_score", 70))

            _save_alert_config(config)
            flash("Alert settings saved successfully!", "success")
        except Exception as e:
            flash(f"Error saving settings: {e}", "error")

        return redirect(url_for("settings_alerts"))

    # GET: load current config
    config = _load_alert_config()
    email_log = safe_query("SELECT * FROM email_alert_log LIMIT 5")
    for r in email_log:
        r["sent_at"] = serialize_datetime(r.get("sent_at"))

    return render_template("alert_settings.html", config=config, email_log=email_log)


def _load_alert_config():
    """Load alert config from YAML file."""
    if os.path.exists(ALERT_CONFIG_PATH):
        try:
            with open(ALERT_CONFIG_PATH) as f:
                return yaml.safe_load(f) or {}
        except Exception:
            pass
    return {
        "email": {"smtp_host": "smtp.gmail.com", "smtp_port": 587, "smtp_user": "", "smtp_password": ""},
        "doctors": [{"name": "Doctor", "email": "", "receives_severity": ["CRITICAL", "HIGH"]}],
        "rate_limiting": {"cooldown_minutes": 5},
        "thresholds": {"send_on_fall_detected": True, "send_on_anomaly": False, "minimum_risk_score": 70},
    }


def _save_alert_config(config):
    """Save alert config to YAML file."""
    os.makedirs(os.path.dirname(ALERT_CONFIG_PATH), exist_ok=True)
    with open(ALERT_CONFIG_PATH, "w") as f:
        yaml.dump(config, f, default_flow_style=False)


if __name__ == "__main__":
    connect_to_cassandra()
    app.run(host="0.0.0.0", port=5000)
