"""
Smart Health Monitoring IoT — Flask Dashboard
Displays live patient status, risk scores, anomalies, alerts, and email settings.
"""

import logging
import os
import time
from datetime import timezone

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
    return value.replace(tzinfo=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# =============================================================================
# Dashboard Routes
# =============================================================================

@app.route("/")
def index():
    """Main dashboard page."""
    # Get latest status for all patients
    patients = []
    for pid in PATIENT_IDS:
        rows = safe_query(
            "SELECT * FROM patient_latest_status WHERE patient_id = %s", [pid]
        )
        if rows:
            row = rows[0]
            row["reading_time"] = serialize_datetime(row.get("reading_time"))
            row["processed_at"] = serialize_datetime(row.get("processed_at"))
            patients.append(row)
        else:
            patients.append({"patient_id": pid, "predicted_status": "WAITING"})

    # Get recent alerts
    alerts = safe_query(
        "SELECT * FROM patient_alerts LIMIT 10"
    )
    for a in alerts:
        a["alert_time"] = serialize_datetime(a.get("alert_time"))

    # Get recent readings
    readings = safe_query(
        "SELECT * FROM sensor_readings LIMIT 20"
    )
    for r in readings:
        r["reading_time"] = serialize_datetime(r.get("reading_time"))
        r["processed_at"] = serialize_datetime(r.get("processed_at"))

    return render_template("index.html", patients=patients, alerts=alerts, readings=readings)


@app.route("/api/latest")
def api_latest():
    """API: Get latest status for all patients."""
    patients = []
    for pid in PATIENT_IDS:
        rows = safe_query(
            "SELECT * FROM patient_latest_status WHERE patient_id = %s", [pid]
        )
        if rows:
            row = rows[0]
            row["reading_time"] = serialize_datetime(row.get("reading_time"))
            row["processed_at"] = serialize_datetime(row.get("processed_at"))
            patients.append(row)
    return jsonify(patients)


@app.route("/api/alerts")
def api_alerts():
    """API: Get recent alerts with optional severity filter."""
    severity = request.args.get("severity")
    rows = safe_query("SELECT * FROM patient_alerts LIMIT 20")
    for r in rows:
        r["alert_time"] = serialize_datetime(r.get("alert_time"))
    if severity:
        rows = [r for r in rows if r.get("alert_severity") == severity.upper()]
    return jsonify(rows)


@app.route("/api/readings/<patient_id>")
def api_readings(patient_id):
    """API: Get reading history for a specific patient."""
    rows = safe_query(
        "SELECT * FROM sensor_readings WHERE patient_id = %s LIMIT 50", [patient_id]
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
        notifier = AlertEmailNotifier(config_path=ALERT_CONFIG_PATH)
        success = notifier.test_connection()
        if success:
            return jsonify({"status": "sent"})
        else:
            return jsonify({"status": "failed", "error": "SMTP connection failed or email alerting disabled."})
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
