"""
Email Alert Notification System for Smart Health Monitoring IoT System.
Sends automated HTML email alerts to configured doctors when critical health events occur.
"""

import logging
import os
import smtplib
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import yaml

logger = logging.getLogger(__name__)


class AlertEmailNotifier:
    """Sends automated email alerts for critical patient health events."""

    def __init__(self, config_path=None, cassandra_session=None):
        self.cassandra_session = cassandra_session
        self.rate_limit_tracker = {}  # (patient_id, alert_type) -> last_sent_datetime
        self.hourly_counter = 0
        self.hourly_reset_time = datetime.utcnow()
        self.enabled = False

        # Load configuration
        self.config = self._load_config(config_path)
        self._apply_env_overrides()

        # Check if email is enabled
        if self.config.get("email", {}).get("smtp_user"):
            self.enabled = True
            logger.info("Email alerting is ENABLED.")
        else:
            logger.info("Email alerting is disabled. Set ALERT_SMTP_USER to enable.")

    def _load_config(self, config_path):
        """Load configuration from YAML file."""
        if config_path and os.path.exists(config_path):
            try:
                with open(config_path, "r") as f:
                    return yaml.safe_load(f) or {}
            except Exception as e:
                logger.warning(f"Could not load config from {config_path}: {e}")
        return {
            "email": {"smtp_host": "smtp.gmail.com", "smtp_port": 587, "smtp_user": "",
                      "smtp_password": "", "sender_name": "Smart Health Monitor", "use_tls": True},
            "doctors": [{"name": "Doctor", "email": "", "receives_severity": ["CRITICAL", "HIGH"],
                         "patients": "all"}],
            "rate_limiting": {"enabled": True, "cooldown_minutes": 5, "max_emails_per_hour": 20},
            "thresholds": {"send_on_fall_detected": True, "send_on_anomaly": False,
                           "minimum_risk_score": 70},
        }

    def _apply_env_overrides(self):
        """Override config values with environment variables."""
        email_cfg = self.config.setdefault("email", {})
        email_cfg["smtp_host"] = os.getenv("ALERT_SMTP_HOST", email_cfg.get("smtp_host", "smtp.gmail.com"))
        email_cfg["smtp_port"] = int(os.getenv("ALERT_SMTP_PORT", email_cfg.get("smtp_port", 587)))
        email_cfg["smtp_user"] = os.getenv("ALERT_SMTP_USER", email_cfg.get("smtp_user", ""))
        email_cfg["smtp_password"] = os.getenv("ALERT_SMTP_PASSWORD", email_cfg.get("smtp_password", ""))

        doctor_email = os.getenv("ALERT_DOCTOR_EMAIL")
        doctor_name = os.getenv("ALERT_DOCTOR_NAME")
        if doctor_email:
            doctors = self.config.setdefault("doctors", [{}])
            if doctors:
                doctors[0]["email"] = doctor_email
                if doctor_name:
                    doctors[0]["name"] = doctor_name

        min_severity = os.getenv("ALERT_MIN_SEVERITY")
        if min_severity:
            doctors = self.config.get("doctors", [{}])
            severity_list = [s.strip() for s in min_severity.split(",")]
            if doctors:
                doctors[0]["receives_severity"] = severity_list

        cooldown = os.getenv("ALERT_COOLDOWN_MINUTES")
        if cooldown:
            self.config.setdefault("rate_limiting", {})["cooldown_minutes"] = int(cooldown)

        self.dashboard_url = os.getenv("DASHBOARD_BASE_URL", "http://localhost:5000")

    def should_send(self, patient_id, alert_type, alert_severity, risk_score):
        """Determine if an email should be sent based on rules and rate limiting."""
        if not self.enabled:
            return False

        # Check severity against doctor config
        doctors = self.config.get("doctors", [])
        if not doctors:
            return False

        doctor = doctors[0]
        allowed_severities = doctor.get("receives_severity", ["CRITICAL", "HIGH"])
        if alert_severity not in allowed_severities:
            return False

        # Check minimum risk score
        min_risk = self.config.get("thresholds", {}).get("minimum_risk_score", 70)
        if risk_score is not None and risk_score < min_risk:
            # Exception: always send for falls if configured
            send_on_fall = self.config.get("thresholds", {}).get("send_on_fall_detected", True)
            if not (send_on_fall and alert_type == "FALL_DETECTED"):
                return False

        # Rate limiting: check cooldown
        rate_cfg = self.config.get("rate_limiting", {})
        if rate_cfg.get("enabled", True):
            cooldown = rate_cfg.get("cooldown_minutes", 5)
            key = (patient_id, alert_type)
            last_sent = self.rate_limit_tracker.get(key)
            if last_sent and (datetime.utcnow() - last_sent) < timedelta(minutes=cooldown):
                return False

            # Check hourly limit
            max_per_hour = rate_cfg.get("max_emails_per_hour", 20)
            if (datetime.utcnow() - self.hourly_reset_time) > timedelta(hours=1):
                self.hourly_counter = 0
                self.hourly_reset_time = datetime.utcnow()
            if self.hourly_counter >= max_per_hour:
                return False

        return True

    def build_subject(self, record):
        """Build email subject line."""
        severity = record.get("alert_severity", "UNKNOWN")
        patient_id = record.get("patient_id", "unknown")
        alert_type = record.get("alert_type", "ALERT")
        return f"[Smart Health Monitor] {severity} Alert — {patient_id} — {alert_type}"

    def _get_vital_color(self, vital_name, value):
        """Get color for a vital sign value based on thresholds."""
        from ml.model_utils import derive_vital_status
        try:
            status = derive_vital_status(vital_name, float(value))
        except (ValueError, TypeError):
            return "#6c757d"
        colors = {"NORMAL": "#28a745", "WARNING": "#fd7e14", "CRITICAL": "#dc3545", "EMERGENCY": "#7b0000"}
        return colors.get(status, "#6c757d")

    def build_email_body(self, record):
        """Build fully rendered HTML email body."""
        severity = record.get("alert_severity", "UNKNOWN")
        alert_type = record.get("alert_type", "UNKNOWN")
        patient_id = record.get("patient_id", "unknown")
        timestamp = record.get("reading_time") or record.get("timestamp", "N/A")
        alert_message = record.get("alert_message", "")

        # Header color
        header_colors = {
            "CRITICAL": "#7b0000", "EMERGENCY": "#7b0000",
            "HIGH": "#dc3545", "MEDIUM": "#fd7e14", "LOW": "#ffc107"
        }
        header_color = header_colors.get(severity, "#6c757d")

        # Vital signs
        hr = record.get("heart_rate", "N/A")
        spo2 = record.get("spo2", "N/A")
        temp = record.get("temperature", "N/A")
        sys_bp = record.get("systolic_bp", "N/A")
        dia_bp = record.get("diastolic_bp", "N/A")
        rr = record.get("respiratory_rate", "N/A")
        glucose = record.get("glucose_level", "N/A")

        # AI predictions
        predicted_status = record.get("predicted_status", "N/A")
        risk_score = record.get("risk_score", "N/A")
        is_anomaly = record.get("is_anomaly", False)
        predicted_hr = record.get("predicted_next_heart_rate", "N/A")
        fall_detected = record.get("fall_detected", False)

        # Patient profile
        age = record.get("age", "N/A")
        gender = record.get("gender", "N/A")
        chronic = record.get("chronic_condition", "N/A")
        smoker = record.get("smoker", "N/A")
        medication = record.get("medication", "N/A")

        processed_at = record.get("processed_at", datetime.utcnow().isoformat())

        html = f"""
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5;">
<div style="max-width: 600px; margin: 0 auto; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">

<!-- Header Banner -->
<div style="background: {header_color}; color: white; padding: 20px; text-align: center;">
    <h1 style="margin: 0; font-size: 22px;">⚠️ HEALTH ALERT — {severity}</h1>
    <p style="margin: 5px 0 0; opacity: 0.9;">Smart Health Monitoring IoT System</p>
</div>

<!-- Alert Summary -->
<div style="padding: 20px; border-bottom: 1px solid #eee;">
    <h2 style="margin: 0 0 15px; font-size: 18px;">Alert Summary</h2>
    <table style="width: 100%; border-collapse: collapse;">
        <tr><td style="padding: 8px 0;"><strong>⚠️ Alert Type:</strong></td><td>{alert_type}</td></tr>
        <tr><td style="padding: 8px 0;"><strong>🔴 Severity:</strong></td><td><span style="background:{header_color}; color:white; padding:3px 8px; border-radius:4px;">{severity}</span></td></tr>
        <tr><td style="padding: 8px 0;"><strong>👤 Patient:</strong></td><td>{patient_id}</td></tr>
        <tr><td style="padding: 8px 0;"><strong>🕐 Time:</strong></td><td>{timestamp}</td></tr>
        <tr><td style="padding: 8px 0;"><strong>💬 Message:</strong></td><td>{alert_message}</td></tr>
    </table>
</div>

<!-- Vital Signs Table -->
<div style="padding: 20px; border-bottom: 1px solid #eee;">
    <h2 style="margin: 0 0 15px; font-size: 18px;">Vital Signs</h2>
    <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
        <tr style="background: #f8f9fa;"><th style="padding: 10px; text-align: left;">Vital</th><th style="padding: 10px; text-align: right;">Value</th></tr>
        <tr><td style="padding: 10px; border-bottom: 1px solid #eee;">❤️ Heart Rate</td><td style="padding: 10px; text-align: right; border-bottom: 1px solid #eee;">{hr} bpm</td></tr>
        <tr><td style="padding: 10px; border-bottom: 1px solid #eee;">🫁 SpO2</td><td style="padding: 10px; text-align: right; border-bottom: 1px solid #eee;">{spo2}%</td></tr>
        <tr><td style="padding: 10px; border-bottom: 1px solid #eee;">🌡️ Temperature</td><td style="padding: 10px; text-align: right; border-bottom: 1px solid #eee;">{temp} °C</td></tr>
        <tr><td style="padding: 10px; border-bottom: 1px solid #eee;">💉 Blood Pressure</td><td style="padding: 10px; text-align: right; border-bottom: 1px solid #eee;">{sys_bp}/{dia_bp} mmHg</td></tr>
        <tr><td style="padding: 10px; border-bottom: 1px solid #eee;">🌬️ Respiratory Rate</td><td style="padding: 10px; text-align: right; border-bottom: 1px solid #eee;">{rr} /min</td></tr>
        <tr><td style="padding: 10px;">🍬 Glucose</td><td style="padding: 10px; text-align: right;">{glucose} mg/dL</td></tr>
    </table>
</div>

<!-- AI Prediction Summary -->
<div style="padding: 20px; border-bottom: 1px solid #eee;">
    <h2 style="margin: 0 0 15px; font-size: 18px;">AI Prediction Summary</h2>
    <table style="width: 100%; border-collapse: collapse;">
        <tr><td style="padding: 8px 0;"><strong>🤖 AI Predicted Status:</strong></td><td>{predicted_status}</td></tr>
        <tr><td style="padding: 8px 0;"><strong>📊 Risk Score:</strong></td><td>{risk_score} / 100</td></tr>
        <tr><td style="padding: 8px 0;"><strong>🔬 Anomaly Detected:</strong></td><td>{"Yes ⚠️" if is_anomaly else "No"}</td></tr>
        <tr><td style="padding: 8px 0;"><strong>💓 Next Heart Rate (predicted):</strong></td><td>{predicted_hr} bpm</td></tr>
    </table>
</div>

{"<div style='padding: 20px; background: #fff3cd; border-bottom: 1px solid #eee;'><h2 style='margin: 0; color: #856404;'>🚨 FALL DETECTED</h2><p style='margin: 10px 0 0;'>Patient may require immediate physical check.</p></div>" if fall_detected else ""}

<!-- Patient Profile -->
<div style="padding: 20px; border-bottom: 1px solid #eee;">
    <h2 style="margin: 0 0 15px; font-size: 18px;">Patient Profile</h2>
    <table style="width: 100%; border-collapse: collapse;">
        <tr><td style="padding: 5px 0;"><strong>Age:</strong></td><td>{age}</td></tr>
        <tr><td style="padding: 5px 0;"><strong>Gender:</strong></td><td>{gender}</td></tr>
        <tr><td style="padding: 5px 0;"><strong>Chronic Condition:</strong></td><td>{chronic}</td></tr>
        <tr><td style="padding: 5px 0;"><strong>Smoker:</strong></td><td>{smoker}</td></tr>
        <tr><td style="padding: 5px 0;"><strong>Medication:</strong></td><td>{medication}</td></tr>
    </table>
</div>

<!-- Dashboard Button -->
<div style="padding: 20px; text-align: center;">
    <a href="{self.dashboard_url}" style="display: inline-block; background: #007bff; color: white; padding: 12px 30px; border-radius: 5px; text-decoration: none; font-weight: bold;">View Patient Dashboard →</a>
</div>

<!-- Footer -->
<div style="padding: 20px; background: #f8f9fa; text-align: center; font-size: 12px; color: #6c757d;">
    <p style="margin: 5px 0;"><strong>Smart Health Monitoring IoT System</strong></p>
    <p style="margin: 5px 0;">University Project — Educational Simulation Only</p>
    <p style="margin: 5px 0; color: #dc3545;"><strong>⚠️ This is NOT a certified medical device.</strong></p>
    <p style="margin: 5px 0;">Sent at: {processed_at} UTC</p>
</div>

</div>
</body>
</html>
"""
        return html

    def send_alert_email(self, record):
        """Send an alert email for a patient event. Returns True if sent successfully."""
        patient_id = record.get("patient_id", "unknown")
        alert_type = record.get("alert_type", "UNKNOWN")
        alert_severity = record.get("alert_severity", "NONE")
        risk_score = record.get("risk_score", 0)

        if not self.should_send(patient_id, alert_type, alert_severity, risk_score):
            return False

        try:
            subject = self.build_subject(record)
            html_body = self.build_email_body(record)

            email_cfg = self.config.get("email", {})
            smtp_host = email_cfg.get("smtp_host", "smtp.gmail.com")
            smtp_port = email_cfg.get("smtp_port", 587)
            smtp_user = email_cfg.get("smtp_user", "")
            smtp_password = email_cfg.get("smtp_password", "")
            sender_name = email_cfg.get("sender_name", "Smart Health Monitor")
            use_tls = email_cfg.get("use_tls", True)

            doctors = self.config.get("doctors", [])

            for doctor in doctors:
                recipient = doctor.get("email", "")
                if not recipient:
                    continue

                msg = MIMEMultipart("alternative")
                msg["Subject"] = subject
                msg["From"] = f"{sender_name} <{smtp_user}>"
                msg["To"] = recipient
                msg.attach(MIMEText(html_body, "html"))

                with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
                    if use_tls:
                        server.starttls()
                    if smtp_user and smtp_password:
                        server.login(smtp_user, smtp_password)
                    server.sendmail(smtp_user or "noreply@health-monitor.local", recipient, msg.as_string())

                logger.info(f"Email sent to {recipient} for {patient_id} [{alert_type}]")

            # Update rate limit tracker
            key = (patient_id, alert_type)
            self.rate_limit_tracker[key] = datetime.utcnow()
            self.hourly_counter += 1

            # Log to Cassandra
            self._log_to_cassandra(record, subject, "sent", "")

            return True

        except Exception as e:
            logger.error(f"Email send failed for {patient_id}: {e}")
            self._log_to_cassandra(record, "", "failed", str(e))
            return False

    def _log_to_cassandra(self, record, subject, status, error_message):
        """Write email log entry to Cassandra."""
        if not self.cassandra_session:
            return

        try:
            doctors = self.config.get("doctors", [])
            recipient = doctors[0].get("email", "") if doctors else ""

            self.cassandra_session.execute(
                """INSERT INTO smart_health.email_alert_log
                   (patient_id, sent_at, alert_type, alert_severity, recipient_email,
                    subject, status, error_message)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    record.get("patient_id", "unknown"),
                    datetime.utcnow(),
                    record.get("alert_type", ""),
                    record.get("alert_severity", ""),
                    recipient,
                    subject,
                    status,
                    error_message,
                ),
            )
        except Exception as e:
            logger.warning(f"Failed to log email to Cassandra: {e}")

    def test_connection(self):
        """Test SMTP connection. Returns True if successful."""
        if not self.enabled:
            logger.info("Email alerting disabled. Cannot test connection.")
            return False

        try:
            email_cfg = self.config.get("email", {})
            smtp_host = email_cfg.get("smtp_host", "smtp.gmail.com")
            smtp_port = email_cfg.get("smtp_port", 587)
            smtp_user = email_cfg.get("smtp_user", "")
            smtp_password = email_cfg.get("smtp_password", "")
            use_tls = email_cfg.get("use_tls", True)

            with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
                if use_tls:
                    server.starttls()
                if smtp_user and smtp_password:
                    server.login(smtp_user, smtp_password)

            logger.info(f"SMTP connection test successful: {smtp_host}:{smtp_port}")
            return True

        except Exception as e:
            logger.error(f"SMTP connection test failed: {e}")
            return False
