CREATE KEYSPACE IF NOT EXISTS smart_health
WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

-- Table A: All enriched sensor readings
CREATE TABLE IF NOT EXISTS smart_health.sensor_readings (
    patient_id                  text,
    reading_time                timestamp,
    age                         int,
    gender                      text,
    weight                      double,
    height                      double,
    bmi                         double,
    heart_rate                  int,
    spo2                        double,
    temperature                 double,
    systolic_bp                 int,
    diastolic_bp                int,
    respiratory_rate            int,
    glucose_level               double,
    skin_temperature            double,
    activity_level              text,
    exercise_type               text,
    exercise_intensity          text,
    steps                       int,
    stress_level                text,
    sleep_duration              double,
    sleep_quality               text,
    fall_detected               boolean,
    battery_level               double,
    chronic_condition           text,
    smoker                      text,
    medication                  text,
    predicted_disease_simulated text,
    rule_status                 text,
    predicted_status            text,
    risk_score                  double,
    risk_level                  text,
    is_anomaly                  boolean,
    anomaly_score               double,
    anomaly_type                text,
    predicted_next_heart_rate   double,
    alert_type                  text,
    alert_severity              text,
    alert_message               text,
    model_version               text,
    processed_at                timestamp,
    PRIMARY KEY ((patient_id), reading_time)
) WITH CLUSTERING ORDER BY (reading_time DESC);

-- Table B: Alert events only
CREATE TABLE IF NOT EXISTS smart_health.patient_alerts (
    patient_id       text,
    alert_time       timestamp,
    alert_type       text,
    alert_severity   text,
    alert_message    text,
    predicted_status text,
    risk_score       double,
    is_anomaly       boolean,
    fall_detected    boolean,
    heart_rate       int,
    spo2             double,
    temperature      double,
    systolic_bp      int,
    diastolic_bp     int,
    respiratory_rate int,
    processed_at     timestamp,
    PRIMARY KEY ((patient_id), alert_time)
) WITH CLUSTERING ORDER BY (alert_time DESC);

-- Table C: Latest status per patient (single row per patient)
CREATE TABLE IF NOT EXISTS smart_health.patient_latest_status (
    patient_id          text PRIMARY KEY,
    reading_time        timestamp,
    predicted_status    text,
    risk_score          double,
    risk_level          text,
    is_anomaly          boolean,
    anomaly_score       double,
    alert_type          text,
    alert_severity      text,
    alert_message       text,
    heart_rate          int,
    spo2                double,
    temperature         double,
    systolic_bp         int,
    diastolic_bp        int,
    respiratory_rate    int,
    glucose_level       double,
    battery_level       double,
    processed_at        timestamp
);

-- Table D: Email alert log
CREATE TABLE IF NOT EXISTS smart_health.email_alert_log (
    patient_id      text,
    sent_at         timestamp,
    alert_type      text,
    alert_severity  text,
    recipient_email text,
    subject         text,
    status          text,
    error_message   text,
    PRIMARY KEY ((patient_id), sent_at)
) WITH CLUSTERING ORDER BY (sent_at DESC);
