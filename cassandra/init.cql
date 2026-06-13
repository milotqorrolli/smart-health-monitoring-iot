CREATE KEYSPACE IF NOT EXISTS smart_health
WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

-- Table A: All enriched sensor readings
CREATE TABLE IF NOT EXISTS smart_health.sensor_readings (
    patient_id                  text,
    reading_time                timestamp,
    vitals_sensor_id            text,
    bp_sensor_id                text,
    glucose_sensor_id           text,
    activity_sensor_id          text,
    fall_sensor_id              text,
    source_sensor_ids           text,
    source_sensor_types         text,
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
    source_sensor_ids   text,
    source_sensor_types text,
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
    predicted_next_heart_rate double,
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

-- Table E: Latest metadata and health for every simulated sensor.
CREATE TABLE IF NOT EXISTS smart_health.sensor_metadata (
    sensor_id         text PRIMARY KEY,
    patient_id        text,
    sensor_type       text,
    kafka_topic       text,
    last_reading_time timestamp,
    battery_level     double,
    last_status       text,
    last_seen_at      timestamp
);

-- Table F: One-minute aggregation windows for demo-ready analytics.
CREATE TABLE IF NOT EXISTS smart_health.patient_minute_metrics (
    patient_id              text,
    window_start            timestamp,
    reading_count           int,
    avg_heart_rate          double,
    min_spo2                double,
    max_temperature         double,
    max_systolic_bp         int,
    max_risk_score          double,
    high_or_critical_alerts int,
    PRIMARY KEY ((patient_id), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

-- Backward-compatible schema updates for already-created local Cassandra volumes.
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS vitals_sensor_id text;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS bp_sensor_id text;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS glucose_sensor_id text;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS activity_sensor_id text;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS fall_sensor_id text;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS source_sensor_ids text;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS source_sensor_types text;
ALTER TABLE smart_health.patient_latest_status ADD IF NOT EXISTS source_sensor_ids text;
ALTER TABLE smart_health.patient_latest_status ADD IF NOT EXISTS source_sensor_types text;
ALTER TABLE smart_health.patient_latest_status ADD IF NOT EXISTS predicted_next_heart_rate double;
