-- Smart Health Monitoring IoT - Cassandra Schema
-- Creates keyspace and three main tables for the streaming system

CREATE KEYSPACE IF NOT EXISTS smart_health
WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE smart_health;

-- Table 1: Complete sensor readings with AI enrichment
CREATE TABLE IF NOT EXISTS smart_health.sensor_readings (
  patient_id text,
  reading_time timestamp,
  -- Demographics
  age int,
  gender text,
  weight double,
  height double,
  bmi double,
  -- Vital Signs
  heart_rate int,
  spo2 double,
  temperature double,
  systolic_bp int,
  diastolic_bp int,
  respiratory_rate int,
  glucose_level int,
  skin_temperature double,
  -- Activity & Lifestyle
  activity_level text,
  exercise_type text,
  exercise_intensity text,
  steps int,
  stress_level text,
  sleep_duration double,
  sleep_quality text,
  screen_time double,
  notifications_received int,
  -- Sensor Data
  fall_detected boolean,
  battery_level double,
  -- Medical History
  chronic_condition text,
  smoker text,
  medication text,
  predicted_disease_simulated text,
  -- AI/ML Enrichment
  rule_status text,
  predicted_status text,
  risk_score double,
  risk_level text,
  is_anomaly boolean,
  anomaly_score double,
  anomaly_type text,
  predicted_next_heart_rate double,
  -- Alerts
  alert_type text,
  alert_severity text,
  alert_message text,
  -- Metadata
  model_version text,
  processed_at timestamp,
  PRIMARY KEY ((patient_id), reading_time)
) WITH CLUSTERING ORDER BY (reading_time DESC)
AND default_time_to_live = 2592000
AND gc_grace_seconds = 864000;

-- Table 2: Patient alerts for critical events
CREATE TABLE IF NOT EXISTS smart_health.patient_alerts (
  patient_id text,
  alert_time timestamp,
  alert_type text,
  alert_severity text,
  alert_message text,
  predicted_status text,
  risk_score double,
  is_anomaly boolean,
  anomaly_score double,
  anomaly_type text,
  fall_detected boolean,
  heart_rate int,
  spo2 double,
  temperature double,
  systolic_bp int,
  diastolic_bp int,
  respiratory_rate int,
  glucose_level int,
  battery_level double,
  chronic_condition text,
  processed_at timestamp,
  PRIMARY KEY ((patient_id), alert_time)
) WITH CLUSTERING ORDER BY (alert_time DESC)
AND default_time_to_live = 7776000
AND gc_grace_seconds = 864000;

-- Table 3: Latest status for each patient (for fast dashboard updates)
CREATE TABLE IF NOT EXISTS smart_health.patient_latest_status (
  patient_id text PRIMARY KEY,
  reading_time timestamp,
  age int,
  gender text,
  predicted_status text,
  risk_score double,
  risk_level text,
  is_anomaly boolean,
  anomaly_score double,
  anomaly_type text,
  alert_type text,
  alert_severity text,
  alert_message text,
  heart_rate int,
  spo2 double,
  temperature double,
  systolic_bp int,
  diastolic_bp int,
  respiratory_rate int,
  glucose_level int,
  battery_level double,
  chronic_condition text,
  smoker text,
  medication text,
  predicted_next_heart_rate double,
  processed_at timestamp
) WITH default_time_to_live = 2592000
AND gc_grace_seconds = 864000;

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS ON smart_health.sensor_readings (predicted_status);
CREATE INDEX IF NOT EXISTS ON smart_health.sensor_readings (alert_severity);
CREATE INDEX IF NOT EXISTS ON smart_health.sensor_readings (risk_level);
CREATE INDEX IF NOT EXISTS ON smart_health.patient_alerts (alert_type);
CREATE INDEX IF NOT EXISTS ON smart_health.patient_alerts (alert_severity);

-- Optional: Create materialized views for aggregations (uncomment if needed)
-- CREATE MATERIALIZED VIEW IF NOT EXISTS smart_health.alerts_by_severity AS
--   SELECT alert_severity, patient_id, alert_time, alert_message
--   FROM smart_health.patient_alerts
--   WHERE alert_severity IS NOT NULL AND patient_id IS NOT NULL AND alert_time IS NOT NULL
--   PRIMARY KEY (alert_severity, alert_time, patient_id);

