CREATE KEYSPACE IF NOT EXISTS smart_health
WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

CREATE TABLE IF NOT EXISTS smart_health.sensor_readings (
  patient_id text,
  reading_time timestamp,
  age int,
  gender text,
  weight double,
  height double,
  bmi double,
  heart_rate int,
  spo2 double,
  temperature double,
  systolic_bp int,
  diastolic_bp int,
  respiratory_rate int,
  glucose_level double,
  skin_temperature double,
  activity_level text,
  exercise_type text,
  exercise_intensity text,
  steps int,
  stress_level text,
  sleep_duration double,
  sleep_quality text,
  screen_time double,
  notifications_received int,
  fall_detected boolean,
  battery_level double,
  chronic_condition text,
  smoker text,
  medication text,
  predicted_disease_simulated text,
  rule_status text,
  predicted_status text,
  risk_score double,
  risk_level text,
  is_anomaly boolean,
  anomaly_score double,
  anomaly_type text,
  predicted_next_heart_rate double,
  alert_type text,
  alert_severity text,
  alert_message text,
  model_version text,
  processed_at timestamp,
  PRIMARY KEY ((patient_id), reading_time)
) WITH CLUSTERING ORDER BY (reading_time DESC);

ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS age int;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS gender text;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS weight double;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS height double;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS bmi double;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS glucose_level double;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS skin_temperature double;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS activity_level text;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS exercise_type text;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS exercise_intensity text;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS steps int;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS stress_level text;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS sleep_duration double;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS sleep_quality text;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS screen_time double;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS notifications_received int;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS fall_detected boolean;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS battery_level double;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS chronic_condition text;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS smoker text;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS medication text;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS predicted_disease_simulated text;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS rule_status text;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS predicted_status text;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS risk_score double;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS risk_level text;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS is_anomaly boolean;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS anomaly_score double;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS anomaly_type text;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS predicted_next_heart_rate double;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS alert_type text;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS alert_severity text;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS alert_message text;
ALTER TABLE smart_health.sensor_readings ADD IF NOT EXISTS model_version text;

CREATE TABLE IF NOT EXISTS smart_health.patient_alerts (
  patient_id text,
  alert_time timestamp,
  alert_type text,
  alert_severity text,
  alert_message text,
  risk_level text,
  predicted_status text,
  risk_score double,
  is_anomaly boolean,
  anomaly_score double,
  predicted_next_heart_rate double,
  fall_detected boolean,
  heart_rate int,
  spo2 double,
  temperature double,
  systolic_bp int,
  diastolic_bp int,
  respiratory_rate int,
  processed_at timestamp,
  PRIMARY KEY ((patient_id), alert_time)
) WITH CLUSTERING ORDER BY (alert_time DESC);

ALTER TABLE smart_health.patient_alerts ADD IF NOT EXISTS risk_level text;
ALTER TABLE smart_health.patient_alerts ADD IF NOT EXISTS anomaly_score double;
ALTER TABLE smart_health.patient_alerts ADD IF NOT EXISTS predicted_next_heart_rate double;

CREATE TABLE IF NOT EXISTS smart_health.patient_latest_status (
  patient_id text PRIMARY KEY,
  reading_time timestamp,
  predicted_status text,
  risk_score double,
  risk_level text,
  is_anomaly boolean,
  anomaly_score double,
  predicted_next_heart_rate double,
  alert_type text,
  alert_severity text,
  alert_message text,
  heart_rate int,
  spo2 double,
  temperature double,
  systolic_bp int,
  diastolic_bp int,
  respiratory_rate int,
  glucose_level double,
  battery_level double,
  processed_at timestamp
);

ALTER TABLE smart_health.patient_latest_status ADD IF NOT EXISTS predicted_next_heart_rate double;
