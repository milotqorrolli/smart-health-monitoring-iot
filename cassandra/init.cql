CREATE KEYSPACE IF NOT EXISTS smart_health
WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

CREATE TABLE IF NOT EXISTS smart_health.sensor_readings (
  patient_id text,
  reading_time timestamp,
  heart_rate int,
  spo2 double,
  temperature double,
  systolic_bp int,
  diastolic_bp int,
  respiratory_rate int,
  status text,
  processed_at timestamp,
  PRIMARY KEY ((patient_id), reading_time)
) WITH CLUSTERING ORDER BY (reading_time DESC);
