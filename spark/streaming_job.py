"""
Smart Health Monitoring IoT - Spark Structured Streaming Job

Reads messages from Kafka, applies ML models, generates alerts, and writes to Cassandra.
"""

import os
import logging
import pickle
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, from_json, to_timestamp, when, lit, struct,
    array_join, concat_ws
)
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType, BooleanType
)


# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "smart-health-data")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "smart_health")
MODELS_PATH = os.getenv("MODELS_PATH", "/models")

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Input schema from producer
INPUT_SCHEMA = StructType([
    StructField("patient_id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("weight", DoubleType(), True),
    StructField("height", DoubleType(), True),
    StructField("bmi", DoubleType(), True),
    StructField("heart_rate", IntegerType(), True),
    StructField("spo2", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("systolic_bp", IntegerType(), True),
    StructField("diastolic_bp", IntegerType(), True),
    StructField("respiratory_rate", IntegerType(), True),
    StructField("glucose_level", IntegerType(), True),
    StructField("skin_temperature", DoubleType(), True),
    StructField("activity_level", StringType(), True),
    StructField("exercise_type", StringType(), True),
    StructField("exercise_intensity", StringType(), True),
    StructField("steps", IntegerType(), True),
    StructField("stress_level", StringType(), True),
    StructField("sleep_duration", DoubleType(), True),
    StructField("sleep_quality", StringType(), True),
    StructField("screen_time", DoubleType(), True),
    StructField("notifications_received", IntegerType(), True),
    StructField("fall_detected", BooleanType(), True),
    StructField("battery_level", DoubleType(), True),
    StructField("chronic_condition", StringType(), True),
    StructField("smoker", StringType(), True),
    StructField("medication", StringType(), True),
    StructField("predicted_disease_simulated", StringType(), True),
])


class ModelInference:
    """Load and apply ML models for inference."""

    def __init__(self, models_path: str = MODELS_PATH):
        self.models_path = Path(models_path)
        self.models_loaded = False
        self.status_classifier = None
        self.risk_regressor = None
        self.anomaly_detector = None
        self.anomaly_scaler = None
        self.heart_rate_forecaster = None
        self.heart_rate_scaler = None
        self.load_models()

    def load_models(self):
        """Load trained models from disk."""
        try:
            model_path = self.models_path / "status_classifier.pkl"
            if model_path.exists():
                with open(model_path, "rb") as f:
                    self.status_classifier = pickle.load(f)
                logger.info("Loaded status classifier")
            else:
                logger.warning(f"Status classifier not found at {model_path}")

            risk_path = self.models_path / "risk_regressor.pkl"
            if risk_path.exists():
                with open(risk_path, "rb") as f:
                    self.risk_regressor = pickle.load(f)
                logger.info("Loaded risk regressor")
            else:
                logger.warning(f"Risk regressor not found at {risk_path}")

            anom_path = self.models_path / "anomaly_detector.pkl"
            if anom_path.exists():
                with open(anom_path, "rb") as f:
                    self.anomaly_detector = pickle.load(f)
                logger.info("Loaded anomaly detector")

                scaler_path = self.models_path / "anomaly_scaler.pkl"
                if scaler_path.exists():
                    with open(scaler_path, "rb") as f:
                        self.anomaly_scaler = pickle.load(f)
                    logger.info("Loaded anomaly scaler")
            else:
                logger.warning(f"Anomaly detector not found at {anom_path}")

            hr_path = self.models_path / "heart_rate_forecaster.pkl"
            if hr_path.exists():
                with open(hr_path, "rb") as f:
                    self.heart_rate_forecaster = pickle.load(f)
                logger.info("Loaded heart rate forecaster")

                hr_scaler_path = self.models_path / "heart_rate_scaler.pkl"
                if hr_scaler_path.exists():
                    with open(hr_scaler_path, "rb") as f:
                        self.heart_rate_scaler = pickle.load(f)
                    logger.info("Loaded heart rate scaler")
            else:
                logger.warning(f"Heart rate forecaster not found at {hr_path}")

            self.models_loaded = (
                self.status_classifier is not None
                and self.risk_regressor is not None
                and self.anomaly_detector is not None
            )
            if self.models_loaded:
                logger.info("All models loaded successfully!")
            else:
                logger.warning("Some models are missing. Using rule-based fallback.")

        except Exception as e:
            logger.error(f"Error loading models: {e}")
            self.models_loaded = False

    def predict_status(self, row: dict) -> str:
        """Predict health status using ML model or fallback to rules."""
        if self.status_classifier is None:
            return self._derive_status_rule_based(row)

        try:
            df = pd.DataFrame([row])
            prediction = self.status_classifier.predict(df)[0]
            return prediction
        except Exception as e:
            logger.warning(f"Status prediction failed: {e}. Using rule-based fallback.")
            return self._derive_status_rule_based(row)

    def predict_risk_score(self, row: dict) -> float:
        """Predict risk score using ML model or fallback."""
        if self.risk_regressor is None:
            return self._derive_risk_score_rule_based(row)

        try:
            df = pd.DataFrame([row])
            risk_score = self.risk_regressor.predict(df)[0]
            return float(np.clip(risk_score, 0, 100))
        except Exception as e:
            logger.warning(f"Risk prediction failed: {e}. Using rule-based fallback.")
            return self._derive_risk_score_rule_based(row)

    def predict_anomaly(self, row: dict) -> tuple:
        """Detect anomalies using ML model."""
        if self.anomaly_detector is None or self.anomaly_scaler is None:
            return False, 0.0

        try:
            vitals = [
                row.get("heart_rate", 72),
                row.get("spo2", 98),
                row.get("temperature", 37),
                row.get("systolic_bp", 120),
                row.get("diastolic_bp", 80),
                row.get("respiratory_rate", 16),
                row.get("glucose_level", 100),
                row.get("skin_temperature", 34),
            ]
            X = np.array(vitals).reshape(1, -1)
            X_scaled = self.anomaly_scaler.transform(X)
            is_anomaly = self.anomaly_detector.predict(X_scaled)[0] == -1
            anomaly_score = -self.anomaly_detector.score_samples(X_scaled)[0]
            return bool(is_anomaly), float(anomaly_score)
        except Exception as e:
            logger.warning(f"Anomaly detection failed: {e}")
            return False, 0.0

    def predict_next_heart_rate(self, row: dict) -> Optional[float]:
        """Predict next heart rate using previous values."""
        if self.heart_rate_forecaster is None or self.heart_rate_scaler is None:
            return None

        try:
            # For simplicity, use current HR and nearby values
            hr = row.get("heart_rate", 72)
            T1, T2, T3 = hr, hr + random.randint(-5, 5), hr + random.randint(-5, 5)
            X = np.array([[T1, T2, T3]])
            X_scaled = self.heart_rate_scaler.transform(X)
            prediction = self.heart_rate_forecaster.predict(X_scaled)[0]
            return float(np.clip(prediction, 30, 200))
        except Exception as e:
            logger.warning(f"Heart rate forecast failed: {e}")
            return None

    @staticmethod
    def _derive_status_rule_based(row: dict) -> str:
        """Rule-based status derivation (fallback)."""
        hr = row.get("heart_rate", 72)
        spo2 = row.get("spo2", 98)
        temp = row.get("temperature", 37)
        sys_bp = row.get("systolic_bp", 120)
        dia_bp = row.get("diastolic_bp", 80)
        rr = row.get("respiratory_rate", 16)
        glucose = row.get("glucose_level", 100)
        fall = row.get("fall_detected", False)

        # Emergency
        if (hr < 40 or hr > 150 or spo2 < 85 or temp >= 40 or sys_bp > 200 or
            dia_bp > 130 or rr < 8 or rr > 35 or glucose < 50 or glucose > 250):
            return "EMERGENCY"

        # Critical
        if (hr < 50 or hr > 130 or spo2 < 90 or temp >= 39 or sys_bp > 180 or
            dia_bp > 120 or rr < 10 or rr > 30 or glucose < 70 or glucose > 200 or
            (fall and hr > 100)):
            return "CRITICAL"

        # Warning
        if (hr < 60 or hr > 100 or spo2 < 95 or temp < 36 or temp > 37.8 or
            sys_bp > 140 or dia_bp > 90 or rr < 12 or rr > 20 or
            glucose < 80 or glucose > 140 or fall):
            return "WARNING"

        return "NORMAL"

    @staticmethod
    def _derive_risk_score_rule_based(row: dict) -> float:
        """Rule-based risk score derivation (fallback)."""
        status = ModelInference._derive_status_rule_based(row)
        mapping = {"NORMAL": 20, "WARNING": 50, "CRITICAL": 75, "EMERGENCY": 95}
        return float(mapping.get(status, 50))


def add_ai_predictions(batch_df: DataFrame, batch_id: int) -> DataFrame:
    """Apply AI models and generate predictions using pandas for inference."""
    if batch_df.rdd.isEmpty():
        return batch_df

    # Convert to pandas for ML inference
    pdf = batch_df.toPandas()

    # Initialize inference engine (done once per batch for efficiency)
    inference = ModelInference(MODELS_PATH)

    # Apply predictions
    predictions = []
    for _, row in pdf.iterrows():
        row_dict = row.to_dict()

        # AI predictions
        predicted_status = inference.predict_status(row_dict)
        risk_score = inference.predict_risk_score(row_dict)
        is_anomaly, anomaly_score = inference.predict_anomaly(row_dict)
        predicted_next_hr = inference.predict_next_heart_rate(row_dict)

        # Rule-based status as fallback
        rule_status = inference._derive_status_rule_based(row_dict)

        # Risk level
        if risk_score < 25:
            risk_level = "LOW"
        elif risk_score < 45:
            risk_level = "MEDIUM"
        elif risk_score < 70:
            risk_level = "HIGH"
        else:
            risk_level = "CRITICAL"

        # Anomaly type
        anomaly_type = "None"
        if is_anomaly:
            vitals_ok = all([
                60 <= row_dict.get("heart_rate", 72) <= 100,
                row_dict.get("spo2", 98) >= 95,
                36 <= row_dict.get("temperature", 37) <= 37.8,
            ])
            anomaly_type = "High Risk Vitals" if not vitals_ok else "Sensor Anomaly"

        # Alert generation
        alert_type = "NO_ALERT"
        alert_severity = "NONE"
        alert_message = "No alert."

        if row_dict.get("fall_detected", False):
            alert_type = "FALL_DETECTED"
            alert_severity = "HIGH" if risk_score >= 70 else "MEDIUM"
            alert_message = "Fall detected: patient may require immediate assistance."

        elif predicted_status == "EMERGENCY" or risk_score >= 85:
            alert_type = "EMERGENCY_HEALTH_ALERT"
            alert_severity = "CRITICAL"
            vitals_summary = f"HR:{row_dict.get('heart_rate')}, SpO2:{row_dict.get('spo2')}%, T:{row_dict.get('temperature')}°C"
            alert_message = f"Emergency alert: critical vital signs detected. {vitals_summary}"

        elif predicted_status == "CRITICAL" or risk_score >= 70:
            alert_type = "HIGH_RISK_ALERT"
            alert_severity = "HIGH"
            alert_message = f"High risk alert: patient health deteriorating (Risk: {risk_score:.0f}%)"

        elif predicted_status == "WARNING" or risk_score >= 45:
            alert_type = "WARNING_HEALTH_ALERT"
            alert_severity = "MEDIUM"
            alert_message = f"Warning alert: abnormal readings detected (Risk: {risk_score:.0f}%)"

        if is_anomaly and alert_type == "NO_ALERT":
            alert_type = "ANOMALY_DETECTED"
            alert_severity = "MEDIUM"
            alert_message = f"Anomaly detected: unusual sensor values ({anomaly_type})"

        if row_dict.get("battery_level", 100) < 15 and alert_type == "NO_ALERT":
            alert_type = "LOW_SENSOR_BATTERY"
            alert_severity = "LOW"
            alert_message = f"Low sensor battery: {row_dict.get('battery_level'):.0f}%"

        predictions.append({
            "patient_id": row_dict.get("patient_id"),
            "timestamp": row_dict.get("timestamp"),
            "reading_time": row_dict.get("timestamp"),
            "age": row_dict.get("age"),
            "gender": row_dict.get("gender"),
            "weight": row_dict.get("weight"),
            "height": row_dict.get("height"),
            "bmi": row_dict.get("bmi"),
            "heart_rate": row_dict.get("heart_rate"),
            "spo2": row_dict.get("spo2"),
            "temperature": row_dict.get("temperature"),
            "systolic_bp": row_dict.get("systolic_bp"),
            "diastolic_bp": row_dict.get("diastolic_bp"),
            "respiratory_rate": row_dict.get("respiratory_rate"),
            "glucose_level": row_dict.get("glucose_level"),
            "skin_temperature": row_dict.get("skin_temperature"),
            "activity_level": row_dict.get("activity_level"),
            "exercise_type": row_dict.get("exercise_type"),
            "exercise_intensity": row_dict.get("exercise_intensity"),
            "steps": row_dict.get("steps"),
            "stress_level": row_dict.get("stress_level"),
            "sleep_duration": row_dict.get("sleep_duration"),
            "sleep_quality": row_dict.get("sleep_quality"),
            "screen_time": row_dict.get("screen_time"),
            "notifications_received": row_dict.get("notifications_received"),
            "fall_detected": row_dict.get("fall_detected"),
            "battery_level": row_dict.get("battery_level"),
            "chronic_condition": row_dict.get("chronic_condition"),
            "smoker": row_dict.get("smoker"),
            "medication": row_dict.get("medication"),
            "predicted_disease_simulated": row_dict.get("predicted_disease_simulated"),
            "rule_status": rule_status,
            "predicted_status": predicted_status,
            "risk_score": risk_score,
            "risk_level": risk_level,
            "is_anomaly": is_anomaly,
            "anomaly_score": anomaly_score,
            "anomaly_type": anomaly_type,
            "predicted_next_heart_rate": predicted_next_hr,
            "alert_type": alert_type,
            "alert_severity": alert_severity,
            "alert_message": alert_message,
            "model_version": "v1.0.0",
            "processed_at": pd.Timestamp.now().isoformat(),
        })

    result_df = pd.DataFrame(predictions)
    return SparkSession.getActiveSession().createDataFrame(result_df)


def write_batch_to_cassandra(batch_df: DataFrame, batch_id: int):
    """Write enriched data to Cassandra."""
    if batch_df.rdd.isEmpty():
        return

    try:
        # Write to sensor_readings table
        (batch_df.write
         .format("org.apache.spark.sql.cassandra")
         .mode("append")
         .option("table", "sensor_readings")
         .option("keyspace", CASSANDRA_KEYSPACE)
         .save())

        # Write critical alerts to patient_alerts table
        alerts_df = batch_df.filter(col("alert_severity").isin(["HIGH", "CRITICAL"]))
        if not alerts_df.rdd.isEmpty():
            (alerts_df.select(
                col("patient_id"),
                col("reading_time").alias("alert_time"),
                col("alert_type"),
                col("alert_severity"),
                col("alert_message"),
                col("predicted_status"),
                col("risk_score"),
                col("is_anomaly"),
                col("fall_detected"),
                col("heart_rate"),
                col("spo2"),
                col("temperature"),
                col("systolic_bp"),
                col("diastolic_bp"),
                col("respiratory_rate"),
                col("processed_at"),
            ).write
             .format("org.apache.spark.sql.cassandra")
             .mode("append")
             .option("table", "patient_alerts")
             .option("keyspace", CASSANDRA_KEYSPACE)
             .save())

        logger.info(f"Batch {batch_id}: {batch_df.count()} records written to Cassandra")

    except Exception as e:
        logger.error(f"Error writing batch {batch_id} to Cassandra: {e}")


def main() -> None:
    """Main Spark streaming job."""
    spark = (
        SparkSession.builder
        .appName("SmartHealthStructuredStreaming")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", "9042")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    logger.info("=" * 100)
    logger.info("Smart Health Monitoring IoT - Spark Streaming Job")
    logger.info("=" * 100)
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}, Topic: {KAFKA_TOPIC}")
    logger.info(f"Cassandra: {CASSANDRA_HOST}, Keyspace: {CASSANDRA_KEYSPACE}")
    logger.info(f"Models Path: {MODELS_PATH}")
    logger.info("=" * 100)

    # Read from Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON and convert timestamp
    parsed_df = (
        kafka_df.select(from_json(col("value").cast("string"), INPUT_SCHEMA).alias("reading"))
        .select("reading.*")
        .withColumn("reading_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    )

    # Apply AI predictions using foreachBatch
    enriched_df = (
        parsed_df.writeStream
        .foreachBatch(lambda batch_df, batch_id: write_batch_to_cassandra(
            add_ai_predictions(batch_df, batch_id), batch_id))
        .outputMode("append")
        .option("checkpointLocation", "/tmp/smart-health-checkpoint")
        .start()
    )

    logger.info("Streaming query started. Waiting for termination...")
    enriched_df.awaitTermination()


if __name__ == "__main__":
    # Add random import for heart rate forecast
    import random
    main()
