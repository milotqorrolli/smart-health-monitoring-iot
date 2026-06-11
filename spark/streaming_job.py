import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col as spark_col
from pyspark.sql.functions import from_json
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


for helper_path in ("/ml", str(Path(__file__).resolve().parents[1] / "ml")):
    if helper_path not in sys.path:
        sys.path.insert(0, helper_path)

try:
    from model_utils import (
        ENRICHED_FIELDS,
        MODEL_FEATURES,
        MODEL_VERSION,
        STATUS_TO_SEVERITY,
        derive_status,
        detect_anomaly_type,
        fill_record_defaults,
        generate_alert,
        risk_from_status,
        risk_level,
        safe_float,
    )
except Exception as exc:
    raise RuntimeError(f"Unable to import shared model utilities: {exc}") from exc


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "smart-health-data")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "smart_health")
MODEL_DIR = Path(os.getenv("MODEL_DIR", "/models"))
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/tmp/smart-health-checkpoint")

SENSOR_TABLE = "sensor_readings"
ALERT_TABLE = "patient_alerts"
LATEST_TABLE = "patient_latest_status"

INT_COLUMNS = {
    "age",
    "heart_rate",
    "systolic_bp",
    "diastolic_bp",
    "respiratory_rate",
    "steps",
    "notifications_received",
}
DOUBLE_COLUMNS = {
    "weight",
    "height",
    "bmi",
    "spo2",
    "temperature",
    "glucose_level",
    "skin_temperature",
    "sleep_duration",
    "screen_time",
    "battery_level",
    "risk_score",
    "anomaly_score",
    "predicted_next_heart_rate",
}
BOOLEAN_COLUMNS = {"fall_detected", "is_anomaly"}
TIMESTAMP_COLUMNS = {"reading_time", "processed_at", "alert_time"}

SENSOR_COLUMNS = ENRICHED_FIELDS
ALERT_COLUMNS = [
    "patient_id",
    "alert_time",
    "alert_type",
    "alert_severity",
    "alert_message",
    "risk_level",
    "predicted_status",
    "risk_score",
    "is_anomaly",
    "anomaly_score",
    "predicted_next_heart_rate",
    "fall_detected",
    "heart_rate",
    "spo2",
    "temperature",
    "systolic_bp",
    "diastolic_bp",
    "respiratory_rate",
    "processed_at",
]
LATEST_COLUMNS = [
    "patient_id",
    "reading_time",
    "predicted_status",
    "risk_score",
    "risk_level",
    "is_anomaly",
    "anomaly_score",
    "predicted_next_heart_rate",
    "alert_type",
    "alert_severity",
    "alert_message",
    "heart_rate",
    "spo2",
    "temperature",
    "systolic_bp",
    "diastolic_bp",
    "respiratory_rate",
    "glucose_level",
    "battery_level",
    "processed_at",
]

schema = StructType(
    [
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
        StructField("glucose_level", DoubleType(), True),
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
    ]
)

MODELS: dict[str, Any] | None = None
MODEL_WARNING_PRINTED = False
HEART_RATE_HISTORY: dict[str, list[float]] = {}


def load_models() -> dict[str, Any]:
    global MODELS, MODEL_WARNING_PRINTED
    if MODELS is not None:
        return MODELS

    required_paths = {
        "status": MODEL_DIR / "status_classifier.pkl",
        "risk": MODEL_DIR / "risk_regressor.pkl",
        "anomaly": MODEL_DIR / "anomaly_detector.pkl",
        "forecast": MODEL_DIR / "heart_rate_forecaster.pkl",
    }
    support_paths = {
        "preprocessing": MODEL_DIR / "preprocessing_pipeline.pkl",
        "feature_schema": MODEL_DIR / "feature_schema.json",
        "metadata": MODEL_DIR / "model_metadata.json",
        "metrics": MODEL_DIR / "model_metrics.json",
    }
    models: dict[str, Any] = {}
    missing_paths = [path for path in required_paths.values() if not path.exists()]
    if len(missing_paths) == len(required_paths) and not MODEL_WARNING_PRINTED:
        print("ML models not found. Using rule-based fallback.", flush=True)

    for name, path in required_paths.items():
        if path.exists():
            try:
                models[name] = joblib.load(path)
                print(f"Loaded {name} model from {path}", flush=True)
            except Exception as exc:
                print(f"WARNING: failed to load {name} model from {path}: {exc}", flush=True)
        elif not MODEL_WARNING_PRINTED and len(missing_paths) != len(required_paths):
            print(f"WARNING: model artifact not found: {path}. Rule fallback will be used for that output.", flush=True)

    if support_paths["preprocessing"].exists():
        try:
            models["preprocessing"] = joblib.load(support_paths["preprocessing"])
            print(f"Loaded preprocessing pipeline from {support_paths['preprocessing']}", flush=True)
        except Exception as exc:
            print(f"WARNING: failed to load preprocessing pipeline: {exc}", flush=True)

    for artifact_name in ["feature_schema", "metadata", "metrics"]:
        path = support_paths[artifact_name]
        if path.exists():
            try:
                models[artifact_name] = json.loads(path.read_text(encoding="utf-8"))
                print(f"Loaded {artifact_name} artifact from {path}", flush=True)
            except Exception as exc:
                print(f"WARNING: failed to read {artifact_name} artifact from {path}: {exc}", flush=True)

    MODEL_WARNING_PRINTED = True
    MODELS = models
    return MODELS


def parse_times(raw_timestamps: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(raw_timestamps, utc=True, errors="coerce")
    parsed = parsed.fillna(pd.Timestamp.now(tz="UTC"))
    return parsed.dt.tz_convert("UTC").dt.tz_localize(None)


def normalize_batch(pdf: pd.DataFrame) -> pd.DataFrame:
    records = [fill_record_defaults(record) for record in pdf.to_dict(orient="records")]
    normalized = pd.DataFrame(records)
    normalized["reading_time"] = parse_times(pdf.get("timestamp", pd.Series([""] * len(pdf))))
    return normalized


def predict_status(pdf: pd.DataFrame, models: dict[str, Any]) -> list[str]:
    rule_statuses = [derive_status(record) for record in pdf.to_dict(orient="records")]
    model = models.get("status")
    if model is None:
        return rule_statuses
    try:
        return [str(value).upper() for value in model.predict(pdf[MODEL_FEATURES])]
    except Exception as exc:
        print(f"WARNING: status model inference failed; using rule fallback: {exc}", flush=True)
        return rule_statuses


def predict_risk(pdf: pd.DataFrame, statuses: list[str], models: dict[str, Any]) -> list[float]:
    model = models.get("risk")
    if model is None:
        return [risk_from_status(status) for status in statuses]
    try:
        return [round(float(np.clip(value, 0, 100)), 2) for value in model.predict(pdf[MODEL_FEATURES])]
    except Exception as exc:
        print(f"WARNING: risk model inference failed; using rule fallback: {exc}", flush=True)
        return [risk_from_status(status) for status in statuses]


def predict_anomalies(pdf: pd.DataFrame, models: dict[str, Any]) -> tuple[list[bool], list[float]]:
    model = models.get("anomaly")
    if model is None:
        anomaly_flags = [STATUS_TO_SEVERITY.get(derive_status(record), 0) >= 2 for record in pdf.to_dict(orient="records")]
        anomaly_scores = [75.0 if flag else 5.0 for flag in anomaly_flags]
        return anomaly_flags, anomaly_scores
    try:
        raw_pred = model.predict(pdf[MODEL_FEATURES])
        flags = [bool(value == -1) for value in raw_pred]
        if hasattr(model, "decision_function"):
            decision = np.asarray(model.decision_function(pdf[MODEL_FEATURES]), dtype=float)
            inverted = -decision
            spread = float(inverted.max() - inverted.min())
            if spread > 0:
                scores = ((inverted - inverted.min()) / spread) * 100.0
            else:
                scores = np.where(np.asarray(flags), 80.0, 5.0)
        else:
            scores = np.where(np.asarray(flags), 80.0, 5.0)
        return flags, [round(float(np.clip(score, 0, 100)), 2) for score in scores]
    except Exception as exc:
        print(f"WARNING: anomaly model inference failed; using rule fallback: {exc}", flush=True)
        anomaly_flags = [STATUS_TO_SEVERITY.get(derive_status(record), 0) >= 2 for record in pdf.to_dict(orient="records")]
        anomaly_scores = [75.0 if flag else 5.0 for flag in anomaly_flags]
        return anomaly_flags, anomaly_scores


def predict_next_heart_rate(row: pd.Series, models: dict[str, Any]) -> float:
    patient_id = str(row["patient_id"])
    history = HEART_RATE_HISTORY.setdefault(patient_id, [])
    history.append(float(row["heart_rate"]))
    HEART_RATE_HISTORY[patient_id] = history[-5:]

    if len(HEART_RATE_HISTORY[patient_id]) < 5 or models.get("forecast") is None:
        return round(float(row["heart_rate"]), 2)

    values = HEART_RATE_HISTORY[patient_id]
    features = pd.DataFrame(
        [
            {
                "hr_lag_1": values[-1],
                "hr_lag_2": values[-2],
                "hr_lag_3": values[-3],
                "hr_lag_4": values[-4],
                "hr_lag_5": values[-5],
            }
        ]
    )
    try:
        return round(float(models["forecast"].predict(features)[0]), 2)
    except Exception as exc:
        print(f"WARNING: heart-rate forecast failed; using current value: {exc}", flush=True)
        return round(float(row["heart_rate"]), 2)


def enrich_batch(pdf: pd.DataFrame) -> pd.DataFrame:
    models = load_models()
    enriched = normalize_batch(pdf)
    enriched["rule_status"] = [derive_status(record) for record in enriched.to_dict(orient="records")]
    enriched["predicted_status"] = predict_status(enriched, models)
    enriched["risk_score"] = predict_risk(enriched, enriched["predicted_status"].tolist(), models)
    enriched["risk_level"] = [risk_level(score) for score in enriched["risk_score"]]

    anomaly_flags, anomaly_scores = predict_anomalies(enriched, models)
    enriched["is_anomaly"] = anomaly_flags
    enriched["anomaly_score"] = anomaly_scores
    enriched["anomaly_type"] = [detect_anomaly_type(record) for record in enriched.to_dict(orient="records")]
    enriched["predicted_next_heart_rate"] = [
        predict_next_heart_rate(row, models) for _, row in enriched.iterrows()
    ]

    alerts = [generate_alert(record) for record in enriched.to_dict(orient="records")]
    alert_df = pd.DataFrame(alerts)
    enriched = pd.concat([enriched.reset_index(drop=True), alert_df.reset_index(drop=True)], axis=1)

    has_inference_models = any(name in models for name in ["status", "risk", "anomaly", "forecast"])
    version = models.get("metadata", {}).get("model_version", MODEL_VERSION)
    enriched["model_version"] = version if has_inference_models else f"{MODEL_VERSION}-rules"
    enriched["processed_at"] = datetime.now(timezone.utc).replace(tzinfo=None)

    for column in SENSOR_COLUMNS:
        if column not in enriched.columns:
            enriched[column] = None

    return enriched[SENSOR_COLUMNS]


def cast_for_cassandra(df, columns: list[str]):
    output = df.select(*columns)
    for column in columns:
        if column in INT_COLUMNS:
            output = output.withColumn(column, spark_col(column).cast("int"))
        elif column in DOUBLE_COLUMNS:
            output = output.withColumn(column, spark_col(column).cast("double"))
        elif column in BOOLEAN_COLUMNS:
            output = output.withColumn(column, spark_col(column).cast("boolean"))
        elif column in TIMESTAMP_COLUMNS:
            output = output.withColumn(column, spark_col(column).cast("timestamp"))
    return output


def write_table(df, table_name: str, columns: list[str]) -> None:
    output = cast_for_cassandra(df, columns)
    (
        output.write.format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table=table_name, keyspace=CASSANDRA_KEYSPACE)
        .save()
    )


def write_batch_to_cassandra(batch_df, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    spark = batch_df.sparkSession
    try:
        pdf = batch_df.toPandas()
        if pdf.empty:
            return

        enriched_pdf = enrich_batch(pdf)
        enriched_df = spark.createDataFrame(enriched_pdf)

        write_table(enriched_df, SENSOR_TABLE, SENSOR_COLUMNS)

        alerts_pdf = enriched_pdf[enriched_pdf["alert_type"] != "NO_ALERT"].copy()
        if not alerts_pdf.empty:
            alerts_pdf["alert_time"] = alerts_pdf["reading_time"]
            alerts_df = spark.createDataFrame(alerts_pdf[ALERT_COLUMNS])
            write_table(alerts_df, ALERT_TABLE, ALERT_COLUMNS)

        latest_pdf = (
            enriched_pdf.sort_values("reading_time")
            .drop_duplicates(subset=["patient_id"], keep="last")
            .copy()
        )
        latest_df = spark.createDataFrame(latest_pdf[LATEST_COLUMNS])
        write_table(latest_df, LATEST_TABLE, LATEST_COLUMNS)

        print(f"Processed batch {batch_id}: {len(enriched_pdf)} readings, {len(alerts_pdf)} alerts", flush=True)
    except Exception as exc:
        print(f"ERROR: failed to process batch {batch_id}: {exc}", flush=True)


def main() -> None:
    spark = (
        SparkSession.builder.appName("SmartHealthStructuredStreaming")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        kafka_df.select(from_json(spark_col("value").cast("string"), schema).alias("reading"))
        .select("reading.*")
        .where(spark_col("patient_id").isNotNull())
    )

    query = (
        parsed_df.writeStream.foreachBatch(write_batch_to_cassandra)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
