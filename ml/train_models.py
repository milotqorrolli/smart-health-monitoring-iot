"""
ML Model Training for Smart Health Monitoring IoT System.
Trains all four models: status classifier, risk regressor, anomaly detector, HR forecaster.
"""

import json
import os
import sys
import warnings
from datetime import datetime

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import (
    GradientBoostingClassifier,
    GradientBoostingRegressor,
    IsolationForest,
    RandomForestClassifier,
    RandomForestRegressor,
)
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    precision_score,
    r2_score,
    recall_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

warnings.filterwarnings("ignore")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from prepare_datasets import (
    prepare_anomaly_detection_data,
    prepare_heart_rate_forecasting_data,
    prepare_risk_regression_data,
    prepare_status_classification_data,
)
from model_utils import CATEGORICAL_FEATURES, NUMERIC_FEATURES, BOOLEAN_FEATURES

MODELS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "models")
os.makedirs(MODELS_DIR, exist_ok=True)

ALL_METRICS = {}


def build_preprocessing_pipeline(feature_names):
    """Build a scikit-learn preprocessing pipeline for the feature set."""
    numeric_cols = [c for c in feature_names if c in NUMERIC_FEATURES or c in BOOLEAN_FEATURES]
    categorical_cols = [c for c in feature_names if c in CATEGORICAL_FEATURES]

    numeric_transformer = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
    ])

    categorical_transformer = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="constant", fill_value="Unknown")),
        ("encoder", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numeric_cols),
            ("cat", categorical_transformer, categorical_cols),
        ],
        remainder="drop",
    )

    return preprocessor, numeric_cols, categorical_cols


def train_status_classifier():
    """Train status classification model."""
    print("\n" + "=" * 60)
    print("  TRAINING: Status Classification Model")
    print("=" * 60)

    df = prepare_status_classification_data()
    if df.empty:
        print("  SKIPPED: No training data available.")
        return

    feature_cols = [c for c in df.columns if c != "status"]
    X = df[feature_cols]
    y = df["status"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # Train RandomForest
    rf = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
    rf.fit(X_train, y_train)
    rf_pred = rf.predict(X_test)
    rf_f1 = f1_score(y_test, rf_pred, average="macro", zero_division=0)

    # Train GradientBoosting
    gb = GradientBoostingClassifier(n_estimators=100, random_state=42)
    gb.fit(X_train, y_train)
    gb_pred = gb.predict(X_test)
    gb_f1 = f1_score(y_test, gb_pred, average="macro", zero_division=0)

    # Select best model
    if gb_f1 > rf_f1:
        best_model = gb
        best_pred = gb_pred
        best_name = "GradientBoostingClassifier"
        best_f1 = gb_f1
    else:
        best_model = rf
        best_pred = rf_pred
        best_name = "RandomForestClassifier"
        best_f1 = rf_f1

    print(f"  Best model: {best_name} (F1={best_f1:.4f})")
    print(f"  RF F1: {rf_f1:.4f}, GB F1: {gb_f1:.4f}")

    # Save model
    model_path = os.path.join(MODELS_DIR, "status_classifier.pkl")
    joblib.dump(best_model, model_path)
    print(f"  Saved: {model_path}")

    # Metrics
    acc = accuracy_score(y_test, best_pred)
    prec = precision_score(y_test, best_pred, average="macro", zero_division=0)
    rec = recall_score(y_test, best_pred, average="macro", zero_division=0)
    cm = confusion_matrix(y_test, best_pred).tolist()
    report = classification_report(y_test, best_pred, zero_division=0)

    print(f"  Accuracy: {acc:.4f}")
    print(f"  Precision (macro): {prec:.4f}")
    print(f"  Recall (macro): {rec:.4f}")
    print(f"  F1 (macro): {best_f1:.4f}")
    print(f"\n  Classification Report:\n{report}")

    ALL_METRICS["status_classifier"] = {
        "algorithm": best_name,
        "accuracy": round(acc, 4),
        "precision_macro": round(prec, 4),
        "recall_macro": round(rec, 4),
        "f1_macro": round(best_f1, 4),
        "confusion_matrix": cm,
        "feature_names": feature_cols,
        "classes": list(best_model.classes_),
    }

    return feature_cols


def train_risk_regressor():
    """Train risk score regression model."""
    print("\n" + "=" * 60)
    print("  TRAINING: Risk Score Regression Model")
    print("=" * 60)

    df = prepare_risk_regression_data()
    if df.empty:
        print("  SKIPPED: No training data available.")
        return

    feature_cols = [c for c in df.columns if c != "risk_score"]
    X = df[feature_cols]
    y = df["risk_score"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Train RandomForest
    rf = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
    rf.fit(X_train, y_train)
    rf_pred = rf.predict(X_test)
    rf_rmse = np.sqrt(mean_squared_error(y_test, rf_pred))

    # Train GradientBoosting
    gb = GradientBoostingRegressor(n_estimators=100, random_state=42)
    gb.fit(X_train, y_train)
    gb_pred = gb.predict(X_test)
    gb_rmse = np.sqrt(mean_squared_error(y_test, gb_pred))

    # Select best model (lower RMSE)
    if gb_rmse < rf_rmse:
        best_model = gb
        best_pred = gb_pred
        best_name = "GradientBoostingRegressor"
        best_rmse = gb_rmse
    else:
        best_model = rf
        best_pred = rf_pred
        best_name = "RandomForestRegressor"
        best_rmse = rf_rmse

    print(f"  Best model: {best_name} (RMSE={best_rmse:.4f})")
    print(f"  RF RMSE: {rf_rmse:.4f}, GB RMSE: {gb_rmse:.4f}")

    # Save model
    model_path = os.path.join(MODELS_DIR, "risk_regressor.pkl")
    joblib.dump(best_model, model_path)
    print(f"  Saved: {model_path}")

    # Metrics
    mae = mean_absolute_error(y_test, best_pred)
    r2 = r2_score(y_test, best_pred)

    print(f"  MAE: {mae:.4f}")
    print(f"  RMSE: {best_rmse:.4f}")
    print(f"  R2: {r2:.4f}")

    ALL_METRICS["risk_regressor"] = {
        "algorithm": best_name,
        "mae": round(mae, 4),
        "rmse": round(best_rmse, 4),
        "r2": round(r2, 4),
        "feature_names": feature_cols,
    }

    return feature_cols


def train_anomaly_detector():
    """Train anomaly detection model using IsolationForest."""
    print("\n" + "=" * 60)
    print("  TRAINING: Anomaly Detection Model")
    print("=" * 60)

    df, labels = prepare_anomaly_detection_data()
    if df.empty:
        print("  SKIPPED: No training data available.")
        return

    feature_cols = list(df.columns)

    # Train IsolationForest
    iso_forest = IsolationForest(contamination=0.1, random_state=42, n_jobs=-1)
    iso_forest.fit(df)

    # Save model
    model_path = os.path.join(MODELS_DIR, "anomaly_detector.pkl")
    joblib.dump(iso_forest, model_path)
    print(f"  Saved: {model_path}")

    # Evaluate
    predictions = iso_forest.predict(df)
    n_anomalies = (predictions == -1).sum()
    contamination_ratio = n_anomalies / len(predictions)

    print(f"  Contamination ratio: {contamination_ratio:.4f}")
    print(f"  Anomalies detected in training: {n_anomalies} / {len(predictions)}")

    metrics = {
        "algorithm": "IsolationForest",
        "contamination": 0.1,
        "n_anomalies_detected": int(n_anomalies),
        "total_samples": len(predictions),
        "contamination_ratio": round(contamination_ratio, 4),
        "feature_names": feature_cols,
    }

    # If supervised labels available, compute precision/recall
    if labels is not None and labels.sum() > 0:
        # Convert IsolationForest output (-1=anomaly, 1=normal) to (1=anomaly, 0=normal)
        iso_labels = (predictions == -1).astype(int)
        labels_binary = labels.values.astype(int)

        if len(iso_labels) == len(labels_binary):
            prec = precision_score(labels_binary, iso_labels, zero_division=0)
            rec = recall_score(labels_binary, iso_labels, zero_division=0)
            f1 = f1_score(labels_binary, iso_labels, zero_division=0)
            print(f"  Supervised evaluation (vs Anomaly_Flag):")
            print(f"    Precision: {prec:.4f}")
            print(f"    Recall: {rec:.4f}")
            print(f"    F1: {f1:.4f}")
            metrics["precision"] = round(prec, 4)
            metrics["recall"] = round(rec, 4)
            metrics["f1"] = round(f1, 4)

    ALL_METRICS["anomaly_detector"] = metrics
    return feature_cols


def train_heart_rate_forecaster():
    """Train heart rate forecasting model using lag features."""
    print("\n" + "=" * 60)
    print("  TRAINING: Heart Rate Forecasting Model")
    print("=" * 60)

    df = prepare_heart_rate_forecasting_data()
    if df.empty:
        print("  SKIPPED: No training data available.")
        return

    feature_cols = [c for c in df.columns if c != "next_heart_rate"]
    X = df[feature_cols]
    y = df["next_heart_rate"]

    if len(X) < 100:
        print(f"  WARNING: Only {len(X)} samples available (< 100). Training anyway.")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Train RandomForest
    rf = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
    rf.fit(X_train, y_train)
    rf_pred = rf.predict(X_test)
    rf_rmse = np.sqrt(mean_squared_error(y_test, rf_pred))

    # Train GradientBoosting
    gb = GradientBoostingRegressor(n_estimators=100, random_state=42)
    gb.fit(X_train, y_train)
    gb_pred = gb.predict(X_test)
    gb_rmse = np.sqrt(mean_squared_error(y_test, gb_pred))

    # Select best
    if gb_rmse < rf_rmse:
        best_model = gb
        best_pred = gb_pred
        best_name = "GradientBoostingRegressor"
        best_rmse = gb_rmse
    else:
        best_model = rf
        best_pred = rf_pred
        best_name = "RandomForestRegressor"
        best_rmse = rf_rmse

    print(f"  Best model: {best_name} (RMSE={best_rmse:.4f})")

    # Save model
    model_path = os.path.join(MODELS_DIR, "heart_rate_forecaster.pkl")
    joblib.dump(best_model, model_path)
    print(f"  Saved: {model_path}")

    # Metrics
    mae = mean_absolute_error(y_test, best_pred)
    r2 = r2_score(y_test, best_pred)

    print(f"  MAE: {mae:.4f}")
    print(f"  RMSE: {best_rmse:.4f}")
    print(f"  R2: {r2:.4f}")

    ALL_METRICS["heart_rate_forecaster"] = {
        "algorithm": best_name,
        "mae": round(mae, 4),
        "rmse": round(best_rmse, 4),
        "r2": round(r2, 4),
        "feature_names": feature_cols,
        "n_samples": len(X),
    }

    return feature_cols


def build_and_save_preprocessing_pipeline():
    """Build and save the full preprocessing pipeline and feature schema."""
    print("\n" + "=" * 60)
    print("  BUILDING: Preprocessing Pipeline")
    print("=" * 60)

    # Define the standard feature set used across models
    numeric_cols = [
        "heart_rate", "spo2", "temperature", "systolic_bp", "diastolic_bp",
        "respiratory_rate", "glucose_level", "skin_temperature", "battery_level",
        "age", "weight", "height", "bmi", "steps", "sleep_duration",
    ]
    categorical_cols = [
        "gender", "activity_level", "exercise_type", "exercise_intensity",
        "stress_level", "sleep_quality", "chronic_condition", "smoker", "medication",
    ]
    boolean_cols = ["fall_detected"]

    all_features = numeric_cols + categorical_cols + boolean_cols

    # Build pipeline
    numeric_transformer = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
    ])

    categorical_transformer = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="constant", fill_value="Unknown")),
        ("encoder", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])

    boolean_transformer = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="constant", fill_value=0)),
    ])

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numeric_cols),
            ("cat", categorical_transformer, categorical_cols),
            ("bool", boolean_transformer, boolean_cols),
        ],
        remainder="drop",
    )

    # Fit on a dummy DataFrame to initialize the pipeline
    dummy_data = {}
    for col in numeric_cols:
        dummy_data[col] = [70.0, 80.0, 90.0]
    for col in categorical_cols:
        dummy_data[col] = ["Unknown", "Unknown", "Unknown"]
    for col in boolean_cols:
        dummy_data[col] = [0, 1, 0]

    dummy_df = pd.DataFrame(dummy_data)
    preprocessor.fit(dummy_df)

    # Save preprocessing pipeline
    pipeline_path = os.path.join(MODELS_DIR, "preprocessing_pipeline.pkl")
    joblib.dump(preprocessor, pipeline_path)
    print(f"  Saved: {pipeline_path}")

    # Save feature schema
    feature_schema = {
        "numeric_features": numeric_cols,
        "categorical_features": categorical_cols,
        "boolean_features": boolean_cols,
        "all_features": all_features,
        "feature_order": all_features,
    }
    schema_path = os.path.join(MODELS_DIR, "feature_schema.json")
    with open(schema_path, "w") as f:
        json.dump(feature_schema, f, indent=2)
    print(f"  Saved: {schema_path}")

    return all_features


def save_metadata():
    """Save model metadata and metrics."""
    metadata = {
        "model_version": "1.0.0",
        "training_date": datetime.utcnow().isoformat() + "Z",
        "python_version": sys.version,
        "dataset_sources": [
            "Synthetic_patient-HealthCare-Monitoring_dataset.csv",
            "human_vital_signs_dataset_2024.csv",
            "personal_health_data.csv",
            "patients_data_with_alerts.xlsx",
            "healthcare_iot_target_dataset_5000.csv",
            "heart_rate.csv",
            "Oxygen Dataset Final.csv",
            "Health data.csv",
        ],
        "models": {
            "status_classifier": "models/status_classifier.pkl",
            "risk_regressor": "models/risk_regressor.pkl",
            "anomaly_detector": "models/anomaly_detector.pkl",
            "heart_rate_forecaster": "models/heart_rate_forecaster.pkl",
            "preprocessing_pipeline": "models/preprocessing_pipeline.pkl",
        },
    }

    metadata_path = os.path.join(MODELS_DIR, "model_metadata.json")
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"  Saved: {metadata_path}")

    metrics_path = os.path.join(MODELS_DIR, "model_metrics.json")
    with open(metrics_path, "w") as f:
        json.dump(ALL_METRICS, f, indent=2)
    print(f"  Saved: {metrics_path}")


def main():
    print("=" * 60)
    print("  SMART HEALTH MONITORING IoT — ML MODEL TRAINING")
    print("=" * 60)
    print(f"  Started at: {datetime.utcnow().isoformat()}Z")
    print(f"  Models directory: {MODELS_DIR}")

    # Train all models
    train_status_classifier()
    train_risk_regressor()
    train_anomaly_detector()
    train_heart_rate_forecaster()

    # Build preprocessing pipeline
    build_and_save_preprocessing_pipeline()

    # Save metadata
    save_metadata()

    print("\n" + "=" * 60)
    print("  TRAINING COMPLETE")
    print("=" * 60)
    print(f"  Finished at: {datetime.utcnow().isoformat()}Z")
    print(f"  Models saved to: {MODELS_DIR}")
    print(f"  Metrics saved to: {os.path.join(MODELS_DIR, 'model_metrics.json')}")


if __name__ == "__main__":
    main()
