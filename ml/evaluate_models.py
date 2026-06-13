"""
Model Evaluation Script for Smart Health Monitoring IoT System.
Loads trained models and prints comprehensive evaluation metrics.
"""

import json
import os
import sys

import joblib
import numpy as np
import pandas as pd
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

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from prepare_datasets import (
    prepare_anomaly_detection_data,
    prepare_heart_rate_forecasting_data,
    prepare_risk_regression_data,
    prepare_status_classification_data,
)

MODELS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "models")


def evaluate_status_classifier():
    """Evaluate the status classification model."""
    print("\n" + "=" * 60)
    print("  EVALUATION: Status Classifier")
    print("=" * 60)

    model_path = os.path.join(MODELS_DIR, "status_classifier.pkl")
    if not os.path.exists(model_path):
        print("  Model not found. Run train_models.py first.")
        return

    model = joblib.load(model_path)
    df = prepare_status_classification_data()
    if df.empty:
        return

    feature_cols = [c for c in df.columns if c != "status"]
    X = df[feature_cols]
    y = df["status"]

    predictions = model.predict(X)

    print(f"  Accuracy: {accuracy_score(y, predictions):.4f}")
    print(f"  Precision (macro): {precision_score(y, predictions, average='macro', zero_division=0):.4f}")
    print(f"  Recall (macro): {recall_score(y, predictions, average='macro', zero_division=0):.4f}")
    print(f"  F1 (macro): {f1_score(y, predictions, average='macro', zero_division=0):.4f}")
    print(f"\n  Confusion Matrix:\n{confusion_matrix(y, predictions)}")
    print(f"\n  Classification Report:\n{classification_report(y, predictions, zero_division=0)}")


def evaluate_risk_regressor():
    """Evaluate the risk regression model."""
    print("\n" + "=" * 60)
    print("  EVALUATION: Risk Regressor")
    print("=" * 60)

    model_path = os.path.join(MODELS_DIR, "risk_regressor.pkl")
    if not os.path.exists(model_path):
        print("  Model not found. Run train_models.py first.")
        return

    model = joblib.load(model_path)
    df = prepare_risk_regression_data()
    if df.empty:
        return

    feature_cols = [c for c in df.columns if c != "risk_score"]
    X = df[feature_cols]
    y = df["risk_score"]

    predictions = model.predict(X)

    mae = mean_absolute_error(y, predictions)
    rmse = np.sqrt(mean_squared_error(y, predictions))
    r2 = r2_score(y, predictions)

    print(f"  MAE: {mae:.4f}")
    print(f"  RMSE: {rmse:.4f}")
    print(f"  R2: {r2:.4f}")


def evaluate_anomaly_detector():
    """Evaluate the anomaly detection model."""
    print("\n" + "=" * 60)
    print("  EVALUATION: Anomaly Detector")
    print("=" * 60)

    model_path = os.path.join(MODELS_DIR, "anomaly_detector.pkl")
    if not os.path.exists(model_path):
        print("  Model not found. Run train_models.py first.")
        return

    model = joblib.load(model_path)
    df, labels = prepare_anomaly_detection_data()
    if df.empty:
        return

    predictions = model.predict(df)
    n_anomalies = (predictions == -1).sum()

    print(f"  Total samples: {len(predictions)}")
    print(f"  Anomalies detected: {n_anomalies}")
    print(f"  Contamination ratio: {n_anomalies / len(predictions):.4f}")

    if labels is not None and labels.sum() > 0:
        iso_labels = (predictions == -1).astype(int)
        labels_binary = labels.values[:len(iso_labels)].astype(int)
        print(f"  Precision: {precision_score(labels_binary, iso_labels, zero_division=0):.4f}")
        print(f"  Recall: {recall_score(labels_binary, iso_labels, zero_division=0):.4f}")
        print(f"  F1: {f1_score(labels_binary, iso_labels, zero_division=0):.4f}")


def evaluate_heart_rate_forecaster():
    """Evaluate the heart rate forecasting model."""
    print("\n" + "=" * 60)
    print("  EVALUATION: Heart Rate Forecaster")
    print("=" * 60)

    model_path = os.path.join(MODELS_DIR, "heart_rate_forecaster.pkl")
    if not os.path.exists(model_path):
        print("  Model not found. Run train_models.py first.")
        return

    model = joblib.load(model_path)
    df = prepare_heart_rate_forecasting_data()
    if df.empty:
        return

    feature_cols = [c for c in df.columns if c != "next_heart_rate"]
    X = df[feature_cols]
    y = df["next_heart_rate"]

    predictions = model.predict(X)

    mae = mean_absolute_error(y, predictions)
    rmse = np.sqrt(mean_squared_error(y, predictions))
    r2 = r2_score(y, predictions)

    print(f"  MAE: {mae:.4f}")
    print(f"  RMSE: {rmse:.4f}")
    print(f"  R2: {r2:.4f}")


def main():
    print("=" * 60)
    print("  SMART HEALTH MONITORING IoT — MODEL EVALUATION")
    print("=" * 60)

    # Load metrics file if available
    metrics_path = os.path.join(MODELS_DIR, "model_metrics.json")
    if os.path.exists(metrics_path):
        with open(metrics_path) as f:
            metrics = json.load(f)
        print(f"\n  Saved metrics from training:")
        print(json.dumps(metrics, indent=2))

    evaluate_status_classifier()
    evaluate_risk_regressor()
    evaluate_anomaly_detector()
    evaluate_heart_rate_forecaster()

    print("\n" + "=" * 60)
    print("  EVALUATION COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
