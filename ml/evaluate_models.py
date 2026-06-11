from __future__ import annotations

import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    precision_score,
    r2_score,
    recall_score,
)

try:
    from .model_utils import MODEL_FEATURES, STATUS_LABELS
    from .prepare_datasets import MODEL_DIR, build_forecasting_dataset, load_all_training_data
except ImportError:
    from model_utils import MODEL_FEATURES, STATUS_LABELS
    from prepare_datasets import MODEL_DIR, build_forecasting_dataset, load_all_training_data


def regression_metrics(y_true: pd.Series, y_pred: np.ndarray) -> dict[str, float]:
    return {
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "r2": float(r2_score(y_true, y_pred)),
    }


def load_model(name: str):
    path = MODEL_DIR / name
    if not path.exists():
        raise FileNotFoundError(f"Missing model artifact: {path}")
    return joblib.load(path)


def main() -> None:
    data = load_all_training_data()
    x = data[MODEL_FEATURES]
    metrics = {}

    status_model = load_model("status_classifier.pkl")
    status_pred = status_model.predict(x)
    status_true = data["target_status"].astype(str).str.upper()
    metrics["status_classifier"] = {
        "accuracy": float(accuracy_score(status_true, status_pred)),
        "precision_macro": float(precision_score(status_true, status_pred, average="macro", zero_division=0)),
        "recall_macro": float(recall_score(status_true, status_pred, average="macro", zero_division=0)),
        "f1_macro": float(f1_score(status_true, status_pred, average="macro", zero_division=0)),
        "classification_report": classification_report(status_true, status_pred, labels=STATUS_LABELS, zero_division=0, output_dict=True),
    }

    risk_model = load_model("risk_regressor.pkl")
    risk_true = pd.to_numeric(data["risk_score_target"], errors="coerce").fillna(50).clip(0, 100)
    risk_pred = np.clip(risk_model.predict(x), 0, 100)
    metrics["risk_regressor"] = regression_metrics(risk_true, risk_pred)

    anomaly_model = load_model("anomaly_detector.pkl")
    anomaly_true = pd.to_numeric(data["anomaly_label"], errors="coerce").fillna(0).astype(int)
    anomaly_pred = (anomaly_model.predict(x) == -1).astype(int)
    metrics["anomaly_detector"] = {
        "precision": float(precision_score(anomaly_true, anomaly_pred, zero_division=0)),
        "recall": float(recall_score(anomaly_true, anomaly_pred, zero_division=0)),
        "f1": float(f1_score(anomaly_true, anomaly_pred, zero_division=0)),
        "predicted_anomaly_rate": float(anomaly_pred.mean()),
    }

    forecast = build_forecasting_dataset()
    forecast_model = load_model("heart_rate_forecaster.pkl")
    forecast_features = ["hr_lag_1", "hr_lag_2", "hr_lag_3", "hr_lag_4", "hr_lag_5"]
    forecast_pred = forecast_model.predict(forecast[forecast_features])
    metrics["heart_rate_forecaster"] = regression_metrics(forecast["next_heart_rate"], forecast_pred)

    output_path = MODEL_DIR / "model_metrics_latest_eval.json"
    output_path.write_text(json.dumps(metrics, indent=2, ensure_ascii=True), encoding="utf-8")
    print(json.dumps(metrics, indent=2, ensure_ascii=True))
    print(f"Saved evaluation metrics to {output_path}")


if __name__ == "__main__":
    main()
