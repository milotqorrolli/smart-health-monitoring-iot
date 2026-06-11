from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import IsolationForest, RandomForestClassifier, RandomForestRegressor
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
from sklearn.preprocessing import OneHotEncoder

try:
    from .model_utils import (
        CATEGORICAL_FEATURES,
        MODEL_FEATURES,
        MODEL_VERSION,
        NUMERIC_FEATURES,
        STATUS_LABELS,
        feature_schema_payload,
    )
    from .prepare_datasets import MODEL_DIR, build_forecasting_dataset, load_all_training_data
except ImportError:
    from model_utils import (
        CATEGORICAL_FEATURES,
        MODEL_FEATURES,
        MODEL_VERSION,
        NUMERIC_FEATURES,
        STATUS_LABELS,
        feature_schema_payload,
    )
    from prepare_datasets import MODEL_DIR, build_forecasting_dataset, load_all_training_data


def make_one_hot_encoder() -> OneHotEncoder:
    try:
        return OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    except TypeError:
        return OneHotEncoder(handle_unknown="ignore", sparse=False)


def make_preprocessor() -> ColumnTransformer:
    numeric_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
        ]
    )
    categorical_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", make_one_hot_encoder()),
        ]
    )
    return ColumnTransformer(
        transformers=[
            ("numeric", numeric_pipeline, NUMERIC_FEATURES),
            ("categorical", categorical_pipeline, CATEGORICAL_FEATURES),
        ],
        remainder="drop",
    )


def ensure_model_features(df: pd.DataFrame) -> pd.DataFrame:
    output = df.copy()
    for column in MODEL_FEATURES:
        if column not in output.columns:
            output[column] = np.nan
    return output[MODEL_FEATURES]


def regression_metrics(y_true: pd.Series, y_pred: np.ndarray) -> dict[str, float]:
    return {
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "r2": float(r2_score(y_true, y_pred)),
    }


def train_status_classifier(data: pd.DataFrame) -> tuple[Pipeline, dict[str, Any]]:
    x = ensure_model_features(data)
    y = data["target_status"].astype(str).str.upper()
    stratify = y if y.value_counts().min() >= 2 else None
    x_train, x_test, y_train, y_test = train_test_split(
        x,
        y,
        test_size=0.2,
        random_state=42,
        stratify=stratify,
    )
    model = Pipeline(
        steps=[
            ("preprocess", make_preprocessor()),
            (
                "model",
                RandomForestClassifier(
                    n_estimators=160,
                    random_state=42,
                    class_weight="balanced",
                    min_samples_leaf=2,
                    n_jobs=-1,
                ),
            ),
        ]
    )
    model.fit(x_train, y_train)
    y_pred = model.predict(x_test)
    metrics = {
        "accuracy": float(accuracy_score(y_test, y_pred)),
        "precision_macro": float(precision_score(y_test, y_pred, average="macro", zero_division=0)),
        "recall_macro": float(recall_score(y_test, y_pred, average="macro", zero_division=0)),
        "f1_macro": float(f1_score(y_test, y_pred, average="macro", zero_division=0)),
        "classification_report": classification_report(y_test, y_pred, labels=STATUS_LABELS, zero_division=0, output_dict=True),
        "confusion_matrix": confusion_matrix(y_test, y_pred, labels=STATUS_LABELS).tolist(),
        "labels": STATUS_LABELS,
    }
    return model, metrics


def train_risk_regressor(data: pd.DataFrame) -> tuple[Pipeline, dict[str, Any]]:
    x = ensure_model_features(data)
    y = pd.to_numeric(data["risk_score_target"], errors="coerce").fillna(50).clip(0, 100)
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=42)
    model = Pipeline(
        steps=[
            ("preprocess", make_preprocessor()),
            (
                "model",
                RandomForestRegressor(
                    n_estimators=180,
                    random_state=42,
                    min_samples_leaf=2,
                    n_jobs=-1,
                ),
            ),
        ]
    )
    model.fit(x_train, y_train)
    y_pred = np.clip(model.predict(x_test), 0, 100)
    return model, regression_metrics(y_test, y_pred)


def train_anomaly_detector(data: pd.DataFrame) -> tuple[Pipeline, dict[str, Any]]:
    x = ensure_model_features(data)
    y = pd.to_numeric(data.get("anomaly_label", 0), errors="coerce").fillna(0).astype(int)
    model = Pipeline(
        steps=[
            ("preprocess", make_preprocessor()),
            (
                "model",
                IsolationForest(
                    n_estimators=180,
                    contamination=0.08,
                    random_state=42,
                    n_jobs=-1,
                ),
            ),
        ]
    )
    model.fit(x)
    predictions = (model.predict(x) == -1).astype(int)
    metrics = {
        "precision": float(precision_score(y, predictions, zero_division=0)),
        "recall": float(recall_score(y, predictions, zero_division=0)),
        "f1": float(f1_score(y, predictions, zero_division=0)),
        "labeled_rows": int(y.notna().sum()),
        "predicted_anomaly_rate": float(predictions.mean()),
    }
    return model, metrics


def train_heart_rate_forecaster() -> tuple[Pipeline, dict[str, Any]]:
    forecast = build_forecasting_dataset()
    feature_columns = ["hr_lag_1", "hr_lag_2", "hr_lag_3", "hr_lag_4", "hr_lag_5"]
    x = forecast[feature_columns]
    y = forecast["next_heart_rate"]
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=42)
    model = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            (
                "model",
                RandomForestRegressor(
                    n_estimators=160,
                    random_state=42,
                    min_samples_leaf=2,
                    n_jobs=-1,
                ),
            ),
        ]
    )
    model.fit(x_train, y_train)
    y_pred = model.predict(x_test)
    return model, regression_metrics(y_test, y_pred)


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def main() -> None:
    MODEL_DIR.mkdir(exist_ok=True)
    print("Loading and preparing datasets...", flush=True)
    data = load_all_training_data()
    x = ensure_model_features(data)

    preprocessor = make_preprocessor()
    preprocessor.fit(x)
    joblib.dump(preprocessor, MODEL_DIR / "preprocessing_pipeline.pkl")

    print("Training status classifier...", flush=True)
    status_model, status_metrics = train_status_classifier(data)
    joblib.dump(status_model, MODEL_DIR / "status_classifier.pkl")

    print("Training risk regressor...", flush=True)
    risk_model, risk_metrics = train_risk_regressor(data)
    joblib.dump(risk_model, MODEL_DIR / "risk_regressor.pkl")

    print("Training anomaly detector...", flush=True)
    anomaly_model, anomaly_metrics = train_anomaly_detector(data)
    joblib.dump(anomaly_model, MODEL_DIR / "anomaly_detector.pkl")

    print("Training heart-rate forecaster...", flush=True)
    forecast_model, forecast_metrics = train_heart_rate_forecaster()
    joblib.dump(forecast_model, MODEL_DIR / "heart_rate_forecaster.pkl")

    metrics = {
        "model_version": MODEL_VERSION,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "training_rows": int(len(data)),
        "status_classifier": status_metrics,
        "risk_regressor": risk_metrics,
        "anomaly_detector": anomaly_metrics,
        "heart_rate_forecaster": forecast_metrics,
    }
    metadata = {
        "model_version": MODEL_VERSION,
        "trained_at": metrics["trained_at"],
        "artifacts": {
            "status_classifier": "status_classifier.pkl",
            "risk_regressor": "risk_regressor.pkl",
            "anomaly_detector": "anomaly_detector.pkl",
            "heart_rate_forecaster": "heart_rate_forecaster.pkl",
            "preprocessing_pipeline": "preprocessing_pipeline.pkl",
        },
        "training_rows": int(len(data)),
        "source_datasets": sorted(data["source_dataset"].dropna().unique().tolist()),
        "medical_disclaimer": "Educational IoT simulation only. Not for diagnosis or clinical use.",
    }
    save_json(MODEL_DIR / "model_metrics.json", metrics)
    save_json(MODEL_DIR / "model_metadata.json", metadata)
    save_json(MODEL_DIR / "feature_schema.json", feature_schema_payload())

    print(json.dumps(metrics, indent=2, ensure_ascii=True), flush=True)
    print(f"Saved model artifacts to {MODEL_DIR}", flush=True)


if __name__ == "__main__":
    main()
