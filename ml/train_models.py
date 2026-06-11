"""
Model Training for Smart Health Monitoring IoT System

Trains four models:
1. Status Classification Model (RandomForest)
2. Risk Score Regression Model (RandomForest)
3. Anomaly Detection Model (IsolationForest)
4. Heart Rate Forecasting Model (RandomForest)

Also creates preprocessing pipelines and feature schemas.
"""

import pickle
import json
import logging
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor, IsolationForest
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder, LabelEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    confusion_matrix, mean_absolute_error, mean_squared_error, r2_score
)

from prepare_datasets import DatasetLoader
from model_utils import (
    CATEGORICAL_FEATURES,
    NUMERIC_FEATURES,
    derive_status,
    derive_risk_level,
    get_feature_schema,
    save_feature_schema,
    ALL_INPUT_FEATURES,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MODELS_PATH = Path(__file__).parent.parent / "models"


class SmartHealthModelTrainer:
    """Train and save models for the Smart Health Monitoring system."""

    def __init__(self, models_path: Path = MODELS_PATH):
        self.models_path = models_path
        self.models_path.mkdir(exist_ok=True)
        self.loader = DatasetLoader()
        self.metrics = {}

    def create_preprocessing_pipeline(self) -> Pipeline:
        """Create a preprocessing pipeline for the unified schema."""
        numeric_features = [f for f in NUMERIC_FEATURES if f != "fall_detected"]
        categorical_features = [f for f in CATEGORICAL_FEATURES if f in ALL_INPUT_FEATURES]

        numeric_transformer = Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
            ]
        )

        categorical_transformer = Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="constant", fill_value="Unknown")),
                ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
            ]
        )

        preprocessor = ColumnTransformer(
            transformers=[
                ("num", numeric_transformer, numeric_features),
                ("cat", categorical_transformer, categorical_features),
            ]
        )

        return preprocessor

    def train_status_classifier(self) -> Dict[str, Any]:
        """Train status classification model."""
        logger.info("=" * 80)
        logger.info("Training Status Classification Model")
        logger.info("=" * 80)

        try:
            X, y = self.loader.prepare_status_classification_data()
        except Exception as e:
            logger.error(f"Failed to prepare data: {e}")
            return {}

        if X.shape[0] < 10:
            logger.error("Insufficient training data for status classifier")
            return {}

        # Encode target
        le = LabelEncoder()
        y_encoded = le.fit_transform(y)

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y_encoded, test_size=0.2, random_state=42, stratify=y_encoded
        )

        # Create pipeline
        preprocessor = self.create_preprocessing_pipeline()
        pipeline = Pipeline([
            ("preprocessor", preprocessor),
            ("classifier", RandomForestClassifier(
                n_estimators=100,
                max_depth=15,
                min_samples_split=5,
                min_samples_leaf=2,
                random_state=42,
                n_jobs=-1,
            ))
        ])

        # Train
        pipeline.fit(X_train, y_train)

        # Evaluate
        y_pred = pipeline.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred, average="weighted", zero_division=0)
        recall = recall_score(y_test, y_pred, average="weighted", zero_division=0)
        f1 = f1_score(y_test, y_pred, average="weighted", zero_division=0)

        logger.info(f"Status Classifier Metrics:")
        logger.info(f"  Accuracy:  {accuracy:.4f}")
        logger.info(f"  Precision: {precision:.4f}")
        logger.info(f"  Recall:    {recall:.4f}")
        logger.info(f"  F1-Score:  {f1:.4f}")

        # Save model
        model_path = self.models_path / "status_classifier.pkl"
        with open(model_path, "wb") as f:
            pickle.dump(pipeline, f)
        logger.info(f"Saved to: {model_path}")

        # Save label encoder
        le_path = self.models_path / "status_label_encoder.pkl"
        with open(le_path, "wb") as f:
            pickle.dump(le, f)

        metrics = {
            "accuracy": float(accuracy),
            "precision": float(precision),
            "recall": float(recall),
            "f1_score": float(f1),
            "classes": list(le.classes_),
            "training_samples": len(X_train),
            "test_samples": len(X_test),
        }

        self.metrics["status_classifier"] = metrics
        return metrics

    def train_risk_regressor(self) -> Dict[str, Any]:
        """Train risk score regression model."""
        logger.info("=" * 80)
        logger.info("Training Risk Score Regression Model")
        logger.info("=" * 80)

        try:
            X, y = self.loader.prepare_risk_regression_data()
        except Exception as e:
            logger.error(f"Failed to prepare data: {e}")
            return {}

        if X.shape[0] < 10:
            logger.error("Insufficient training data for risk regressor")
            return {}

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        # Create pipeline
        preprocessor = self.create_preprocessing_pipeline()
        pipeline = Pipeline([
            ("preprocessor", preprocessor),
            ("regressor", RandomForestRegressor(
                n_estimators=100,
                max_depth=15,
                min_samples_split=5,
                min_samples_leaf=2,
                random_state=42,
                n_jobs=-1,
            ))
        ])

        # Train
        pipeline.fit(X_train, y_train)

        # Evaluate
        y_pred = pipeline.predict(X_test)
        mae = mean_absolute_error(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        r2 = r2_score(y_test, y_pred)

        logger.info(f"Risk Regressor Metrics:")
        logger.info(f"  MAE:  {mae:.4f}")
        logger.info(f"  RMSE: {rmse:.4f}")
        logger.info(f"  R²:   {r2:.4f}")

        # Save model
        model_path = self.models_path / "risk_regressor.pkl"
        with open(model_path, "wb") as f:
            pickle.dump(pipeline, f)
        logger.info(f"Saved to: {model_path}")

        metrics = {
            "mae": float(mae),
            "rmse": float(rmse),
            "r2_score": float(r2),
            "training_samples": len(X_train),
            "test_samples": len(X_test),
        }

        self.metrics["risk_regressor"] = metrics
        return metrics

    def train_anomaly_detector(self) -> Dict[str, Any]:
        """Train anomaly detection model using IsolationForest."""
        logger.info("=" * 80)
        logger.info("Training Anomaly Detection Model")
        logger.info("=" * 80)

        try:
            X = self.loader.prepare_anomaly_detection_data()
        except Exception as e:
            logger.error(f"Failed to prepare data: {e}")
            return {}

        if X.shape[0] < 10:
            logger.error("Insufficient training data for anomaly detector")
            return {}

        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        # Train IsolationForest
        model = IsolationForest(
            contamination=0.05,
            random_state=42,
            n_estimators=100,
        )
        y_pred = model.fit_predict(X_scaled)
        y_scores = model.score_samples(X_scaled)

        # Count anomalies
        n_anomalies = np.sum(y_pred == -1)
        anomaly_rate = n_anomalies / len(y_pred)

        logger.info(f"Anomaly Detector Metrics:")
        logger.info(f"  Anomalies Found: {n_anomalies} ({anomaly_rate*100:.2f}%)")
        logger.info(f"  Anomaly Scores: min={y_scores.min():.4f}, max={y_scores.max():.4f}")

        # Save model and scaler
        model_path = self.models_path / "anomaly_detector.pkl"
        with open(model_path, "wb") as f:
            pickle.dump(model, f)
        logger.info(f"Saved model to: {model_path}")

        scaler_path = self.models_path / "anomaly_scaler.pkl"
        with open(scaler_path, "wb") as f:
            pickle.dump(scaler, f)
        logger.info(f"Saved scaler to: {scaler_path}")

        metrics = {
            "anomalies_detected": int(n_anomalies),
            "anomaly_rate": float(anomaly_rate),
            "min_anomaly_score": float(y_scores.min()),
            "max_anomaly_score": float(y_scores.max()),
            "training_samples": len(X),
        }

        self.metrics["anomaly_detector"] = metrics
        return metrics

    def train_heart_rate_forecaster(self) -> Dict[str, Any]:
        """Train heart rate forecasting model."""
        logger.info("=" * 80)
        logger.info("Training Heart Rate Forecasting Model")
        logger.info("=" * 80)

        try:
            X, y = self.loader.prepare_heart_rate_forecast_data()
        except Exception as e:
            logger.error(f"Failed to prepare data: {e}")
            return {}

        if X.shape[0] < 10:
            logger.error("Insufficient training data for heart rate forecaster")
            return {}

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        # Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)

        # Train model
        model = RandomForestRegressor(
            n_estimators=100,
            max_depth=15,
            min_samples_split=5,
            min_samples_leaf=2,
            random_state=42,
            n_jobs=-1,
        )
        model.fit(X_train_scaled, y_train)

        # Evaluate
        y_pred = model.predict(X_test_scaled)
        mae = mean_absolute_error(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        r2 = r2_score(y_test, y_pred)

        logger.info(f"Heart Rate Forecaster Metrics:")
        logger.info(f"  MAE:  {mae:.4f}")
        logger.info(f"  RMSE: {rmse:.4f}")
        logger.info(f"  R²:   {r2:.4f}")

        # Save model and scaler
        model_path = self.models_path / "heart_rate_forecaster.pkl"
        with open(model_path, "wb") as f:
            pickle.dump(model, f)
        logger.info(f"Saved model to: {model_path}")

        scaler_path = self.models_path / "heart_rate_scaler.pkl"
        with open(scaler_path, "wb") as f:
            pickle.dump(scaler, f)
        logger.info(f"Saved scaler to: {scaler_path}")

        metrics = {
            "mae": float(mae),
            "rmse": float(rmse),
            "r2_score": float(r2),
            "training_samples": len(X_train),
            "test_samples": len(X_test),
        }

        self.metrics["heart_rate_forecaster"] = metrics
        return metrics

    def save_metadata(self):
        """Save model metadata and metrics."""
        metadata = {
            "models": {
                "status_classifier": {
                    "type": "classification",
                    "algorithm": "RandomForestClassifier",
                    "file": "status_classifier.pkl",
                    "input_features": ALL_INPUT_FEATURES,
                    "output": "predicted_status",
                    "classes": ["NORMAL", "WARNING", "CRITICAL", "EMERGENCY"],
                },
                "risk_regressor": {
                    "type": "regression",
                    "algorithm": "RandomForestRegressor",
                    "file": "risk_regressor.pkl",
                    "input_features": ALL_INPUT_FEATURES,
                    "output": "risk_score",
                    "range": [0, 100],
                },
                "anomaly_detector": {
                    "type": "anomaly_detection",
                    "algorithm": "IsolationForest",
                    "file": "anomaly_detector.pkl",
                    "scaler_file": "anomaly_scaler.pkl",
                    "input_features": ["heart_rate", "spo2", "temperature", "systolic_bp",
                                     "diastolic_bp", "respiratory_rate", "glucose_level", "skin_temperature"],
                    "output": ["is_anomaly", "anomaly_score"],
                },
                "heart_rate_forecaster": {
                    "type": "regression",
                    "algorithm": "RandomForestRegressor",
                    "file": "heart_rate_forecaster.pkl",
                    "scaler_file": "heart_rate_scaler.pkl",
                    "input_features": ["T1", "T2", "T3"],
                    "output": "predicted_next_heart_rate",
                },
            },
            "metrics": self.metrics,
            "feature_schema": get_feature_schema(),
        }

        metadata_path = self.models_path / "model_metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"Saved metadata to: {metadata_path}")

        # Save feature schema
        schema_path = self.models_path / "feature_schema.json"
        save_feature_schema(schema_path)
        logger.info(f"Saved feature schema to: {schema_path}")

        # Save metrics
        metrics_path = self.models_path / "model_metrics.json"
        with open(metrics_path, "w") as f:
            json.dump(self.metrics, f, indent=2)
        logger.info(f"Saved metrics to: {metrics_path}")

    def train_all_models(self):
        """Train all models."""
        logger.info("\n\n")
        logger.info("╔" + "=" * 78 + "╗")
        logger.info("║" + " " * 78 + "║")
        logger.info("║" + "SMART HEALTH MONITORING IoT - MODEL TRAINING".center(78) + "║")
        logger.info("║" + " " * 78 + "║")
        logger.info("╚" + "=" * 78 + "╝")
        logger.info("\n")

        self.train_status_classifier()
        self.train_risk_regressor()
        self.train_anomaly_detector()
        self.train_heart_rate_forecaster()

        self.save_metadata()

        logger.info("\n\n")
        logger.info("╔" + "=" * 78 + "╗")
        logger.info("║" + " " * 78 + "║")
        logger.info("║" + "TRAINING COMPLETE".center(78) + "║")
        logger.info("║" + " " * 78 + "║")
        logger.info("╚" + "=" * 78 + "╝")
        logger.info("\n")

        return self.metrics


if __name__ == "__main__":
    trainer = SmartHealthModelTrainer()
    trainer.train_all_models()
