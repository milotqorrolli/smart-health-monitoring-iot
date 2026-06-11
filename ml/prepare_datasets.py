"""
Dataset Preparation for Smart Health Monitoring IoT System

Loads, cleans, and prepares datasets for model training.
Maps all datasets to the unified schema.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Tuple, Optional
import logging

from model_utils import (
    derive_status,
    ALL_INPUT_FEATURES,
    get_default_values,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATASETS_PATH = Path(__file__).parent.parent / "datasets"


class DatasetLoader:
    """Load and prepare datasets for model training."""

    def __init__(self, datasets_path: Path = DATASETS_PATH):
        self.datasets_path = datasets_path
        self.default_values = get_default_values()

    def load_synthetic_patient_dataset(self) -> pd.DataFrame:
        """Load Synthetic_patient-HealthCare-Monitoring_dataset.csv"""
        path = self.datasets_path / "Synthetic_patient-HealthCare-Monitoring_dataset.csv"
        df = pd.read_csv(path)
        logger.info(f"Loaded Synthetic Patient Dataset: {df.shape}")

        # Rename columns to match unified schema
        df = df.rename(
            columns={
                "Patient Number": "patient_id",
                "Heart Rate (bpm)": "heart_rate",
                "SpO2 Level (%)": "spo2",
                "Systolic Blood Pressure (mmHg)": "systolic_bp",
                "Diastolic Blood Pressure (mmHg)": "diastolic_bp",
                "Body Temperature (°C)": "temperature",
                "Fall Detection": "fall_detected",
                "Predicted Disease": "predicted_disease_simulated",
                "Data Accuracy (%)": "data_accuracy",
            }
        )

        # Create unified schema columns
        df = self._create_unified_columns(df)

        # Derive status from vital signs if not present
        if "status" not in df.columns:
            df["status"] = df.apply(derive_status, axis=1)

        logger.info(f"Processed Synthetic Patient Dataset: {df.shape}")
        return df

    def load_human_vital_signs_dataset(self) -> pd.DataFrame:
        """Load human_vital_signs_dataset_2024.csv"""
        path = self.datasets_path / "human_vital_signs_dataset_2024.csv"
        df = pd.read_csv(path)
        logger.info(f"Loaded Human Vital Signs Dataset: {df.shape}")

        # Rename columns
        column_mapping = {
            "Heart Rate": "heart_rate",
            "Respiratory Rate": "respiratory_rate",
            "Body Temperature": "temperature",
            "Oxygen Saturation": "spo2",
            "Systolic Blood Pressure": "systolic_bp",
            "Diastolic Blood Pressure": "diastolic_bp",
            "Age": "age",
            "Gender": "gender",
            "Weight": "weight",
            "Height": "height",
            "Derived_BMI": "bmi",
            "Risk Category": "risk_category",
        }

        for old, new in column_mapping.items():
            if old in df.columns:
                df = df.rename(columns={old: new})

        # Map risk category to numeric risk score
        if "risk_category" in df.columns:
            risk_mapping = {
                "Low Risk": 25,
                "Medium Risk": 55,
                "High Risk": 80,
                "Critical Risk": 95,
            }
            df["risk_score"] = df["risk_category"].map(risk_mapping)
            df["risk_score"] = df["risk_score"].fillna(50)
        else:
            df["risk_score"] = 50

        # Create unified schema
        df = self._create_unified_columns(df)

        # Derive status
        if "status" not in df.columns:
            df["status"] = df.apply(derive_status, axis=1)

        logger.info(f"Processed Human Vital Signs Dataset: {df.shape}")
        return df

    def load_personal_health_data(self) -> pd.DataFrame:
        """Load personal_health_data.csv"""
        path = self.datasets_path / "personal_health_data.csv"
        df = pd.read_csv(path)
        logger.info(f"Loaded Personal Health Data: {df.shape}")

        # Rename columns
        column_mapping = {
            "User_ID": "patient_id",
            "Health_Score": "health_score",
            "Anomaly_Flag": "anomaly_flag",
            "Sleep": "sleep_duration",
            "Stress": "stress_level",
            "BloodOxygen": "spo2",
        }

        for old, new in column_mapping.items():
            if old in df.columns:
                df = df.rename(columns={old: new})

        # Compute risk score from health score
        if "health_score" in df.columns:
            df["risk_score"] = 100 - df["health_score"]
        else:
            df["risk_score"] = 50

        # Create unified schema
        df = self._create_unified_columns(df)

        logger.info(f"Processed Personal Health Data: {df.shape}")
        return df

    def load_healthcare_iot_target_dataset(self) -> pd.DataFrame:
        """Load healthcare_iot_target_dataset_5000.csv"""
        path = self.datasets_path / "healthcare_iot_target_dataset_5000.csv"
        df = pd.read_csv(path)
        logger.info(f"Loaded Healthcare IoT Target Dataset: {df.shape}")

        # Map columns
        column_mapping = {
            "patient_id": "patient_id",
            "target_blood_pressure_systolic": "systolic_bp",
            "target_blood_pressure_diastolic": "diastolic_bp",
            "target_heart_rate": "heart_rate",
            "battery_level": "battery_level",
            "sensor_type": "sensor_type",
        }

        for old, new in column_mapping.items():
            if old in df.columns:
                df = df.rename(columns={old: new})

        # Create unified schema
        df = self._create_unified_columns(df)

        logger.info(f"Processed Healthcare IoT Target Dataset: {df.shape}")
        return df

    def load_heart_rate_forecast_data(self) -> pd.DataFrame:
        """Load heart_rate.csv for time series forecasting."""
        path = self.datasets_path / "heart_rate.csv"
        df = pd.read_csv(path)
        logger.info(f"Loaded Heart Rate Data: {df.shape}")

        # Create lag features
        for i in range(1, 5):
            col_name = f"T{i}" if f"T{i}" in df.columns else f"hr_lag_{i}"
            if col_name not in df.columns and f"T{i}" in df.columns:
                df = df.rename(columns={f"T{i}": col_name})

        logger.info(f"Processed Heart Rate Data: {df.shape}")
        return df

    def _create_unified_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure all unified schema columns exist with appropriate defaults."""
        for feature in ALL_INPUT_FEATURES:
            if feature not in df.columns:
                df[feature] = self.default_values.get(feature)

        # Add patient_id if not exists
        if "patient_id" not in df.columns:
            df["patient_id"] = f"patient-{np.arange(len(df)) % 5 + 1}"

        return df

    def prepare_status_classification_data(self) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Prepare training data for status classification.
        Returns X (features) and y (target status).
        """
        dfs = []

        # Load primary datasets
        try:
            dfs.append(self.load_synthetic_patient_dataset())
        except Exception as e:
            logger.warning(f"Could not load Synthetic Patient Dataset: {e}")

        try:
            dfs.append(self.load_human_vital_signs_dataset())
        except Exception as e:
            logger.warning(f"Could not load Human Vital Signs Dataset: {e}")

        # Combine datasets
        df = pd.concat(dfs, ignore_index=True)
        df = df.dropna(subset=["status"])

        logger.info(f"Combined training data for status classification: {df.shape}")

        # Select input features
        X = df[ALL_INPUT_FEATURES].copy()
        y = df["status"].copy()

        # Fill missing values
        for col in X.columns:
            if col in ["gender", "activity_level", "exercise_type", "stress_level", "chronic_condition"]:
                X[col] = X[col].fillna("Unknown")
            else:
                X[col] = X[col].fillna(self.default_values.get(col, 0))

        logger.info(f"Status classification data: X={X.shape}, y={y.shape}")
        return X, y

    def prepare_risk_regression_data(self) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Prepare training data for risk score regression.
        Returns X (features) and y (risk_score).
        """
        dfs = []

        try:
            dfs.append(self.load_personal_health_data())
        except Exception as e:
            logger.warning(f"Could not load Personal Health Data: {e}")

        try:
            dfs.append(self.load_human_vital_signs_dataset())
        except Exception as e:
            logger.warning(f"Could not load Human Vital Signs Dataset: {e}")

        try:
            dfs.append(self.load_healthcare_iot_target_dataset())
        except Exception as e:
            logger.warning(f"Could not load Healthcare IoT Target Dataset: {e}")

        df = pd.concat(dfs, ignore_index=True)
        df = df.dropna(subset=["risk_score"])

        logger.info(f"Combined training data for risk regression: {df.shape}")

        X = df[ALL_INPUT_FEATURES].copy()
        y = df["risk_score"].copy()

        # Fill missing values
        for col in X.columns:
            if col in ["gender", "activity_level", "exercise_type", "stress_level", "chronic_condition"]:
                X[col] = X[col].fillna("Unknown")
            else:
                X[col] = X[col].fillna(self.default_values.get(col, 0))

        # Ensure risk_score is in valid range
        y = y.clip(0, 100)

        logger.info(f"Risk regression data: X={X.shape}, y={y.shape}")
        return X, y

    def prepare_anomaly_detection_data(self) -> pd.DataFrame:
        """
        Prepare training data for anomaly detection.
        Returns X with both normal and anomalous samples.
        """
        dfs = []

        try:
            dfs.append(self.load_personal_health_data())
        except Exception as e:
            logger.warning(f"Could not load Personal Health Data: {e}")

        try:
            dfs.append(self.load_synthetic_patient_dataset())
        except Exception as e:
            logger.warning(f"Could not load Synthetic Patient Dataset: {e}")

        df = pd.concat(dfs, ignore_index=True)

        # Select vital features for anomaly detection
        vital_cols = [col for col in VITAL_FEATURES if col in df.columns]
        X = df[vital_cols].copy()

        # Fill missing values
        for col in X.columns:
            X[col] = X[col].fillna(self.default_values.get(col, 0))

        logger.info(f"Anomaly detection data: X={X.shape}")
        return X

    def prepare_heart_rate_forecast_data(self) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Prepare training data for heart rate forecasting.
        Creates lag features and target variable.
        """
        try:
            df = self.load_heart_rate_forecast_data()
        except Exception as e:
            logger.warning(f"Could not load Heart Rate Data: {e}")
            # Create synthetic data if loading fails
            df = pd.DataFrame({
                "T1": np.random.randint(60, 100, 100),
                "T2": np.random.randint(60, 100, 100),
                "T3": np.random.randint(60, 100, 100),
                "T4": np.random.randint(60, 100, 100),
            })

        # Use T1-T3 as features, T4 as target
        X = df[["T1", "T2", "T3"]].copy()
        y = df["T4"].copy() if "T4" in df.columns else df.iloc[:, -1].copy()

        # Fill any missing values
        X = X.fillna(X.mean())
        y = y.fillna(y.mean())

        logger.info(f"Heart rate forecast data: X={X.shape}, y={y.shape}")
        return X, y


if __name__ == "__main__":
    loader = DatasetLoader()

    # Test loading datasets
    print("\n" + "=" * 80)
    print("Testing Dataset Loading")
    print("=" * 80 + "\n")

    try:
        X, y = loader.prepare_status_classification_data()
        print(f"Status Classification Data: {X.shape}, target: {y.shape}")
        print(f"  Classes: {y.unique()}")
    except Exception as e:
        print(f"Status Classification Error: {e}")

    try:
        X, y = loader.prepare_risk_regression_data()
        print(f"Risk Regression Data: {X.shape}, target: {y.shape}")
        print(f"  Risk Score Range: {y.min():.2f} - {y.max():.2f}")
    except Exception as e:
        print(f"Risk Regression Error: {e}")

    try:
        X = loader.prepare_anomaly_detection_data()
        print(f"Anomaly Detection Data: {X.shape}")
    except Exception as e:
        print(f"Anomaly Detection Error: {e}")

    try:
        X, y = loader.prepare_heart_rate_forecast_data()
        print(f"Heart Rate Forecast Data: {X.shape}, target: {y.shape}")
        print(f"  Heart Rate Range: {y.min():.0f} - {y.max():.0f}")
    except Exception as e:
        print(f"Heart Rate Forecast Error: {e}")

    print("\n" + "=" * 80)
