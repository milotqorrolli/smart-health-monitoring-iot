"""
Dataset preparation pipeline for Smart Health Monitoring IoT System.
Loads, cleans, normalizes, and merges selected datasets into unified training DataFrames.
"""

import os
import sys
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

DATASETS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "datasets")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from model_utils import derive_status, derive_risk_score_from_status


def load_synthetic_patient_monitoring():
    """Load and map Synthetic_patient-HealthCare-Monitoring_dataset.csv"""
    filepath = os.path.join(DATASETS_DIR, "Synthetic_patient-HealthCare-Monitoring_dataset.csv")
    if not os.path.exists(filepath):
        print(f"  WARNING: {filepath} not found")
        return pd.DataFrame()

    df = pd.read_csv(filepath)
    print(f"  Loaded Synthetic_patient-HealthCare-Monitoring: {df.shape}")

    mapped = pd.DataFrame()
    mapped["heart_rate"] = pd.to_numeric(df.get("Heart Rate (bpm)"), errors="coerce")
    mapped["spo2"] = pd.to_numeric(df.get("SpO2 Level (%)"), errors="coerce")
    mapped["systolic_bp"] = pd.to_numeric(df.get("Systolic Blood Pressure (mmHg)"), errors="coerce")
    mapped["diastolic_bp"] = pd.to_numeric(df.get("Diastolic Blood Pressure (mmHg)"), errors="coerce")
    mapped["temperature"] = pd.to_numeric(df.get("Body Temperature (°C)"), errors="coerce")

    # Map fall detection
    fall_col = df.get("Fall Detection")
    if fall_col is not None:
        mapped["fall_detected"] = fall_col.map({"Yes": True, "No": False}).fillna(False).astype(int)
    else:
        mapped["fall_detected"] = 0

    # Normalize alert labels to uppercase
    for col in ["Heart Rate Alert", "SpO2 Level Alert", "Blood Pressure Alert", "Temperature Alert"]:
        if col in df.columns:
            df[col] = df[col].str.upper()

    # Derive status
    mapped["respiratory_rate"] = np.nan
    mapped["glucose_level"] = np.nan
    mapped["status"] = mapped.apply(lambda r: derive_status(r.to_dict()), axis=1)
    mapped["source"] = "synthetic_patient_monitoring"

    return mapped


def load_human_vital_signs():
    """Load and map human_vital_signs_dataset_2024.csv"""
    filepath = os.path.join(DATASETS_DIR, "human_vital_signs_dataset_2024.csv")
    if not os.path.exists(filepath):
        print(f"  WARNING: {filepath} not found")
        return pd.DataFrame()

    df = pd.read_csv(filepath)
    print(f"  Loaded human_vital_signs_2024: {df.shape}")

    mapped = pd.DataFrame()
    mapped["heart_rate"] = pd.to_numeric(df.get("Heart Rate"), errors="coerce")
    mapped["respiratory_rate"] = pd.to_numeric(df.get("Respiratory Rate"), errors="coerce")
    mapped["temperature"] = pd.to_numeric(df.get("Body Temperature"), errors="coerce")
    mapped["spo2"] = pd.to_numeric(df.get("Oxygen Saturation"), errors="coerce")
    mapped["systolic_bp"] = pd.to_numeric(df.get("Systolic Blood Pressure"), errors="coerce")
    mapped["diastolic_bp"] = pd.to_numeric(df.get("Diastolic Blood Pressure"), errors="coerce")
    mapped["age"] = pd.to_numeric(df.get("Age"), errors="coerce")
    mapped["gender"] = df.get("Gender")
    mapped["weight"] = pd.to_numeric(df.get("Weight (kg)"), errors="coerce")
    mapped["height"] = pd.to_numeric(df.get("Height (m)"), errors="coerce")
    mapped["bmi"] = pd.to_numeric(df.get("Derived_BMI"), errors="coerce")
    mapped["fall_detected"] = 0
    mapped["glucose_level"] = np.nan

    # Map Risk Category to risk_score
    risk_map = {"Low Risk": 25, "Medium Risk": 55, "High Risk": 80, "Critical Risk": 95}
    mapped["risk_score"] = df.get("Risk Category", pd.Series()).map(risk_map)

    # Derive status
    mapped["status"] = mapped.apply(lambda r: derive_status(r.to_dict()), axis=1)
    mapped["source"] = "human_vital_signs"

    return mapped


def load_personal_health_data():
    """Load and map personal_health_data.csv"""
    filepath = os.path.join(DATASETS_DIR, "personal_health_data.csv")
    if not os.path.exists(filepath):
        print(f"  WARNING: {filepath} not found")
        return pd.DataFrame()

    df = pd.read_csv(filepath)
    print(f"  Loaded personal_health_data: {df.shape}")

    mapped = pd.DataFrame()
    mapped["heart_rate"] = pd.to_numeric(df.get("Heart_Rate"), errors="coerce")
    mapped["spo2"] = pd.to_numeric(df.get("Blood_Oxygen_Level"), errors="coerce")
    mapped["skin_temperature"] = pd.to_numeric(df.get("Skin_Temperature"), errors="coerce")
    mapped["sleep_duration"] = pd.to_numeric(df.get("Sleep_Duration"), errors="coerce")
    mapped["stress_level"] = df.get("Stress_Level")
    mapped["chronic_condition"] = df.get("Medical_Conditions")
    mapped["age"] = pd.to_numeric(df.get("Age"), errors="coerce")
    mapped["gender"] = df.get("Gender")
    mapped["weight"] = pd.to_numeric(df.get("Weight"), errors="coerce")

    # Height normalization: if > 3.0, it's in centimeters
    height = pd.to_numeric(df.get("Height"), errors="coerce")
    if height is not None and height.max() > 3.0:
        height = height / 100.0
        print("  NOTE: Height converted from cm to meters")
    mapped["height"] = height

    # Risk score from Health_Score
    health_score = pd.to_numeric(df.get("Health_Score"), errors="coerce")
    mapped["risk_score"] = 100 - health_score

    # Anomaly flag
    mapped["anomaly_flag"] = pd.to_numeric(df.get("Anomaly_Flag"), errors="coerce")

    mapped["systolic_bp"] = np.nan
    mapped["diastolic_bp"] = np.nan
    mapped["temperature"] = mapped["skin_temperature"]
    mapped["respiratory_rate"] = np.nan
    mapped["glucose_level"] = np.nan
    mapped["fall_detected"] = 0

    mapped["status"] = mapped.apply(lambda r: derive_status(r.to_dict()), axis=1)
    mapped["source"] = "personal_health_data"

    return mapped


def load_patients_data_with_alerts():
    """Load and map patients_data_with_alerts.xlsx"""
    filepath = os.path.join(DATASETS_DIR, "patients_data_with_alerts.xlsx")
    if not os.path.exists(filepath):
        print(f"  WARNING: {filepath} not found")
        return pd.DataFrame()

    try:
        df = pd.read_excel(filepath)
    except Exception as e:
        print(f"  WARNING: Could not read xlsx file: {e}")
        return pd.DataFrame()

    print(f"  Loaded patients_data_with_alerts: {df.shape}")

    mapped = pd.DataFrame()
    mapped["heart_rate"] = pd.to_numeric(df.get("Heart Rate (bpm)"), errors="coerce")
    mapped["spo2"] = pd.to_numeric(df.get("SpO2 Level (%)"), errors="coerce")
    mapped["systolic_bp"] = pd.to_numeric(df.get("Systolic Blood Pressure (mmHg)"), errors="coerce")
    mapped["diastolic_bp"] = pd.to_numeric(df.get("Diastolic Blood Pressure (mmHg)"), errors="coerce")
    mapped["temperature"] = pd.to_numeric(df.get("Body Temperature (°C)"), errors="coerce")

    # Normalize alert labels to uppercase
    alert_cols = ["Heart Rate Alert", "SpO2 Level Alert", "Blood Pressure Alert", "Temperature Alert"]
    for col in alert_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()
            df[col] = df[col].replace({"NORMAL": "NORMAL", "ABNORMAL": "ABNORMAL", "LOW": "LOW", "HIGH": "HIGH"})

    fall_col = df.get("Fall Detection")
    if fall_col is not None:
        mapped["fall_detected"] = fall_col.map({"Yes": True, "No": False}).fillna(False).astype(int)
    else:
        mapped["fall_detected"] = 0

    mapped["respiratory_rate"] = np.nan
    mapped["glucose_level"] = np.nan
    mapped["status"] = mapped.apply(lambda r: derive_status(r.to_dict()), axis=1)
    mapped["source"] = "patients_data_with_alerts"

    return mapped


def load_healthcare_iot_target():
    """Load and map healthcare_iot_target_dataset_5000.csv"""
    filepath = os.path.join(DATASETS_DIR, "healthcare_iot_target_dataset_5000.csv")
    if not os.path.exists(filepath):
        print(f"  WARNING: {filepath} not found")
        return pd.DataFrame()

    df = pd.read_csv(filepath)
    print(f"  Loaded healthcare_iot_target: {df.shape}")

    mapped = pd.DataFrame()
    mapped["temperature"] = pd.to_numeric(df.get("Temperature (°C)"), errors="coerce")
    mapped["systolic_bp"] = pd.to_numeric(df.get("Systolic_BP (mmHg)"), errors="coerce")
    mapped["diastolic_bp"] = pd.to_numeric(df.get("Diastolic_BP (mmHg)"), errors="coerce")
    mapped["heart_rate"] = pd.to_numeric(df.get("Heart_Rate (bpm)"), errors="coerce")
    mapped["battery_level"] = pd.to_numeric(df.get("Device_Battery_Level (%)"), errors="coerce")

    # Map Target_Health_Status
    status_map = {"Healthy": "NORMAL", "Unhealthy": "WARNING"}
    mapped["target_status"] = df.get("Target_Health_Status", pd.Series()).map(status_map).fillna("NORMAL")

    # Risk score from status
    risk_map_iot = {"Healthy": 25, "Unhealthy": 75}
    mapped["risk_score"] = df.get("Target_Health_Status", pd.Series()).map(risk_map_iot)

    mapped["spo2"] = np.nan
    mapped["respiratory_rate"] = np.nan
    mapped["glucose_level"] = np.nan
    mapped["fall_detected"] = 0

    # Derive actual status using thresholds (may upgrade from WARNING)
    mapped["status"] = mapped.apply(lambda r: derive_status(r.to_dict()), axis=1)
    mapped["source"] = "healthcare_iot_target"

    return mapped


def load_oxygen_dataset():
    """Load and map Oxygen Dataset Final.csv with imputation."""
    filepath = os.path.join(DATASETS_DIR, "Oxygen Dataset Final.csv")
    if not os.path.exists(filepath):
        print(f"  WARNING: {filepath} not found")
        return pd.DataFrame()

    df = pd.read_csv(filepath)
    print(f"  Loaded Oxygen Dataset Final: {df.shape}")

    from sklearn.impute import SimpleImputer

    # Apply median imputation for numeric columns
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    imputer = SimpleImputer(strategy="median")
    df[numeric_cols] = imputer.fit_transform(df[numeric_cols])

    mapped = pd.DataFrame()
    mapped["spo2"] = pd.to_numeric(df.get("spo2"), errors="coerce")
    mapped["heart_rate"] = pd.to_numeric(df.get("pr"), errors="coerce")  # pulse rate as proxy
    mapped["oxy_flow"] = pd.to_numeric(df.get("oxy_flow"), errors="coerce")

    mapped["temperature"] = np.nan
    mapped["systolic_bp"] = np.nan
    mapped["diastolic_bp"] = np.nan
    mapped["respiratory_rate"] = np.nan
    mapped["glucose_level"] = np.nan
    mapped["fall_detected"] = 0
    mapped["skin_temperature"] = np.nan
    mapped["battery_level"] = np.nan

    mapped["status"] = mapped.apply(lambda r: derive_status(r.to_dict()), axis=1)
    mapped["source"] = "oxygen_dataset"

    return mapped


def load_health_data():
    """Load and map Health data.csv"""
    filepath = os.path.join(DATASETS_DIR, "Health data.csv")
    if not os.path.exists(filepath):
        print(f"  WARNING: {filepath} not found")
        return pd.DataFrame()

    df = pd.read_csv(filepath)
    print(f"  Loaded Health data: {df.shape}")

    mapped = pd.DataFrame()

    # Try different possible column names
    for col_name in ["Pulse", "pulse", "Heart Rate", "heart_rate"]:
        if col_name in df.columns:
            mapped["heart_rate"] = pd.to_numeric(df[col_name], errors="coerce")
            break
    if "heart_rate" not in mapped.columns:
        mapped["heart_rate"] = np.nan

    for col_name in ["Body Temperature", "body_temperature", "Temperature", "temperature"]:
        if col_name in df.columns:
            mapped["temperature"] = pd.to_numeric(df[col_name], errors="coerce")
            break
    if "temperature" not in mapped.columns:
        mapped["temperature"] = np.nan

    for col_name in ["SpO2", "spo2", "Oxygen Saturation"]:
        if col_name in df.columns:
            mapped["spo2"] = pd.to_numeric(df[col_name], errors="coerce")
            break
    if "spo2" not in mapped.columns:
        mapped["spo2"] = np.nan

    # Status mapping
    for col_name in ["Status", "status"]:
        if col_name in df.columns:
            mapped["original_status"] = df[col_name]
            break

    mapped["systolic_bp"] = np.nan
    mapped["diastolic_bp"] = np.nan
    mapped["respiratory_rate"] = np.nan
    mapped["glucose_level"] = np.nan
    mapped["fall_detected"] = 0

    mapped["status"] = mapped.apply(lambda r: derive_status(r.to_dict()), axis=1)
    mapped["source"] = "health_data"

    return mapped


def load_heart_rate_data():
    """Load heart_rate.csv for forecasting model."""
    filepath = os.path.join(DATASETS_DIR, "heart_rate.csv")
    if not os.path.exists(filepath):
        print(f"  WARNING: {filepath} not found")
        return pd.DataFrame()

    df = pd.read_csv(filepath)
    print(f"  Loaded heart_rate: {df.shape}")
    return df


def prepare_status_classification_data():
    """Prepare combined dataset for status classification model."""
    print("\n--- Preparing Status Classification Data ---")

    dfs = []
    dfs.append(load_synthetic_patient_monitoring())
    dfs.append(load_human_vital_signs())
    dfs.append(load_patients_data_with_alerts())
    dfs.append(load_healthcare_iot_target())
    dfs.append(load_health_data())

    # Filter out empty DataFrames
    dfs = [df for df in dfs if not df.empty]

    if not dfs:
        print("  ERROR: No data available for status classification!")
        return pd.DataFrame()

    combined = pd.concat(dfs, ignore_index=True)

    # Select features available across datasets
    feature_cols = ["heart_rate", "spo2", "temperature", "systolic_bp",
                    "diastolic_bp", "respiratory_rate", "glucose_level", "fall_detected"]
    available_cols = [c for c in feature_cols if c in combined.columns]

    result = combined[available_cols + ["status"]].copy()

    # Fill missing numeric values with median
    for col in available_cols:
        if col != "fall_detected":
            median_val = result[col].median()
            result[col] = result[col].fillna(median_val if not np.isnan(median_val) else 0)
        else:
            result[col] = result[col].fillna(0)

    # Drop rows with missing status
    result = result.dropna(subset=["status"])
    print(f"  Status classification dataset: {result.shape}")
    print(f"  Class distribution:\n{result['status'].value_counts().to_string()}")

    return result


def prepare_risk_regression_data():
    """Prepare combined dataset for risk score regression model."""
    print("\n--- Preparing Risk Regression Data ---")

    dfs = []

    # personal_health_data with Health_Score-based risk
    personal = load_personal_health_data()
    if not personal.empty and "risk_score" in personal.columns:
        dfs.append(personal)

    # human_vital_signs with Risk Category-based risk
    human = load_human_vital_signs()
    if not human.empty and "risk_score" in human.columns:
        dfs.append(human)

    # healthcare_iot_target
    iot = load_healthcare_iot_target()
    if not iot.empty and "risk_score" in iot.columns:
        dfs.append(iot)

    dfs = [df for df in dfs if not df.empty]

    if not dfs:
        print("  ERROR: No data available for risk regression!")
        return pd.DataFrame()

    combined = pd.concat(dfs, ignore_index=True)

    # For rows without explicit risk_score, derive from status
    mask = combined["risk_score"].isna()
    combined.loc[mask, "risk_score"] = combined.loc[mask, "status"].map(
        {"NORMAL": 20, "WARNING": 50, "CRITICAL": 75, "EMERGENCY": 92}
    )

    feature_cols = ["heart_rate", "spo2", "temperature", "systolic_bp",
                    "diastolic_bp", "respiratory_rate", "glucose_level",
                    "skin_temperature", "battery_level", "fall_detected"]
    available_cols = [c for c in feature_cols if c in combined.columns]

    result = combined[available_cols + ["risk_score"]].copy()

    # Fill missing values
    for col in available_cols:
        if col != "fall_detected":
            median_val = result[col].median()
            result[col] = result[col].fillna(median_val if not pd.isna(median_val) else 0)
        else:
            result[col] = result[col].fillna(0)

    result = result.dropna(subset=["risk_score"])
    # Clip risk_score to 0-100
    result["risk_score"] = result["risk_score"].clip(0, 100)

    print(f"  Risk regression dataset: {result.shape}")
    print(f"  Risk score stats: mean={result['risk_score'].mean():.1f}, "
          f"std={result['risk_score'].std():.1f}")

    return result


def prepare_anomaly_detection_data():
    """Prepare dataset for anomaly detection model."""
    print("\n--- Preparing Anomaly Detection Data ---")

    dfs = []

    personal = load_personal_health_data()
    if not personal.empty:
        dfs.append(personal)

    iot = load_healthcare_iot_target()
    if not iot.empty:
        dfs.append(iot)

    oxygen = load_oxygen_dataset()
    if not oxygen.empty:
        dfs.append(oxygen)

    dfs = [df for df in dfs if not df.empty]

    if not dfs:
        print("  ERROR: No data available for anomaly detection!")
        return pd.DataFrame(), None

    combined = pd.concat(dfs, ignore_index=True)

    feature_cols = ["heart_rate", "spo2", "temperature", "systolic_bp",
                    "diastolic_bp", "respiratory_rate", "glucose_level",
                    "skin_temperature", "battery_level"]
    available_cols = [c for c in feature_cols if c in combined.columns]

    result = combined[available_cols].copy()

    # Fill missing with median
    for col in available_cols:
        median_val = result[col].median()
        result[col] = result[col].fillna(median_val if not pd.isna(median_val) else 0)

    # Get anomaly labels if available
    labels = combined.get("anomaly_flag")

    print(f"  Anomaly detection dataset: {result.shape}")
    if labels is not None:
        labels = labels.fillna(0).astype(int)
        print(f"  Anomaly labels available: {labels.sum()} anomalies out of {len(labels)} samples")

    return result, labels


def prepare_heart_rate_forecasting_data():
    """Prepare dataset for heart rate forecasting using lag features."""
    print("\n--- Preparing Heart Rate Forecasting Data ---")

    df = load_heart_rate_data()
    if df.empty:
        return pd.DataFrame()

    # Stack all four time series (T1, T2, T3, T4)
    all_series = []
    for col in ["T1", "T2", "T3", "T4"]:
        if col in df.columns:
            series = df[col].dropna().values
            all_series.append(series)

    if not all_series:
        print("  ERROR: No time series columns found!")
        return pd.DataFrame()

    # Create supervised dataset with lag features
    n_lags = 5
    rows = []

    for series in all_series:
        for i in range(n_lags, len(series)):
            row = {}
            for lag in range(1, n_lags + 1):
                row[f"hr_lag_{lag}"] = series[i - lag]
            row["next_heart_rate"] = series[i]
            rows.append(row)

    result = pd.DataFrame(rows)

    if len(result) < 100:
        print(f"  WARNING: Only {len(result)} samples after creating lag features (< 100).")
        print("  Model will still be trained for demo purposes.")
    else:
        print(f"  Heart rate forecasting dataset: {result.shape}")

    return result


if __name__ == "__main__":
    print("=" * 60)
    print("  DATASET PREPARATION")
    print("=" * 60)

    status_df = prepare_status_classification_data()
    risk_df = prepare_risk_regression_data()
    anomaly_df, anomaly_labels = prepare_anomaly_detection_data()
    hr_df = prepare_heart_rate_forecasting_data()

    print("\n" + "=" * 60)
    print("  PREPARATION COMPLETE")
    print("=" * 60)
