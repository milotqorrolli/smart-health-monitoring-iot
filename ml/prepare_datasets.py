from __future__ import annotations

import os
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd

try:
    from .model_utils import (
        FIELD_DEFAULTS,
        INPUT_FIELDS,
        STATUS_TO_SEVERITY,
        derive_status,
        derive_status_from_alert_columns,
        fill_record_defaults,
        risk_from_status,
        safe_float,
    )
except ImportError:
    from model_utils import (
        FIELD_DEFAULTS,
        INPUT_FIELDS,
        STATUS_TO_SEVERITY,
        derive_status,
        derive_status_from_alert_columns,
        fill_record_defaults,
        risk_from_status,
        safe_float,
    )


ROOT_DIR = Path(__file__).resolve().parents[1]
DATASET_DIR = ROOT_DIR / "datasets"
MODEL_DIR = ROOT_DIR / "models"
MAX_ROWS_PER_DATASET = int(os.getenv("MAX_ROWS_PER_DATASET", "5000"))


def read_table(filename: str, max_rows: int | None = MAX_ROWS_PER_DATASET) -> pd.DataFrame:
    path = DATASET_DIR / filename
    if not path.exists():
        print(f"Skipping missing dataset: {filename}", flush=True)
        return pd.DataFrame()
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, nrows=max_rows)
    return pd.read_excel(path, nrows=max_rows)


def _copy_columns(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    output = pd.DataFrame(index=df.index)
    for source, target in mapping.items():
        if source in df.columns:
            output[target] = df[source]
    return output


def _status_from_risk_score(score: float) -> str:
    if score >= 85:
        return "EMERGENCY"
    if score >= 70:
        return "CRITICAL"
    if score >= 45:
        return "WARNING"
    return "NORMAL"


def _map_status(value: object) -> str | None:
    text = str(value or "").strip().upper()
    if not text or text == "NAN":
        return None
    if text in {"NORMAL", "LOW", "LOW RISK", "HEALTHY", "STABLE"}:
        return "NORMAL"
    if text in {"WARNING", "MEDIUM", "MEDIUM RISK", "MODERATE", "ELEVATED"}:
        return "WARNING"
    if text in {"CRITICAL", "HIGH", "HIGH RISK", "UNHEALTHY", "ABNORMAL"}:
        return "CRITICAL"
    if text in {"EMERGENCY", "CRITICAL RISK", "SEVERE"}:
        return "EMERGENCY"
    return None


def _risk_from_category(value: object) -> float | None:
    mapped = _map_status(value)
    if mapped is None:
        return None
    return {
        "NORMAL": 25.0,
        "WARNING": 55.0,
        "CRITICAL": 80.0,
        "EMERGENCY": 95.0,
    }[mapped]


def _add_defaults(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    for field, default in FIELD_DEFAULTS.items():
        if field not in df.columns:
            df[field] = default

    records = [fill_record_defaults(record) for record in df[INPUT_FIELDS].to_dict(orient="records")]
    normalized = pd.DataFrame(records)
    for column in df.columns:
        if column not in normalized.columns:
            normalized[column] = df[column].values
    return normalized


def _finalize(
    df: pd.DataFrame,
    source_dataset: str,
    status_builder: Callable[[pd.Series], str] | None = None,
    risk_builder: Callable[[pd.Series], float | None] | None = None,
    anomaly_builder: Callable[[pd.Series], int | None] | None = None,
) -> pd.DataFrame:
    if df.empty:
        return df
    df = _add_defaults(df)
    df["source_dataset"] = source_dataset

    if status_builder is None:
        df["target_status"] = df.apply(lambda row: derive_status(row), axis=1)
    else:
        df["target_status"] = df.apply(status_builder, axis=1)
        df["target_status"] = df.apply(
            lambda row: row["target_status"] if row["target_status"] in STATUS_TO_SEVERITY else derive_status(row),
            axis=1,
        )

    if risk_builder is None:
        df["risk_score_target"] = df["target_status"].map(risk_from_status)
    else:
        df["risk_score_target"] = df.apply(risk_builder, axis=1)
        df["risk_score_target"] = df.apply(
            lambda row: safe_float(row["risk_score_target"], risk_from_status(row["target_status"])),
            axis=1,
        )

    if anomaly_builder is None:
        df["anomaly_label"] = df["target_status"].isin(["CRITICAL", "EMERGENCY"]).astype(int)
    else:
        df["anomaly_label"] = df.apply(anomaly_builder, axis=1)
        df["anomaly_label"] = df["anomaly_label"].fillna(df["target_status"].isin(["CRITICAL", "EMERGENCY"]).astype(int))
        df["anomaly_label"] = df["anomaly_label"].astype(int)

    df["risk_score_target"] = df["risk_score_target"].clip(0, 100)
    return df


def load_synthetic_patient() -> pd.DataFrame:
    df = read_table("Synthetic_patient-HealthCare-Monitoring_dataset.csv")
    mapped = _copy_columns(
        df,
        {
            "Patient Number": "patient_id",
            "Heart Rate (bpm)": "heart_rate",
            "SpO2 Level (%)": "spo2",
            "Systolic Blood Pressure (mmHg)": "systolic_bp",
            "Diastolic Blood Pressure (mmHg)": "diastolic_bp",
            "Body Temperature (C)": "temperature",
            "Body Temperature (°C)": "temperature",
            "Fall Detection": "fall_detected",
            "Predicted Disease": "predicted_disease_simulated",
        },
    )
    for alert_col in ["Heart Rate Alert", "SpO2 Level Alert", "Blood Pressure Alert", "Temperature Alert"]:
        if alert_col in df.columns:
            mapped[alert_col] = df[alert_col]
    return _finalize(
        mapped,
        "Synthetic_patient-HealthCare-Monitoring_dataset.csv",
        status_builder=lambda row: derive_status_from_alert_columns(row),
    )


def load_patients_alerts() -> pd.DataFrame:
    df = read_table("patients_data_with_alerts.xlsx")
    mapped = _copy_columns(
        df,
        {
            "Patient Number": "patient_id",
            "Heart Rate (bpm)": "heart_rate",
            "SpO2 Level (%)": "spo2",
            "Systolic Blood Pressure (mmHg)": "systolic_bp",
            "Diastolic Blood Pressure (mmHg)": "diastolic_bp",
            "Body Temperature (C)": "temperature",
            "Body Temperature (°C)": "temperature",
            "Fall Detection": "fall_detected",
            "Predicted Disease": "predicted_disease_simulated",
        },
    )
    for alert_col in ["Heart Rate Alert", "SpO2 Level Alert", "Blood Pressure Alert", "Temperature Alert"]:
        if alert_col in df.columns:
            mapped[alert_col] = df[alert_col]
    return _finalize(
        mapped,
        "patients_data_with_alerts.xlsx",
        status_builder=lambda row: derive_status_from_alert_columns(row),
    )


def load_human_vitals() -> pd.DataFrame:
    df = read_table("human_vital_signs_dataset_2024.csv")
    mapped = _copy_columns(
        df,
        {
            "Patient ID": "patient_id",
            "Timestamp": "timestamp",
            "Heart Rate": "heart_rate",
            "Respiratory Rate": "respiratory_rate",
            "Body Temperature": "temperature",
            "Oxygen Saturation": "spo2",
            "Systolic Blood Pressure": "systolic_bp",
            "Diastolic Blood Pressure": "diastolic_bp",
            "Age": "age",
            "Gender": "gender",
            "Weight (kg)": "weight",
            "Height (m)": "height",
            "Derived_BMI": "bmi",
        },
    )
    if "Risk Category" in df.columns:
        mapped["risk_category_raw"] = df["Risk Category"]
    return _finalize(
        mapped,
        "human_vital_signs_dataset_2024.csv",
        status_builder=lambda row: _map_status(row.get("risk_category_raw")) or derive_status(row),
        risk_builder=lambda row: _risk_from_category(row.get("risk_category_raw")),
    )


def load_personal_health_joined() -> pd.DataFrame:
    personal = read_table("personal_health_data.csv")
    if personal.empty:
        return personal

    activity = read_table("activity_environment_data.csv")
    digital = read_table("digital_interaction_data.csv")
    merged = personal
    if not activity.empty:
        merged = merged.merge(activity, on=["User_ID", "Timestamp"], how="left")
    if not digital.empty:
        merged = merged.merge(digital, on=["User_ID", "Timestamp"], how="left")

    mapped = _copy_columns(
        merged,
        {
            "User_ID": "patient_id",
            "Timestamp": "timestamp",
            "Age": "age",
            "Gender": "gender",
            "Weight": "weight",
            "Height": "height",
            "Medical_Conditions": "chronic_condition",
            "Medication": "medication",
            "Smoker": "smoker",
            "Sleep_Duration": "sleep_duration",
            "Heart_Rate": "heart_rate",
            "Blood_Oxygen_Level": "spo2",
            "Stress_Level": "stress_level",
            "Skin_Temperature": "skin_temperature",
            "Steps": "steps",
            "Exercise_Type": "exercise_type",
            "Exercise_Intensity": "exercise_intensity",
            "Battery_Level": "battery_level",
            "Notifications_Received": "notifications_received",
            "Screen_Time": "screen_time",
        },
    )
    if "Health_Score" in merged.columns:
        mapped["health_score"] = merged["Health_Score"]
    if "Anomaly_Flag" in merged.columns:
        mapped["anomaly_raw"] = merged["Anomaly_Flag"]

    mapped["sleep_quality"] = np.select(
        [
            pd.to_numeric(mapped.get("sleep_duration", pd.Series(index=mapped.index)), errors="coerce") < 6,
            pd.to_numeric(mapped.get("sleep_duration", pd.Series(index=mapped.index)), errors="coerce") >= 7,
        ],
        ["Poor", "Good"],
        default="Fair",
    )
    mapped["activity_level"] = np.select(
        [
            pd.to_numeric(mapped.get("steps", pd.Series(index=mapped.index)), errors="coerce") < 2500,
            pd.to_numeric(mapped.get("steps", pd.Series(index=mapped.index)), errors="coerce") > 8000,
        ],
        ["Resting", "Active"],
        default="Light",
    )
    return _finalize(
        mapped,
        "personal_health_data.csv + activity_environment_data.csv + digital_interaction_data.csv",
        risk_builder=lambda row: 100.0 - safe_float(row.get("health_score"), 65.0),
        anomaly_builder=lambda row: int(str(row.get("anomaly_raw", "0")).strip().lower() in {"1", "true", "yes", "anomaly"}),
    )


def load_healthcare_iot_target() -> pd.DataFrame:
    df = read_table("healthcare_iot_target_dataset_5000.csv")
    mapped = _copy_columns(
        df,
        {
            "Patient_ID": "patient_id",
            "Timestamp": "timestamp",
            "Temperature (C)": "temperature",
            "Temperature (°C)": "temperature",
            "Systolic_BP (mmHg)": "systolic_bp",
            "Diastolic_BP (mmHg)": "diastolic_bp",
            "Heart_Rate (bpm)": "heart_rate",
            "Device_Battery_Level (%)": "battery_level",
            "Battery_Level (%)": "battery_level",
        },
    )
    if "Target_Health_Status" in df.columns:
        mapped["target_health_status_raw"] = df["Target_Health_Status"]
    return _finalize(
        mapped,
        "healthcare_iot_target_dataset_5000.csv",
        status_builder=lambda row: _map_status(row.get("target_health_status_raw")) or derive_status(row),
        risk_builder=lambda row: _risk_from_category(row.get("target_health_status_raw")),
    )


def load_health_data() -> pd.DataFrame:
    df = read_table("Health data.csv")
    mapped = _copy_columns(
        df,
        {
            "pulse": "heart_rate",
            "body temperature": "temperature",
            "SpO2": "spo2",
        },
    )
    if "Status" in df.columns:
        mapped["status_raw"] = df["Status"]
    return _finalize(
        mapped,
        "Health data.csv",
        status_builder=lambda row: _map_status(row.get("status_raw")) or derive_status(row),
    )


def load_oxygen_data() -> pd.DataFrame:
    df = read_table("Oxygen Dataset Final.csv")
    mapped = _copy_columns(
        df,
        {
            "age": "age",
            "gender": "gender",
            "spo2": "spo2",
            "pr": "heart_rate",
        },
    )
    return _finalize(mapped, "Oxygen Dataset Final.csv")


def load_diabetes_data() -> pd.DataFrame:
    df = read_table("diabetes_dataset.csv")
    mapped = _copy_columns(
        df,
        {
            "age": "age",
            "gender": "gender",
            "smoking_status": "smoker",
            "sleep_hours_per_day": "sleep_duration",
            "screen_time_hours_per_day": "screen_time",
            "bmi": "bmi",
            "systolic_bp": "systolic_bp",
            "diastolic_bp": "diastolic_bp",
            "heart_rate": "heart_rate",
            "glucose_fasting": "glucose_level",
        },
    )
    if "diabetes_risk_score" in df.columns:
        mapped["risk_raw"] = df["diabetes_risk_score"]
    return _finalize(
        mapped,
        "diabetes_dataset.csv",
        risk_builder=lambda row: min(100.0, safe_float(row.get("risk_raw"), 50.0) * (100.0 if safe_float(row.get("risk_raw"), 50.0) <= 1 else 1.0)),
        status_builder=lambda row: _status_from_risk_score(
            min(100.0, safe_float(row.get("risk_raw"), 50.0) * (100.0 if safe_float(row.get("risk_raw"), 50.0) <= 1 else 1.0))
        ),
    )


def load_updated_version() -> pd.DataFrame:
    df = read_table("updated_version.csv")
    mapped = _copy_columns(
        df,
        {
            "age": "age",
            "sex": "gender",
            "systolic_bp": "systolic_bp",
            "diastolic_bp": "diastolic_bp",
            "smoking": "smoker",
            "diabetes": "chronic_condition",
        },
    )
    if "heart_attack" in df.columns:
        mapped["heart_attack_raw"] = df["heart_attack"]
    return _finalize(
        mapped,
        "updated_version.csv",
        risk_builder=lambda row: 85.0 if str(row.get("heart_attack_raw", "0")).strip() in {"1", "true", "yes"} else 35.0,
        status_builder=lambda row: "CRITICAL" if str(row.get("heart_attack_raw", "0")).strip() in {"1", "true", "yes"} else derive_status(row),
    )


def load_all_training_data() -> pd.DataFrame:
    loaders = [
        load_synthetic_patient,
        load_patients_alerts,
        load_human_vitals,
        load_personal_health_joined,
        load_healthcare_iot_target,
        load_health_data,
        load_oxygen_data,
        load_diabetes_data,
        load_updated_version,
    ]
    frames = []
    for loader in loaders:
        try:
            frame = loader()
            if not frame.empty:
                frames.append(frame)
                print(f"Loaded {len(frame):,} rows from {frame['source_dataset'].iloc[0]}", flush=True)
        except Exception as exc:
            print(f"Skipping loader {loader.__name__}: {exc}", flush=True)

    if not frames:
        raise RuntimeError("No training datasets could be loaded.")

    combined = pd.concat(frames, ignore_index=True, sort=False)
    combined = _add_defaults(combined)
    combined["target_status"] = combined["target_status"].fillna(combined.apply(lambda row: derive_status(row), axis=1))
    combined["risk_score_target"] = pd.to_numeric(combined["risk_score_target"], errors="coerce").fillna(
        combined["target_status"].map(risk_from_status)
    )
    combined["anomaly_label"] = pd.to_numeric(combined["anomaly_label"], errors="coerce").fillna(0).astype(int)
    return combined


def build_forecasting_dataset() -> pd.DataFrame:
    df = read_table("heart_rate.csv", max_rows=None)
    rows = []
    for series_name in df.columns:
        values = pd.to_numeric(df[series_name], errors="coerce").dropna().astype(float).tolist()
        for index in range(5, len(values)):
            rows.append(
                {
                    "series": series_name,
                    "hr_lag_1": values[index - 1],
                    "hr_lag_2": values[index - 2],
                    "hr_lag_3": values[index - 3],
                    "hr_lag_4": values[index - 4],
                    "hr_lag_5": values[index - 5],
                    "next_heart_rate": values[index],
                }
            )
    if not rows:
        raise RuntimeError("heart_rate.csv did not produce forecasting lag rows.")
    return pd.DataFrame(rows)


def main() -> None:
    MODEL_DIR.mkdir(exist_ok=True)
    training = load_all_training_data()
    forecast = build_forecasting_dataset()
    training_path = MODEL_DIR / "prepared_training_data.csv"
    forecast_path = MODEL_DIR / "prepared_heart_rate_forecasting.csv"
    training.to_csv(training_path, index=False)
    forecast.to_csv(forecast_path, index=False)
    print(f"Saved unified training data: {training_path} ({len(training):,} rows)")
    print(f"Saved heart-rate forecasting data: {forecast_path} ({len(forecast):,} rows)")


if __name__ == "__main__":
    main()
