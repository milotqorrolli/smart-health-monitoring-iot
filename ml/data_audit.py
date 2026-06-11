"""
Data Audit Script for Smart Health Monitoring IoT System

This script audits all datasets in the datasets/ folder and prints:
- Dataset name
- Shape (rows, columns)
- Column names
- Missing values
- Selected use case
- Whether the dataset is used or ignored
- Reason for use or ignore
"""

import os
import pandas as pd
import json
from pathlib import Path

# Dataset base path
DATASETS_PATH = Path(__file__).parent.parent / "datasets"


def audit_csv_dataset(file_path: str) -> dict:
    """Audit a CSV dataset."""
    try:
        df = pd.read_csv(file_path)
        return {
            "shape": df.shape,
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "missing_values": df.isnull().sum().to_dict(),
            "sample_rows": df.head(2).to_dict(orient="records"),
        }
    except Exception as e:
        return {"error": str(e)}


def audit_xlsx_dataset(file_path: str) -> dict:
    """Audit an Excel dataset."""
    try:
        df = pd.read_excel(file_path)
        return {
            "shape": df.shape,
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "missing_values": df.isnull().sum().to_dict(),
            "sample_rows": df.head(2).to_dict(orient="records"),
        }
    except Exception as e:
        return {"error": str(e)}


DATASET_DEFINITIONS = {
    "Synthetic_patient-HealthCare-Monitoring_dataset.csv": {
        "use": True,
        "use_case": "PRIMARY: Status classification, alert labels, vital signs baseline",
        "reason": "Core vital signs dataset with fall detection and disease prediction labels",
        "key_fields": [
            "Heart Rate (bpm)",
            "SpO2 Level (%)",
            "Systolic Blood Pressure (mmHg)",
            "Diastolic Blood Pressure (mmHg)",
            "Body Temperature (°C)",
            "Fall Detection",
            "Predicted Disease",
            "Heart Rate Alert",
            "SpO2 Level Alert",
            "Blood Pressure Alert",
            "Temperature Alert",
        ],
    },
    "patients_data_with_alerts.xlsx": {
        "use": True,
        "use_case": "PRIMARY: Alert validation, status classification training",
        "reason": "Provides alert labels for supervised learning and status classification",
        "key_fields": ["alert_type", "alert_severity", "patient_status"],
    },
    "human_vital_signs_dataset_2024.csv": {
        "use": True,
        "use_case": "PRIMARY: Demographics, vital signs, risk category classification",
        "reason": "Includes age, gender, BMI, derived features, and risk categories",
        "key_fields": [
            "Age",
            "Gender",
            "Weight",
            "Height",
            "Derived_BMI",
            "Risk Category",
            "Heart Rate",
            "Body Temperature",
            "Oxygen Saturation",
        ],
    },
    "personal_health_data.csv": {
        "use": True,
        "use_case": "PRIMARY: Health score → risk score conversion, anomaly flag support",
        "reason": "Provides health scores and anomaly flags for risk computation",
        "key_fields": [
            "User_ID",
            "Timestamp",
            "Health_Score",
            "Anomaly_Flag",
            "Sleep",
            "Stress",
            "ECG",
            "BloodOxygen",
            "SkinTemperature",
        ],
    },
    "activity_environment_data.csv": {
        "use": True,
        "use_case": "PRIMARY: Activity features - steps, exercise, calories, environment",
        "reason": "Provides activity level, exercise type, intensity, and environmental data",
        "key_fields": [
            "User_ID",
            "Timestamp",
            "Steps",
            "Calories",
            "Distance",
            "ExerciseType",
            "ExerciseDuration",
            "ExerciseIntensity",
            "Temperature",
            "Altitude",
            "UVExposure",
        ],
    },
    "digital_interaction_data.csv": {
        "use": True,
        "use_case": "PRIMARY: Screen time and notifications for behavioral features",
        "reason": "Provides notifications received and screen time behavioral indicators",
        "key_fields": ["User_ID", "Timestamp", "NotificationsReceived", "ScreenTime"],
    },
    "healthcare_iot_target_dataset_5000.csv": {
        "use": True,
        "use_case": "SECONDARY: IoT sensor targets, battery level, sensor type",
        "reason": "Provides IoT-specific data including battery levels and sensor metadata",
        "key_fields": ["patient_id", "target_health_status", "battery_level", "sensor_type"],
    },
    "heart_rate.csv": {
        "use": True,
        "use_case": "SECONDARY: Heart rate time series forecasting",
        "reason": "Provides heart rate time series for training heart rate forecasting model",
        "key_fields": ["T1", "T2", "T3", "T4"],
    },
    "Health data.csv": {
        "use": True,
        "use_case": "SECONDARY: Lightweight vital signs auxiliary data",
        "reason": "Provides basic vital signs for enrichment if needed",
        "key_fields": ["pulse", "body_temperature", "SpO2", "status"],
    },
    "Oxygen Dataset Final.csv": {
        "use": True,
        "use_case": "SECONDARY: Oxygen-related features for risk enrichment",
        "reason": "Provides SpO2 and oxygen flow data for anomaly detection",
        "key_fields": ["SpO2", "pulse_rate", "oxygen_flow"],
    },
    "diabetes_dataset.csv": {
        "use": False,
        "use_case": "OPTIONAL (not used in main version)",
        "reason": "Disease-specific dataset; not aligned with real-time health monitoring simulator. Could be added as optional disease risk enrichment in future.",
        "key_fields": [],
    },
    "updated_version.csv": {
        "use": False,
        "use_case": "OPTIONAL (not used in main version)",
        "reason": "Cardiovascular risk enrichment dataset; not primary for main IoT stream. Could be used for cross-validation in future.",
        "key_fields": [],
    },
    "Synthetic-Infant-Health-Data.csv": {
        "use": False,
        "use_case": "IGNORE",
        "reason": "Infant/pediatric dataset does not align with adult smart health monitoring simulator. Not applicable to project scope.",
        "key_fields": [],
    },
    "healthcare_patient_journey.csv": {
        "use": False,
        "use_case": "IGNORE",
        "reason": "Hospital administrative/journey data, not real-time IoT sensor data. Not aligned with streaming pipeline architecture. Could be mentioned as future work for longitudinal analysis.",
        "key_fields": [],
    },
}


def print_audit_report():
    """Print comprehensive audit report for all datasets."""
    print("\n" + "=" * 100)
    print("SMART HEALTH MONITORING IoT - DATASET AUDIT REPORT")
    print("=" * 100 + "\n")

    total_datasets = 0
    used_datasets = 0
    ignored_datasets = 0

    for filename in sorted(os.listdir(DATASETS_PATH)):
        file_path = DATASETS_PATH / filename

        if not os.path.isfile(file_path):
            continue

        if filename.endswith(".csv"):
            stats = audit_csv_dataset(file_path)
        elif filename.endswith(".xlsx"):
            stats = audit_xlsx_dataset(file_path)
        else:
            continue

        total_datasets += 1
        definition = DATASET_DEFINITIONS.get(filename, {})
        is_used = definition.get("use", False)

        if is_used:
            used_datasets += 1
            status_str = "✓ USED"
        else:
            ignored_datasets += 1
            status_str = "✗ IGNORED"

        print(f"\n{status_str} | {filename}")
        print("-" * 100)

        if "error" in stats:
            print(f"  ERROR: {stats['error']}")
            continue

        shape = stats.get("shape", ("N/A", "N/A"))
        print(f"  Shape:          {shape[0]} rows × {shape[1]} columns")
        print(f"  Columns:        {', '.join(stats.get('columns', [])[:10])}")
        if len(stats.get('columns', [])) > 10:
            print(f"                  ... and {len(stats.get('columns', [])) - 10} more")

        missing = stats.get("missing_values", {})
        missing_summary = {k: v for k, v in missing.items() if v > 0}
        if missing_summary:
            print(f"  Missing Data:   {json.dumps(missing_summary, indent=18)}")
        else:
            print(f"  Missing Data:   None")

        print(f"\n  Use Case:       {definition.get('use_case', 'Not defined')}")
        print(f"  Reason:         {definition.get('reason', 'Not defined')}")

        key_fields = definition.get("key_fields", [])
        if key_fields:
            print(f"  Key Fields:     {', '.join(key_fields[:5])}")
            if len(key_fields) > 5:
                print(f"                  ... and {len(key_fields) - 5} more")

    print("\n" + "=" * 100)
    print(f"SUMMARY: {used_datasets} datasets USED | {ignored_datasets} datasets IGNORED | {total_datasets} total")
    print("=" * 100 + "\n")

    # Create summary JSON
    summary = {
        "total_datasets": total_datasets,
        "used_count": used_datasets,
        "ignored_count": ignored_datasets,
        "datasets": DATASET_DEFINITIONS,
    }

    summary_path = Path(__file__).parent.parent / "models" / "dataset_audit.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"Audit summary saved to: {summary_path}\n")


if __name__ == "__main__":
    print_audit_report()
