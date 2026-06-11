from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[1]
DATASET_DIR = ROOT_DIR / "datasets"
MODEL_DIR = ROOT_DIR / "models"

DATASET_STRATEGY: dict[str, dict[str, Any]] = {
    "Synthetic_patient-HealthCare-Monitoring_dataset.csv": {
        "used": True,
        "use_case": "Primary vital signs, fall detection, alert labels, simulated disease labels.",
        "reason": "Directly matches the real-time patient monitoring domain and contains alert columns.",
    },
    "patients_data_with_alerts.xlsx": {
        "used": True,
        "use_case": "Additional alert labels for status classification validation.",
        "reason": "Uses the same schema as the synthetic patient monitoring dataset.",
    },
    "human_vital_signs_dataset_2024.csv": {
        "used": True,
        "use_case": "Vital signs, demographics, derived BMI/MAP, and risk category labels.",
        "reason": "Strong fit for adult vital-sign risk classification and regression.",
    },
    "personal_health_data.csv": {
        "used": True,
        "use_case": "Wearable profile, Health_Score risk target, sleep, stress, ECG, SpO2, skin temperature.",
        "reason": "Provides wearable context and supervised anomaly support.",
    },
    "activity_environment_data.csv": {
        "used": True,
        "use_case": "Activity, exercise, environment, and battery data joined to personal_health_data.",
        "reason": "Adds wearable activity and device context using User_ID and Timestamp.",
    },
    "digital_interaction_data.csv": {
        "used": True,
        "use_case": "Notifications and screen time joined to personal_health_data.",
        "reason": "Adds digital interaction context using User_ID and Timestamp.",
    },
    "healthcare_iot_target_dataset_5000.csv": {
        "used": True,
        "use_case": "IoT sensor targets, target health status, battery level, and sensor type.",
        "reason": "Useful for health status and risk target enrichment.",
    },
    "heart_rate.csv": {
        "used": True,
        "use_case": "Heart rate forecasting using lag features.",
        "reason": "Contains multiple heart-rate time series suitable for next-step forecasting.",
    },
    "Health data.csv": {
        "used": True,
        "use_case": "Auxiliary pulse, body temperature, SpO2, and status labels.",
        "reason": "Small lightweight dataset that maps cleanly to core vitals.",
    },
    "Oxygen Dataset Final.csv": {
        "used": True,
        "use_case": "Optional oxygen-related SpO2 and pulse enrichment.",
        "reason": "Adds SpO2/pulse samples for anomaly and status patterns.",
    },
    "diabetes_dataset.csv": {
        "used": True,
        "use_case": "Optional risk enrichment through glucose, BMI, BP, smoking, and diabetes risk score.",
        "reason": "Useful for risk regression but not used as the main streaming target.",
    },
    "updated_version.csv": {
        "used": True,
        "use_case": "Optional cardiovascular risk enrichment.",
        "reason": "Provides BP, smoking, diabetes, and heart attack risk context.",
    },
    "Synthetic-Infant-Health-Data.csv": {
        "used": False,
        "use_case": "Ignored for the main version.",
        "reason": "Pediatric/infant disease data does not align with the adult wearable simulator.",
    },
    "healthcare_patient_journey.csv": {
        "used": False,
        "use_case": "Future work only.",
        "reason": "Administrative hospital journey data is not real-time IoT sensor telemetry.",
    },
}


def read_dataset(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    raise ValueError(f"Unsupported dataset type: {path.name}")


def audit_dataset(path: Path) -> dict[str, Any]:
    strategy = DATASET_STRATEGY.get(
        path.name,
        {
            "used": False,
            "use_case": "Not selected.",
            "reason": "Dataset is not part of the documented project strategy.",
        },
    )
    try:
        df = read_dataset(path)
        missing = {column: int(count) for column, count in df.isna().sum().items() if int(count) > 0}
        return {
            "dataset": path.name,
            "shape": [int(df.shape[0]), int(df.shape[1])],
            "columns": [str(column) for column in df.columns],
            "missing_values": missing,
            "selected_use_case": strategy["use_case"],
            "used": bool(strategy["used"]),
            "reason": strategy["reason"],
        }
    except Exception as exc:
        return {
            "dataset": path.name,
            "shape": None,
            "columns": [],
            "missing_values": {},
            "selected_use_case": strategy["use_case"],
            "used": bool(strategy["used"]),
            "reason": f"{strategy['reason']} Read failed: {exc}",
        }


def main() -> None:
    MODEL_DIR.mkdir(exist_ok=True)
    audits = []
    for path in sorted(DATASET_DIR.iterdir()):
        if path.suffix.lower() not in {".csv", ".xlsx", ".xls"}:
            continue
        audits.append(audit_dataset(path))

    for item in audits:
        print("=" * 80)
        print(f"Dataset: {item['dataset']}")
        print(f"Shape: {item['shape']}")
        print(f"Columns: {', '.join(item['columns'])}")
        print(f"Missing values: {json.dumps(item['missing_values'], ensure_ascii=True)}")
        print(f"Selected use case: {item['selected_use_case']}")
        print(f"Used: {item['used']}")
        print(f"Reason: {item['reason']}")

    output_path = MODEL_DIR / "dataset_audit.json"
    output_path.write_text(json.dumps(audits, indent=2, ensure_ascii=True), encoding="utf-8")
    print("=" * 80)
    print(f"Saved dataset audit to {output_path}")


if __name__ == "__main__":
    main()
