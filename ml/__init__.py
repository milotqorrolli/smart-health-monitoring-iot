"""
Smart Health Monitoring IoT - ML Module

This module contains:
- Data audit utilities
- Dataset preparation
- Model training
- Model utilities
- Feature schema definitions
"""

from .model_utils import (
    derive_status,
    derive_risk_level,
    get_feature_schema,
    HealthStatus,
    AlertType,
    AlertSeverity,
)

__all__ = [
    "derive_status",
    "derive_risk_level",
    "get_feature_schema",
    "HealthStatus",
    "AlertType",
    "AlertSeverity",
]
