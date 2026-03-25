"""Thin wrapper around SparkSession factory."""

from __future__ import annotations

from config.settings import Settings
from config.spark_config import build_spark_session


def create_session(settings: Settings, app_name: str):
    """Build Spark session for local Delta workflows."""
    return build_spark_session(settings, app_name=app_name)
