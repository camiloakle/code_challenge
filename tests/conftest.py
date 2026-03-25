"""Pytest fixtures."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest


def _java_available() -> bool:
    return shutil.which("java") is not None


@pytest.fixture(scope="module")
def spark_session():
    """Local Spark with Delta — skipped if Java is not installed."""
    if not _java_available():
        pytest.skip("Java 11+ required for PySpark")
    from config.settings import Settings
    from config.spark_config import build_spark_session

    settings = Settings()
    spark = build_spark_session(settings, "pytest")
    yield spark
    spark.stop()


@pytest.fixture
def project_root() -> Path:
    return Path(__file__).resolve().parent.parent
