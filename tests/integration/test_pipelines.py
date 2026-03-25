"""End-to-end medallion flow (skipped without Java)."""

from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture
def tmp_data_paths(tmp_path, monkeypatch):
    """Redirect data_root to a temp directory."""
    raw = tmp_path / "raw"
    raw.mkdir(parents=True)
    root = tmp_path / "data"
    root.mkdir(parents=True)
    import shutil as sh

    here = Path(__file__).resolve().parent.parent / "fixtures"
    # Spark-style: part-*.parquet under a directory (not one fixed filename)
    hist_dir = raw / "historical_transactions"
    hist_dir.mkdir(parents=True)
    sh.copy(
        here / "sample_transactions.parquet",
        hist_dir / "part-00000-tid-test-1-c000.snappy.parquet",
    )
    sh.copy(here / "sample_merchants.csv", raw / "merchants.csv")

    monkeypatch.setenv("DATA_ROOT", str(root))
    monkeypatch.setenv("RAW_TRANSACTIONS_PATH", str(hist_dir))
    monkeypatch.setenv("RAW_MERCHANTS_PATH", str(raw / "merchants.csv"))

    return root


def test_bronze_silver_gold_q1(tmp_data_paths):
    _ = tmp_data_paths
    if not shutil.which("java"):
        pytest.skip("no java")
    from config.settings import Settings
    from pipelines.bronze_ingestion import run_bronze
    from pipelines.runner import run_question
    from pipelines.silver_builder import run_silver

    settings = Settings()
    run_bronze(settings)
    run_silver(settings)
    run_question("q1", settings)

    gold = Path(settings.gold_uri) / "q1_results"
    assert gold.exists()
    df = pd.read_parquet(gold)
    assert "Month" in df.columns
    assert "Purchase Total" in df.columns
