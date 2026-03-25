"""Tests for resolve_raw_transactions_path (directory vs legacy file)."""

from __future__ import annotations

import pytest

from config.settings import Settings
from pipelines.bronze_ingestion import resolve_raw_transactions_path


def test_resolve_prefers_existing_directory(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    d = tmp_path / "data" / "raw" / "historical_transactions"
    d.mkdir(parents=True)
    marker = d / "part-00000-test.snappy.parquet"
    marker.touch()
    p = resolve_raw_transactions_path(Settings())
    assert p.resolve() == d.resolve()


def test_resolve_legacy_parquet_if_dir_absent(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    raw = tmp_path / "data" / "raw"
    raw.mkdir(parents=True)
    legacy = raw / "historical_transactions.parquet"
    legacy.touch()
    p = resolve_raw_transactions_path(Settings())
    assert p.resolve() == legacy.resolve()


def test_resolve_loose_part_parquet_in_raw(tmp_path, monkeypatch):
    """Download as single part-*.snappy.parquet directly under data/raw/."""
    monkeypatch.chdir(tmp_path)
    raw = tmp_path / "data" / "raw"
    raw.mkdir(parents=True)
    (raw / "part-00000-tid-x.snappy.parquet").touch()
    p = resolve_raw_transactions_path(Settings())
    assert p == str(raw / "part-*.parquet")


def test_explicit_missing_path_does_not_use_legacy(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    raw = tmp_path / "data" / "raw"
    raw.mkdir(parents=True)
    (raw / "historical_transactions.parquet").touch()
    missing = tmp_path / "custom" / "missing.parquet"
    monkeypatch.setenv("RAW_TRANSACTIONS_PATH", str(missing))
    with pytest.raises(FileNotFoundError):
        resolve_raw_transactions_path(Settings())
