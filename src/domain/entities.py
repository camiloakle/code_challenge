"""Lightweight domain records (non-Spark)."""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DataLayerPaths:
    """Resolved paths for medallion layers."""

    bronze_root: Path
    silver_root: Path
    gold_root: Path


@dataclass(frozen=True)
class QuestionId:
    """Gold question identifier."""

    value: str
