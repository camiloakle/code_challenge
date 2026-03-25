"""Protocol interfaces for repositories and pipelines."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from pyspark.sql import DataFrame


@runtime_checkable
class SparkReader(Protocol):
    """Reads a DataFrame from a logical source."""

    def read(self, path: str) -> DataFrame:
        """Load data from storage."""
        ...


@runtime_checkable
class SparkWriter(Protocol):
    """Writes a DataFrame to storage."""

    def write(self, df: DataFrame, path: str, mode: str = "overwrite") -> None:
        """Persist DataFrame."""
        ...


@runtime_checkable
class PipelineStep(Protocol):
    """Single step in a medallion or gold pipeline."""

    def run(self) -> DataFrame:
        """Execute transformation and return result."""
        ...
