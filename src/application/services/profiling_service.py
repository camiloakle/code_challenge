"""Basic profiling helpers for notebooks and QA."""

from __future__ import annotations

from pyspark.sql import DataFrame


class ProfilingService:
    """Computes row counts and summary stats."""

    def row_count(self, df: DataFrame) -> int:
        """Return total rows."""
        return df.count()

    def describe_numeric(self, df: DataFrame, columns: list[str]) -> DataFrame:
        """Delegate to Spark describe for selected columns."""
        return df.select(*columns).describe()
