"""Strategy base for Q1–Q5."""

from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class BaseGoldStrategy(ABC):
    """Each question implements transform(silver_df) -> gold_df."""

    @abstractmethod
    def transform(self, silver_df: DataFrame) -> DataFrame:
        """Pure aggregation / analysis on cleaned Silver data."""
        ...
