"""Abstract repository contracts (implemented in infrastructure)."""

from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class SilverTableRepository(ABC):
    """Access to enriched Silver transactions."""

    @abstractmethod
    def load(self) -> DataFrame:
        """Read the Silver Delta table."""
        ...


class GoldWriter(ABC):
    """Write Gold layer outputs."""

    @abstractmethod
    def write_parquet(self, df: DataFrame, relative_path: str) -> None:
        """Write Parquet under Gold root."""
        ...
