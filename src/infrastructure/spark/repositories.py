"""Delta / Parquet repository implementations.

Raw Parquet (Spark-style ``part-*.snappy.parquet`` folders) is ingested in
``pipelines/bronze_ingestion``. Silver/Gold read only layer roots below.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame

from config.settings import Settings
from src.core.exceptions import StorageError
from src.domain.repositories import GoldWriter, SilverTableRepository
from src.shared.logger import get_logger

logger = get_logger(__name__)


class DeltaSilverRepository(SilverTableRepository):
    """Reads Silver enriched_transactions Delta table."""

    def __init__(
        self,
        spark,
        settings: Settings,
        table_name: str = "enriched_transactions",
    ) -> None:
        self._spark = spark
        self._settings = settings
        self._table_name = table_name
        self._path = Path(settings.silver_uri) / table_name

    def load(self) -> DataFrame:
        """Load Silver Delta table."""
        path = str(self._path)
        logger.info("silver_read", extra={"path": path})
        try:
            return self._spark.read.format("delta").load(path)
        except Exception as exc:
            raise StorageError(f"Failed to read Silver at {path}") from exc


class ParquetGoldWriter(GoldWriter):
    """Writes Parquet datasets under Gold root."""

    def __init__(self, spark, settings: Settings) -> None:
        self._spark = spark
        self._settings = settings
        self._gold = Path(settings.gold_uri)

    def write_parquet(self, df: DataFrame, relative_path: str) -> None:
        """Write Parquet (single partition for small outputs)."""
        out = self._gold / relative_path
        out.mkdir(parents=True, exist_ok=True)
        path_str = str(out)
        logger.info("gold_write", extra={"path": path_str})
        try:
            df.coalesce(1).write.mode("overwrite").parquet(path_str)
        except Exception as exc:
            raise StorageError(f"Failed to write Gold at {path_str}") from exc
