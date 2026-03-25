"""Application settings via Pydantic."""

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for paths, Spark, and quality thresholds."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    environment: Literal["dev", "staging", "prod"] = "dev"

    data_root: Path = Field(default=Path("./data"))

    bronze_path: str = "bronze/"
    silver_path: str = "silver/"
    gold_path: str = "gold/"

    # Directory with Spark-style part-*.snappy.parquet (preferred); see docs/ASSUMPTIONS.md
    raw_transactions_path: Path = Field(default=Path("data/raw/historical_transactions"))
    raw_merchants_path: Path = Field(default=Path("data/raw/merchants.csv"))

    spark_driver_memory: str = "4g"
    spark_executor_instances: int = 4
    spark_shuffle_partitions: int = 200

    max_null_percentage: float = 0.05

    @property
    def bronze_uri(self) -> str:
        """Filesystem URI for Bronze Delta root."""
        return str((self.data_root / self.bronze_path).resolve())

    @property
    def silver_uri(self) -> str:
        """Filesystem URI for Silver Delta root."""
        return str((self.data_root / self.silver_path).resolve())

    @property
    def gold_uri(self) -> str:
        """Filesystem URI for Gold Parquet root."""
        return str((self.data_root / self.gold_path).resolve())


settings = Settings()
