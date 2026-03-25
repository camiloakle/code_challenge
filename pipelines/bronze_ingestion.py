"""Bronze layer: ingest raw CSV / Parquet into Delta tables with metadata."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from config.settings import Settings
from src.infrastructure.spark.session import create_session
from src.infrastructure.storage.local_client import ensure_dir
from src.shared.logger import configure_logging, get_logger

logger = get_logger(__name__)


def _loose_part_parquet_glob(data_raw: Path) -> str | None:
    """Return a glob for loose ``part-*.parquet`` files under ``data/raw``."""
    matches = list(data_raw.glob("part-*.parquet"))
    if not matches:
        return None
    # Spark: glob path reads matching parquet files only.
    return str(data_raw / "part-*.parquet")


def resolve_raw_transactions_path(settings: Settings) -> Path | str:
    """Resolve Parquet source for transactions (directory, file, or glob).

    Spark exports often use ``part-*-tid-*.snappy.parquet`` under
    ``data/raw/historical_transactions/``. Reading the **directory** loads all
    parts. A single ``.parquet`` file is also valid.

    If the default directory is missing, fall back to
    ``{data_root}/raw/historical_transactions.parquet`` (legacy layout), then
    to ``data/raw/part-*.parquet`` when downloads land as a loose part file.
    Missing explicit ``RAW_TRANSACTIONS_PATH`` targets do not fall back.
    """
    primary = settings.raw_transactions_path.expanduser().resolve()
    if primary.exists():
        return primary

    data_raw = settings.data_root.expanduser().resolve() / "raw"
    expected_default_dir = (data_raw / "historical_transactions").resolve()
    legacy_file = (data_raw / "historical_transactions.parquet").resolve()

    if primary == expected_default_dir:
        if legacy_file.exists():
            return legacy_file
        loose = _loose_part_parquet_glob(data_raw)
        if loose:
            return loose

    msg = (
        f"Transactions input not found: {primary}. "
        "Use data/raw/historical_transactions/ with part-*.parquet, "
        "or data/raw/historical_transactions.parquet, loose "
        "data/raw/part-*.parquet, or set RAW_TRANSACTIONS_PATH."
    )
    raise FileNotFoundError(msg)


def _with_ingest_meta(df, source_name: str):
    """Add ingestion timestamp and source file name."""
    ts = F.lit(datetime.now(timezone.utc).isoformat())
    out = df.withColumn("_ingested_at", ts)
    return out.withColumn("_bronze_source", F.lit(source_name))


def run_bronze(settings: Settings) -> None:
    """Read raw files and write Bronze Delta tables."""
    configure_logging()
    spark = create_session(settings, "bronze-ingestion")
    tx_path = resolve_raw_transactions_path(settings)
    mer_path = settings.raw_merchants_path

    if not mer_path.exists():
        raise FileNotFoundError(
            f"Merchants not found: {mer_path}. "
            "Copy merchants-subset.csv to data/raw/merchants.csv or set path."
        )

    bronze_root = Path(settings.bronze_uri)
    ensure_dir(bronze_root)

    # Spark loads all part files when the path is a directory.
    logger.info("reading_transactions", extra={"path": str(tx_path)})
    transactions = spark.read.parquet(str(tx_path))
    transactions = _with_ingest_meta(transactions, str(tx_path))

    logger.info("reading_merchants", extra={"path": str(mer_path)})
    read_csv = spark.read.option("header", True).option("inferSchema", True)
    merchants = read_csv.csv(str(mer_path))
    merchants = _with_ingest_meta(merchants, str(mer_path))

    tx_out = str(bronze_root / "transactions")
    mer_out = str(bronze_root / "merchants")
    transactions.write.format("delta").mode("overwrite").save(tx_out)
    merchants.write.format("delta").mode("overwrite").save(mer_out)
    logger.info(
        "bronze_complete",
        extra={"transactions": tx_out, "merchants": mer_out},
    )
    spark.stop()


def main() -> None:
    """CLI entry."""
    parser = argparse.ArgumentParser(description="Bronze ingestion pipeline")
    parser.add_argument(
        "--env",
        default="dev",
        help="Environment label (settings use .env)",
    )
    _ = parser.parse_args()
    settings = Settings()
    run_bronze(settings)


if __name__ == "__main__":
    main()
