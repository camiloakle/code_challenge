"""Silver layer: join + CleaningService only (no Gold logic)."""

from __future__ import annotations

import argparse
from pathlib import Path

from config.settings import Settings
from src.application.services.cleaning_service import CleaningService
from src.application.services.merchant_resolution_service import (
    MerchantResolutionService,
)
from src.infrastructure.spark.session import create_session
from src.infrastructure.storage.local_client import ensure_dir
from src.shared.logger import configure_logging, get_logger

logger = get_logger(__name__)


def run_silver(settings: Settings) -> None:
    """Read Bronze Delta, apply cleaning rules, write Silver Delta."""
    configure_logging()
    spark = create_session(settings, "silver-builder")

    bronze = Path(settings.bronze_uri)
    tx_in = str(bronze / "transactions")
    mer_in = str(bronze / "merchants")

    transactions = spark.read.format("delta").load(tx_in)
    merchants = spark.read.format("delta").load(mer_in)

    resolver = MerchantResolutionService()
    merchants_resolved, merchants_audit = resolver.resolve(merchants)

    silver_root = Path(settings.silver_uri)
    ensure_dir(silver_root)
    resolved_path = str(silver_root / "merchants_resolved")
    audit_path = str(silver_root / "merchants_duplicates_audit")
    merchants_resolved.write.format("delta").mode("overwrite").save(
        resolved_path
    )
    merchants_audit.write.format("delta").mode("overwrite").save(audit_path)

    cleaner = CleaningService()
    enriched = cleaner.join_and_clean(transactions, merchants_resolved)

    out = str(silver_root / "enriched_transactions")
    enriched.write.format("delta").mode("overwrite").save(out)
    logger.info(
        "silver_complete",
        extra={
            "enriched_transactions": out,
            "merchants_resolved": resolved_path,
            "merchants_duplicates_audit": audit_path,
        },
    )
    spark.stop()


def main() -> None:
    """CLI entry."""
    parser = argparse.ArgumentParser(description="Silver builder pipeline")
    parser.add_argument("--env", default="dev")
    _ = parser.parse_args()
    settings = Settings()
    run_silver(settings)


if __name__ == "__main__":
    main()
