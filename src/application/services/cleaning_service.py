"""Centralized cleaning rules for Silver layer only.

These rules MUST NOT be duplicated in Gold (Q1–Q5) pipelines.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, lit

from src.shared.constants import (
    ALT_AMOUNT,
    ALT_PURCHASE_TS,
    COL_AMOUNT,
    COL_CATEGORY,
    COL_MERCHANT_ID,
    COL_MERCHANT_NAME,
    COL_PURCHASE_TS,
    UNKNOWN_CATEGORY,
)


class CleaningService:
    """Applies merchant name fallback and null category handling after join."""

    def align_to_challenge_schema(self, transactions: DataFrame) -> DataFrame:
        """Map alternate raw column names to the internal Gold contract."""
        cols = set(transactions.columns)
        out = transactions
        if COL_PURCHASE_TS not in cols and ALT_PURCHASE_TS in cols:
            out = out.withColumnRenamed(ALT_PURCHASE_TS, COL_PURCHASE_TS)
        if COL_AMOUNT not in cols and ALT_AMOUNT in cols:
            out = out.withColumnRenamed(ALT_AMOUNT, COL_AMOUNT)
        return out

    def join_and_clean(
        self, transactions: DataFrame, merchants: DataFrame
    ) -> DataFrame:
        """Left join merchants; apply coalesce rules from the challenge brief.

        Args:
            transactions: Bronze transactions DataFrame.
            merchants: Merchants with one row per merchant_id (Silver uses
                `MerchantResolutionService` after Bronze ingest).

        Returns:
            Enriched DataFrame with cleaned merchant_name and category.
        """
        transactions = self.align_to_challenge_schema(transactions)
        m = merchants.select(
            col(COL_MERCHANT_ID),
            col(COL_MERCHANT_NAME).alias("_merchant_name_lookup"),
        )
        joined = transactions.join(m, on=COL_MERCHANT_ID, how="left")
        with_name = joined.withColumn(
            COL_MERCHANT_NAME,
            coalesce(col("_merchant_name_lookup"), col(COL_MERCHANT_ID)),
        ).drop("_merchant_name_lookup")
        return with_name.withColumn(
            COL_CATEGORY,
            coalesce(col(COL_CATEGORY), lit(UNKNOWN_CATEGORY)),
        )
