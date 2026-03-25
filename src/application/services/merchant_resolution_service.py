"""Resolve duplicate merchant_id rows to one golden record per id (Silver).

Without this, joins multiply rows when merchant_id repeats in Bronze.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.shared.constants import COL_MERCHANT_ID

# Deterministic tie-break order (documented in docs/ASSUMPTIONS.md)
RESOLUTION_RANK_COL = "resolution_rank"
RESOLUTION_WINNER_COL = "is_resolved_winner"

_COL_ACTIVE_LAG12 = "active_months_lag12"
_COL_ACTIVE_LAG6 = "active_months_lag6"
_COL_ACTIVE_LAG3 = "active_months_lag3"
_COL_AVG_PURCHASES_LAG12 = "avg_purchases_lag12"
_COL_MERCHANT_NAME = "merchant_name"
_COL_SALES_RANGE = "most_recent_sales_range"
_COL_PURCHASES_RANGE = "most_recent_purchases_range"


def _ordinal_ae_best_first(col_name: str):
    """Data Dictionary: A > B > C > D > E. Map to 1..5 (1 = best); unknown -> null."""
    c = F.upper(F.trim(F.col(col_name)))
    return (
        F.when(c == "A", 1)
        .when(c == "B", 2)
        .when(c == "C", 3)
        .when(c == "D", 4)
        .when(c == "E", 5)
        .otherwise(None)
    )


def _resolution_window(merchants: DataFrame):
    """Deterministic order: recent brackets (A–E), then activity lags, then name."""
    order_exprs = []
    cols = merchants.columns
    if _COL_SALES_RANGE in cols:
        order_exprs.append(_ordinal_ae_best_first(_COL_SALES_RANGE).asc_nulls_last())
    if _COL_PURCHASES_RANGE in cols:
        order_exprs.append(
            _ordinal_ae_best_first(_COL_PURCHASES_RANGE).asc_nulls_last()
        )
    order_exprs.extend(
        [
            F.col(_COL_ACTIVE_LAG12).desc_nulls_last(),
            F.col(_COL_ACTIVE_LAG6).desc_nulls_last(),
            F.col(_COL_ACTIVE_LAG3).desc_nulls_last(),
            F.col(_COL_AVG_PURCHASES_LAG12).desc_nulls_last(),
            F.col(_COL_MERCHANT_NAME).asc_nulls_last(),
        ]
    )
    return Window.partitionBy(COL_MERCHANT_ID).orderBy(*order_exprs)


class MerchantResolutionService:
    """One row per merchant_id for joins; audit rows when duplicates exist."""

    def resolve(self, merchants: DataFrame) -> tuple[DataFrame, DataFrame]:
        """Return (resolved_merchants, audit_only_duplicate_ids).

        - `resolved_merchants`: same schema as input, one row per merchant_id.
        - `audit`: duplicate merchant_id rows with `resolution_rank` and
          `is_resolved_winner`.
        """
        ranked = merchants.withColumn(
            RESOLUTION_RANK_COL,
            F.row_number().over(_resolution_window(merchants)),
        )
        resolved = ranked.filter(F.col(RESOLUTION_RANK_COL) == 1).drop(
            RESOLUTION_RANK_COL
        )

        dup_ids = (
            merchants.groupBy(COL_MERCHANT_ID)
            .count()
            .filter(F.col("count") > 1)
            .select(COL_MERCHANT_ID)
        )
        audit = ranked.join(dup_ids, COL_MERCHANT_ID, "inner").withColumn(
            RESOLUTION_WINNER_COL,
            F.col(RESOLUTION_RANK_COL) == 1,
        )
        return resolved, audit
