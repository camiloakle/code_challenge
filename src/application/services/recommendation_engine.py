"""Installment recommendation logic (Q5e) — formulas from challenge brief."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

from src.shared.constants import (
    COL_AMOUNT,
    EXPECTED_PROFIT_MARGIN,
    GROSS_PROFIT_MARGIN,
    THRESHOLD_TOLERANCE,
)


class RecommendationEngine:
    """Computes expected value vs single-payment profit threshold."""

    def apply_installment_decision(self, df: DataFrame) -> DataFrame:
        """Add columns for Q5e: installments_recommended, expected_profit_margin, risk_score, assumptions.

        Args:
            df: Silver DataFrame with amount column.

        Returns:
            DataFrame with recommendation columns appended.
        """
        a = F.col(COL_AMOUNT)
        profit_full = a * F.lit(0.25) * F.lit(0.771)
        profit_default = a * F.lit(0.50) * F.lit(0.25) * F.lit(0.229)
        expected_value = profit_full + profit_default
        threshold = a * F.lit(GROSS_PROFIT_MARGIN) * F.lit(THRESHOLD_TOLERANCE)
        assumptions = F.array(
            F.lit("Gross profit margin 25%"),
            F.lit("Default rate 22.9% per month"),
            F.lit("Payment before default 50% of principal"),
            F.lit("Equal monthly installments"),
        )
        return (
            df.withColumn("expected_profit_margin", F.lit(EXPECTED_PROFIT_MARGIN).cast(FloatType()))
            .withColumn("risk_score", F.lit(0.229).cast(FloatType()))
            .withColumn("assumptions", assumptions)
            .withColumn(
                "installments_recommended",
                F.when(expected_value >= threshold, F.lit(True)).otherwise(F.lit(False)),
            )
        )
