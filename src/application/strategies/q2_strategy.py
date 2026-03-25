"""Q2: Average sale amount per merchant in each state (challenge PDF)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.application.strategies.base_strategy import BaseGoldStrategy
from src.shared.constants import COL_AMOUNT, COL_MERCHANT_NAME, COL_STATE_ID


class Q2Strategy(BaseGoldStrategy):
    """Merchant × state average ticket; largest averages first."""

    def transform(self, silver_df: DataFrame) -> DataFrame:
        """Columns: Merchant, State ID, Average Amount (PDF naming)."""
        return (
            silver_df.groupBy(COL_MERCHANT_NAME, COL_STATE_ID)
            .agg(F.avg(COL_AMOUNT).alias("Average Amount"))
            .withColumnRenamed(COL_MERCHANT_NAME, "Merchant")
            .withColumnRenamed(COL_STATE_ID, "State ID")
            .select("Merchant", "State ID", "Average Amount")
            .orderBy(F.col("Average Amount").desc(), F.col("Merchant").asc(), F.col("State ID").asc())
        )
