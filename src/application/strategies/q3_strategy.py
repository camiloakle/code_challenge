"""Q3: Top 3 hours by sales amount per product category (challenge PDF)."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.application.strategies.base_strategy import BaseGoldStrategy
from src.shared.constants import COL_AMOUNT, COL_CATEGORY, COL_PURCHASE_TS


class Q3Strategy(BaseGoldStrategy):
    """Per category: top 3 hours by sum(purchase_amount); Hour as HH00 (e.g. 1300)."""

    def transform(self, silver_df: DataFrame) -> DataFrame:
        """Columns: category, Hour (int HH00), total_amount, hour_of_day."""
        with_hour = silver_df.withColumn("hour_of_day", F.hour(F.col(COL_PURCHASE_TS)))
        by_cat_hour = with_hour.groupBy(COL_CATEGORY, "hour_of_day").agg(
            F.sum(COL_AMOUNT).alias("total_amount")
        )
        w = Window.partitionBy(COL_CATEGORY).orderBy(
            F.col("total_amount").desc(),
            F.col("hour_of_day").asc(),
        )
        ranked = by_cat_hour.withColumn("_rank", F.row_number().over(w)).filter(
            F.col("_rank") <= 3
        )
        return (
            ranked.drop("_rank")
            .withColumn("Hour", F.col("hour_of_day") * 100)
            .select(
                F.col(COL_CATEGORY).alias("category"),
                "Hour",
                "total_amount",
                "hour_of_day",
            )
            .orderBy(F.col("category").asc(), F.col("total_amount").desc())
        )
