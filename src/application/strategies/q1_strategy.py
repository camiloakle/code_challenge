"""Q1: Top 5 merchants by purchase total per month and city (challenge PDF)."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.application.strategies.base_strategy import BaseGoldStrategy
from src.shared.constants import COL_AMOUNT, COL_CITY_ID, COL_MERCHANT_NAME, COL_PURCHASE_TS


class Q1Strategy(BaseGoldStrategy):
    """Top 5 merchants by sum(amount) per (Month, City), deterministic tie-break."""

    def transform(self, silver_df: DataFrame) -> DataFrame:
        """Return columns: Month, City, Merchant, Purchase Total, No of sales."""
        base = silver_df.withColumn(
            "Month", F.date_format(F.col(COL_PURCHASE_TS), "yyyy-MM")
        ).withColumn("City", F.concat(F.lit("City_"), F.col(COL_CITY_ID).cast("string")))
        agg = (
            base.groupBy("Month", "City", COL_MERCHANT_NAME)
            .agg(
                F.sum(COL_AMOUNT).alias("Purchase Total"),
                F.count(F.lit(1)).alias("No of sales"),
            )
            .withColumnRenamed(COL_MERCHANT_NAME, "Merchant")
        )
        w = Window.partitionBy("Month", "City").orderBy(
            F.col("Purchase Total").desc(),
            F.col("Merchant").asc(),
        )
        return (
            agg.withColumn("_rank", F.row_number().over(w))
            .filter(F.col("_rank") <= 5)
            .drop("_rank")
            .select("Month", "City", "Merchant", "Purchase Total", "No of sales")
            .orderBy(
                F.col("Month"),
                F.col("City"),
                F.col("Purchase Total").desc(),
                F.col("Merchant").asc(),
            )
        )
