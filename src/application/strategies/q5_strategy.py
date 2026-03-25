"""Q5: Strategic advisor with business-driven and statistically sound recommendations."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.application.services.recommendation_engine import RecommendationEngine
from src.application.strategies.base_strategy import BaseGoldStrategy
from src.shared.constants import (
    COL_AMOUNT,
    COL_CATEGORY,
    COL_CITY_ID,
    COL_INSTALLMENTS,
    COL_MERCHANT_NAME,
    COL_PURCHASE_TS,
    DEFAULT_RATE_MONTHLY,
    GROSS_PROFIT_MARGIN,
    PAY_BEFORE_DEFAULT_FRACTION,
)


class Q5Strategy(BaseGoldStrategy):
    """Transaction-level EV model + aggregated tables for decision making."""

    def __init__(self) -> None:
        self._engine = RecommendationEngine()

    def transform(self, silver_df: DataFrame) -> DataFrame:
        """Per-row expected-value model for installments vs no-installments."""
        core = silver_df.select(
            COL_MERCHANT_NAME,
            COL_CATEGORY,
            COL_CITY_ID,
            COL_AMOUNT,
            COL_INSTALLMENTS,
        )
        with_rec = self._engine.apply_installment_decision(core).withColumn(
            "installments_flag",
            F.when(F.col(COL_INSTALLMENTS) > 0, F.lit("with_installments")).otherwise(
                F.lit("without_installments")
            ),
        )

        expected_revenue_installments = (
            (F.lit(1.0 - DEFAULT_RATE_MONTHLY) * F.col(COL_AMOUNT))
            + (
                F.lit(DEFAULT_RATE_MONTHLY)
                * F.lit(PAY_BEFORE_DEFAULT_FRACTION)
                * F.col(COL_AMOUNT)
            )
        )
        expected_profit_installments = expected_revenue_installments * F.lit(
            GROSS_PROFIT_MARGIN
        )
        expected_profit_no_installments = F.col(COL_AMOUNT) * F.lit(GROSS_PROFIT_MARGIN)

        return with_rec.select(
            COL_MERCHANT_NAME,
            COL_CATEGORY,
            COL_CITY_ID,
            COL_AMOUNT,
            COL_INSTALLMENTS,
            "installments_flag",
            expected_revenue_installments.alias("expected_revenue_installments"),
            expected_profit_installments.alias("expected_profit_installments"),
            expected_profit_no_installments.alias("expected_profit_no_installments"),
            (
                expected_profit_installments - expected_profit_no_installments
            ).alias("expected_profit_delta_installments"),
            "installments_recommended",
            "expected_profit_margin",
            "risk_score",
            "assumptions",
        )

    def advisory_metrics(self, silver_df: DataFrame) -> DataFrame:
        """Unified long-format metrics for Q5a–e with segmentation and concentration."""
        tx = silver_df.withColumn(
            "installments_flag",
            F.when(F.col(COL_INSTALLMENTS) > 0, F.lit("with_installments")).otherwise(
                F.lit("without_installments")
            ),
        )

        expected_revenue_installments = (
            (F.lit(1.0 - DEFAULT_RATE_MONTHLY) * F.col(COL_AMOUNT))
            + (
                F.lit(DEFAULT_RATE_MONTHLY)
                * F.lit(PAY_BEFORE_DEFAULT_FRACTION)
                * F.col(COL_AMOUNT)
            )
        )
        expected_profit_installments = expected_revenue_installments * F.lit(
            GROSS_PROFIT_MARGIN
        )
        expected_profit_no_installments = F.col(COL_AMOUNT) * F.lit(GROSS_PROFIT_MARGIN)

        tx_ev = tx.withColumn(
            "expected_revenue",
            F.when(
                F.col("installments_flag") == "with_installments",
                expected_revenue_installments,
            ).otherwise(F.col(COL_AMOUNT)),
        ).withColumn(
            "expected_profit",
            F.when(
                F.col("installments_flag") == "with_installments",
                expected_profit_installments,
            ).otherwise(expected_profit_no_installments),
        )

        # Window "global" explícito para evitar warnings de Spark:
        # "No Partition Defined for Window operation! Moving all data to a single partition..."
        global_part = Window.partitionBy(F.lit(1))

        def _shape(df: DataFrame, section: str, detail_col: str) -> DataFrame:
            total_amount_window = F.sum("total_amount").over(
                global_part.orderBy(F.col("total_amount").desc())
            )
            return (
                df.withColumn("avg_ticket", F.col("total_amount") / F.col("transaction_count"))
                .withColumn("section", F.lit(section))
                .withColumn("detail_key", F.col(detail_col).cast("string"))
                .withColumn("detail_label", F.col(detail_col).cast("string"))
                .withColumn("expected_revenue", F.lit(None).cast("double"))
                .withColumn("expected_profit", F.lit(None).cast("double"))
                .withColumn("segment", F.lit(None).cast("string"))
                .withColumn(
                    "share_of_total",
                    F.col("total_amount") / F.sum("total_amount").over(global_part),
                )
                .withColumn(
                    "cumulative_share",
                    total_amount_window / F.sum("total_amount").over(global_part),
                )
                .withColumn("extra_note", F.lit(None).cast("string"))
                .select(
                    "section",
                    "detail_key",
                    "detail_label",
                    "transaction_count",
                    "total_amount",
                    "avg_ticket",
                    "expected_revenue",
                    "expected_profit",
                    "share_of_total",
                    "cumulative_share",
                    "segment",
                    "extra_note",
                )
            )

        # q5a: cities with a balanced score (volume + revenue + avg ticket)
        city_base = (
            tx_ev.filter(F.col(COL_CITY_ID).isNotNull())
            .groupBy(COL_CITY_ID)
            .agg(
                F.count(F.lit(1)).alias("transaction_count"),
                F.sum(COL_AMOUNT).alias("total_amount"),
            )
        )
        city_scored = city_base.withColumn(
            "avg_ticket",
            F.col("total_amount") / F.col("transaction_count"),
        ).withColumn(
            "city_score",
            (
                0.4 * F.percent_rank().over(global_part.orderBy(F.col("transaction_count")))
                + 0.4 * F.percent_rank().over(global_part.orderBy(F.col("total_amount")))
                + 0.2 * F.percent_rank().over(global_part.orderBy(F.col("avg_ticket")))
            ),
        )
        q5a = (
            city_scored.orderBy(F.col("city_score").desc())
            .limit(40)
            .withColumn("section", F.lit("q5a_cities"))
            .withColumn("detail_key", F.col(COL_CITY_ID).cast("string"))
            .withColumn("detail_label", F.concat(F.lit("City_"), F.col(COL_CITY_ID).cast("string")))
            .withColumn("expected_revenue", F.lit(None).cast("double"))
            .withColumn("expected_profit", F.lit(None).cast("double"))
            .withColumn("segment", F.lit(None).cast("string"))
            .withColumn(
                "share_of_total",
                F.col("total_amount") / F.sum("total_amount").over(global_part),
            )
            .withColumn(
                "cumulative_share",
                F.sum("total_amount").over(global_part.orderBy(F.col("city_score").desc()))
                / F.sum("total_amount").over(global_part),
            )
            .withColumn("extra_note", F.lit("score=0.4*volume+0.4*revenue+0.2*avg_ticket"))
            .select(
                "section",
                "detail_key",
                "detail_label",
                "transaction_count",
                "total_amount",
                "avg_ticket",
                "expected_revenue",
                "expected_profit",
                "share_of_total",
                "cumulative_share",
                "segment",
                "extra_note",
            )
        )

        # q5b: categories with segmentation (high-volume / high-value / balanced)
        category_base = tx_ev.groupBy(COL_CATEGORY).agg(
            F.count(F.lit(1)).alias("transaction_count"),
            F.sum(COL_AMOUNT).alias("total_amount"),
        )
        category_scored = category_base.withColumn(
            "avg_ticket",
            F.col("total_amount") / F.col("transaction_count"),
        ).withColumn(
            "segment",
            F.when(
                (F.ntile(4).over(global_part.orderBy(F.col("transaction_count").desc())) == 1)
                & (F.ntile(4).over(global_part.orderBy(F.col("avg_ticket").desc())) > 2),
                F.lit("high_volume"),
            )
            .when(
                (F.ntile(4).over(global_part.orderBy(F.col("avg_ticket").desc())) == 1)
                & (F.ntile(4).over(global_part.orderBy(F.col("transaction_count").desc())) > 2),
                F.lit("high_value"),
            )
            .otherwise(F.lit("balanced")),
        )
        q5b = (
            category_scored.orderBy(F.col("total_amount").desc())
            .withColumn("section", F.lit("q5b_categories"))
            .withColumn("detail_key", F.col(COL_CATEGORY))
            .withColumn("detail_label", F.col(COL_CATEGORY))
            .withColumn("expected_revenue", F.lit(None).cast("double"))
            .withColumn("expected_profit", F.lit(None).cast("double"))
            .withColumn(
                "share_of_total",
                F.col("total_amount") / F.sum("total_amount").over(global_part),
            )
            .withColumn(
                "cumulative_share",
                F.sum("total_amount").over(global_part.orderBy(F.col("total_amount").desc()))
                / F.sum("total_amount").over(global_part),
            )
            .withColumn("extra_note", F.lit("segment based on transaction_count and avg_ticket quartiles"))
            .select(
                "section",
                "detail_key",
                "detail_label",
                "transaction_count",
                "total_amount",
                "avg_ticket",
                "expected_revenue",
                "expected_profit",
                "share_of_total",
                "cumulative_share",
                "segment",
                "extra_note",
            )
        )

        # q5c: monthly trends
        q5c = _shape(
            tx_ev.withColumn("month", F.date_format(F.col(COL_PURCHASE_TS), "yyyy-MM"))
            .groupBy("month")
            .agg(
                F.count(F.lit(1)).alias("transaction_count"),
                F.sum(COL_AMOUNT).alias("total_amount"),
            )
            .orderBy("month"),
            "q5c_months",
            "month",
        )

        # q5d: hours profile
        q5d = _shape(
            tx_ev.withColumn("hour_of_day", F.hour(F.col(COL_PURCHASE_TS)))
            .groupBy("hour_of_day")
            .agg(
                F.count(F.lit(1)).alias("transaction_count"),
                F.sum(COL_AMOUNT).alias("total_amount"),
            ),
            "q5d_hours",
            "hour_of_day",
        )

        # q5e: installments vs no-installments (overall)
        q5e = (
            tx_ev.groupBy("installments_flag")
            .agg(
                F.count(F.lit(1)).alias("transaction_count"),
                F.sum(COL_AMOUNT).alias("total_amount"),
                F.sum("expected_revenue").alias("expected_revenue"),
                F.sum("expected_profit").alias("expected_profit"),
            )
            .withColumn("avg_ticket", F.col("total_amount") / F.col("transaction_count"))
            .withColumn("section", F.lit("q5e_installments_overall"))
            .withColumn("detail_key", F.col("installments_flag"))
            .withColumn("detail_label", F.col("installments_flag"))
            .withColumn("segment", F.lit(None).cast("string"))
            .withColumn(
                "share_of_total",
                F.col("total_amount") / F.sum("total_amount").over(global_part),
            )
            .withColumn(
                "cumulative_share",
                F.sum("total_amount").over(global_part.orderBy(F.col("total_amount").desc()))
                / F.sum("total_amount").over(global_part),
            )
            .withColumn("extra_note", F.lit("default_rate=22.9%, recovery=50%, gross_margin=25%"))
            .select(
                "section",
                "detail_key",
                "detail_label",
                "transaction_count",
                "total_amount",
                "avg_ticket",
                "expected_revenue",
                "expected_profit",
                "share_of_total",
                "cumulative_share",
                "segment",
                "extra_note",
            )
        )
        # Backward-compat alias expected by `scripts/validate_challenge_results.py`
        q5_installments_impact = q5e.withColumn("section", F.lit("q5_installments_impact"))

        # q5e segmented by category
        q5e_category = (
            tx_ev.groupBy(COL_CATEGORY, "installments_flag")
            .agg(
                F.count(F.lit(1)).alias("transaction_count"),
                F.sum(COL_AMOUNT).alias("total_amount"),
                F.sum("expected_revenue").alias("expected_revenue"),
                F.sum("expected_profit").alias("expected_profit"),
            )
            .withColumn("avg_ticket", F.col("total_amount") / F.col("transaction_count"))
            .withColumn("section", F.lit("q5e_installments_by_category"))
            .withColumn("detail_key", F.concat_ws("|", F.col(COL_CATEGORY), F.col("installments_flag")))
            .withColumn("detail_label", F.concat_ws(" | ", F.col(COL_CATEGORY), F.col("installments_flag")))
            .withColumn("segment", F.col(COL_CATEGORY))
            .withColumn(
                "share_of_total",
                F.col("total_amount") / F.sum("total_amount").over(global_part),
            )
            .withColumn("cumulative_share", F.lit(None).cast("double"))
            .withColumn("extra_note", F.lit("segmented installment comparison by category"))
            .select(
                "section",
                "detail_key",
                "detail_label",
                "transaction_count",
                "total_amount",
                "avg_ticket",
                "expected_revenue",
                "expected_profit",
                "share_of_total",
                "cumulative_share",
                "segment",
                "extra_note",
            )
        )

        # q5e segmented by city (optional/ideal)
        q5e_city = (
            tx_ev.filter(F.col(COL_CITY_ID).isNotNull())
            .groupBy(COL_CITY_ID, "installments_flag")
            .agg(
                F.count(F.lit(1)).alias("transaction_count"),
                F.sum(COL_AMOUNT).alias("total_amount"),
                F.sum("expected_revenue").alias("expected_revenue"),
                F.sum("expected_profit").alias("expected_profit"),
            )
            .withColumn("avg_ticket", F.col("total_amount") / F.col("transaction_count"))
            .withColumn("section", F.lit("q5e_installments_by_city"))
            .withColumn("detail_key", F.concat_ws("|", F.col(COL_CITY_ID).cast("string"), F.col("installments_flag")))
            .withColumn("detail_label", F.concat(F.lit("City_"), F.col(COL_CITY_ID).cast("string"), F.lit(" | "), F.col("installments_flag")))
            .withColumn("segment", F.col(COL_CITY_ID).cast("string"))
            .withColumn(
                "share_of_total",
                F.col("total_amount") / F.sum("total_amount").over(global_part),
            )
            .withColumn("cumulative_share", F.lit(None).cast("double"))
            .withColumn("extra_note", F.lit("segmented installment comparison by city"))
            .select(
                "section",
                "detail_key",
                "detail_label",
                "transaction_count",
                "total_amount",
                "avg_ticket",
                "expected_revenue",
                "expected_profit",
                "share_of_total",
                "cumulative_share",
                "segment",
                "extra_note",
            )
        )

        return (
            q5a.unionByName(q5b)
            .unionByName(q5c)
            .unionByName(q5d)
            .unionByName(q5e)
            .unionByName(q5_installments_impact)
            .unionByName(q5e_category)
            .unionByName(q5e_city)
        )
