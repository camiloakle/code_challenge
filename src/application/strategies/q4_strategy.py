"""Q4: City metrics, global merchant popularity, city×category association (challenge PDF)."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.application.strategies.base_strategy import BaseGoldStrategy
from src.shared.constants import COL_CATEGORY, COL_CITY_ID, COL_MERCHANT_ID, COL_MERCHANT_NAME


def _cramers_v(contingency: np.ndarray) -> float:
    """Cramér's V for independence of two categorical variables (city × category)."""
    obs = np.asarray(contingency, dtype=float)
    r, c = obs.shape
    if r < 2 or c < 2:
        return float("nan")
    n = float(obs.sum())
    if n <= 0:
        return float("nan")
    row_s = obs.sum(axis=1, keepdims=True)
    col_s = obs.sum(axis=0, keepdims=True)
    expected = row_s @ col_s / n
    chi2 = float(np.sum((obs - expected) ** 2 / np.maximum(expected, 1e-12)))
    k = min(r - 1, c - 1)
    if k <= 0:
        return float("nan")
    return math.sqrt(chi2 / (n * k))


class Q4Strategy(BaseGoldStrategy):
    """City-level stats, global top merchants + primary city, Cramér's V (global)."""

    _MIN_TX_PER_CITY_FOR_ASSOCIATION = 50
    TOP_K_GLOBAL_MERCHANTS = 100

    def _compute_cramers_v_city_category(self, base: DataFrame) -> float:
        """Contingency city×category (cities with >= min tx); return Cramér's V."""
        heavy = (
            base.groupBy(COL_CITY_ID)
            .count()
            .filter(F.col("count") >= self._MIN_TX_PER_CITY_FOR_ASSOCIATION)
            .select(COL_CITY_ID)
        )
        cc = (
            base.join(heavy, COL_CITY_ID, "inner")
            .groupBy(COL_CITY_ID, COL_CATEGORY)
            .count()
        )
        rows = cc.collect()
        if not rows:
            return float("nan")
        pdf = pd.DataFrame(
            [(r[COL_CITY_ID], r[COL_CATEGORY], int(r["count"])) for r in rows],
            columns=[COL_CITY_ID, COL_CATEGORY, "count"],
        )
        pivot = pdf.pivot_table(
            index=COL_CITY_ID,
            columns=COL_CATEGORY,
            values="count",
            aggfunc="sum",
            fill_value=0.0,
        )
        return _cramers_v(pivot.values)

    def transform(self, silver_df: DataFrame) -> DataFrame:
        """Per-city volume metrics only (no repeated global association scalar)."""
        base = silver_df.filter(F.col(COL_CITY_ID).isNotNull())
        return (
            base.groupBy(COL_CITY_ID)
            .agg(
                F.count(F.lit(1)).alias("total_transactions"),
                F.countDistinct(COL_MERCHANT_ID).alias("distinct_merchants"),
                F.countDistinct(COL_CATEGORY).alias("distinct_categories"),
            )
            .withColumn("City", F.concat(F.lit("City_"), F.col(COL_CITY_ID).cast("string")))
            .orderBy(F.col("total_transactions").desc())
        )

    def transform_city_category_association(self, silver_df: DataFrame) -> DataFrame:
        """Single-row global Cramér's V (city × category contingency)."""
        base = silver_df.filter(F.col(COL_CITY_ID).isNotNull())
        v_val = self._compute_cramers_v_city_category(base)
        spark = silver_df.sparkSession
        return spark.createDataFrame(
            [
                (
                    float(v_val),
                    "cramers_v_city_x_category_global",
                    int(self._MIN_TX_PER_CITY_FOR_ASSOCIATION),
                )
            ],
            [
                "city_category_cramers_v",
                "metric_description",
                "min_city_tx_for_contingency",
            ],
        )

    def transform_top_merchants_global(self, silver_df: DataFrame) -> DataFrame:
        """Top K merchants globally by transaction count + primary city (max tx in city)."""
        base = silver_df.filter(F.col(COL_CITY_ID).isNotNull())
        g = base.groupBy(COL_MERCHANT_ID, COL_MERCHANT_NAME).agg(
            F.count(F.lit(1)).alias("total_transactions"),
            F.countDistinct(COL_CITY_ID).alias("n_cities_active"),
        )
        ordered_top = g.orderBy(
            F.col("total_transactions").desc(),
            F.col(COL_MERCHANT_ID).asc(),
        ).limit(self.TOP_K_GLOBAL_MERCHANTS)
        top_rows = ordered_top.collect()
        if not top_rows:
            return silver_df.sparkSession.createDataFrame(
                [],
                schema="global_rank int, merchant_id string, merchant_name string, total_transactions long, primary_city_id int, City string, tx_in_primary_city long, n_cities_active long",
            )
        top = silver_df.sparkSession.createDataFrame(
            [
                {
                    "global_rank": idx + 1,
                    COL_MERCHANT_ID: row[COL_MERCHANT_ID],
                    COL_MERCHANT_NAME: row[COL_MERCHANT_NAME],
                    "total_transactions": row["total_transactions"],
                    "n_cities_active": row["n_cities_active"],
                }
                for idx, row in enumerate(top_rows)
            ]
        )

        city_vol = base.groupBy(COL_MERCHANT_ID, COL_CITY_ID).agg(
            F.count(F.lit(1)).alias("tx_in_city")
        )
        w2 = Window.partitionBy(COL_MERCHANT_ID).orderBy(
            F.col("tx_in_city").desc(),
            F.col(COL_CITY_ID).asc(),
        )
        primary = (
            city_vol.withColumn("_rn", F.row_number().over(w2))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .select(
                COL_MERCHANT_ID,
                F.col(COL_CITY_ID).alias("primary_city_id"),
                F.col("tx_in_city").alias("tx_in_primary_city"),
            )
        )
        # Tie-break fallback: min(city_id) if primary window ever misses (left join)
        fallback = base.groupBy(COL_MERCHANT_ID).agg(
            F.min(COL_CITY_ID).alias("fallback_city_id")
        )
        joined = (
            top.join(primary, COL_MERCHANT_ID, "left")
            .join(fallback, COL_MERCHANT_ID, "left")
            .withColumn(
                "primary_city_id",
                F.coalesce(F.col("primary_city_id"), F.col("fallback_city_id")),
            )
            .drop("fallback_city_id")
        )
        # Refresh tx in primary city when coalesced from fallback
        tx_map = city_vol.select(
            COL_MERCHANT_ID,
            F.col(COL_CITY_ID).alias("_pc"),
            F.col("tx_in_city").alias("_txc"),
        )
        out = (
            joined.alias("j")
            .join(
                tx_map.alias("t"),
                (F.col(f"j.{COL_MERCHANT_ID}") == F.col(f"t.{COL_MERCHANT_ID}"))
                & (F.col("j.primary_city_id") == F.col("t._pc")),
                how="left",
            )
            .select("j.*", F.col("t._pc").alias("_pc"), F.col("t._txc").alias("_txc"))
            .withColumn(
                "tx_in_primary_city",
                F.coalesce(F.col("_txc"), F.col("tx_in_primary_city")),
            )
            .drop("_pc", "_txc")
            .withColumn(
                "City",
                F.concat(F.lit("City_"), F.col("primary_city_id").cast("string")),
            )
            .select(
                "global_rank",
                F.col(COL_MERCHANT_ID).alias("merchant_id"),
                F.col(COL_MERCHANT_NAME).alias("merchant_name"),
                "total_transactions",
                "primary_city_id",
                "City",
                "tx_in_primary_city",
                "n_cities_active",
            )
            .orderBy(F.col("global_rank").asc())
        )
        return out

    def transform_top_merchants_distribution_by_city(
        self, top_merchants_global_df: DataFrame
    ) -> DataFrame:
        """How top-K global merchants distribute across primary cities."""
        k = float(self.TOP_K_GLOBAL_MERCHANTS)
        dist = (
            top_merchants_global_df.groupBy("primary_city_id", "City")
            .agg(F.count(F.lit(1)).alias("n_top_merchants_in_top_k"))
            .withColumn("pct_of_top_k", F.col("n_top_merchants_in_top_k") / F.lit(k) * 100.0)
            .orderBy(F.col("n_top_merchants_in_top_k").desc(), F.col("primary_city_id").asc())
        )
        return dist

    def transform_merchant_popularity_by_city(self, silver_df: DataFrame) -> DataFrame:
        """Complementary: most transactions per merchant within each city (local popularity)."""
        base = silver_df.filter(F.col(COL_CITY_ID).isNotNull())
        per_m = base.groupBy(COL_CITY_ID, COL_MERCHANT_ID, COL_MERCHANT_NAME).agg(
            F.count(F.lit(1)).alias("transaction_count")
        )
        w = Window.partitionBy(COL_CITY_ID).orderBy(
            F.col("transaction_count").desc(),
            F.col(COL_MERCHANT_ID).asc(),
        )
        return (
            per_m.withColumn("popularity_rank", F.row_number().over(w))
            .filter(F.col("popularity_rank") <= 5)
            .withColumn("City", F.concat(F.lit("City_"), F.col(COL_CITY_ID).cast("string")))
            .select(
                "City",
                F.col(COL_CITY_ID).alias("city_id"),
                F.col(COL_MERCHANT_ID).alias("merchant_id"),
                F.col(COL_MERCHANT_NAME).alias("merchant_name"),
                "transaction_count",
                "popularity_rank",
            )
            .orderBy(F.col("city_id"), F.col("popularity_rank").asc())
        )
