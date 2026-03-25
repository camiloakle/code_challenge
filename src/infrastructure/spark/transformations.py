"""Reusable Spark transforms not tied to a single question."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.shared.constants import COL_PURCHASE_TS


def with_month_column(df: DataFrame, col_name: str = "month") -> DataFrame:
    """Add first day of month from purchase_ts."""
    return df.withColumn(col_name, F.date_trunc("month", F.col(COL_PURCHASE_TS)))


def with_hour_column(df: DataFrame, col_name: str = "hour_of_day") -> DataFrame:
    """Add hour (0-23) from purchase_ts."""
    return df.withColumn(col_name, F.hour(F.col(COL_PURCHASE_TS)))
