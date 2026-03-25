"""MerchantResolutionService — requires Spark."""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from src.application.services.merchant_resolution_service import (
    RESOLUTION_RANK_COL,
    RESOLUTION_WINNER_COL,
    MerchantResolutionService,
)


@pytest.mark.spark
def test_resolve_picks_row_by_activity_ladders(spark_session) -> None:
    spark = spark_session
    rows = [
        ("m1", "Low", "C", "C", 1, 1, 1, 10.0),
        ("m1", "High", "C", "C", 5, 5, 5, 1.0),
        ("m2", "Only", "A", "A", 1, 1, 1, 1.0),
    ]
    mer = spark.createDataFrame(
        rows,
        [
            "merchant_id",
            "merchant_name",
            "most_recent_sales_range",
            "most_recent_purchases_range",
            "active_months_lag12",
            "active_months_lag6",
            "active_months_lag3",
            "avg_purchases_lag12",
        ],
    )
    resolved, audit = MerchantResolutionService().resolve(mer)
    assert resolved.count() == 2
    winner = resolved.filter("merchant_id = 'm1'").collect()[0]
    assert winner["merchant_name"] == "High"

    assert audit.count() == 2
    assert RESOLUTION_RANK_COL in audit.columns
    assert RESOLUTION_WINNER_COL in audit.columns
    winners = audit.filter(F.col(RESOLUTION_WINNER_COL)).collect()
    assert len(winners) == 1
    assert winners[0]["merchant_name"] == "High"


@pytest.mark.spark
def test_resolve_prefers_better_most_recent_sales_range(spark_session) -> None:
    """Same activity; A should beat E on sales_range (Data Dictionary order)."""
    spark = spark_session
    rows = [
        ("m1", "BestSales", "A", "E", 3, 3, 3, 5.0),
        ("m1", "WorstSales", "E", "A", 3, 3, 3, 5.0),
    ]
    mer = spark.createDataFrame(
        rows,
        [
            "merchant_id",
            "merchant_name",
            "most_recent_sales_range",
            "most_recent_purchases_range",
            "active_months_lag12",
            "active_months_lag6",
            "active_months_lag3",
            "avg_purchases_lag12",
        ],
    )
    resolved, _ = MerchantResolutionService().resolve(mer)
    w = resolved.filter("merchant_id = 'm1'").collect()[0]
    assert w["merchant_name"] == "BestSales"


@pytest.mark.spark
def test_resolve_prefers_better_purchases_when_sales_tied(spark_session) -> None:
    spark = spark_session
    rows = [
        ("m1", "HighPurch", "C", "A", 2, 2, 2, 4.0),
        ("m1", "LowPurch", "C", "E", 2, 2, 2, 4.0),
    ]
    mer = spark.createDataFrame(
        rows,
        [
            "merchant_id",
            "merchant_name",
            "most_recent_sales_range",
            "most_recent_purchases_range",
            "active_months_lag12",
            "active_months_lag6",
            "active_months_lag3",
            "avg_purchases_lag12",
        ],
    )
    resolved, _ = MerchantResolutionService().resolve(mer)
    w = resolved.filter("merchant_id = 'm1'").collect()[0]
    assert w["merchant_name"] == "HighPurch"
