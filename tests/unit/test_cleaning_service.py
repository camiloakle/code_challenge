"""Cleaning rules — requires Spark."""

from __future__ import annotations

import pytest

from src.application.services.cleaning_service import CleaningService
from src.shared.constants import UNKNOWN_CATEGORY


@pytest.mark.spark
def test_merchant_name_fallback_and_category(spark_session) -> None:
    spark = spark_session
    tx = spark.createDataFrame(
        [
            ("m1", 10.0, None),
            ("m_missing", 5.0, "X"),
        ],
        ["merchant_id", "amount", "category"],
    )
    mer = spark.createDataFrame(
        [("m1", "Merchant One")],
        ["merchant_id", "merchant_name"],
    )
    out = CleaningService().join_and_clean(tx, mer)
    rows = {
        r["merchant_id"]: (r["merchant_name"], r["category"])
        for r in out.collect()
    }
    assert rows["m1"][0] == "Merchant One"
    assert rows["m_missing"][0] == "m_missing"
    assert rows["m1"][1] == UNKNOWN_CATEGORY
    assert rows["m_missing"][1] == "X"


@pytest.mark.spark
def test_aligns_alternate_date_and_amount_columns(spark_session) -> None:
    """Maps purchase_date / purchase_amount to contract column names."""
    spark = spark_session
    tx = spark.createDataFrame(
        [
            ("m1", "2024-01-15", 99.5, "Retail"),
        ],
        ["merchant_id", "purchase_date", "purchase_amount", "category"],
    )
    mer = spark.createDataFrame(
        [("m1", "Merchant One")],
        ["merchant_id", "merchant_name"],
    )
    out = CleaningService().join_and_clean(tx, mer)
    assert "purchase_ts" in out.columns
    assert "amount" in out.columns
    row = out.collect()[0]
    assert row["amount"] == 99.5
