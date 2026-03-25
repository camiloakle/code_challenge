"""ValidationService — requires Spark for F.col."""

from __future__ import annotations

import pytest

from src.application.services.validation_service import ValidationService
from src.core.exceptions import DataValidationError


@pytest.mark.spark
def test_assert_null_ratio_passes(spark_session) -> None:
    vs = ValidationService(0.05)
    rows = [(i, str(i)) for i in range(100)]
    rows[0] = (0, None)
    df = spark_session.createDataFrame(rows, ["id", "col_a"])
    vs.assert_null_ratio(df, "col_a", "test")


@pytest.mark.spark
def test_assert_null_ratio_raises(spark_session) -> None:
    vs = ValidationService(0.05)
    rows = [(i, None if i < 10 else str(i)) for i in range(100)]
    df = spark_session.createDataFrame(rows, ["id", "col_a"])
    with pytest.raises(DataValidationError, match="null ratio"):
        vs.assert_null_ratio(df, "col_a", "test")


@pytest.mark.spark
def test_assert_null_ratio_empty_frame(spark_session) -> None:
    vs = ValidationService(0.05)
    df = spark_session.createDataFrame([], schema="id INT, col_a STRING")
    vs.assert_null_ratio(df, "col_a", "test")
