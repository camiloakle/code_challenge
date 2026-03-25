"""ProfilingService."""

from unittest.mock import MagicMock

from src.application.services.profiling_service import ProfilingService


def test_row_count() -> None:
    df = MagicMock()
    df.count.return_value = 42
    assert ProfilingService().row_count(df) == 42


def test_describe_delegates() -> None:
    df = MagicMock()
    out = MagicMock()
    df.select.return_value.describe.return_value = out
    assert ProfilingService().describe_numeric(df, ["a"]) is out
