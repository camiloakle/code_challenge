"""Decorators."""

from src.shared.decorators import log_duration


def test_log_duration_runs_function() -> None:
    called = []

    @log_duration("x")
    def f() -> str:
        called.append(1)
        return "ok"

    assert f() == "ok"
    assert called == [1]
