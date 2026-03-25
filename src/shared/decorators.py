"""Lightweight decorators for timing (optional use in services)."""

from __future__ import annotations

import functools
import time
from collections.abc import Callable
from typing import ParamSpec, TypeVar

from src.shared.logger import get_logger

logger = get_logger(__name__)

P = ParamSpec("P")
R = TypeVar("R")


def log_duration(name: str) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Log wall-clock duration of a function call."""

    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            start = time.perf_counter()
            try:
                return fn(*args, **kwargs)
            finally:
                elapsed = time.perf_counter() - start
                logger.info(f"{name}_duration_ms={elapsed * 1000:.2f}")

        return wrapper

    return decorator
