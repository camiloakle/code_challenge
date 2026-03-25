"""Structured logging using loguru."""

from __future__ import annotations

import sys
from typing import Any

from loguru import logger


def configure_logging(level: str = "INFO") -> None:
    """Configure loguru for console JSON-like key=value output."""
    logger.remove()
    logger.add(
        sys.stderr,
        level=level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | {level} | {message}",
    )


def get_logger(name: str) -> Any:
    """Return a namespaced logger (loguru bound)."""
    return logger.bind(component=name)
