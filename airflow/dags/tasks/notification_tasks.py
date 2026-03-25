"""Placeholder for Slack/email notifications."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def notify_success(context: dict) -> None:
    """Hook for on-success notifications."""
    logger.info("DAG success: %s", context.get("dag_run"))


def notify_failure(context: dict) -> None:
    """Hook for on-failure notifications."""
    logger.error("DAG failure: %s", context.get("dag_run"))
