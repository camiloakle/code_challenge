"""Base classes for template-method style pipelines."""

from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

from src.shared.logger import get_logger

logger = get_logger(__name__)


class BaseSparkPipeline(ABC):
    """Template Method: shared logging and hook for concrete pipelines."""

    def __init__(self, name: str) -> None:
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def execute(self) -> DataFrame:
        """Run pipeline with structured logging."""
        logger.info(f"pipeline_start pipeline={self._name}")
        try:
            result = self.run()
            logger.info(f"pipeline_end pipeline={self._name}")
            return result
        except Exception:
            logger.exception(f"pipeline_failed pipeline={self._name}")
            raise

    @abstractmethod
    def run(self) -> DataFrame:
        """Subclasses implement transformation logic."""
        ...
