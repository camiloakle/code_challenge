"""Simple null-ratio checks (lightweight alternative to Great Expectations)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.core.exceptions import DataValidationError
from src.shared.logger import get_logger

logger = get_logger(__name__)


class ValidationService:
    """Validates that critical columns are within acceptable null ratios."""

    def __init__(self, max_null_percentage: float) -> None:
        self._max_null_percentage = max_null_percentage

    def assert_null_ratio(self, df: DataFrame, column: str, label: str = "") -> None:
        """Raise DataValidationError if null fraction exceeds threshold.

        Args:
            df: Input DataFrame.
            column: Column to check.
            label: Context for logging.

        Raises:
            DataValidationError: When null percentage is too high.
        """
        total = df.count()
        if total == 0:
            logger.warning("validation_empty_frame", extra={"label": label})
            return
        nulls = df.filter(F.col(column).isNull()).count()
        ratio = nulls / total
        if ratio > self._max_null_percentage:
            raise DataValidationError(
                f"{label}: column {column} null ratio {ratio:.4f} exceeds {self._max_null_percentage}"
            )
