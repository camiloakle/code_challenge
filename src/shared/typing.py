"""Type aliases for Spark-heavy code."""

from __future__ import annotations

from typing import Any

# PySpark DataFrame is not always imported at type-check time
SparkDataFrame = Any
