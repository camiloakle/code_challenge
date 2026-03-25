"""Factory wiring."""

from unittest.mock import MagicMock

import pytest

from config.settings import Settings
from pipelines.factory import build_gold_pipeline


def test_factory_rejects_unknown_question() -> None:
    spark = MagicMock()
    with pytest.raises(ValueError, match="Unknown question"):
        build_gold_pipeline("q99", spark, Settings())
