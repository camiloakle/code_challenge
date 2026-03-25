"""Gold pipelines: read Silver, apply strategy, write Parquet."""

from __future__ import annotations

from pyspark.sql import DataFrame

from src.application.strategies.base_strategy import BaseGoldStrategy
from src.core.base import BaseSparkPipeline
from src.domain.repositories import GoldWriter, SilverTableRepository


class GoldQuestionPipeline(BaseSparkPipeline):
    """Template: load Silver → strategy → write Gold."""

    def __init__(
        self,
        name: str,
        silver_repo: SilverTableRepository,
        gold_writer: GoldWriter,
        strategy: BaseGoldStrategy,
        gold_subpath: str,
    ) -> None:
        super().__init__(name=name)
        self._silver_repo = silver_repo
        self._gold_writer = gold_writer
        self._strategy = strategy
        self._gold_subpath = gold_subpath

    def run(self) -> DataFrame:
        """Load Silver, transform, persist Gold, return result."""
        silver_df = self._silver_repo.load()
        gold_df = self._strategy.transform(silver_df)
        self._gold_writer.write_parquet(gold_df, self._gold_subpath)
        return gold_df
