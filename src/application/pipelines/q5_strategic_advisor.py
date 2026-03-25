"""Q5 pipeline wiring — transaction-level Q5e + advisory summary Parquet."""

from __future__ import annotations

from pyspark.sql import DataFrame

from src.application.pipelines.base_pipeline import GoldQuestionPipeline
from src.application.strategies.q5_strategy import Q5Strategy
from src.domain.repositories import GoldWriter, SilverTableRepository


class Q5GoldPipeline(GoldQuestionPipeline):
    """Writes `q5_results` (per-tx) and `q5_advisory_summary` (Q5a–d + installments)."""

    def run(self) -> DataFrame:
        silver_df = self._silver_repo.load()
        strategy: Q5Strategy = self._strategy  # type: ignore[assignment]
        gold_tx = strategy.transform(silver_df)
        self._gold_writer.write_parquet(gold_tx, self._gold_subpath)
        metrics = strategy.advisory_metrics(silver_df)
        self._gold_writer.write_parquet(metrics, "q5_advisory_summary")
        return gold_tx


def build_q5_pipeline(
    silver_repo: SilverTableRepository, gold_writer: GoldWriter
) -> Q5GoldPipeline:
    """Factory for Q5."""
    return Q5GoldPipeline(
        name="q5_strategic_advisor",
        silver_repo=silver_repo,
        gold_writer=gold_writer,
        strategy=Q5Strategy(),
        gold_subpath="q5_results",
    )
