"""Q2 pipeline wiring."""

from __future__ import annotations

from src.application.pipelines.base_pipeline import GoldQuestionPipeline
from src.application.strategies.q2_strategy import Q2Strategy
from src.domain.repositories import GoldWriter, SilverTableRepository


def build_q2_pipeline(
    silver_repo: SilverTableRepository, gold_writer: GoldWriter
) -> GoldQuestionPipeline:
    """Factory for Q2."""
    return GoldQuestionPipeline(
        name="q2_avg_by_state",
        silver_repo=silver_repo,
        gold_writer=gold_writer,
        strategy=Q2Strategy(),
        gold_subpath="q2_results",
    )
