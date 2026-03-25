"""Q1 pipeline wiring."""

from __future__ import annotations

from src.application.pipelines.base_pipeline import GoldQuestionPipeline
from src.application.strategies.q1_strategy import Q1Strategy
from src.domain.repositories import GoldWriter, SilverTableRepository


def build_q1_pipeline(
    silver_repo: SilverTableRepository, gold_writer: GoldWriter
) -> GoldQuestionPipeline:
    """Factory for Q1."""
    return GoldQuestionPipeline(
        name="q1_top_merchants",
        silver_repo=silver_repo,
        gold_writer=gold_writer,
        strategy=Q1Strategy(),
        gold_subpath="q1_results",
    )
