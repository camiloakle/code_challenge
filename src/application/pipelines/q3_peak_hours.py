"""Q3 pipeline wiring."""

from __future__ import annotations

from src.application.pipelines.base_pipeline import GoldQuestionPipeline
from src.application.strategies.q3_strategy import Q3Strategy
from src.domain.repositories import GoldWriter, SilverTableRepository


def build_q3_pipeline(
    silver_repo: SilverTableRepository, gold_writer: GoldWriter
) -> GoldQuestionPipeline:
    """Factory for Q3."""
    return GoldQuestionPipeline(
        name="q3_peak_hours",
        silver_repo=silver_repo,
        gold_writer=gold_writer,
        strategy=Q3Strategy(),
        gold_subpath="q3_results",
    )
