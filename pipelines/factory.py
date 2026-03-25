"""Factory for Gold pipelines (Strategy + Repository wiring)."""

from __future__ import annotations

from pyspark.sql import SparkSession

from config.settings import Settings
from src.application.pipelines.q1_top_merchants import build_q1_pipeline
from src.application.pipelines.q2_avg_by_state import build_q2_pipeline
from src.application.pipelines.q3_peak_hours import build_q3_pipeline
from src.application.pipelines.q4_location_correlation import build_q4_pipeline
from src.application.pipelines.q5_strategic_advisor import build_q5_pipeline
from src.core.base import BaseSparkPipeline
from src.infrastructure.spark.repositories import DeltaSilverRepository, ParquetGoldWriter


def build_gold_pipeline(
    question: str,
    spark: SparkSession,
    settings: Settings,
) -> BaseSparkPipeline:
    """Return configured Gold pipeline for q1..q5."""
    silver_repo = DeltaSilverRepository(spark, settings)
    gold_writer = ParquetGoldWriter(spark, settings)
    q = question.lower().strip()
    if q == "q1":
        return build_q1_pipeline(silver_repo, gold_writer)
    if q == "q2":
        return build_q2_pipeline(silver_repo, gold_writer)
    if q == "q3":
        return build_q3_pipeline(silver_repo, gold_writer)
    if q == "q4":
        return build_q4_pipeline(silver_repo, gold_writer)
    if q == "q5":
        return build_q5_pipeline(silver_repo, gold_writer)
    raise ValueError(f"Unknown question: {question}")
