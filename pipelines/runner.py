"""Run Gold questions against Silver (never Bronze/Raw)."""

from __future__ import annotations

import argparse

from config.settings import Settings
from pipelines.factory import build_gold_pipeline
from src.infrastructure.spark.session import create_session
from src.shared.logger import configure_logging, get_logger

logger = get_logger(__name__)


def run_question(question: str, settings: Settings) -> None:
    """Execute a single Gold pipeline."""
    spark = create_session(settings, f"gold-{question}")
    try:
        pipeline = build_gold_pipeline(question, spark, settings)
        pipeline.execute()
    finally:
        spark.stop()


def run_all(settings: Settings) -> None:
    """Run Q1 through Q5 sequentially."""
    for q in ("q1", "q2", "q3", "q4", "q5"):
        run_question(q, settings)


def main() -> None:
    """CLI."""
    configure_logging()
    parser = argparse.ArgumentParser(description="Gold layer runner")
    parser.add_argument("--question", "-q", help="Question id: q1..q5")
    parser.add_argument("--all", action="store_true", help="Run all questions")
    parser.add_argument("--env", default="dev")
    args = parser.parse_args()
    settings = Settings()

    if args.all:
        run_all(settings)
    elif args.question:
        run_question(args.question, settings)
    else:
        parser.error("Provide --question q1..q5 or --all")


if __name__ == "__main__":
    main()
