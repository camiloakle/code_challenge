"""Q4 pipeline wiring — all outputs under data/gold/q4/."""

from __future__ import annotations

from pyspark.sql import DataFrame

from src.application.pipelines.base_pipeline import GoldQuestionPipeline
from src.application.strategies.q4_strategy import Q4Strategy
from src.domain.repositories import GoldWriter, SilverTableRepository


class Q4GoldPipeline(GoldQuestionPipeline):
    """Writes q4 outputs in a single namespace folder."""

    def run(self) -> DataFrame:
        silver_df = self._silver_repo.load()
        strategy: Q4Strategy = self._strategy  # type: ignore[assignment]

        main_df = strategy.transform(silver_df)
        self._gold_writer.write_parquet(main_df, self._gold_subpath)

        assoc = strategy.transform_city_category_association(silver_df)
        self._gold_writer.write_parquet(assoc, "q4/city_category_association")

        top_global = strategy.transform_top_merchants_global(silver_df)
        self._gold_writer.write_parquet(top_global, "q4/top_merchants_global")

        dist = strategy.transform_top_merchants_distribution_by_city(top_global)
        self._gold_writer.write_parquet(dist, "q4/top_merchants_distribution_by_city")

        pop_df = strategy.transform_merchant_popularity_by_city(silver_df)
        self._gold_writer.write_parquet(pop_df, "q4/merchant_popularity_by_city")

        return main_df


def build_q4_pipeline(
    silver_repo: SilverTableRepository, gold_writer: GoldWriter
) -> Q4GoldPipeline:
    """Factory for Q4."""
    return Q4GoldPipeline(
        name="q4_location_correlation",
        silver_repo=silver_repo,
        gold_writer=gold_writer,
        strategy=Q4Strategy(),
        gold_subpath="q4/results",
    )
