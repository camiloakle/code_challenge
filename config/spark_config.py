"""Spark session builder with Delta Lake support."""

from __future__ import annotations

from typing import TYPE_CHECKING

from delta import configure_spark_with_delta_pip

from config.settings import Settings

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def build_spark_session(settings: Settings, app_name: str = "billups-challenge") -> SparkSession:
    """Create a local SparkSession configured for Delta Lake on the local filesystem.

    Args:
        settings: Application settings (memory, shuffle partitions).
        app_name: Spark application name.

    Returns:
        Configured SparkSession.
    """
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.sql.shuffle.partitions", str(settings.spark_shuffle_partitions))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    # Pip-installed PySpark does not put delta JARs on the classpath; this adds
    # spark.jars.packages so DeltaCatalog loads (see delta.pip_utils).
    builder = configure_spark_with_delta_pip(builder)
    return builder.getOrCreate()
