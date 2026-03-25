"""S3 client placeholder for Databricks / cloud deployments."""

from __future__ import annotations

from typing import Any


class S3Client:
    """Thin boto3 wrapper — extend when deploying to cloud."""

    def __init__(self, **kwargs: Any) -> None:
        self._kwargs = kwargs

    def download(self, bucket: str, key: str, dest: str) -> None:
        """Download object to local path (not implemented in challenge stub)."""
        raise NotImplementedError("Use DBFS or local paths for the challenge environment.")
