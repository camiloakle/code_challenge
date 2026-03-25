"""S3 stub."""

import pytest

from src.infrastructure.storage.s3_client import S3Client


def test_download_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        S3Client().download("b", "k", "d")
