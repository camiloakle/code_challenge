"""Local storage helpers."""

from pathlib import Path

from src.infrastructure.storage.local_client import ensure_dir


def test_ensure_dir_creates_path(tmp_path: Path) -> None:
    d = tmp_path / "a" / "b"
    ensure_dir(d)
    assert d.is_dir()
