"""Local filesystem helpers."""

from pathlib import Path


def ensure_dir(path: Path) -> None:
    """Create directory if missing."""
    path.mkdir(parents=True, exist_ok=True)
