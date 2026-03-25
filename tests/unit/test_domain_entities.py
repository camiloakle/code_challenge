"""Domain entities."""

from pathlib import Path

from src.domain.entities import DataLayerPaths, QuestionId


def test_data_layer_paths() -> None:
    p = DataLayerPaths(Path("/a"), Path("/b"), Path("/c"))
    assert p.bronze_root == Path("/a")


def test_question_id() -> None:
    assert QuestionId("q1").value == "q1"
