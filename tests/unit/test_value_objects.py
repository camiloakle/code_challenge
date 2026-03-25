"""Value objects."""

from src.domain.value_objects import InstallmentAssumptions


def test_installment_assumptions() -> None:
    v = InstallmentAssumptions(0.25, 0.229, 0.5)
    assert v.gross_profit_margin == 0.25
