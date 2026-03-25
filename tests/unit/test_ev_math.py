"""Q5e expected value — closed-form check (no Spark)."""

from src.shared.constants import EXPECTED_PROFIT_MARGIN, GROSS_PROFIT_MARGIN, THRESHOLD_TOLERANCE


def test_expected_profit_margin_matches_challenge_formula() -> None:
    """EV = A*0.25*0.771 + A*0.5*0.25*0.229 = A * 0.221375."""
    a = 1000.0
    profit_full = a * 0.25 * 0.771
    profit_default = a * 0.50 * 0.25 * 0.229
    expected_value = profit_full + profit_default
    assert abs(expected_value / a - EXPECTED_PROFIT_MARGIN) < 1e-9


def test_accept_threshold_is_80_percent_of_ideal_single_payment_profit() -> None:
    a = 500.0
    profit_full = a * 0.25 * 0.771
    profit_default = a * 0.50 * 0.25 * 0.229
    ev = profit_full + profit_default
    threshold = a * GROSS_PROFIT_MARGIN * THRESHOLD_TOLERANCE
    assert ev >= threshold
