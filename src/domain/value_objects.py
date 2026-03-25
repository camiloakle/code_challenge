"""Value objects."""

from dataclasses import dataclass


@dataclass(frozen=True)
class InstallmentAssumptions:
    """Documented parameters for Q5e expected value."""

    gross_profit_margin: float
    default_rate_monthly: float
    pay_before_default_fraction: float
