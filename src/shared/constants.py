"""Column names and literals aligned with the challenge data contract."""

# Transaction input (bronze / raw parquet)
COL_MERCHANT_ID = "merchant_id"
COL_AMOUNT = "amount"
COL_CATEGORY = "category"
COL_PURCHASE_TS = "purchase_ts"
COL_CITY_ID = "city_id"
COL_STATE_ID = "state_id"
COL_INSTALLMENTS = "installments"

# Alternate names seen in some Parquet extracts (normalized in Silver only)
ALT_PURCHASE_TS = "purchase_date"
ALT_AMOUNT = "purchase_amount"

# Merchant input (CSV)
COL_MERCHANT_NAME = "merchant_name"

# Silver enriched
COL_MERCHANT_NAME_ENRICHED = "merchant_name"

# Q5e
UNKNOWN_CATEGORY = "Unknown category"

# Assumptions for installments EV (challenge PDF)
GROSS_PROFIT_MARGIN = 0.25
DEFAULT_RATE_MONTHLY = 0.229
PAY_BEFORE_DEFAULT_FRACTION = 0.50
PROB_PAY_FULL = 1.0 - DEFAULT_RATE_MONTHLY  # 0.771
EV_PROFIT_FULL_COEF = GROSS_PROFIT_MARGIN * PROB_PAY_FULL  # amount * this
EV_PROFIT_DEFAULT_COEF = (
    PAY_BEFORE_DEFAULT_FRACTION * GROSS_PROFIT_MARGIN * DEFAULT_RATE_MONTHLY
)
# ~0.221375
EXPECTED_PROFIT_MARGIN = EV_PROFIT_FULL_COEF + EV_PROFIT_DEFAULT_COEF
# 80% of ideal single-payment profit
THRESHOLD_TOLERANCE = 0.8
