# file: config_dbg7_tiny_sizes.py (optional override)
from decimal import Decimal

EXECUTION_MODE = "semi_auto"

CAPITAL = Decimal("15")
MAX_CYCLES = 5

MIN_NET_YIELD_PCT = Decimal("0.05")
SPREAD_DECAY_PER_TRANSFER = Decimal("0.30")

TARGET_FLOAT = {
    "BTC": Decimal("0.0005"),
    "ETH": Decimal("0.01"),
    "SOL": Decimal("0.5"),
}

MAX_DAILY_LOSS = Decimal("10")
MAX_CYCLES_PER_HOUR = 2
SPREAD_COLLAPSE_THRESHOLD = Decimal("0.20")
