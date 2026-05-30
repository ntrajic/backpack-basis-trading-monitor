# file: .env.example_dbg7
# Exchange credentials
KRAKEN_API_KEY="your_kraken_key"
KRAKEN_API_SECRET="your_kraken_secret_base64"
CDC_API_KEY="your_cryptocom_key"
CDC_API_SECRET="your_cryptocom_secret"

# Deposit / withdrawal wiring
KRAKEN_DEPOSIT_ADDRESS="your_kraken_btc_deposit_address"
# Crypto.com deposit addresses are configured in each exchange UI and referenced by key name on Kraken

# Engine mode
EXECUTION_MODE="semi_auto"   # advisory | semi_auto | full_auto

# Capital + float
CAPITAL="25"
MAX_CYCLES="10"
MIN_NET_YIELD_PCT="0.05"
SPREAD_DECAY_PER_TRANSFER="0.30"

TARGET_FLOAT_BTC="0.001"
TARGET_FLOAT_ETH="0.02"
TARGET_FLOAT_SOL="1.0"

# Risk guardrails
MAX_DAILY_LOSS="20"
MAX_CYCLES_PER_HOUR="3"
SPREAD_COLLAPSE_THRESHOLD="0.20"
