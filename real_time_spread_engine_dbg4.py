# real_time_spread_engine_dbg4.py
# ================================
# CHANGES FROM dbg3:
#   - All tick/debug logging removed. Only profitable spreads print.
#   - Multi-cycle projection engine: given a profitable spread right now,
#     compute how many sequential capital-recycle cycles can be squeezed
#     out before the spread mathematically closes to zero net profit.
#   - Deduplication guard: same spread direction is not re-printed if the
#     last alert was < MIN_ALERT_INTERVAL_SEC ago (avoids flooding on
#     the same tick repeated many times).
#   - Capital is now a single CAPITAL config (not tiers) representing the
#     real wallet balance you are deploying per cycle.
#
# CYCLE MODEL REALITY CHECK (printed in every alert):
#   Cycle 0  = immediate execution right now at current prices.
#   Cycle 1+ = re-execution *IF* spread remains profitable after the
#              on-chain transfer of the prior cycle's proceeds completes.
#              Transfer time is modelled as a config constant per asset.
#              We project forward using an exponential spread-decay model
#              (spread narrows toward zero over time as arbitrageurs and
#              market makers close the gap). The decay constant is
#              configurable. In practice you must re-check live prices
#              before each cycle - these are projections only.
#
# MULTI-CYCLE CAPITAL COMPOUNDING:
#   Each cycle's starting capital = previous cycle's (capital + net_profit).
#   Cycles are counted until net_profit_for_cycle <= 0 OR max cycles hit.
"""
_dbg4.py — Silent mode + multi-cycle projection engine
What changed from dbg3: All tick/debug logging is gone.
Only profitable spreads print.
The core new feature is the cycle projection table:
---------------------------------------------------
Cycle 0 = execute right now at current prices
Cycle 1+ = projected forward using a spread decay model —
           the spread narrows by SPREAD_DECAY_PER_TRANSFER (default 30%) per
           transfer window, compounding the capital from each cycle's
           profit into the next
Cycles are projected until the net yield drops below MIN_NET_YIELD_PCT (0.05%)
A deduplication guard prevents the same direction from flooding stdout —
one alert per direction per MIN_ALERT_INTERVAL_SEC

Tune SPREAD_DECAY_PER_TRANSFER to taste:
0.0 = optimistic static spread,
0.50 = aggressive decay assumption.
"""

import asyncio
import json
import sys
import time
import websockets
from decimal import Decimal, ROUND_DOWN
from dataclasses import dataclass, field
from typing import Optional

# ───────────────────────────────────────────────────────────────────
# CONFIGURATION
# ───────────────────────────────────────────────────────────────────

# Single capital amount deployed per cycle (USDT equivalent)
CAPITAL = Decimal("1000")

# Maximum cycles to project (safety cap)
MAX_CYCLES = 20

# Do not re-alert the same symbol+direction within this many seconds
MIN_ALERT_INTERVAL_SEC = 10

# Spread decay model: each transfer-period the spread shrinks by this fraction.
# 0.30 = spread loses 30% of its width per transfer window (conservative).
# Set to 0.0 to model a completely static spread (optimistic upper bound).
SPREAD_DECAY_PER_TRANSFER = Decimal("0.30")

# Estimated on-chain transfer times (minutes) per asset - used for decay model
TRANSFER_MINUTES = {
    "BTC": 30,   # ~3 confirmations on Bitcoin
    "ETH": 5,    # ~1 confirmation post-merge
    "SOL": 1,    # ~1 slot finality
}

# Minimum net yield % per cycle to count as "worth executing"
MIN_NET_YIELD_PCT = Decimal("0.05")   # 0.05%

FEES = {
    "kraken": {
        "taker": Decimal("0.0026"),
        "maker": Decimal("0.0016"),
        "withdrawal": {
            "BTC": Decimal("0.0004"),
            "ETH": Decimal("0.0035"),
            "SOL": Decimal("0.005"),
        },
    },
    "cryptocom": {
        "taker": Decimal("0.0026"),
        "maker": Decimal("0.0000"),
        "withdrawal": {
            "BTC": Decimal("0.0005"),
            "ETH": Decimal("0.004"),
            "SOL": Decimal("0.008"),
        },
    },
}

KRAKEN_SYMBOL_MAP = {"XBT": "BTC", "ETH": "ETH", "SOL": "SOL"}

# ───────────────────────────────────────────────────────────────────
# LIVE ORDER BOOK STATE
# ───────────────────────────────────────────────────────────────────

SYMBOLS = ["BTC", "ETH", "SOL"]

order_books: dict = {
    sym: {
        "kraken":    {"bid": None, "ask": None},
        "cryptocom": {"bid": None, "ask": None},
    }
    for sym in SYMBOLS
}

# Alert dedup: (symbol, buy_venue) -> last alert unix timestamp
_last_alert: dict[tuple, float] = {}

# ───────────────────────────────────────────────────────────────────
# CYCLE MATH ENGINE
# ───────────────────────────────────────────────────────────────────

@dataclass
class CycleResult:
    cycle_num:        int
    capital_in:       Decimal
    buy_price:        Decimal
    sell_price:       Decimal
    trade_size:       Decimal
    arrival_amount:   Decimal
    entry_fee:        Decimal
    exit_fee:         Decimal
    withdrawal_fee:   Decimal
    gross_revenue:    Decimal
    net_profit:       Decimal
    net_pct:          Decimal
    capital_out:      Decimal   # capital_in + net_profit, available for next cycle


def _compute_single_cycle(
    symbol: str,
    buy_venue: str,
    sell_venue: str,
    buy_price: Decimal,
    sell_price: Decimal,
    capital: Decimal,
) -> Optional[CycleResult]:
    """Return a CycleResult if profitable, None otherwise."""
    buy_key  = buy_venue.lower().replace(".", "")
    sell_key = sell_venue.lower().replace(".", "")

    taker_buy  = FEES[buy_key]["taker"]
    taker_sell = FEES[sell_key]["taker"]
    w_fee      = FEES[buy_key]["withdrawal"][symbol]

    trade_size     = capital / buy_price
    gross_cost     = buy_price * trade_size          # == capital (by definition)
    entry_fee      = gross_cost * taker_buy

    arrival_amount = trade_size - w_fee
    if arrival_amount <= 0:
        return None

    gross_revenue = arrival_amount * sell_price
    exit_fee      = gross_revenue * taker_sell

    net_profit = gross_revenue - gross_cost - entry_fee - exit_fee
    if net_profit <= 0:
        return None

    net_pct    = (net_profit / gross_cost) * 100
    if net_pct < MIN_NET_YIELD_PCT:
        return None

    return CycleResult(
        cycle_num      = 0,            # caller fills this in
        capital_in     = capital,
        buy_price      = buy_price,
        sell_price     = sell_price,
        trade_size     = trade_size,
        arrival_amount = arrival_amount,
        entry_fee      = entry_fee,
        exit_fee       = exit_fee,
        withdrawal_fee = w_fee,
        gross_revenue  = gross_revenue,
        net_profit     = net_profit,
        net_pct        = net_pct,
        capital_out    = capital + net_profit,
    )


def project_cycles(
    symbol: str,
    buy_venue: str,
    sell_venue: str,
    buy_price: Decimal,
    sell_price: Decimal,
) -> list[CycleResult]:
    """
    Project as many profitable cycles as exist given spread decay.

    For cycle N:
      spread_N = initial_spread * (1 - SPREAD_DECAY_PER_TRANSFER) ^ N
      new_sell_price_N = buy_price + spread_N
      capital_N = cycle_{N-1}.capital_out  (compounding)
    """
    results: list[CycleResult] = []
    initial_spread = sell_price - buy_price
    capital        = CAPITAL

    for n in range(MAX_CYCLES):
        decay_factor = (1 - SPREAD_DECAY_PER_TRANSFER) ** n
        projected_spread    = initial_spread * decay_factor
        projected_sell      = buy_price + projected_spread

        r = _compute_single_cycle(
            symbol, buy_venue, sell_venue,
            buy_price, projected_sell, capital
        )
        if r is None:
            break   # spread no longer profitable — stop projecting

        r.cycle_num = n
        results.append(r)
        capital = r.capital_out   # compound into next cycle

    return results


# ───────────────────────────────────────────────────────────────────
# OUTPUT FORMATTER
# ───────────────────────────────────────────────────────────────────

def _fmt(d: Decimal, places: int = 4) -> str:
    quant = Decimal(10) ** -places
    return str(d.quantize(quant, rounding=ROUND_DOWN))


def print_opportunity(
    symbol: str,
    buy_venue: str,
    sell_venue: str,
    cycles: list[CycleResult],
) -> None:
    c0        = cycles[0]
    total_pnl = sum(c.net_profit for c in cycles)
    n_cycles  = len(cycles)
    transfer_min = TRANSFER_MINUTES[symbol]
    total_time_min = transfer_min * n_cycles

    bar = "═" * 78
    print(f"\n{bar}", flush=True)
    print(
        f"  🔥 PROFITABLE SPREAD  |  {symbol}  |  "
        f"BUY {buy_venue} → SELL {sell_venue}",
        flush=True,
    )
    print(bar, flush=True)

    print(
        f"  Prices      : buy ${_fmt(c0.buy_price, 2)}  ·  "
        f"sell ${_fmt(c0.sell_price, 2)}  ·  "
        f"gross gap ${_fmt(c0.sell_price - c0.buy_price, 4)}",
        flush=True,
    )
    print(
        f"  Capital/cyc : ${_fmt(CAPITAL, 2)} USDT  ·  "
        f"min yield gate {MIN_NET_YIELD_PCT}%  ·  "
        f"spread decay {SPREAD_DECAY_PER_TRANSFER*100:.0f}%/transfer",
        flush=True,
    )
    print(
        f"  Transfer est: ~{transfer_min} min/cycle  ·  "
        f"{n_cycles} viable cycle(s)  ·  "
        f"~{total_time_min} min total window",
        flush=True,
    )
    print(f"  {'─'*74}", flush=True)

    # Per-cycle table
    print(
        f"  {'Cycle':>5}  {'Capital In':>12}  {'Buy $':>10}  "
        f"{'Sell $':>10}  {'Net Profit':>11}  {'Yield%':>7}  {'Capital Out':>13}",
        flush=True,
    )
    print(f"  {'─'*74}", flush=True)
    for c in cycles:
        print(
            f"  {c.cycle_num:>5}  "
            f"${_fmt(c.capital_in, 2):>11}  "
            f"${_fmt(c.buy_price, 2):>9}  "
            f"${_fmt(c.sell_price, 2):>9}  "
            f"+${_fmt(c.net_profit, 4):>10}  "
            f"{_fmt(c.net_pct, 4):>6}%  "
            f"${_fmt(c.capital_out, 2):>12}",
            flush=True,
        )
    print(f"  {'─'*74}", flush=True)
    print(
        f"  TOTAL NET P&L across {n_cycles} cycle(s): "
        f"+${_fmt(total_pnl, 4)} USDT  "
        f"({_fmt((total_pnl/CAPITAL)*100, 4)}% on base capital)",
        flush=True,
    )
    print(f"  ⚠  Cycle 1+ prices are PROJECTIONS using decay model — verify live before executing.", flush=True)
    print(bar, flush=True)


# ───────────────────────────────────────────────────────────────────
# SPREAD EVALUATION (called on every tick)
# ───────────────────────────────────────────────────────────────────

def evaluate_spread(symbol: str) -> None:
    book  = order_books[symbol]
    k_ask = book["kraken"]["ask"]
    k_bid = book["kraken"]["bid"]
    c_ask = book["cryptocom"]["ask"]
    c_bid = book["cryptocom"]["bid"]

    for buy_venue, buy_price, sell_venue, sell_price in [
        ("Kraken",     k_ask, "Crypto.com", c_bid),
        ("Crypto.com", c_ask, "Kraken",     k_bid),
    ]:
        if not (buy_price and sell_price):
            continue

        dedup_key = (symbol, buy_venue)
        now = time.monotonic()
        if now - _last_alert.get(dedup_key, 0) < MIN_ALERT_INTERVAL_SEC:
            continue

        cycles = project_cycles(symbol, buy_venue, sell_venue, buy_price, sell_price)
        if not cycles:
            continue

        _last_alert[dedup_key] = now
        print_opportunity(symbol, buy_venue, sell_venue, cycles)


# ───────────────────────────────────────────────────────────────────
# KRAKEN WEBSOCKET STREAM
# ───────────────────────────────────────────────────────────────────

async def stream_kraken() -> None:
    url   = "wss://ws.kraken.com/v2"
    pairs = [f"{sym}/USD" for sym in SYMBOLS]

    async for ws in websockets.connect(url):
        try:
            await ws.send(json.dumps({
                "method": "subscribe",
                "params": {"channel": "ticker", "symbol": pairs},
            }))
            async for raw in ws:
                msg = json.loads(raw)
                if msg.get("channel") == "ticker" and "data" in msg:
                    pkt        = msg["data"][0]
                    kraken_base = pkt["symbol"].split("/")[0]
                    sym         = KRAKEN_SYMBOL_MAP.get(kraken_base)
                    if sym:
                        order_books[sym]["kraken"]["bid"] = Decimal(str(pkt["bid"]))
                        order_books[sym]["kraken"]["ask"] = Decimal(str(pkt["ask"]))
                        evaluate_spread(sym)
        except websockets.ConnectionClosed:
            await asyncio.sleep(1)
        except Exception as exc:
            print(f"[!] Kraken error: {exc}", flush=True)


# ───────────────────────────────────────────────────────────────────
# CRYPTO.COM WEBSOCKET STREAM
# ───────────────────────────────────────────────────────────────────

async def _cdc_heartbeat(ws, msg_id: int) -> None:
    await ws.send(json.dumps({"id": msg_id, "method": "public/respond-heartbeat"}))


async def stream_cryptocom() -> None:
    url      = "wss://stream.crypto.com/v2/market"
    channels = [f"ticker.{sym}_USDT" for sym in SYMBOLS]

    async for ws in websockets.connect(url):
        try:
            await ws.send(json.dumps({
                "id": 1, "method": "subscribe",
                "params": {"channels": channels}, "nonce": 1,
            }))
            async for raw in ws:
                msg = json.loads(raw)
                if msg.get("method") == "public/heartbeat":
                    await _cdc_heartbeat(ws, msg["id"])
                    continue
                if msg.get("method") == "subscription":
                    tick_list = msg.get("result", {}).get("data")
                    if not tick_list:
                        continue
                    pkt  = tick_list[0]
                    sym  = pkt["i"].split("_")[0]
                    if sym in order_books:
                        order_books[sym]["cryptocom"]["bid"] = Decimal(str(pkt["b"]))
                        order_books[sym]["cryptocom"]["ask"] = Decimal(str(pkt["a"]))
                        evaluate_spread(sym)
        except websockets.ConnectionClosed:
            await asyncio.sleep(1)
        except Exception as exc:
            print(f"[!] Crypto.com error: {exc}", flush=True)


# ───────────────────────────────────────────────────────────────────
# MAIN
# ───────────────────────────────────────────────────────────────────

async def main() -> None:
    sys.stdout.reconfigure(line_buffering=True)
    print("⚡ Spread engine active — printing profitable opportunities only.", flush=True)
    print(f"   Capital: ${CAPITAL} · Min yield: {MIN_NET_YIELD_PCT}% · Decay: {SPREAD_DECAY_PER_TRANSFER*100:.0f}%/transfer\n", flush=True)
    await asyncio.gather(stream_kraken(), stream_cryptocom())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nEngine stopped.")
