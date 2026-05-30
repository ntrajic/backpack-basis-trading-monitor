# arbitrage_scanner_dbg7.py
#
# Mode A: Scanner → dbg7 engine callback integration.
#
# This is a minimal example wiring. Replace the "get_live_quotes"
# stub with your existing websocket / REST feed logic.

import asyncio
from decimal import Decimal
from typing import Dict, Tuple

from real_time_spread_engine_dbg7_sc import execute_spread_opportunity
from real_time_spread_engine_dbg7 import MIN_NET_YIELD_PCT

SYMBOLS = ["BTC", "ETH", "SOL"]
VENUES = ["Kraken", "Crypto.com"]


async def get_live_quotes() -> Dict[str, Dict[str, Dict[str, float]]]:
    """
    Stub: replace with your real orderbook / ticker feed.

    Returns:
        {
          "BTC": {
             "Kraken": {"bid": 73480.0, "ask": 73485.0},
             "Crypto.com": {"bid": 73490.0, "ask": 73495.0},
          },
          ...
        }
    """
    raise NotImplementedError("Wire this to your existing scanner feed.")


def best_spread_for_symbol(
    symbol: str,
    books: Dict[str, Dict[str, Dict[str, float]]],
) -> Tuple[str, str, float, float, Decimal]:
    """
    Compute best cross-venue spread for symbol.
    Returns (buy_venue, sell_venue, buy_price, sell_price, net_spread_pct)
    or (None, None, 0, 0, Decimal(0)) if no valid spread.
    """
    venues = books.get(symbol, {})
    if "Kraken" not in venues or "Crypto.com" not in venues:
        return None, None, 0.0, 0.0, Decimal("0")

    k = venues["Kraken"]
    c = venues["Crypto.com"]

    # Two directions: buy Kraken / sell Crypto.com, and reverse
    candidates = []

    # Buy Kraken ask, sell Crypto.com bid
    if k["ask"] and c["bid"]:
        spread = Decimal(str(c["bid"])) - Decimal(str(k["ask"]))
        pct = spread / Decimal(str(k["ask"])) * Decimal("100")
        candidates.append(("Kraken", "Crypto.com", k["ask"], c["bid"], pct))

    # Buy Crypto.com ask, sell Kraken bid
    if c["ask"] and k["bid"]:
        spread = Decimal(str(k["bid"])) - Decimal(str(c["ask"]))
        pct = spread / Decimal(str(c["ask"])) * Decimal("100")
        candidates.append(("Crypto.com", "Kraken", c["ask"], k["bid"], pct))

    if not candidates:
        return None, None, 0.0, 0.0, Decimal("0")

    # Pick best positive spread
    best = max(candidates, key=lambda x: x[4])
    if best[4] <= 0:
        return None, None, 0.0, 0.0, Decimal("0")
    return best


async def scanner_loop():
    while True:
        books = await get_live_quotes()

        for sym in SYMBOLS:
            buy_venue, sell_venue, buy_price, sell_price, pct = best_spread_for_symbol(sym, books)
            if not buy_venue:
                continue

            if pct < MIN_NET_YIELD_PCT:
                continue

            print(f"[SCANNER] {sym} spread {pct:.4f}%  BUY {buy_venue} @ {buy_price}  SELL {sell_venue} @ {sell_price}")
            await execute_spread_opportunity(sym, buy_venue, sell_venue, buy_price, sell_price)

        await asyncio.sleep(0.5)


if __name__ == "__main__":
    asyncio.run(scanner_loop())
