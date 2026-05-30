# arbitrage_scanner_topofbookorder_dbg7.py
#
# Mode A: top-of-book scanner that calls dbg7 engine.

import asyncio
from decimal import Decimal
from typing import Dict

from real_time_spread_engine_dbg7_sc import execute_spread_opportunity
from real_time_spread_engine_dbg7 import MIN_NET_YIELD_PCT

SYMBOLS = ["BTC", "ETH", "SOL"]


async def get_top_of_book() -> Dict:
    """
    Replace with your existing top-of-book scanner logic.
    Expected shape:
      {
        "BTC": {
          "Kraken": {"bid": ..., "ask": ...},
          "Crypto.com": {"bid": ..., "ask": ...},
        },
        ...
      }
    """
    raise NotImplementedError("Wire this to your top-of-book feed.")


async def top_of_book_loop():
    while True:
        books = await get_top_of_book()

        for sym in SYMBOLS:
            venues = books.get(sym, {})
            if "Kraken" not in venues or "Crypto.com" not in venues:
                continue

            k = venues["Kraken"]
            c = venues["Crypto.com"]

            # Buy cheaper, sell more expensive
            candidates = []

            if k["ask"] and c["bid"]:
                spread = Decimal(str(c["bid"])) - Decimal(str(k["ask"]))
                pct = spread / Decimal(str(k["ask"])) * Decimal("100")
                candidates.append(("Kraken", "Crypto.com", k["ask"], c["bid"], pct))

            if c["ask"] and k["bid"]:
                spread = Decimal(str(k["bid"])) - Decimal(str(c["ask"]))
                pct = spread / Decimal(str(c["ask"])) * Decimal("100")
                candidates.append(("Crypto.com", "Kraken", c["ask"], k["bid"], pct))

            if not candidates:
                continue

            buy_venue, sell_venue, buy_price, sell_price, pct = max(candidates, key=lambda x: x[4])
            if pct < MIN_NET_YIELD_PCT:
                continue

            print(f"[TOB] {sym} spread {pct:.4f}%  BUY {buy_venue} @ {buy_price}  SELL {sell_venue} @ {sell_price}")
            await execute_spread_opportunity(sym, buy_venue, sell_venue, buy_price, sell_price)

        await asyncio.sleep(0.25)


if __name__ == "__main__":
    asyncio.run(top_of_book_loop())
