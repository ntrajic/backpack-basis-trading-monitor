# arbitrage_scanner_topofbookorder_dbg7_ws.py
#
# Mode A + WS: top-of-book scanner using market_feeds_ws.

import asyncio
from decimal import Decimal
from typing import Dict

from market_feeds_ws import start_kraken_ws, start_cryptocom_ws, get_top_of_book
from real_time_spread_engine_dbg7_sc_ws import execute_cycle_with_logging
from real_time_spread_engine_dbg7 import (
    MIN_NET_YIELD_PCT,
    project_cycles,
    ExecutionPlan,
    SYMBOLS,
)


async def top_of_book_loop():
    await start_kraken_ws()
    await start_cryptocom_ws()

    await asyncio.sleep(3)

    while True:
        books = await get_top_of_book()

        for sym in SYMBOLS:
            venues = books.get(sym, {})
            if "Kraken" not in venues or "Crypto.com" not in venues:
                continue

            k = venues["Kraken"]
            c = venues["Crypto.com"]

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

            print(f"[TOB_WS] {sym} spread {pct:.4f}%  BUY {buy_venue} @ {buy_price}  SELL {sell_venue} @ {sell_price}")

            cycles = project_cycles(sym, buy_venue, sell_venue, Decimal(str(buy_price)), Decimal(str(sell_price)))
            if not cycles:
                continue

            plan = ExecutionPlan(
                symbol=sym,
                buy_venue=buy_venue,
                sell_venue=sell_venue,
                cycle=cycles[0],
                cycle_index=0,
                total_cycles=len(cycles),
            )
            await execute_cycle_with_logging(plan)

        await asyncio.sleep(0.25)


if __name__ == "__main__":
    asyncio.run(top_of_book_loop())
