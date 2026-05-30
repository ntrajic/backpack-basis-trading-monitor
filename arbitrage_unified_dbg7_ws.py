# arbitrage_unified_dbg7_ws.py
#
# Unified bot (Mode C) using same WS feeds + logging.
#
# Mode C + WS: unified scanner + engine + logging.

import asyncio
from decimal import Decimal
from typing import Dict, Tuple

from market_feeds_ws import start_kraken_ws, start_cryptocom_ws, get_live_quotes
from real_time_spread_engine_dbg7 import (
    project_cycles,
    ExecutionPlan,
    MIN_NET_YIELD_PCT,
    SYMBOLS,
)
from real_time_spread_engine_dbg7_sc_ws import execute_cycle_with_logging


def best_spread_for_symbol(
    symbol: str,
    books: Dict[str, Dict[str, Dict[str, float]]],
) -> Tuple[str, str, float, float, Decimal]:
    venues = books.get(symbol, {})
    if "Kraken" not in venues or "Crypto.com" not in venues:
        return None, None, 0.0, 0.0, Decimal("0")

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
        return None, None, 0.0, 0.0, Decimal("0")

    best = max(candidates, key=lambda x: x[4])
    if best[4] <= 0:
        return None, None, 0.0, 0.0, Decimal("0")
    return best


async def unified_loop():
    await start_kraken_ws()
    await start_cryptocom_ws()

    await asyncio.sleep(3)

    while True:
        books = await get_live_quotes()

        for sym in SYMBOLS:
            buy_venue, sell_venue, buy_price, sell_price, pct = best_spread_for_symbol(sym, books)
            if not buy_venue:
                continue

            if pct < MIN_NET_YIELD_PCT:
                continue

            print(f"[UNIFIED_WS] {sym} spread {pct:.4f}%  BUY {buy_venue} @ {buy_price}  SELL {sell_venue} @ {sell_price}")

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

        await asyncio.sleep(0.5)


if __name__ == "__main__":
    asyncio.run(unified_loop())
