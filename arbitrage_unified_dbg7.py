# arbitrage_unified_dbg7.py
#
# Mode C: Unified scanner + dbg7 engine in a single process.
# - Reads live quotes
# - Detects spreads
# - Executes cycles via dbg7
# - Enforces all dbg7 safety guardrails

import asyncio
from decimal import Decimal
from typing import Dict, Tuple

from real_time_spread_engine_dbg7 import (
    project_cycles,
    ExecutionPlan,
    execute_cycle,
    MIN_NET_YIELD_PCT,
    SYMBOLS,
)

# Hard boundary: this file owns both scanning AND execution.


async def get_live_quotes() -> Dict[str, Dict[str, Dict[str, float]]]:
    """
    Replace with your real Kraken/Crypto.com orderbook feed.
    """
    raise NotImplementedError("Wire this to your production feed.")


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
    while True:
        books = await get_live_quotes()

        for sym in SYMBOLS:
            buy_venue, sell_venue, buy_price, sell_price, pct = best_spread_for_symbol(sym, books)
            if not buy_venue:
                continue

            if pct < MIN_NET_YIELD_PCT:
                continue

            print(f"[UNIFIED] {sym} spread {pct:.4f}%  BUY {buy_venue} @ {buy_price}  SELL {sell_venue} @ {sell_price}")

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
            await execute_cycle(plan)

        await asyncio.sleep(0.5)


if __name__ == "__main__":
    asyncio.run(unified_loop())
