# file: real_time_spread_engine_dbg7_sc.py
# real_time_spread_engine_dbg7_sc.py
#
# Option A: Scanners call dbg7 engine (callback integration)
#
# Thin wrapper around real_time_spread_engine_dbg7.py
# exposing a simple "execute_spread_opportunity" API
# for scanners to call.

from decimal import Decimal
from typing import List

from real_time_spread_engine_dbg7 import (
    ExecutionPlan,
    CycleResult,
    project_cycles,
    execute_cycle,
)

# Hard boundary: this file is ONLY for scanner integration (Mode A).


async def execute_spread_opportunity(
    symbol: str,
    buy_venue: str,
    sell_venue: str,
    buy_price: float,
    sell_price: float,
) -> None:
    """
    Called by scanners when they detect a profitable spread.

    symbol: "BTC" | "ETH" | "SOL"
    buy_venue: "Kraken" or "Crypto.com"
    sell_venue: "Kraken" or "Crypto.com"
    buy_price / sell_price: top-of-book prices from scanner
    """
    buy_p = Decimal(str(buy_price))
    sell_p = Decimal(str(sell_price))

    cycles: List[CycleResult] = project_cycles(symbol, buy_venue, sell_venue, buy_p, sell_p)
    if not cycles:
        return

    # For now, execute only the first (best) cycle
    plan = ExecutionPlan(
        symbol=symbol,
        buy_venue=buy_venue,
        sell_venue=sell_venue,
        cycle=cycles[0],
        cycle_index=0,
        total_cycles=len(cycles),
    )
    await execute_cycle(plan)
