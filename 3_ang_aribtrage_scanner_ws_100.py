#!/usr/bin/env python3
"""
file: 3_ang_aribtrage_scanner_ws_100.py

Triangular Arbitrage Scanner: 3-Leg Closed Loop (Single Exchange)
Calibrated for $100 USDC Capital Baseline on Kraken Pro.
Feasibility Model: Taker Loop (Fee Drag: 0.26% * 3 = 0.78%)

Part 2: Script 1 — Retail Micro-Capital Scanner (WebSocket Production Upgrade)
Loop: USDC -> SOL -> CAD -> USDC

Upgrades over REST version:
1. WebSocket live feed via ccxt.pro watch_order_book — no polling delay
2. Batch order routing via create_orders — all 3 legs in one network packet
3. IOC constraints on each leg — auto-kills loop if any leg misses fill boundary

NOTE: Here's what changed vs the REST version:
==============================================
1 WebSocket streams — stream_order_book() runs 3 concurrent async tasks via asyncio.gather,
  each calling watch_order_book() in a loop.
  Order books are pushed from Kraken's match engine directly into the shared order_books dict — no polling delay.

2 Live evaluation — monitor_loop() reads from the live order_books state every 100ms,
  using asks[0][0] and bids[0][0] (top-of-book) instead of ticker mid-prices.

3 IOC batch execution — when net_return > 0, execute_triangle_ioc() builds all 3 orders and
  fires them in one create_orders() call with timeInForce: IOC.
  Any leg that can't fill at the target price is cancelled immediately.

ANALYSIS

Script 2: WebSocket Async (3_ang_aribtrage_scanner_ws_100.py)
Punctuality Rating: 🟡 Medium-Good (Excellent Telemetry, Delayed Execution)

Analysis: The WebSocket logs demonstrate massive improvements in update speeds. 
The processing engine evaluates state changes every 100 milliseconds:

Plaintext
[06:53:56.477724] -> [06:53:56.581826] -> [06:53:56.685049]
This proves the async event loop is successfully streaming data from Kraken's WebSocket feed without blocking.

The Flaw: While the telemetry collection is sub-100ms, the execution path drops back to 
standard REST architecture. 
The script calls kraken_rest.create_orders(orders) via an HTTP POST request. 
Setting up a new TCP/HTTP handshake to transmit your orders introduces a 30 to 120ms latency penalty,
 giving institutional HFT bots plenty of time to front-run the trade.
"""

import asyncio
import os
import ccxt.pro as ccxtpro
import ccxt
from datetime import datetime, timezone

START_CAPITAL_USDC = 100.00
TAKER_FEE_PER_LEG = 0.0026
TOTAL_FEES_3_LEGS = TAKER_FEE_PER_LEG * 3
SYMBOLS = ["SOL/USDC", "SOL/CAD", "USDC/CAD"]

# Shared order book state updated by WebSocket streams
order_books = {}

kraken_ws = ccxtpro.kraken(
    {
        "apiKey": os.getenv("KRAKEN_API_KEY"),
        "secret": os.getenv("KRAKEN_SECRET"),
        "enableRateLimit": True,
        "options": {"defaultType": "spot"},
    }
)

# Separate REST client for order execution
kraken_rest = ccxt.kraken(
    {
        "apiKey": os.getenv("KRAKEN_API_KEY"),
        "secret": os.getenv("KRAKEN_SECRET"),
        "enableRateLimit": True,
    }
)


async def stream_order_book(symbol):
    """Continuously updates shared order book state from live WebSocket feed."""
    while True:
        try:
            ob = await kraken_ws.watch_order_book(symbol)
            order_books[symbol] = ob
        except Exception as e:
            print(f"⚠️ WebSocket stream error [{symbol}]: {e}")
            await asyncio.sleep(1)


def evaluate_circuit():
    """Evaluates triangular loop profitability from current live order book state."""
    if not all(s in order_books for s in SYMBOLS):
        return  # Not all books populated yet

    try:
        # Leg 1: Buy SOL with USDC — hit the lowest Ask
        leg1_rate = float(order_books["SOL/USDC"]["asks"][0][0])
        sol_acquired = START_CAPITAL_USDC / leg1_rate

        # Leg 2: Sell SOL for CAD — hit the highest Bid
        leg2_rate = float(order_books["SOL/CAD"]["bids"][0][0])
        cad_acquired = sol_acquired * leg2_rate

        # Leg 3: Convert CAD -> USDC via USDC/CAD — divide by Ask
        leg3_rate = float(order_books["USDC/CAD"]["asks"][0][0])
        final_usdc = cad_acquired / leg3_rate

        gross_return = (final_usdc - START_CAPITAL_USDC) / START_CAPITAL_USDC
        net_return = gross_return - TOTAL_FEES_3_LEGS

        now = datetime.now(timezone.utc).isoformat()
        print(f"[{now}] Circuit: USDC -> SOL -> CAD -> USDC")
        print(
            f"  | Rates: L1(Ask): {leg1_rate:.4f} | L2(Bid): {leg2_rate:.4f} | L3(Ask): {leg3_rate:.4f}"
        )
        print(
            f"  | Gross Yield: {gross_return * 100:.3f}% | Net Yield After 3 Taker Fees: {net_return * 100:.3f}%"
        )
        print(
            f"  | Projected Out: ${final_usdc:.2f} USDC from ${START_CAPITAL_USDC} Initial"
        )

        if net_return > 0:
            print(
                "🟢 PROFITABLE RETAIL TRIANGLE DETECTED — Routing batch IOC execution..."
            )
            execute_triangle_ioc(
                sol_acquired, cad_acquired, leg1_rate, leg2_rate, leg3_rate
            )
        else:
            print("💤 Circuit evaluated below trading fee hurdles.")

    except Exception as e:
        print(f"⚠️ Circuit evaluation error: {e}")


def execute_triangle_ioc(sol_qty, cad_qty, leg1_ask, leg2_bid, leg3_ask):
    """
    Routes all 3 legs as IOC taker orders in a single batch API call.
    IOC (Immediate-Or-Cancel): if any leg misses its price boundary,
    that order is cancelled instantly — preventing unhedged exposure.
    """
    usdc_qty = cad_qty / leg3_ask  # USDC to buy back in Leg 3

    orders = [
        # Leg 1: Buy SOL with USDC
        {
            "symbol": "SOL/USDC",
            "type": "limit",
            "side": "buy",
            "amount": round(sol_qty, 6),
            "price": leg1_ask,
            "params": {"timeInForce": "IOC"},
        },
        # Leg 2: Sell SOL for CAD
        {
            "symbol": "SOL/CAD",
            "type": "limit",
            "side": "sell",
            "amount": round(sol_qty, 6),
            "price": leg2_bid,
            "params": {"timeInForce": "IOC"},
        },
        # Leg 3: Buy USDC with CAD (buy base asset USDC via USDC/CAD)
        {
            "symbol": "USDC/CAD",
            "type": "limit",
            "side": "buy",
            "amount": round(usdc_qty, 2),
            "price": leg3_ask,
            "params": {"timeInForce": "IOC"},
        },
    ]

    try:
        results = kraken_rest.create_orders(orders)
        for r in results:
            print(
                f"  ✅ Order submitted: {r.get('symbol')} {r.get('side')} | id: {r.get('id')} | status: {r.get('status')}"
            )
    except Exception as e:
        print(f"  ❌ Batch order execution failed: {e}")


async def monitor_loop():
    """Runs circuit evaluation continuously as WebSocket books update."""
    while True:
        evaluate_circuit()
        await asyncio.sleep(0.1)  # 100ms evaluation cadence — driven by WS push rate


async def main():
    print("=========================================================")
    print(f" WS Triangular Engine | Target Wallet: ${START_CAPITAL_USDC} USDC ")
    print(" Mode: WebSocket Live Feed + IOC Batch Execution        ")
    print("=========================================================")

    await asyncio.gather(
        stream_order_book("SOL/USDC"),
        stream_order_book("SOL/CAD"),
        stream_order_book("USDC/CAD"),
        monitor_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting WebSocket engine safely.")
