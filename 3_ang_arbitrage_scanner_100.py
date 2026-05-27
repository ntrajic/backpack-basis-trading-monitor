# file name: 3_ang_arbitrage_scanner_100.py
#!/usr/bin/env python3
"""
Triangular Arbitrage Scanner: 3-Leg Closed Loop (Single Exchange)
Calibrated for $100 USDC Capital Baseline on Kraken Pro.
Feasibility Model: Taker Loop (Fee Drag: 0.26% * 3 = 0.78%)

Part 2: Script 1 — Retail Micro-Capital Scanner (3_ang_arbitrage_scanner_100.py)
This script maps out a 3-leg loop (USDC ➔ SOL ➔ CAD ➔ USDC).
It is explicitly calibrated for your $100 capital baseline.
It calculates the exact trading fee friction:
 3 distinct trading legs, each subject to taker fee friction.
(0.26% taker fee multiplied across 3 distinct execution events = 0.78% total fee drag).

VERDICT: 
1 Switch to WebSockets for Processing Live Feeds:
Both scripts above utilize REST polling architecture, which is excellent for checking and testing your setup. 
For production deployment, you will want to implement Kraken's watch_order_book WebSockets protocol via ccxt.pro. 
This upgrades your script to listen to a live stream of changes directly from the match engine, 
rather than waiting on periodic script requests.

2 Utilize Automated Batch Routing (create_orders):
Kraken Pro provides a dedicated API endpoint allowing you to pass an array of multiple distinct orders 
inside a single network call. While this doesn't guarantee atomic execution, 
it ensures that your structural instructions for Leg 1, Leg 2, and Leg 3 hit Kraken's network input queue 
within the exact same data packet.

3 Incorporate IOC Constraints:
When executing the array payload via the API, append the timeInForce: 'IOC' option to each position instruction. 
If Leg 1 fills completely but Leg 2 misses its entry boundary by even a fraction of a cent, 
the engine kills the loop instantly, preventing your account from accumulating unhedged capital slippage.

ANALYSIS

Taker Fee ApplicationThe calculation correctly applies a flat 0.78% multi-leg penalty 
(0.26% $\times$ 3). This is highly accurate for a baseline retail account setup. 
The output logs confirm that the scripts accurately identify that the spreads 
are insufficient to clear this high taker fee hurdle, protecting your $100 capital 
from deprecation.

Punctuality Critique

Script 1: REST Polling (3_ang_arbitrage_scanner_100.py)

Punctuality 
Rating: 🔴 Poor (Completely Unusable for Live Execution)

Analysis: The logs show an execution timestamp delta of exactly 3.16 seconds between loops.

Plaintext[06:28:00.753644] -> [06:28:03.924240] -> [06:28:07.094522]

In high-frequency environments, a 3-second delay is an eternity. 
Arbitrage imbalances on centralized matching engines are usually consumed by institution
al co-located systems within 5 to 50 milliseconds. 
Polling via standard REST requests means you are looking at historical snapshots of 
what the market used to be, rather than its live state.
"""

import ccxt
import time
from datetime import datetime

# Initialize Kraken interface
kraken = ccxt.kraken({"enableRateLimit": True, "options": {"defaultType": "spot"}})

# Define the Closed Circuit Matrix
# Loop Path: Start with USDC -> Buy SOL -> Sell SOL for CAD -> Sell CAD back to USDC
START_CAPITAL_USDC = 100.00
TAKER_FEE_PER_LEG = 0.0026  # 0.26% Kraken standard taker fee
TOTAL_FEES_3_LEGS = TAKER_FEE_PER_LEG * 3


def run_triangular_probe():
    try:
        # Fetch immediate tickers for all three legs simultaneously
        tickers = kraken.fetch_tickers(["SOL/USDC", "SOL/CAD", "USDC/CAD"])

        # Verify all necessary order books responded
        if not all(k in tickers for k in ["SOL/USDC", "SOL/CAD", "USDC/CAD"]):
            return

        # Leg 1: Buy SOL with USDC (Interfacing with the lowest Ask price)
        leg1_rate = float(tickers["SOL/USDC"]["ask"])
        sol_acquired = START_CAPITAL_USDC / leg1_rate

        # Leg 2: Sell SOL for CAD (Interfacing with the highest Bid price)
        leg2_rate = float(tickers["SOL/CAD"]["bid"])
        cad_acquired = sol_acquired * leg2_rate

        # Leg 3: Convert CAD back to USDC
        # Note: Kraken quotes USDC/CAD. To turn CAD back to USDC, we must DIVIDE by the Ask
        leg3_rate = float(tickers["USDC/CAD"]["ask"])
        final_usdc = cad_acquired / leg3_rate

        # Performance Assessment
        gross_return = (final_usdc - START_CAPITAL_USDC) / START_CAPITAL_USDC
        net_return = gross_return - TOTAL_FEES_3_LEGS

        print(f"[{datetime.utcnow().isoformat()}] Circuit: USDC -> SOL -> CAD -> USDC")
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
            print("🟢 PROFITABLE RETAIL TRIANGLE DETECTED! Sending execution alert.")
        else:
            print("💤 Circuit evaluated below trading fee hurdles.")

    except Exception as e:
        print(f"⚠️ Market telemetry collection failure: {e}")


def main():
    print("=========================================================")
    print(
        f" Mini-Capital Triangular Engine | Target Wallet: ${START_CAPITAL_USDC} USDC "
    )
    print("=========================================================")
    while True:
        run_triangular_probe()
        time.sleep(
            3
        )  # Controlled loop spacing to respect baseline connection thresholds


if __name__ == "__main__":
    main()
