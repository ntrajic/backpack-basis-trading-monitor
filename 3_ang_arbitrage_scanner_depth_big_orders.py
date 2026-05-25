#!/usr/bin/env python3
"""
Production Depth Matrix Triangular Engine: Kraken Pro Spot
Simulates complete book consumption for massive scaling blocks ($1k to $100k+).
Utilizes advanced effective volume calculation to protect against thin liquidity depth.

Part 3: Script 2 — Depth Matrix Scale Engine (3_ang_arbitrage_scanner_depth_big_orders.py)
When you scale past $1,000 up to $100,000, you cannot use basic ticker prices. 
Your order will cut deep into the order books of all 3 pairs simultaneously.

This engine pulls down the full raw L2 Order Book Depths for every leg. 
It parses row-by-row to see exactly how far down your capital pushes the price, 
calculating the true Slippage-Adjusted Effective Return.

VERDICT:
log: 
 Telemetry Capture [2026-05-25T03:01:19.755279]
  | Size: $1,000.00 USDC -> Net Yield: -0.8212% | Out: $     999.59 USDC | 🔴 UNPROFITABLE (SLIPPAGE DR
The script is working correctly. Triangular arbitrage on liquid, efficient markets like Kraken 
is almost never profitable after fees because:

Kraken's 0.26% taker fee × 3 legs = 0.78% fee drag to overcome

Market makers have already arbitraged away any spread gaps

The -0.82% you see ($999.62 out of $1000) is essentially just the fee drag (0.78%) plus a tiny bid/ask spread cost (0.04%)

To find profitable loops you'd need to either:
1 Use maker orders (0.16% × 3 = 0.48% drag) — but that requires limit orders and execution risk
2 Look at less liquid pairs on smaller exchanges where mispricings persist longer
3 Monitor cross-exchange arbitrage instead of single-exchange triangular
"""

import ccxt
import time
from datetime import datetime

kraken = ccxt.kraken({'enableRateLimit': True})

# Scaled Target Assessment Parameters
TRADE_SIZES = [1000.0, 5000.0, 10000.0, 50000.0, 100000.0]
TAKER_FEE_PER_LEG = 0.0026
TOTAL_FEES_3_LEGS = TAKER_FEE_PER_LEG * 3

def simulate_buy_depth(order_book_asks, target_input_usd):
    """Calculates exactly how much base asset you get when throwing large USD allocations at the asks."""
    accumulated_base = 0.0
    remaining_usd = target_input_usd
    
    for price, volume, *_ in order_book_asks:
        level_usd_capacity = price * volume
        if remaining_usd <= level_usd_capacity:
            accumulated_base += remaining_usd / price
            remaining_usd = 0
            break
        else:
            accumulated_base += volume
            remaining_usd -= level_usd_capacity
            
    if remaining_usd > 0:
        return None # Book too shallow to fill order size
    return accumulated_base

def simulate_sell_depth(order_book_bids, target_input_base):
    """Calculates exactly how much quote asset (fiat/stable) you get when dumping large base asset quantities into bids."""
    accumulated_quote = 0.0
    remaining_base = target_input_base
    
    for price, volume, *_ in order_book_bids:
        if remaining_base <= volume:
            accumulated_quote += remaining_base * price
            remaining_base = 0
            break
        else:
            accumulated_quote += volume * price
            remaining_base -= volume
            
    if remaining_base > 0:
        return None # Book too shallow to fill order size
    return accumulated_quote

def evaluate_depth_triangles():
    print(f"\n⚡ Telemetry Capture [{datetime.utcnow().isoformat()}]")
    try:
        # Grab deep order books down 50 layers to map structural walls
        sol_usdc_book = kraken.fetch_order_book('SOL/USDC', limit=50)
        sol_cad_book = kraken.fetch_order_book('SOL/CAD', limit=50)
        usdc_cad_book = kraken.fetch_order_book('USDC/CAD', limit=50)
    except Exception as e:
        print(f"Failed to extract depth matrix arrays: {e}")
        return

    for size in TRADE_SIZES:
        # --- Leg 1: Spend 'size' USDC to Buy SOL ---
        sol_allocated = simulate_buy_depth(sol_usdc_book['asks'], size)
        if not sol_allocated:
            print(f"  | Allocation Size ${size:,.2f} USD -> ❌ Aborted: Shallow Kraken SOL/USDC Asks.")
            continue
            
        # --- Leg 2: Dump resulting SOL into the CAD Bid Wall ---
        cad_allocated = simulate_sell_depth(sol_cad_book['bids'], sol_allocated)
        if not cad_allocated:
            print(f"  | Allocation Size ${size:,.2f} USD -> ❌ Aborted: Shallow Kraken SOL/CAD Bids.")
            continue
            
        # --- Leg 3: Spend CAD to buy back USDC via USDC/CAD order book ---
        # Because the pair is USDC/CAD, buying USDC means we are buying the base asset using CAD.
        # Therefore, we treat our CAD as the input currency to consume the ASKS of the USDC/CAD book.
        final_usdc_recovered = simulate_buy_depth(usdc_cad_book['asks'], cad_allocated)
        if not final_usdc_recovered:
            print(f"  | Allocation Size ${size:,.2f} USD -> ❌ Aborted: Shallow Kraken USDC/CAD Asks.")
            continue
            
        # Yield Calculation
        gross_return = (final_usdc_recovered - size) / size
        net_return = gross_return - TOTAL_FEES_3_LEGS
        
        status_flag = "🟢 PROFITABLE COMPLIANT LOOP" if net_return > 0 else "🔴 UNPROFITABLE (SLIPPAGE DRAGGED)"
        print(f"  | Size: ${size:7,.2f} USDC -> Net Yield: {net_return*100:+.4f}% | Out: ${final_usdc_recovered:11,.2f} USDC | {status_flag}")

def main():
    print("=========================================================")
    print(" Institutional Multi-Tier Depth Matrix Engine Simulator ")
    print("=========================================================")
    while True:
        evaluate_depth_triangles()
        time.sleep(6) # Relaxed to accommodate heavy L2 matrix array data tracking

if __name__ == '__main__':
    main()