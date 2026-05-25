#!/usr/bin/env python3
"""
Minimal spatial arbitrage scanner: Kraken Pro <-> Crypto.com Exchange
Focus: SOL/USDC, BTC/USDC, ETH/USDC
"""

import ccxt
import os
import time
from datetime import datetime

# Initialize exchange interfaces
kraken = ccxt.kraken(
    {
        "apiKey": os.getenv("KRAKEN_API_KEY"),
        "secret": os.getenv("KRAKEN_SECRET"),
        "enableRateLimit": True,
    }
)

cryptocom = ccxt.cryptocom(
    {
        "apiKey": os.getenv("CRYPTOCOM_API_KEY"),
        "secret": os.getenv("CRYPTOCOM_SECRET"),
        "enableRateLimit": True,
    }
)

# Market Symbol Mapping Matrix
PAIRS = {
    "SOL": {"kraken": "SOL/USDC", "cryptocom": "SOL_USD"},
    "BTC": {"kraken": "BTC/USDC", "cryptocom": "BTC_USD"},
    "ETH": {"kraken": "ETH/USDC", "cryptocom": "ETH_USD"},
}

# Static Fee Configurations (Taker Base Tiers)
KRAKEN_TAKER = 0.0026  # 0.26%
CRYPTOCOM_TAKER = 0.00075  # 0.075%
TOTAL_TRADE_FEE = KRAKEN_TAKER + CRYPTOCOM_TAKER

# On-Chain Network Withdrawal Fee Slippage (USD Estimated Protections)
WITHDRAWAL_FEES = {
    "SOL": 1.50,  # ~0.01 SOL protection boundary
    "BTC": 6.00,  # Standard BTC mainnet overhead
    "ETH": 4.50,  # Mainnet ERC-20 gas floor proxy
}


def fetch_prices():
    """Fetches real-time tickers and derives mid-point evaluation prices."""
    prices = {}
    for asset, pairs in PAIRS.items():
        try:
            k_ticker = kraken.fetch_ticker(pairs["kraken"])
            c_ticker = cryptocom.fetch_ticker(pairs["cryptocom"])

            # Mid-market proxy calculation
            k_mid = (k_ticker["bid"] + k_ticker["ask"]) / 2
            c_mid = (c_ticker["bid"] + c_ticker["ask"]) / 2

            prices[asset] = {
                "kraken": k_mid,
                "cryptocom": c_mid,
                "timestamp": datetime.utcnow().isoformat(),
            }
        except Exception as e:
            print(f"⚠️ Execution anomaly fetching {asset}: {e}")
    return prices


def calculate_spread(prices):
    """Evaluates mathematical directional spreads minus fee drag factors."""
    opportunities = []

    for asset, p in prices.items():
        k_price = p["kraken"]
        c_price = p["cryptocom"]

        # Scenario Alpha: Buy Kraken -> Sell Crypto.com
        if c_price > k_price:
            gross_spread = (c_price - k_price) / k_price
            net_spread = (
                gross_spread - TOTAL_TRADE_FEE - (WITHDRAWAL_FEES[asset] / k_price)
            )

            if net_spread > 0:
                opportunities.append(
                    {
                        "asset": asset,
                        "direction": "Kraken -> Crypto.com",
                        "gross_spread_pct": gross_spread * 100,
                        "net_spread_pct": net_spread * 100,
                        "buy_venue_price": k_price,
                        "sell_venue_price": c_price,
                    }
                )

        # Scenario Beta: Buy Crypto.com -> Sell Kraken
        if k_price > c_price:
            gross_spread = (k_price - c_price) / c_price
            net_spread = (
                gross_spread - TOTAL_TRADE_FEE - (WITHDRAWAL_FEES[asset] / c_price)
            )

            if net_spread > 0:
                opportunities.append(
                    {
                        "asset": asset,
                        "direction": "Crypto.com -> Kraken",
                        "gross_spread_pct": gross_spread * 100,
                        "net_spread_pct": net_spread * 100,
                        "buy_venue_price": c_price,
                        "sell_venue_price": k_price,
                    }
                )

    return opportunities


def main():
    print("=========================================================")
    print("  Arbitrage Spatial Engine: Kraken Pro <-> Crypto.com   ")
    print("=========================================================")
    print("Press Ctrl+C to terminate probe daemon.\n")

    while True:
        try:
            prices = fetch_prices()
            opportunities = calculate_spread(prices)

            print(f"\n⚡ [{datetime.utcnow().isoformat()}]")
            for asset, p in prices.items():
                print(
                    f"  {asset:4s} | Kraken: ${p['kraken']:10.2f} | Crypto.com: ${p['cryptocom']:10.2f}"
                )

            if opportunities:
                print("\n🟢 PROFITABLE ALPHA DETECTED:")
                for opp in opportunities:
                    print(
                        f"  [{opp['asset']}] {opp['direction']} \n"
                        f"  | Gross Delta: {opp['gross_spread_pct']:.3f}% "
                        f"| Net Target: {opp['net_spread_pct']:.3f}%\n"
                        f"  | Buy Vector: ${opp['buy_venue_price']:.2f} "
                        f"| Sell Vector: ${opp['sell_venue_price']:.2f}"
                    )
            else:
                print("\n💤 Scanning... Markets balanced below fee thresholds.")

            time.sleep(5)  # 5-second polling interval to respect standard rate-limiting

        except KeyboardInterrupt:
            print("\nExiting Probe Daemon safely.")
            break
        except Exception as e:
            print(f"Loop error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
