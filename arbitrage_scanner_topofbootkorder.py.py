#!/usr/bin/env python3
"""
Order-Book Taker Arbitrage Scanner: Kraken Pro <-> Crypto.com Exchange
Accounts for execution slippage by evaluating Ask-to-Bid crossing.
"""

import ccxt
import time
from datetime import datetime

# Initialize exchange interfaces
kraken = ccxt.kraken({
    'apiKey': 'YOUR_KRAKEN_API_KEY',
    'secret': 'YOUR_KRAKEN_SECRET',
    'enableRateLimit': True,
})

cryptocom = ccxt.cryptocom({
    'apiKey': 'YOUR_CRYPTOCOM_API_KEY',
    'secret': 'YOUR_CRYPTOCOM_SECRET',
    'enableRateLimit': True,
})

# Market Symbol Mapping Matrix
PAIRS = {
    'SOL': {'kraken': 'SOL/USDC', 'cryptocom': 'SOL/USDC'},
    'BTC': {'kraken': 'BTC/USDC', 'cryptocom': 'BTC/USDC'},
    'ETH': {'kraken': 'ETH/USDC', 'cryptocom': 'ETH/USDC'},
}

# Static Fee Configurations (Taker Base Tiers)
KRAKEN_TAKER = 0.0026      # 0.26%
CRYPTOCOM_TAKER = 0.00075  # 0.075%
TOTAL_TRADE_FEE = KRAKEN_TAKER + CRYPTOCOM_TAKER

# On-Chain Network Withdrawal Fee Slippage (USD Fixed Protections)
WITHDRAWAL_FEES = {
    'SOL': 1.50,   # ~0.01 SOL protection boundary
    'BTC': 6.00,   # Standard BTC mainnet overhead
    'ETH': 4.50    # Mainnet ERC-20 gas floor proxy
}

def fetch_order_books():
    """Fetches real-time tickers to extract immediate top-of-book order liquidity."""
    books = {}
    for asset, pairs in PAIRS.items():
        try:
            k_ticker = kraken.fetch_ticker(pairs['kraken'])
            c_ticker = cryptocom.fetch_ticker(pairs['cryptocom'])
            
            # Extract distinct order book boundaries
            books[asset] = {
                'kraken': {
                    'bid': float(k_ticker['bid']), # Highest price someone will BUY from you
                    'ask': float(k_ticker['ask'])  # Lowest price someone will SELL to you
                },
                'cryptocom': {
                    'bid': float(c_ticker['bid']),
                    'ask': float(c_ticker['ask'])
                },
                'timestamp': datetime.utcnow().isoformat()
            }
        except Exception as e:
            print(f"⚠️ Execution anomaly fetching order book for {asset}: {e}")
    return books

def calculate_taker_spreads(books):
    """
    Evaluates realistic directional spreads based on immediate taker execution.
    - Path Alpha: Buy Kraken Ask -> Sell Crypto.com Bid
    - Path Beta:  Buy Crypto.com Ask -> Sell Kraken Bid
    """
    opportunities = []
    
    for asset, market in books.items():
        k_bid = market['kraken']['bid']
        k_ask = market['kraken']['ask']
        c_bid = market['cryptocom']['bid']
        c_ask = market['cryptocom']['ask']
        
        # ---------------------------------------------------------------------
        # Scenario Alpha: Buy Kraken (Ask) -> Withdraw -> Sell Crypto.com (Bid)
        # ---------------------------------------------------------------------
        if c_bid > k_ask:
            gross_spread = (c_bid - k_ask) / k_ask
            # Fixed withdrawal costs calculated against the entry asset execution price
            net_spread = gross_spread - TOTAL_TRADE_FEE - (WITHDRAWAL_FEES[asset] / k_ask)
            
            if net_spread > 0:
                opportunities.append({
                    'asset': asset,
                    'direction': 'Kraken (Ask) -> Crypto.com (Bid)',
                    'gross_spread_pct': gross_spread * 100,
                    'net_spread_pct': net_spread * 100,
                    'buy_price': k_ask,
                    'sell_price': c_bid,
                })
                
        # ---------------------------------------------------------------------
        # Scenario Beta: Buy Crypto.com (Ask) -> Withdraw -> Sell Kraken (Bid)
        # ---------------------------------------------------------------------
        if k_bid > c_ask:
            gross_spread = (k_bid - c_ask) / c_ask
            net_spread = gross_spread - TOTAL_TRADE_FEE - (WITHDRAWAL_FEES[asset] / c_ask)
            
            if net_spread > 0:
                opportunities.append({
                    'asset': asset,
                    'direction': 'Crypto.com (Ask) -> Kraken (Bid)',
                    'gross_spread_pct': gross_spread * 100,
                    'net_spread_pct': net_spread * 100,
                    'buy_price': c_ask,
                    'sell_price': k_bid,
                })
                
    return opportunities

def main():
    print("=========================================================")
    print("  Order-Book Taker Scanner: Kraken Pro <-> Crypto.com   ")
    print("  Mode: Immediate Slippage Adjusted (Bid/Ask Crossing)   ")
    print("=========================================================")
    print("Press Ctrl+C to terminate probe daemon.\n")
    
    while True:
        try:
            books = fetch_order_books()
            opportunities = calculate_taker_spreads(books)
            
            print(f"\n⚡ [{datetime.utcnow().isoformat()}]")
            for asset, market in books.items():
                print(f"  {asset:4s} | Kraken:    Bid ${market['kraken']['bid']:10.2f} / Ask ${market['kraken']['ask']:10.2f}")
                print(f"       | Crypto.com: Bid ${market['cryptocom']['bid']:10.2f} / Ask ${market['cryptocom']['ask']:10.2f}")
            
            if opportunities:
                print("\n🟢 REAL TAKER ALPHA DETECTED (FEES ACCUMULATED):")
                for opp in opportunities:
                    print(f"  [{opp['asset']}] {opp['direction']} \n"
                          f"  | Real Gross: {opp['gross_spread_pct']:.3f}% "
                          f"| Real Net: {opp['net_spread_pct']:.3f}%\n"
                          f"  | Action: Buy Ask at ${opp['buy_price']:.2f} "
                          f"| Sell Bid at ${opp['sell_price']:.2f}")
            else:
                print("\n💤 Scanning... No cross-market order book overlap found after fees.")
                
            time.sleep(5) 
            
        except KeyboardInterrupt:
            print("\nExiting Probe Daemon safely.")
            break
        except Exception as e:
            print(f"Loop error: {e}")
            time.sleep(5)

if __name__ == '__main__':
    main()