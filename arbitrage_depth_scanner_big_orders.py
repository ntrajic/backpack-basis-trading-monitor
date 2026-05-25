#!/usr/bin/env python3
"""
Depth Matrix Arbitrage Scanner: Kraken Pro <-> Crypto.com Exchange
Simulates actual order book consumption to evaluate Effective VWAP for large trades.
"""

import ccxt
import time
from datetime import datetime

# Initialize exchange interfaces
kraken = ccxt.kraken({'enableRateLimit': True})
cryptocom = ccxt.cryptocom({'enableRateLimit': True})

# Configurable Parameters
TRADE_SIZE_USD = 2500.00   # Your target test capital scale
POLLING_INTERVAL = 6       # Slightly relaxed to prevent depth endpoint rate-limiting

PAIRS = {
    'SOL': {'kraken': 'SOL/USDC', 'cryptocom': 'SOL/USDC'},
    'BTC': {'kraken': 'BTC/USDC', 'cryptocom': 'BTC/USDC'},
    'ETH': {'kraken': 'ETH/USDC', 'cryptocom': 'ETH/USDC'},
}

# Taker Base Tiers
KRAKEN_TAKER = 0.0026
CRYPTOCOM_TAKER = 0.00075
TOTAL_TRADE_FEE = KRAKEN_TAKER + CRYPTOCOM_TAKER

WITHDRAWAL_FEES = {
    'SOL': 1.50,
    'BTC': 6.00,
    'ETH': 4.50
}

def get_effective_price(order_book_side, target_usd):
    """
    Simulates filling a specific USD order size against an order book side.
    
    Parameters:
        order_book_side (list): list of [price, volume] arrays (bids or asks)
        target_usd (float): total target value to buy/sell in USD
        
    Returns:
        float: The real volume-weighted fill price, or None if depth is insufficient.
    """
    accumulated_usd = 0.0
    accumulated_base = 0.0
    
    for price, volume in order_book_side:
        level_usd = price * volume
        
        if accumulated_usd + level_usd >= target_usd:
            # This order book level contains enough remaining volume to finish the fill
            needed_usd = target_usd - accumulated_usd
            needed_base = needed_usd / price
            accumulated_base += needed_base
            accumulated_usd += needed_usd
            break
        else:
            # Consume the entire level and move deeper into the matrix
            accumulated_usd += level_usd
            accumulated_base += volume
            
    if accumulated_usd < target_usd:
        return None # Market depth too shallow to clear the order size
        
    return accumulated_usd / accumulated_base


def fetch_and_parse_depth():
    """Fetches full order books and computes simulated execution prices."""
    execution_metrics = {}
    
    for asset, pairs in PAIRS.items():
        try:
            # Fetch L2 order books (default depth is usually sufficient, typically 20-50 levels)
            k_book = kraken.fetch_order_book(pairs['kraken'], limit=20)
            c_book = cryptocom.fetch_order_book(pairs['cryptocom'], limit=20)
            
            # Scenario Alpha Paths: Buying the Asks (sellers)
            k_eff_ask = get_effective_price(k_book['asks'], TRADE_SIZE_USD)
            c_eff_ask = get_effective_price(c_book['asks'], TRADE_SIZE_USD)
            
            # Scenario Beta Paths: Selling into the Bids (buyers)
            k_eff_bid = get_effective_price(k_book['bids'], TRADE_SIZE_USD)
            c_eff_bid = get_effective_price(c_book['bids'], TRADE_SIZE_USD)
            
            execution_metrics[asset] = {
                'kraken': {'eff_bid': k_eff_bid, 'eff_ask': k_eff_ask},
                'cryptocom': {'eff_bid': c_eff_bid, 'eff_ask': c_eff_ask},
                'top_of_book': {
                    'k_ask': k_book['asks'][0][0], 'k_bid': k_book['bids'][0][0],
                    'c_ask': c_book['asks'][0][0], 'c_bid': c_book['bids'][0][0]
                }
            }
        except Exception as e:
            print(f"⚠️ Depth retrieval anomaly for {asset}: {e}")
            
    return execution_metrics


def evaluate_opportunities(metrics):
    """Calculates spreads based entirely on effective depth execution prices."""
    opportunities = []
    
    for asset, data in metrics.items():
        k = data['kraken']
        c = data['cryptocom']
        top = data['top_of_book']
        
        # Check for depth failures
        if not all([k['eff_ask'], k['eff_bid'], c['eff_ask'], c['eff_bid']]):
            continue
            
        # Direction 1: Buy Kraken Depth -> Sell Crypto.com Depth
        if c['eff_bid'] > k['eff_ask']:
            gross_spread = (c['eff_bid'] - k['eff_ask']) / k['eff_ask']
            net_spread = gross_spread - TOTAL_TRADE_FEE - (WITHDRAWAL_FEES[asset] / k['eff_ask'])
            
            if net_spread > 0:
                opportunities.append({
                    'asset': asset, 'direction': 'Kraken -> Crypto.com',
                    'gross': gross_spread * 100, 'net': net_spread * 100,
                    'buy_p': k['eff_ask'], 'sell_p': c['eff_bid'],
                    'slippage_pct': ((k['eff_ask'] - top['k_ask']) / top['k_ask']) * 100
                })
                
        # Direction 2: Buy Crypto.com Depth -> Sell Kraken Depth
        if k['eff_bid'] > c['eff_ask']:
            gross_spread = (k['eff_bid'] - c['eff_ask']) / c['eff_ask']
            net_spread = gross_spread - TOTAL_TRADE_FEE - (WITHDRAWAL_FEES[asset] / c_ask)
            
            if net_spread > 0:
                opportunities.append({
                    'asset': asset, 'direction': 'Crypto.com -> Kraken',
                    'gross': gross_spread * 100, 'net': net_spread * 100,
                    'buy_p': c['eff_ask'], 'sell_p': k['eff_bid'],
                    'slippage_pct': ((c['eff_ask'] - top['c_ask']) / top['c_ask']) * 100
                })
                
    return opportunities


def main():
    print("=========================================================")
    print(f" Depth Matrix Scanner | Target Size: ${TRADE_SIZE_USD:,.2f} USD ")
    print("=========================================================")
    
    while True:
        try:
            metrics = fetch_and_parse_depth()
            opps = evaluate_opportunities(metrics)
            
            print(f"\n⚡ [{datetime.utcnow().isoformat()}]")
            for asset, data in metrics.items():
                print(f"  {asset:4s} | Kraken Eff:    Bid ${data['kraken']['eff_bid']:10.2f} / Ask ${data['kraken']['eff_ask']:10.2f}")
                print(f"       | Crypto.com Eff: Bid ${data['cryptocom']['eff_bid']:10.2f} / Ask ${data['cryptocom']['eff_ask']:10.2f}")
                
            if opps:
                print("\n🟢 PROFITABLE DEPTH ALPHA DETECTED:")
                for o in opps:
                    print(f"  [{o['asset']}] {o['direction']} \n"
                          f"  | Net Yield: {o['net']:.3f}% (Gross: {o['gross']:.3f}%) \n"
                          f"  | Simulated Fill Buy: ${o['buy_p']:.2f} | Sell: ${o['sell_p']:.2f}\n"
                          f"  | Estimated Entry Slippage: {o['slippage_pct']:.3f}%")
            else:
                print("\n💤 Scanning depth... No viable spreads broad enough to absorb book slippage.")
                
            time.sleep(POLLING_INTERVAL)
            
        except KeyboardInterrupt:
            print("\nShutting down engine scanner.")
            break
        except Exception as e:
            print(f"Runtime error: {e}")
            time.sleep(5)

if __name__ == '__main__':
    main()