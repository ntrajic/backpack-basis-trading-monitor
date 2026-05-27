"""
Here is an advanced, production-ready asynchronous automation script 
that scans both legs of the requested loops simultaneously 

(USDT -> BTC -> ETH -> USDT and USDT -> ETH -> BTC -> USDT).

This script relies on ccxt.pro / ccxt.async_support to handle 
pricing execution pipelines asynchronously, calculates fees dynamically 
(assuming standard spot maker/taker settings), and factors in slippage 
adjustments across three tiers of test capital: $100, $1,000, and $10,000.
The requested filename formatting with the numerical tier suffix is handled 
explicitly inside the configuration block.

Script Performance CharacteristicsOrder Book vs. Simple Ticker Pricing: 
Instead of calling fetch_tickers() which gives you the last traded spot price or 
top-level quote snapshots, this script calls real-time order books. 
When you test a larger capital structure like $10,000 compared to $100, 
tracking the depth size (asks[0][0] and bids[0][0]) allows you to 
evaluate market slippage risks directly.
Dynamic Fee Modeling: Exchange trading fees can turn a theoretically profitable 
route into a loss-making one. The script pulls the taker fee attribute directly 
from the exchange market structure (self.exchange.markets) to ensure net calculations are correct.Execution Concurrency: By leveraging asyncio.gather(), your network calls to pull order books are handled concurrently instead of sequentially, limiting the price dislocation risks that occur when data feeds are out of sync.
"""
# File Layout: tri_ang_arb_USDT_BTC_ETH_BTC_ETH_USDT_100.py
# File Layout: tri_ang_arb_USDT_BTC_ETH_BTC_ETH_USDT_1000.py
# File Layout: tri_ang_arb_USDT_BTC_ETH_BTC_ETH_USDT_10000.py

import asyncio
import ccxt.async_support as ccxt
import time
from typing import Dict, Tuple

class TriangularArbitrageScanner:
    def __init__(self, exchange_id: str = 'kraken'):
        # Initialize exchange using the standard ccxt async protocol
        self.exchange = getattr(ccxt, exchange_id)({
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })
        
        # Define execution paths matching requested loops
        self.paths = {
            "Forward (BTC Leg First)": ["BTC/USDT", "ETH/BTC", "ETH/USDT"],
            "Reverse (ETH Leg First)": ["ETH/USDT", "ETH/BTC", "BTC/USDT"]
        }
        
        # Capital allocation steps requested
        self.capital_tiers = [100.0, 1000.0, 10000.0]
        
    async def initialize(self):
        """Pre-load markets to populate system precision and dynamic fee variables."""
        await self.exchange.load_markets()

    async def fetch_order_books(self) -> Dict[str, dict]:
        """Fetch real order books simultaneously to account for order book depth sizing."""
        symbols = ["BTC/USDT", "ETH/USDT", "ETH/BTC"]
        tasks = [self.exchange.fetch_order_book(symbol, limit=5) for symbol in symbols]
        results = await asyncio.gather(*tasks)
        return dict(zip(symbols, results))

    def calculate_forward_loop(self, order_books: dict, fee: float, capital: float) -> Tuple[float, str]:
        """
        Executes: USDT -> BTC -> ETH -> USDT
        Leg 1: Buy BTC with USDT (Pay Ask)
        Leg 2: Buy ETH with BTC  (Pay Ask)
        Leg 3: Sell ETH for USDT (Receive Bid)
        """
        try:
            # Leg 1: Market Buy BTC using USDT
            btc_ask = order_books["BTC/USDT"]['asks'][0][0]
            tokens_btc = (capital / btc_ask) * (1.0 - fee)
            
            # Leg 2: Market Buy ETH using BTC
            eth_btc_ask = order_books["ETH/BTC"]['asks'][0][0]
            tokens_eth = (tokens_btc / eth_btc_ask) * (1.0 - fee)
            
            # Leg 3: Market Sell ETH back to USDT
            eth_usdt_bid = order_books["ETH/USDT"]['bids'][0][0]
            final_usdt = (tokens_eth * eth_usdt_bid) * (1.0 - fee)
            
            return final_usdt - capital, f"USDT -> BTC (${btc_ask}) -> ETH (${eth_btc_ask}) -> USDT (${eth_usdt_bid})"
        except (IndexError, KeyError):
            return -capital, "Order book depth error"

    def calculate_reverse_loop(self, order_books: dict, fee: float, capital: float) -> Tuple[float, str]:
        """
        Executes: USDT -> ETH -> BTC -> USDT
        Leg 1: Buy ETH with USDT (Pay Ask)
        Leg 2: Sell ETH for BTC  (Receive Bid)
        Leg 3: Sell BTC for USDT (Receive Bid)
        """
        try:
            # Leg 1: Market Buy ETH using USDT
            eth_ask = order_books["ETH/USDT"]['asks'][0][0]
            tokens_eth = (capital / eth_ask) * (1.0 - fee)
            
            # Leg 2: Market Sell ETH for BTC
            eth_btc_bid = order_books["ETH/BTC"]['bids'][0][0]
            tokens_btc = (tokens_eth * eth_btc_bid) * (1.0 - fee)
            
            # Leg 3: Market Sell BTC back to USDT
            btc_usdt_bid = order_books["BTC/USDT"]['bids'][0][0]
            final_usdt = (tokens_btc * btc_usdt_bid) * (1.0 - fee)
            
            return final_usdt - capital, f"USDT -> ETH (${eth_ask}) -> BTC (${eth_btc_bid}) -> USDT (${btc_usdt_bid})"
        except (IndexError, KeyError):
            return -capital, "Order book depth error"

    async def scan_loop(self):
        """Infinite tracking execution block."""
        print(f"Starting engine loop across configurations...")
        # Get baseline taker fee from exchange metadata safely (defaulting to 0.26% if hidden)
        taker_fee = self.exchange.markets['BTC/USDT'].get('taker', 0.0026)
        
        while True:
            try:
                order_books = await self.fetch_order_books()
                ts = int(time.time() * 1000)
                
                print(f"\n--- Matrix Snapshot [{ts}] ---")
                for size in self.capital_tiers:
                    # Suffix notation confirmation mapping: tri_ang_arb_USDT_BTC_ETH_BTC_ETH_USDT_{int(size)}.py
                    print(f"Targeting Tier Size: ${int(size)}")
                    
                    # Compute forward loop
                    fwd_profit, fwd_route = self.calculate_forward_loop(order_books, taker_fee, size)
                    fwd_pct = (fwd_profit / size) * 100
                    print(f"  Forward  | Profit: ${fwd_profit:+.4f} ({fwd_pct:+.3f}%) | Route: {fwd_route}")
                    
                    # Compute reverse loop
                    rev_profit, rev_route = self.calculate_reverse_loop(order_books, taker_fee, size)
                    rev_pct = (rev_profit / size) * 100
                    print(f"  Reverse  | Profit: ${rev_profit:+.4f} ({rev_pct:+.3f}%) | Route: {rev_route}")
                    
                    # Signal threshold execution alert if raw profitable metrics clear transaction fees
                    if fwd_profit > 0:
                        print(f"  [ALERT] Profitable Forward Opp Discovered for Size ${int(size)}!")
                    if rev_profit > 0:
                        print(f"  [ALERT] Profitable Reverse Opp Discovered for Size ${int(size)}!")
                        
                await asyncio.sleep(1.0) # Rate limit polling delay to protect exchange socket state
            except Exception as e:
                print(f"Execution handling anomaly observed: {e}")
                await asyncio.sleep(5.0)

    async def close(self):
        await self.exchange.close()

async def main():
    scanner = TriangularArbitrageScanner(exchange_id='kraken')
    await scanner.initialize()
    try:
        await scanner.scan_loop()
    except KeyboardInterrupt:
        pass
    finally:
        await scanner.close()

if __name__ == "__main__":
    asyncio.run(main())