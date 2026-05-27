"""
Triangular Arbitrage Scanner: Kraken Pro Spot
Scans both legs simultaneously:
  Forward: USDT -> BTC -> ETH -> USDT
  Reverse: USDT -> ETH -> BTC -> USDT

Capital Tier: $1000 USDT
"""

import asyncio
import ccxt.async_support as ccxt
import time
from typing import Dict, Tuple

CAPITAL = 1000.0


class TriangularArbitrageScanner:
    def __init__(self, exchange_id: str = 'kraken'):
        self.exchange = getattr(ccxt, exchange_id)({
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })

    async def initialize(self):
        """Pre-load markets to populate system precision and dynamic fee variables."""
        await self.exchange.load_markets()

    async def fetch_order_books(self) -> Dict[str, dict]:
        """Fetch real order books simultaneously to account for order book depth sizing."""
        symbols = ["BTC/USDT", "ETH/USDT", "ETH/BTC"]
        tasks = [self.exchange.fetch_order_book(symbol, limit=5) for symbol in symbols]
        results = await asyncio.gather(*tasks)
        return dict(zip(symbols, results))

    def calculate_forward_loop(self, order_books: dict, fee: float) -> Tuple[float, str]:
        """
        Executes: USDT -> BTC -> ETH -> USDT
        Leg 1: Buy BTC with USDT (Pay Ask)
        Leg 2: Buy ETH with BTC  (Pay Ask)
        Leg 3: Sell ETH for USDT (Receive Bid)
        """
        try:
            btc_ask = order_books["BTC/USDT"]['asks'][0][0]
            tokens_btc = (CAPITAL / btc_ask) * (1.0 - fee)

            eth_btc_ask = order_books["ETH/BTC"]['asks'][0][0]
            tokens_eth = (tokens_btc / eth_btc_ask) * (1.0 - fee)

            eth_usdt_bid = order_books["ETH/USDT"]['bids'][0][0]
            final_usdt = (tokens_eth * eth_usdt_bid) * (1.0 - fee)

            return final_usdt - CAPITAL, f"USDT -> BTC (${btc_ask}) -> ETH (${eth_btc_ask}) -> USDT (${eth_usdt_bid})"
        except (IndexError, KeyError):
            return -CAPITAL, "Order book depth error"

    def calculate_reverse_loop(self, order_books: dict, fee: float) -> Tuple[float, str]:
        """
        Executes: USDT -> ETH -> BTC -> USDT
        Leg 1: Buy ETH with USDT (Pay Ask)
        Leg 2: Sell ETH for BTC  (Receive Bid)
        Leg 3: Sell BTC for USDT (Receive Bid)
        """
        try:
            eth_ask = order_books["ETH/USDT"]['asks'][0][0]
            tokens_eth = (CAPITAL / eth_ask) * (1.0 - fee)

            eth_btc_bid = order_books["ETH/BTC"]['bids'][0][0]
            tokens_btc = (tokens_eth * eth_btc_bid) * (1.0 - fee)

            btc_usdt_bid = order_books["BTC/USDT"]['bids'][0][0]
            final_usdt = (tokens_btc * btc_usdt_bid) * (1.0 - fee)

            return final_usdt - CAPITAL, f"USDT -> ETH (${eth_ask}) -> BTC (${eth_btc_bid}) -> USDT (${btc_usdt_bid})"
        except (IndexError, KeyError):
            return -CAPITAL, "Order book depth error"

    async def scan_loop(self):
        """Infinite tracking execution block."""
        print(f"Starting engine loop | Capital Tier: ${CAPITAL}")
        taker_fee = self.exchange.markets['BTC/USDT'].get('taker', 0.0026)

        while True:
            try:
                order_books = await self.fetch_order_books()
                ts = int(time.time() * 1000)

                print(f"\n--- Matrix Snapshot [{ts}] | Tier: ${int(CAPITAL)} ---")

                fwd_profit, fwd_route = self.calculate_forward_loop(order_books, taker_fee)
                fwd_pct = (fwd_profit / CAPITAL) * 100
                print(f"  Forward  | Profit: ${fwd_profit:+.4f} ({fwd_pct:+.3f}%) | Route: {fwd_route}")

                rev_profit, rev_route = self.calculate_reverse_loop(order_books, taker_fee)
                rev_pct = (rev_profit / CAPITAL) * 100
                print(f"  Reverse  | Profit: ${rev_profit:+.4f} ({rev_pct:+.3f}%) | Route: {rev_route}")

                if fwd_profit > 0:
                    print(f"  [ALERT] Profitable Forward Opp Discovered for Size ${int(CAPITAL)}!")
                if rev_profit > 0:
                    print(f"  [ALERT] Profitable Reverse Opp Discovered for Size ${int(CAPITAL)}!")

                await asyncio.sleep(1.0)
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
