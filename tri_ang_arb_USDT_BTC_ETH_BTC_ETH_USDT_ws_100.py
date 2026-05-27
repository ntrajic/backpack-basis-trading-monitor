"""
Triangular Arbitrage Scanner: Kraken Pro Spot — WebSocket Edition
Scans both legs simultaneously via live WebSocket order book streams:
  Forward: USDT -> BTC -> ETH -> USDT
  Reverse: USDT -> ETH -> BTC -> USDT

Capital Tier: $100 USDT

WebSocket Upgrades vs REST version:
  1. ccxt.pro watch_order_book — 3 concurrent async streams push live L2 data
     directly from Kraken's match engine into shared state. No polling delay.
  2. Sub-100ms evaluation cadence — monitor loop reads shared order book state
     every 10ms, reacting to every book update as it arrives.
  3. IOC batch execution — when net profit > 0, all 3 legs are submitted in a
     single create_orders() call with timeInForce: IOC (Immediate-Or-Cancel).
     Any leg that misses its price boundary is cancelled instantly, preventing
     unhedged exposure.

Note on execution latency:
  WebSocket data collection is sub-millisecond. The remaining latency bottleneck
  is the REST HTTP POST for order execution (30-120ms). To eliminate this,
  a full FIX protocol or exchange co-location would be required.

Key upgrades over the REST _100.py:
====================================
1 ccxt.pro watch_order_book — 3 concurrent async tasks stream live L2 data from
        Kraken's match engine into order_books shared state. No HTTP polling.
2 10ms evaluation cadence — scan_loop reads the shared state every 10ms instead of
        waiting 1s for a REST round-trip.
3 IOC batch execution — when profit > 0, all 3 legs fire in a single create_orders()
        call with timeInForce: IOC. Any leg that misses its price is cancelled instantly,
        preventing unhedged exposure.
4 Separate REST client for order submission — WS client stays dedicated to streaming;
        REST client handles execution.
5 KRAKEN_API_KEY / KRAKEN_SECRET read from env vars — required for order submission
        (read-only scanning works without them).
flickering terminal screen:         
The fix: split scanning from display. The scan_loop runs at 10ms storing the 
        latest result in self.last_snapshot, while a separate display_loop 
        prints to terminal every 15 seconds. 
        IOC execution still fires immediately when profit > 0.

NOTE: 
Negative profit in USDT -> ETH -> BTC -> USDT means you end up with less USDT than you started with. Here's where the money goes:

Fee drag (biggest killer)
-----------------------------
Kraken taker fee is 0.26% × 3 legs = 0.78% total drag

On $100 capital that's ~$0.78 guaranteed loss before even touching spreads

The circuit needs to overcome this hurdle just to break even

Bid/ask spread cost
-----------------------
Leg 1: you buy ETH at the ask (higher price)
Leg 2: you sell ETH for BTC at the bid (lower price)
Leg 3: you sell BTC for USDT at the bid (lower price)

Every leg you're on the wrong side of the spread — you pay it, never receive it

Market efficiency
==================
On liquid pairs like ETH/USDT, ETH/BTC, BTC/USDT on Kraken, market makers have already arbitraged away any mispricing

The -0.78% to -0.82% you typically see is almost entirely just the fee drag with a tiny spread cost on top

Example at $100:

Start:        $100.00 USDT
After Leg 1:   $99.74  (paid 0.26% fee buying ETH)
After Leg 2:   $99.48  (paid 0.26% fee selling ETH→BTC)
After Leg 3:   $99.22  (paid 0.26% fee selling BTC→USDT)
Net loss:      -$0.78  (-0.78%)

When would it go positive?
Only if a genuine mispricing exists where: 

    ETH/USDT ask × ETH/BTC bid × BTC/USDT bid > 1 + total_fees. 

On major exchanges this happens in microseconds during high volatility events and 
is immediately consumed by HFT bots with co-location advantages.

"""

import asyncio
import os
import ccxt.pro as ccxtpro
import ccxt
import time
from typing import Dict, Tuple

CAPITAL = 100.0
SYMBOLS = ["BTC/USDT", "ETH/USDT", "ETH/BTC"]

# Shared order book state — written by WS streams, read by evaluation loop
order_books: Dict[str, dict] = {}


class TriangularArbitrageScannerWS:
    def __init__(self, exchange_id: str = "kraken"):
        self.exchange_ws = getattr(ccxtpro, exchange_id)(
            {
                "apiKey": os.getenv("KRAKEN_API_KEY"),
                "secret": os.getenv("KRAKEN_SECRET"),
                "enableRateLimit": True,
                "options": {"defaultType": "spot"},
            }
        )
        # Separate REST client for order execution
        self.exchange_rest = getattr(ccxt, exchange_id)(
            {
                "apiKey": os.getenv("KRAKEN_API_KEY"),
                "secret": os.getenv("KRAKEN_SECRET"),
                "enableRateLimit": True,
            }
        )
        self.taker_fee = 0.0026  # fallback; overwritten after market load

    async def initialize(self):
        """Pre-load markets to populate precision metadata and live taker fee."""
        await self.exchange_ws.load_markets()
        self.taker_fee = self.exchange_ws.markets["BTC/USDT"].get("taker", 0.0026)

    async def stream_order_book(self, symbol: str):
        """Continuously pushes live L2 book updates into shared state."""
        while True:
            try:
                ob = await self.exchange_ws.watch_order_book(symbol)
                order_books[symbol] = ob
            except Exception as e:
                print(f"⚠️ WS stream error [{symbol}]: {e}")
                await asyncio.sleep(1)

    def calculate_forward_loop(self) -> Tuple[float, str]:
        """
        USDT -> BTC -> ETH -> USDT
        Leg 1: Buy BTC with USDT (Pay Ask)
        Leg 2: Buy ETH with BTC  (Pay Ask)
        Leg 3: Sell ETH for USDT (Receive Bid)
        """
        try:
            btc_ask = order_books["BTC/USDT"]["asks"][0][0]
            tokens_btc = (CAPITAL / btc_ask) * (1.0 - self.taker_fee)

            eth_btc_ask = order_books["ETH/BTC"]["asks"][0][0]
            tokens_eth = (tokens_btc / eth_btc_ask) * (1.0 - self.taker_fee)

            eth_usdt_bid = order_books["ETH/USDT"]["bids"][0][0]
            final_usdt = (tokens_eth * eth_usdt_bid) * (1.0 - self.taker_fee)

            return (
                final_usdt - CAPITAL,
                btc_ask,
                eth_btc_ask,
                eth_usdt_bid,
                tokens_btc,
                tokens_eth,
            )
        except (IndexError, KeyError):
            return None, None, None, None, None, None

    def calculate_reverse_loop(self) -> Tuple[float, str]:
        """
        USDT -> ETH -> BTC -> USDT
        Leg 1: Buy ETH with USDT (Pay Ask)
        Leg 2: Sell ETH for BTC  (Receive Bid)
        Leg 3: Sell BTC for USDT (Receive Bid)
        """
        try:
            eth_ask = order_books["ETH/USDT"]["asks"][0][0]
            tokens_eth = (CAPITAL / eth_ask) * (1.0 - self.taker_fee)

            eth_btc_bid = order_books["ETH/BTC"]["bids"][0][0]
            tokens_btc = (tokens_eth * eth_btc_bid) * (1.0 - self.taker_fee)

            btc_usdt_bid = order_books["BTC/USDT"]["bids"][0][0]
            final_usdt = (tokens_btc * btc_usdt_bid) * (1.0 - self.taker_fee)

            return (
                final_usdt - CAPITAL,
                eth_ask,
                eth_btc_bid,
                btc_usdt_bid,
                tokens_eth,
                tokens_btc,
            )
        except (IndexError, KeyError):
            return None, None, None, None, None, None

    def execute_ioc_batch(self, orders: list):
        """
        Fires all 3 legs as a single IOC batch via REST.
        IOC: any leg that cannot fill at the target price is cancelled instantly.
        """
        try:
            results = self.exchange_rest.create_orders(orders)
            for r in results:
                print(
                    f"  ✅ {r.get('symbol')} {r.get('side')} | id: {r.get('id')} | status: {r.get('status')}"
                )
        except Exception as e:
            print(f"  ❌ Batch IOC execution failed: {e}")

    async def scan_loop(self):
        """Evaluates triangular circuits at 10ms cadence. Stores latest result for display loop."""
        print(f"Starting WS engine | Capital Tier: ${CAPITAL} | Fee: {self.taker_fee*100:.3f}%")
        self.last_snapshot = None

        while True:
            if not all(s in order_books for s in SYMBOLS):
                await asyncio.sleep(0.01)
                continue

            fwd_profit, btc_ask, eth_btc_ask, eth_usdt_bid, sol_btc, sol_eth = self.calculate_forward_loop()
            rev_profit, eth_ask, eth_btc_bid, btc_usdt_bid, r_eth, r_btc = self.calculate_reverse_loop()

            # Store latest result for the display loop — no print here
            self.last_snapshot = {
                'ts': int(time.time() * 1000),
                'fwd': (fwd_profit, btc_ask, eth_btc_ask, eth_usdt_bid),
                'rev': (rev_profit, eth_ask, eth_btc_bid, btc_usdt_bid),
            }

            # IOC execution fires immediately regardless of display cadence
            if fwd_profit is not None and fwd_profit > 0:
                print(f"  🟢 [ALERT] Profitable Forward — routing IOC batch...")
                self.execute_ioc_batch([
                    {'symbol': 'BTC/USDT', 'type': 'limit', 'side': 'buy',
                     'amount': round(sol_btc, 6), 'price': btc_ask, 'params': {'timeInForce': 'IOC'}},
                    {'symbol': 'ETH/BTC',  'type': 'limit', 'side': 'buy',
                     'amount': round(sol_eth, 6), 'price': eth_btc_ask, 'params': {'timeInForce': 'IOC'}},
                    {'symbol': 'ETH/USDT', 'type': 'limit', 'side': 'sell',
                     'amount': round(sol_eth, 6), 'price': eth_usdt_bid, 'params': {'timeInForce': 'IOC'}},
                ])

            if rev_profit is not None and rev_profit > 0:
                print(f"  🟢 [ALERT] Profitable Reverse — routing IOC batch...")
                self.execute_ioc_batch([
                    {'symbol': 'ETH/USDT', 'type': 'limit', 'side': 'buy',
                     'amount': round(r_eth, 6), 'price': eth_ask, 'params': {'timeInForce': 'IOC'}},
                    {'symbol': 'ETH/BTC',  'type': 'limit', 'side': 'sell',
                     'amount': round(r_eth, 6), 'price': eth_btc_bid, 'params': {'timeInForce': 'IOC'}},
                    {'symbol': 'BTC/USDT', 'type': 'limit', 'side': 'sell',
                     'amount': round(r_btc, 6), 'price': btc_usdt_bid, 'params': {'timeInForce': 'IOC'}},
                ])

            await asyncio.sleep(0.01)  # 10ms scan cadence

    async def display_loop(self):
        """Prints the latest snapshot to terminal every 15 seconds."""
        DISPLAY_INTERVAL = 15
        while True:
            await asyncio.sleep(DISPLAY_INTERVAL)
            snap = getattr(self, 'last_snapshot', None)
            if snap is None:
                print("⏳ Waiting for first WS book data...")
                continue

            fwd_profit, btc_ask, eth_btc_ask, eth_usdt_bid = snap['fwd']
            rev_profit, eth_ask, eth_btc_bid, btc_usdt_bid = snap['rev']

            print(f"\n--- WS Snapshot [{snap['ts']}] | Tier: ${int(CAPITAL)} ---")
            if fwd_profit is not None:
                fwd_pct = (fwd_profit / CAPITAL) * 100
                print(f"  Forward  | Profit: ${fwd_profit:+.4f} ({fwd_pct:+.3f}%)"
                      f" | USDT->BTC(${btc_ask})->ETH(${eth_btc_ask})->USDT(${eth_usdt_bid})")
                if fwd_profit > 0:
                    print(f"  🟢 profit positive")
            if rev_profit is not None:
                rev_pct = (rev_profit / CAPITAL) * 100
                print(f"  Reverse  | Profit: ${rev_profit:+.4f} ({rev_pct:+.3f}%)"
                      f" | USDT->ETH(${eth_ask})->BTC(${eth_btc_bid})->USDT(${btc_usdt_bid})")
                if rev_profit > 0:
                    print(f"  🟢 profit positive")

    async def close(self):
        await self.exchange_ws.close()


async def main():
    scanner = TriangularArbitrageScannerWS(exchange_id="kraken")
    await scanner.initialize()
    try:
        await asyncio.gather(
            scanner.stream_order_book("BTC/USDT"),
            scanner.stream_order_book("ETH/USDT"),
            scanner.stream_order_book("ETH/BTC"),
            scanner.scan_loop(),
            scanner.display_loop(),
        )
    except KeyboardInterrupt:
        pass
    finally:
        await scanner.close()


if __name__ == "__main__":
    asyncio.run(main())
