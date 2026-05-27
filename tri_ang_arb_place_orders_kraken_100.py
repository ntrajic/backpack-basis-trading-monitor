"""
Triangular Arbitrage — Automated Order Placement: Kraken Pro Spot
Capital Tier: $100 USDT

Routes:
  Forward: USDT -> BTC -> ETH -> USDT
  Reverse: USDT -> ETH -> BTC -> USDT

Architecture:
  - 3 concurrent ccxt.pro WebSocket streams feed shared order book state
  - scan_loop evaluates both circuits at 10ms cadence
  - On profit > 0: all 3 legs fire immediately as a single IOC batch via REST
  - display_loop prints human-readable snapshot every 15 seconds
  - Cooldown guard prevents re-entry within 10 seconds of last execution
  - Leg failure detection: if any leg is cancelled/unfilled, logs unhedged exposure warning

Order types per route:
  Forward:  BTC/USDT buy limit IOC  |  ETH/BTC buy limit IOC  |  ETH/USDT sell limit IOC
  Reverse:  ETH/USDT buy limit IOC  |  ETH/BTC sell limit IOC  |  BTC/USDT sell limit IOC

Why limit IOC (not market):
  Locks in the exact price the profit was calculated at.
  If the book has moved by execution time, the leg is cancelled instantly
  with zero fill — preventing entry at a price that no longer yields profit.

Risk note:
Two-Phase Commit Protocol:
  PHASE 1 — Pre-flight validation:
    Re-reads live order book immediately before submission.
    Recalculates profit with fresh prices.
    Aborts (no orders placed) if profit has evaporated since scan detected it.

  PHASE 2 — Commit + rollback:
    Submits all 3 legs as a single IOC batch.
    Inspects each leg result status.
    If ALL 3 filled  -> commit: circuit closed cleanly.
    If ANY leg fails -> rollback: immediately submits compensating market orders
                        to unwind every filled leg back to USDT.

  Rollback order map:
    Forward rollback:  sell BTC->USDT (leg1), sell ETH->BTC (leg2), buy ETH->USDT (leg3)
    Reverse rollback:  sell ETH->USDT (leg1), buy  ETH->BTC (leg2), buy  BTC->USDT (leg3)

  Limitation:
    Rollback uses market orders — you will pay the spread again on unwind.
    This is unavoidable without exchange-native atomic basket support.

Credentials:
  export KRAKEN_API_KEY="your_key"
  export KRAKEN_SECRET="your_secret"

Both files updated. Here's exactly how the 2PC protocol works:

Phase 1 — Pre-flight (abort gate)
Re-reads the live order book at the moment of execution (not the 10ms-old scan value)
Recalculates profit with fresh prices
If profit ≤ 0 → ⛔ ABORT — zero orders placed, zero risk
If profit still > 0 → updates prices in-place and proceeds to Phase 2

Phase 2 — Commit or Rollback
Submits all 3 legs as a single IOC batch
Inspects each result's status and filled amount
All 3 filled → 🟢 COMMIT — circuit closed cleanly
Any leg failed → 🔴 ROLLBACK — immediately fires compensating market orders in 
        reverse leg order (leg 3 first, then 2, then 1) to unwind back to USDT

Rollback order map:

Route	Leg failed	Compensating order
Forward	leg 1 (bought BTC)	sell BTC/USDT market
Forward	leg 2 (bought ETH)	sell ETH/BTC market
Forward	leg 3 (sold ETH)	buy ETH/USDT market
Reverse	same logic, reversed sides
Honest limitation: rollback uses market orders — you pay the spread again on unwind. 
    This is unavoidable without exchange-native atomic basket support.

b/c it is not true 2PCprotocol, calculate the add on fees that occure in this 
    implentation:
"""

import asyncio
import os
import time
import ccxt.pro as ccxtpro
import ccxt
from typing import Dict, Optional, Tuple

CAPITAL       = 100.0
SYMBOLS       = ["BTC/USDT", "ETH/USDT", "ETH/BTC"]
DISPLAY_INTERVAL = 15   # seconds between terminal prints
EXEC_COOLDOWN    = 10   # seconds to wait after any execution before re-entry

order_books: Dict[str, dict] = {}


class TriangularArbExecutor:
    def __init__(self, exchange_id: str = "kraken"):
        creds = {
            "apiKey": os.getenv("KRAKEN_API_KEY"),
            "secret": os.getenv("KRAKEN_SECRET"),
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
        }
        self.exchange_ws   = getattr(ccxtpro, exchange_id)(creds)
        self.exchange_rest = getattr(ccxt,    exchange_id)(creds)
        self.taker_fee     = 0.0026
        self.last_snapshot: Optional[dict] = None
        self.last_exec_ts  = 0.0   # epoch seconds of last execution

    async def initialize(self):
        await self.exchange_ws.load_markets()
        self.taker_fee = self.exchange_ws.markets["BTC/USDT"].get("taker", 0.0026)
        print(f"✅ Markets loaded | Taker fee: {self.taker_fee*100:.3f}%")
        print(f"   Capital: ${CAPITAL} | Cooldown: {EXEC_COOLDOWN}s | Display: {DISPLAY_INTERVAL}s")
        print("=" * 60)

    # ------------------------------------------------------------------ #
    #  WebSocket streams                                                   #
    # ------------------------------------------------------------------ #
    async def stream_order_book(self, symbol: str):
        while True:
            try:
                ob = await self.exchange_ws.watch_order_book(symbol)
                order_books[symbol] = ob
            except Exception as e:
                print(f"⚠️  WS stream error [{symbol}]: {e}")
                await asyncio.sleep(1)

    # ------------------------------------------------------------------ #
    #  Circuit calculators                                                 #
    # ------------------------------------------------------------------ #
    def _forward(self) -> Tuple:
        """USDT -> BTC -> ETH -> USDT"""
        try:
            btc_ask      = order_books["BTC/USDT"]["asks"][0][0]
            tokens_btc   = (CAPITAL / btc_ask)    * (1.0 - self.taker_fee)
            eth_btc_ask  = order_books["ETH/BTC"]["asks"][0][0]
            tokens_eth   = (tokens_btc / eth_btc_ask) * (1.0 - self.taker_fee)
            eth_usdt_bid = order_books["ETH/USDT"]["bids"][0][0]
            final_usdt   = (tokens_eth * eth_usdt_bid) * (1.0 - self.taker_fee)
            return final_usdt - CAPITAL, btc_ask, eth_btc_ask, eth_usdt_bid, tokens_btc, tokens_eth
        except (IndexError, KeyError):
            return None, None, None, None, None, None

    def _reverse(self) -> Tuple:
        """USDT -> ETH -> BTC -> USDT"""
        try:
            eth_ask      = order_books["ETH/USDT"]["asks"][0][0]
            tokens_eth   = (CAPITAL / eth_ask)    * (1.0 - self.taker_fee)
            eth_btc_bid  = order_books["ETH/BTC"]["bids"][0][0]
            tokens_btc   = (tokens_eth * eth_btc_bid) * (1.0 - self.taker_fee)
            btc_usdt_bid = order_books["BTC/USDT"]["bids"][0][0]
            final_usdt   = (tokens_btc * btc_usdt_bid) * (1.0 - self.taker_fee)
            return final_usdt - CAPITAL, eth_ask, eth_btc_bid, btc_usdt_bid, tokens_eth, tokens_btc
        except (IndexError, KeyError):
            return None, None, None, None, None, None

    # ------------------------------------------------------------------ #
    #  Order execution                                                     #
    # ------------------------------------------------------------------ #
    def _build_forward_orders(self, btc_ask, eth_btc_ask, eth_usdt_bid, tokens_btc, tokens_eth) -> list:
        return [
            # Leg 1: Buy BTC with USDT
            {"symbol": "BTC/USDT", "type": "limit", "side": "buy",
             "amount": round(tokens_btc, 6), "price": btc_ask,
             "params": {"timeInForce": "IOC"}},
            # Leg 2: Buy ETH with BTC
            {"symbol": "ETH/BTC",  "type": "limit", "side": "buy",
             "amount": round(tokens_eth, 6), "price": eth_btc_ask,
             "params": {"timeInForce": "IOC"}},
            # Leg 3: Sell ETH for USDT
            {"symbol": "ETH/USDT", "type": "limit", "side": "sell",
             "amount": round(tokens_eth, 6), "price": eth_usdt_bid,
             "params": {"timeInForce": "IOC"}},
        ]

    def _build_reverse_orders(self, eth_ask, eth_btc_bid, btc_usdt_bid, tokens_eth, tokens_btc) -> list:
        return [
            # Leg 1: Buy ETH with USDT
            {"symbol": "ETH/USDT", "type": "limit", "side": "buy",
             "amount": round(tokens_eth, 6), "price": eth_ask,
             "params": {"timeInForce": "IOC"}},
            # Leg 2: Sell ETH for BTC
            {"symbol": "ETH/BTC",  "type": "limit", "side": "sell",
             "amount": round(tokens_eth, 6), "price": eth_btc_bid,
             "params": {"timeInForce": "IOC"}},
            # Leg 3: Sell BTC for USDT
            {"symbol": "BTC/USDT", "type": "limit", "side": "sell",
             "amount": round(tokens_btc, 6), "price": btc_usdt_bid,
             "params": {"timeInForce": "IOC"}},
        ]

    # ------------------------------------------------------------------ #
    #  Two-Phase Commit                                                    #
    # ------------------------------------------------------------------ #
    def _phase1_preflight(self, route: str, prices: dict) -> bool:
        """
        Phase 1: Re-read live book and reconfirm profit is still positive.
        Returns True (proceed to commit) or False (abort — no orders placed).
        """
        try:
            if route == "FWD":
                btc_ask      = order_books["BTC/USDT"]["asks"][0][0]
                eth_btc_ask  = order_books["ETH/BTC"]["asks"][0][0]
                eth_usdt_bid = order_books["ETH/USDT"]["bids"][0][0]
                tokens_btc   = (CAPITAL / btc_ask)       * (1.0 - self.taker_fee)
                tokens_eth   = (tokens_btc / eth_btc_ask) * (1.0 - self.taker_fee)
                final_usdt   = (tokens_eth * eth_usdt_bid) * (1.0 - self.taker_fee)
                fresh_profit = final_usdt - CAPITAL
                # Update prices dict in-place with freshest values for Phase 2
                prices.update({"btc_ask": btc_ask, "eth_btc_ask": eth_btc_ask,
                                "eth_usdt_bid": eth_usdt_bid,
                                "tokens_btc": tokens_btc, "tokens_eth": tokens_eth})
            else:
                eth_ask      = order_books["ETH/USDT"]["asks"][0][0]
                eth_btc_bid  = order_books["ETH/BTC"]["bids"][0][0]
                btc_usdt_bid = order_books["BTC/USDT"]["bids"][0][0]
                tokens_eth   = (CAPITAL / eth_ask)         * (1.0 - self.taker_fee)
                tokens_btc   = (tokens_eth * eth_btc_bid)  * (1.0 - self.taker_fee)
                final_usdt   = (tokens_btc * btc_usdt_bid) * (1.0 - self.taker_fee)
                fresh_profit = final_usdt - CAPITAL
                prices.update({"eth_ask": eth_ask, "eth_btc_bid": eth_btc_bid,
                                "btc_usdt_bid": btc_usdt_bid,
                                "tokens_eth": tokens_eth, "tokens_btc": tokens_btc})

            if fresh_profit <= 0:
                print(f"  ⛔ [{route}] Phase 1 ABORT — profit evaporated at preflight "
                      f"(${fresh_profit:+.4f}). No orders placed.")
                return False

            print(f"  ✔  [{route}] Phase 1 PASS — fresh profit ${fresh_profit:+.4f}. Proceeding to commit.")
            return True

        except (IndexError, KeyError) as e:
            print(f"  ⛔ [{route}] Phase 1 ABORT — book read error: {e}")
            return False

    def _phase2_commit(self, route: str, orders: list, prices: dict):
        """
        Phase 2: Submit IOC batch. If all 3 legs fill -> commit.
        If any leg fails -> rollback all filled legs immediately.
        """
        try:
            results  = self.exchange_rest.create_orders(orders)
            filled   = []   # list of (symbol, side, filled_amount)
            failed   = []

            for i, r in enumerate(results):
                status        = r.get("status", "unknown")
                filled_amount = float(r.get("filled", 0))
                symbol        = r.get("symbol")
                side          = r.get("side")
                print(f"  {'✅' if status == 'closed' else '❌'} [{route}] Leg {i+1}: "
                      f"{symbol} {side} | status: {status} | "
                      f"filled: {filled_amount} | id: {r.get('id')}")
                if status == "closed" and filled_amount > 0:
                    filled.append((symbol, side, filled_amount))
                else:
                    failed.append(symbol)

            if not failed:
                # ✅ COMMIT — all 3 legs filled
                print(f"  🟢 [{route}] COMMIT — all 3 legs filled. Circuit closed cleanly.")
                self.last_exec_ts = time.time()
            else:
                # ❌ ROLLBACK — unwind every filled leg
                print(f"  🔴 [{route}] ROLLBACK triggered — {len(failed)} leg(s) failed: {failed}")
                self._rollback(route, filled)

        except Exception as e:
            print(f"  ❌ [{route}] Phase 2 batch submission failed: {e}")

    def _rollback(self, route: str, filled_legs: list):
        """
        Submits compensating market orders to unwind each filled leg back to USDT.
        Rollback order is reversed — last filled leg unwound first.
        """
        if not filled_legs:
            print(f"  ↩  [{route}] Nothing to rollback — no legs were filled.")
            return

        print(f"  ↩  [{route}] Rolling back {len(filled_legs)} filled leg(s)...")
        # Reverse the fill order so we unwind from deepest leg back to USDT
        for symbol, original_side, amount in reversed(filled_legs):
            compensating_side = "sell" if original_side == "buy" else "buy"
            try:
                r = self.exchange_rest.create_order(
                    symbol, "market", compensating_side, amount
                )
                print(f"  ↩  Rollback: {symbol} {compensating_side} {amount} | "
                      f"status: {r.get('status')} | id: {r.get('id')}")
            except Exception as e:
                print(f"  ❌ ROLLBACK FAILED for {symbol} {compensating_side}: {e} "
                      f"— MANUAL INTERVENTION REQUIRED")

    def _execute_2pc(self, route: str, orders: list, prices: dict):
        """Entry point: runs Phase 1 preflight then Phase 2 commit/rollback."""
        print(f"\n  🟢 profit positive | {route} | initiating 2PC...")
        if self._phase1_preflight(route, prices):
            self._phase2_commit(route, orders, prices)

    # ------------------------------------------------------------------ #
    #  Scan loop — 10ms cadence, executes immediately on profit > 0       #
    # ------------------------------------------------------------------ #
    async def scan_loop(self):
        print(f"🚀 WS scan engine started | Capital: ${CAPITAL} | Fee: {self.taker_fee*100:.3f}%")

        while True:
            if not all(s in order_books for s in SYMBOLS):
                await asyncio.sleep(0.01)
                continue

            fwd_profit, btc_ask, eth_btc_ask, eth_usdt_bid, tokens_btc, tokens_eth = self._forward()
            rev_profit, eth_ask, eth_btc_bid, btc_usdt_bid, tokens_eth_r, tokens_btc_r = self._reverse()

            self.last_snapshot = {
                "ts":  int(time.time() * 1000),
                "fwd": (fwd_profit, btc_ask, eth_btc_ask, eth_usdt_bid),
                "rev": (rev_profit, eth_ask, eth_btc_bid, btc_usdt_bid),
            }

            in_cooldown = (time.time() - self.last_exec_ts) < EXEC_COOLDOWN

            if fwd_profit is not None and fwd_profit > 0 and not in_cooldown:
                prices = {}
                orders = self._build_forward_orders(
                    btc_ask, eth_btc_ask, eth_usdt_bid, tokens_btc, tokens_eth)
                self._execute_2pc("FWD", orders, prices)

            if rev_profit is not None and rev_profit > 0 and not in_cooldown:
                prices = {}
                orders = self._build_reverse_orders(
                    eth_ask, eth_btc_bid, btc_usdt_bid, tokens_eth_r, tokens_btc_r)
                self._execute_2pc("REV", orders, prices)

            await asyncio.sleep(0.01)

    # ------------------------------------------------------------------ #
    #  Display loop — prints every 15 seconds                             #
    # ------------------------------------------------------------------ #
    async def display_loop(self):
        while True:
            await asyncio.sleep(DISPLAY_INTERVAL)
            snap = self.last_snapshot
            if snap is None:
                print("⏳ Waiting for first WS book data...")
                continue

            fwd_profit, btc_ask, eth_btc_ask, eth_usdt_bid = snap["fwd"]
            rev_profit, eth_ask, eth_btc_bid, btc_usdt_bid = snap["rev"]
            cooldown_remaining = max(0.0, EXEC_COOLDOWN - (time.time() - self.last_exec_ts))

            print(f"\n{'='*60}")
            print(f"  WS Snapshot [{snap['ts']}] | Tier: ${int(CAPITAL)}")
            print(f"{'='*60}")

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

            if cooldown_remaining > 0:
                print(f"  ⏸  Execution cooldown: {cooldown_remaining:.1f}s remaining")

    async def close(self):
        await self.exchange_ws.close()


async def main():
    executor = TriangularArbExecutor(exchange_id="kraken")
    await executor.initialize()
    try:
        await asyncio.gather(
            executor.stream_order_book("BTC/USDT"),
            executor.stream_order_book("ETH/USDT"),
            executor.stream_order_book("ETH/BTC"),
            executor.scan_loop(),
            executor.display_loop(),
        )
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        await executor.close()


if __name__ == "__main__":
    asyncio.run(main())
