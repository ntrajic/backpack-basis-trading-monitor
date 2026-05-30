# market_feeds_ws.py
#
# Shared dual-feed websocket module:
# - Kraken: wss://ws.kraken.com
# - Crypto.com: wss://stream.crypto.com/v2/market
#
# Provides:
#   - ORDER_BOOKS structure
#   - start_kraken_ws()
#   - start_cryptocom_ws()
#   - get_live_quotes()
#   - get_top_of_book()

import asyncio
import json
from decimal import Decimal
from typing import Dict, Any

import websockets

SYMBOLS = ["BTC", "ETH", "SOL"]

# Canonical structure:
# {
#   "BTC": {
#       "Kraken": {"bid": float, "ask": float},
#       "Crypto.com": {"bid": float, "ask": float},
#   },
#   ...
# }
ORDER_BOOKS = {
    sym: {
        "Kraken": {"bid": None, "ask": None},
        "Crypto.com": {"bid": None, "ask": None},
    }
    for sym in SYMBOLS
}

# ─────────────────────────────────────────────────────────────
# Kraken WS
# ─────────────────────────────────────────────────────────────

KRAKEN_WS_URL = "wss://ws.kraken.com"

KRAKEN_PAIRS = {
    "BTC": "XBT/USD",
    "ETH": "ETH/USD",
    "SOL": "SOL/USD",
}

async def _kraken_ws_loop():
    while True:
        try:
            async with websockets.connect(KRAKEN_WS_URL, ping_interval=20, ping_timeout=20) as ws:
                sub_msg = {
                    "event": "subscribe",
                    "pair": list(KRAKEN_PAIRS.values()),
                    "subscription": {"name": "ticker"},
                }
                await ws.send(json.dumps(sub_msg))

                async for msg in ws:
                    data = json.loads(msg)
                    # Ignore heartbeat / system messages
                    if isinstance(data, dict):
                        continue
                    # Format: [channel_id, { "a": [...], "b": [...] }, "ticker", "XBT/USD"]
                    if len(data) < 4:
                        continue
                    payload = data[1]
                    pair = data[3]
                    ask = payload.get("a", [None])[0]
                    bid = payload.get("b", [None])[0]
                    if ask is None or bid is None:
                        continue
                    # Map pair back to symbol
                    sym = None
                    for s, p in KRAKEN_PAIRS.items():
                        if p == pair:
                            sym = s
                            break
                    if not sym:
                        continue
                    ORDER_BOOKS[sym]["Kraken"]["bid"] = float(bid)
                    ORDER_BOOKS[sym]["Kraken"]["ask"] = float(ask)
        except Exception as e:
            print(f"[WS] Kraken loop error: {e}. Reconnecting in 3s...", flush=True)
            await asyncio.sleep(3)


async def start_kraken_ws():
    asyncio.create_task(_kraken_ws_loop())

# ─────────────────────────────────────────────────────────────
# Crypto.com WS
# ─────────────────────────────────────────────────────────────

CDC_WS_URL = "wss://stream.crypto.com/v2/market"

CDC_INSTRUMENTS = {
    "BTC": "BTC_USDT",
    "ETH": "ETH_USDT",
    "SOL": "SOL_USDT",
}

async def _cryptocom_ws_loop():
    while True:
        try:
            async with websockets.connect(CDC_WS_URL, ping_interval=20, ping_timeout=20) as ws:
                sub_msg = {
                    "id": 1,
                    "method": "subscribe",
                    "params": {
                        "channels": [f"ticker.{inst}" for inst in CDC_INSTRUMENTS.values()]
                    },
                    "nonce": 1,
                }
                await ws.send(json.dumps(sub_msg))

                async for msg in ws:
                    data = json.loads(msg)
                    if "result" not in data or "data" not in data["result"]:
                        continue
                    for item in data["result"]["data"]:
                        inst = item.get("i")
                        bid = item.get("b")
                        ask = item.get("k")
                        if bid is None or ask is None:
                            continue
                        sym = None
                        for s, inst_name in CDC_INSTRUMENTS.items():
                            if inst_name == inst:
                                sym = s
                                break
                        if not sym:
                            continue
                        ORDER_BOOKS[sym]["Crypto.com"]["bid"] = float(bid)
                        ORDER_BOOKS[sym]["Crypto.com"]["ask"] = float(ask)
        except Exception as e:
            print(f"[WS] Crypto.com loop error: {e}. Reconnecting in 3s...", flush=True)
            await asyncio.sleep(3)


async def start_cryptocom_ws():
    asyncio.create_task(_cryptocom_ws_loop())

# ─────────────────────────────────────────────────────────────
# Public accessors
# ─────────────────────────────────────────────────────────────

async def get_live_quotes() -> Dict[str, Dict[str, Dict[str, float]]]:
    # In this architecture, ORDER_BOOKS is updated by WS tasks.
    # Here we just return a shallow copy.
    return ORDER_BOOKS


async def get_top_of_book() -> Dict[str, Dict[str, Dict[str, float]]]:
    # Same as get_live_quotes in this design.
    return ORDER_BOOKS
