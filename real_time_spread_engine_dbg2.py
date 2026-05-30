"""
The reason your script is floating on the ⌛ Waiting for initial orderbook updates from both exchanges screen
without printing anything to stdout comes down to how Kraken’s newer WebSocket V2 engine handles market naming
validations internally.

Kraken successfully sent you a [✓] Kraken Subscription Sent acknowledgment message, but silently dropped the
data stream. Here is exactly why:

The Naming Disconnect: In Kraken's older REST API engine, the market for stablecoin trading is denoted as USDT
(e.g. XBTUSDT). However, in their new WebSockets V2 architecture, the platform strictly uses standard asset
syntax rules.

The Specific Asset Pair String: Kraken's WebSockets engine maps stablecoin markets directly to fiat value peg
tags. While you are trying to query USDT, Kraken’s main high-liquidity books for spot trading utilize standard
fiat dollar pairs—namely USD (e.g., BTC/USD, ETH/USD, SOL/USD).

If you pass BTC/USDT to wss://ws.kraken.com/v2, it flags the request as an un-routable instrument or sits open
with zero matching order book ticks, causing your global dictionary data arrays to stay permanently populated
with None. Because of the if not ready: continue loop, the program hangs waiting forever.

To instantly break past this data freeze and see calculations flow onto your stdout, we must map Kraken's
symbols to USD while keeping Crypto.com on USDT (since Crypto.com uses USDT as its primary pool).
The calculation engine will compute cross-market spread anomalies between USD and USDT (which realistically
trade at a near 1:1 ratio).

The Fixed Code
Here is your updated script. It fixes the Kraken string mapping keys, bypasses the buffering freeze, and
prints every live comparison ticker tick directly to your Linux console:
"""
# real_time_spread_engine_dbg2.py
# real_time_spread_engine_dbg3.py
import asyncio
import json
import sys
import websockets
from decimal import Decimal

# -------------------------------------------------------------------
# CONFIGURATION & SIMULATION SETTINGS
# -------------------------------------------------------------------
SYMBOLS = ["BTC", "ETH", "SOL"]
CAPITAL_TIERS = [Decimal("100"), Decimal("1000"), Decimal("2000"), Decimal("3000")]

FEES = {
    "kraken": {
        "taker": Decimal("0.0026"),
        "maker": Decimal("0.0016"),
        "withdrawal": {
            "BTC": Decimal("0.0004"),
            "ETH": Decimal("0.0035"),
            "SOL": Decimal("0.005")
        }
    },
    # BUG 5 FIX: key must be "cryptocom" (no dot) so FEES[venue.lower()] resolves correctly
    "cryptocom": {
        "taker": Decimal("0.0026"),
        "maker": Decimal("0.0000"),
        "withdrawal": {
            "BTC": Decimal("0.0005"),
            "ETH": Decimal("0.004"),
            "SOL": Decimal("0.008")
        }
    }
}

# Live Memory Matrix Tracking Arrays
order_books = {
    "BTC": {"kraken": {"bid": None, "ask": None}, "cryptocom": {"bid": None, "ask": None}},
    "ETH": {"kraken": {"bid": None, "ask": None}, "cryptocom": {"bid": None, "ask": None}},
    "SOL": {"kraken": {"bid": None, "ask": None}, "cryptocom": {"bid": None, "ask": None}}
}

# BUG 1 FIX: Kraken V2 normalises BTC → XBT in all response payloads.
# Map incoming XBT back to our canonical BTC key.
KRAKEN_SYMBOL_MAP = {
    "XBT": "BTC",
    "ETH": "ETH",
    "SOL": "SOL",
}

# -------------------------------------------------------------------
# KRAKEN WEBSOCKETS LAYER (V2 API Engine)
# -------------------------------------------------------------------
async def stream_kraken():
    url = "wss://ws.kraken.com/v2"
    # Kraken V2 accepts "BTC/USD" on subscribe but always echoes back "XBT/USD"
    kraken_subscribe_pairs = [f"{sym}/USD" for sym in SYMBOLS]

    async for ws in websockets.connect(url):
        try:
            subscribe_msg = {
                "method": "subscribe",
                "params": {
                    "channel": "ticker",
                    "symbol": kraken_subscribe_pairs
                }
            }
            await ws.send(json.dumps(subscribe_msg))
            print(f"[✓] Kraken Stream Wire Connected: {kraken_subscribe_pairs}", flush=True)

            async for message in ws:
                data = json.loads(message)

                # Dump unrecognised control frames so nothing silently vanishes
                msg_type = data.get("type", "")
                channel  = data.get("channel", "")

                if channel == "ticker" and "data" in data:
                    packet = data["data"][0]
                    raw_symbol = packet["symbol"]          # e.g. "XBT/USD"
                    kraken_base = raw_symbol.split("/")[0]  # e.g. "XBT"

                    # BUG 1 FIX: translate XBT → BTC (and pass others through)
                    base_asset = KRAKEN_SYMBOL_MAP.get(kraken_base, kraken_base)

                    if base_asset in order_books:
                        order_books[base_asset]["kraken"]["bid"] = Decimal(str(packet["bid"]))
                        order_books[base_asset]["kraken"]["ask"] = Decimal(str(packet["ask"]))
                        evaluate_single_asset_spread(base_asset)
                    else:
                        print(f"[?] Kraken unknown base asset: {kraken_base!r} (raw: {raw_symbol!r})", flush=True)

                elif msg_type not in ("subscriptionStatus", "heartbeat", "") and channel not in ("status", ""):
                    # Print unexpected frames for visibility during debugging
                    print(f"[DBG Kraken] {data}", flush=True)

        except websockets.ConnectionClosed:
            print("[!] Kraken connection dropped. Reconnecting...", flush=True)
            await asyncio.sleep(1)
        except Exception as e:
            print(f"[-] Kraken Stream Exception: {e}", flush=True)


# -------------------------------------------------------------------
# CRYPTO.COM WEBSOCKETS LAYER (V2 API Engine)
# -------------------------------------------------------------------

# BUG 4 FIX: define heartbeat responder outside the reconnect loop so it
# never accidentally captures a stale ws closure reference.
async def _cryptocom_heartbeat(ws, msg_id: int):
    await ws.send(json.dumps({"id": msg_id, "method": "public/respond-heartbeat"}))


async def stream_cryptocom():
    url = "wss://stream.crypto.com/v2/market"
    crypto_channels = [f"ticker.{sym}_USDT" for sym in SYMBOLS]

    async for ws in websockets.connect(url):
        try:
            subscribe_msg = {
                "id": 1,
                "method": "subscribe",
                "params": {"channels": crypto_channels},
                "nonce": 1
            }
            await ws.send(json.dumps(subscribe_msg))
            print(f"[✓] Crypto.com Stream Wire Connected: {crypto_channels}", flush=True)

            async for message in ws:
                data = json.loads(message)

                # BUG 4 FIX: pass ws explicitly, no closure capture
                if data.get("method") == "public/heartbeat":
                    await _cryptocom_heartbeat(ws, data["id"])
                    continue

                # BUG 2 FIX: Crypto.com V2 pushes live ticks under method "subscription",
                # NOT "ticker.update".  The subscription ACK also uses "subscription" but
                # carries no result.data list, so the inner guard handles that safely.
                if data.get("method") == "subscription":
                    result = data.get("result", {})
                    # BUG 3 FIX: payload lives at result["data"], not params["data"]
                    tick_list = result.get("data")
                    if not tick_list:
                        # Subscription confirmation frame – nothing to parse
                        print(f"[DBG Crypto.com] subscription ACK: {result.get('channel', '?')}", flush=True)
                        continue

                    packet = tick_list[0]
                    raw_instrument = packet["i"]            # e.g. "BTC_USDT"
                    base_asset = raw_instrument.split("_")[0]

                    if base_asset in order_books:
                        order_books[base_asset]["cryptocom"]["bid"] = Decimal(str(packet["b"]))
                        order_books[base_asset]["cryptocom"]["ask"] = Decimal(str(packet["a"]))
                        evaluate_single_asset_spread(base_asset)
                    else:
                        print(f"[?] Crypto.com unknown base asset: {base_asset!r}", flush=True)
                else:
                    print(f"[DBG Crypto.com] {data}", flush=True)

        except websockets.ConnectionClosed:
            print("[!] Crypto.com connection dropped. Reconnecting...", flush=True)
            await asyncio.sleep(1)
        except Exception as e:
            print(f"[-] Crypto.com Stream Exception: {e}", flush=True)


# -------------------------------------------------------------------
# EVENT-DRIVEN STREAM PIPE INTERCEPTOR
# -------------------------------------------------------------------
def evaluate_single_asset_spread(symbol):
    book = order_books[symbol]

    k_ask = book["kraken"]["ask"]
    k_bid = book["kraken"]["bid"]
    c_ask = book["cryptocom"]["ask"]
    c_bid = book["cryptocom"]["bid"]

    if k_ask and c_bid:
        process_directional_telemetry(symbol, "Kraken",     k_ask, "Crypto.com", c_bid)
    if c_ask and k_bid:
        process_directional_telemetry(symbol, "Crypto.com", c_ask, "Kraken",     k_bid)


def process_directional_telemetry(symbol, buy_venue, buy_price, sell_venue, sell_price):
    gross_gap      = sell_price - buy_price
    percentage_gap = (gross_gap / buy_price) * 100

    # BUG 6 FIX: flush=True so lines appear immediately even when stdout is
    # redirected to a pipe, a file, or a non-TTY terminal.
    print(
        f" -> [TICK LOG] {symbol:5} | Route: {buy_venue:10} -> {sell_venue:10} "
        f"| Gap: ${gross_gap:9.4f} ({percentage_gap:7.4f}%)",
        flush=True
    )

    if gross_gap <= 0:
        return

    # Capital Matrix Simulation Multi-Tier Array Loop
    for capital in CAPITAL_TIERS:
        trade_size = capital / buy_price

        # BUG 5 FIX: venue display names contain a dot ("Crypto.com") which would
        # make FEES["crypto.com"] raise a KeyError.  Strip the dot before lookup.
        buy_fee_key  = buy_venue.lower().replace(".", "")   # "kraken" or "cryptocom"
        sell_fee_key = sell_venue.lower().replace(".", "")

        buy_fee_pct        = FEES[buy_fee_key]["taker"]
        sell_fee_pct       = FEES[sell_fee_key]["taker"]
        withdrawal_fee_asset = FEES[buy_fee_key]["withdrawal"][symbol]

        gross_cost     = buy_price * trade_size
        entry_fee      = gross_cost * buy_fee_pct

        arrival_amount = trade_size - withdrawal_fee_asset
        if arrival_amount <= 0:
            continue

        gross_revenue = arrival_amount * sell_price
        exit_fee      = gross_revenue * sell_fee_pct

        net_profit = gross_revenue - gross_cost - (entry_fee + exit_fee)
        net_pct    = (net_profit / gross_cost) * 100

        if net_profit > 0:
            print(f"\n🔥 [NOTIFICATION: PROFITABLE EDGE] {symbol}", flush=True)
            print(f"   Route         : BUY {buy_venue} (${buy_price:.2f}) -> MOVE -> SELL {sell_venue} (${sell_price:.2f})", flush=True)
            print(f"   Capital Level : ${capital} USDT (Purchased: {trade_size:.6f} units)", flush=True)
            print(f"   Network Fee   : -{withdrawal_fee_asset} {symbol} (Paid to {buy_venue})", flush=True)
            print(f"   Exchange Fees : Entry Taker: ${entry_fee:.4f} | Exit Taker: ${exit_fee:.4f}", flush=True)
            print(f"   🚀 NET PROFIT : +${net_profit:.4f} USDT ({net_pct:.4f}% Net Yield)", flush=True)
            print("-" * 75, flush=True)


# -------------------------------------------------------------------
# ENGINE ASYNC RUN ORCHESTRATION LAYER
# -------------------------------------------------------------------
async def main():
    # Force line-buffered stdout globally (catches cases where flush=True is missed)
    sys.stdout.reconfigure(line_buffering=True)
    print("⚡ Asynchronous Telemetry Pipe Active. Processing high-frequency hooks...", flush=True)
    await asyncio.gather(
        stream_kraken(),
        stream_cryptocom()
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTelemetry logger shutting down cleanly.")
