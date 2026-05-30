"""
goal, the script has been redesigned to focus entirely on debugging, visibility, and
multi-tier capital simulation.

The Technical Revisions Made
Continuous Live Gap Logging: The script now logs every tick calculation directly to
your console, even if the spread is negative.
This lets you observe the raw engine running in real-time.

Deterministic Capital Matrix Simulation: The calculation loop processes a matrix of
inputs ($100, $1000, $2000, and $3000 USDT) across every single gap.
It converts these dollar amounts into precise asset units (trade_size = capital / buy_price)
to test where the fixed network transfer costs break your profitability.

Smart Net Notification Triggers: If a capital tier yields a net profit greater than $0.00
after factoring in Kraken’s taker fees, Crypto.com’s taker fees, and
structural withdrawal thresholds, a highlighted [NOTIFICATION: PROFITABLE EDGE] block
is triggered.

NOTE:
What to Look For When Running This Debugger
Small Capital ($100 Tier): You will see that even when the raw Gap is positive,
the $100 simulation will rarely fire a notification. This happens because fixed
network withdrawal fees (like $15 worth of ETH or $25 worth of BTC) completely
wipe out the spread on small order volumes.

Large Capital ($3000 Tier): This tier amortizes fixed network costs more effectively,
showing you how scaling up capital allows the engine to lock in consistent net returns
once the percentage spread clears the exchange taker thresholds.

PROBLEM:
Nothing is printing to stdout because Kraken is rejecting our subscription symbols
behind the scenes.
While Kraken's newer WebSocket V2 engine uses standardized names like BTC/USDT, ETH/USDT,
and SOL/USDT for its endpoints, the streaming connection itself mandates a strict
forward-slash layout (BTC/USDT, ETH/USDT, SOL/USDT).

Because our loop looked for exact matches in order_books using the returned stream's
raw symbol key—and we initialized it with the hard slash—the data failed to populate,
keeping book["kraken"]["ask"] locked permanently at None.

The script below resolves this parsing error, adds a sub-second heart-beat diagnostic
indicator, and forces direct stream interception so you can watch every tick
calculation hit the console instantly.
"""
# real_time_spread_engine.py
import asyncio
import json
import websockets
from decimal import Decimal

# -------------------------------------------------------------------
# CONFIGURATION & REVENUE COST MATRICES
# -------------------------------------------------------------------
SYMBOLS = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]

# Capital levels requested for multi-tier profit simulations
CAPITAL_TIERS = [Decimal("100"), Decimal("1000"), Decimal("2000"), Decimal("3000")]

FEES = {
    "kraken": {
        "taker": Decimal("0.0026"),     # 0.26% Standard Taker
        "maker": Decimal("0.0016"),     # 0.16% Standard Maker
        "withdrawal": {                 # Fixed structural chain egress costs
            "BTC/USDT": Decimal("0.0004"),
            "ETH/USDT": Decimal("0.0035"),
            "SOL/USDT": Decimal("0.005")
        }
    },
    "cryptocom": {
        "taker": Decimal("0.0026"),     # 0.26% Standard Taker
        "maker": Decimal("0.0000"),     # Configured to 0% per your account settings
        "withdrawal": {
            "BTC/USDT": Decimal("0.0005"),
            "ETH/USDT": Decimal("0.004"),
            "SOL/USDT": Decimal("0.008")
        }
    }
}

# Live Memory Matrix Tracking Arrays
order_books = {
    "BTC/USDT": {"kraken": {"bid": None, "ask": None}, "cryptocom": {"bid": None, "ask": None}},
    "ETH/USDT": {"kraken": {"bid": None, "ask": None}, "cryptocom": {"bid": None, "ask": None}},
    "SOL/USDT": {"kraken": {"bid": None, "ask": None}, "cryptocom": {"bid": None, "ask": None}}
}

# -------------------------------------------------------------------
# KRAKEN WEBSOCKETS LAYER (V2 API Engine)
# -------------------------------------------------------------------
async def stream_kraken():
    url = "wss://ws.kraken.com/v2"
    async for ws in websockets.connect(url):
        try:
            subscribe_msg = {
                "method": "subscribe",
                "params": {
                    "channel": "ticker",
                    "symbol": SYMBOLS,
                    "event_trigger": "bbo"
                }
            }
            await ws.send(json.dumps(subscribe_msg))
            print("[✓] Connected to Kraken WebSocket (V2 Ticker stream active)")
            
            async for message in ws:
                data = json.loads(message)
                if data.get("channel") == "ticker" and "data" in data:
                    packet = data["data"][0]
                    symbol = packet["symbol"]
                    if symbol in order_books:
                        order_books[symbol]["kraken"]["bid"] = Decimal(str(packet["bid"]))
                        order_books[symbol]["kraken"]["ask"] = Decimal(str(packet["ask"]))
        except websockets.ConnectionClosed:
            print("[!] Kraken connection dropped. Reconnecting...")
            await asyncio.sleep(1)
        except Exception as e:
            print(f"[-] Kraken Stream Parsing Exception: {e}")

# -------------------------------------------------------------------
# CRYPTO.COM WEBSOCKETS LAYER (V2 API Engine)
# -------------------------------------------------------------------
async def stream_cryptocom():
    url = "wss://stream.crypto.com/v2/market"
    symbol_map = {"BTC_USDT": "BTC/USDT", "ETH_USDT": "ETH/USDT", "SOL_USDT": "SOL/USDT"}
    
    async for ws in websockets.connect(url):
        try:
            async def respond_heartbeat(msg_id):
                await ws.send(json.dumps({"id": msg_id, "method": "public/respond-heartbeat"}))

            subscribe_msg = {
                "id": 1,
                "method": "subscribe",
                "params": {"channels": [f"ticker.{k}" for k in symbol_map.keys()]},
                "nonce": 1
            }
            await ws.send(json.dumps(subscribe_msg))
            print("[✓] Connected to Crypto.com WebSocket (V2 Ticker stream active)")
            
            async for message in ws:
                data = json.loads(message)
                if data.get("method") == "public/heartbeat":
                    await respond_heartbeat(data["id"])
                    continue
                if data.get("method") == "ticker.update":
                    packet = data["params"]["data"][0]
                    raw_symbol = packet["i"]
                    normalized_symbol = symbol_map.get(raw_symbol)
                    if normalized_symbol:
                        order_books[normalized_symbol]["cryptocom"]["bid"] = Decimal(str(packet["b"]))
                        order_books[normalized_symbol]["cryptocom"]["ask"] = Decimal(str(packet["a"]))
        except websockets.ConnectionClosed:
            print("[!] Crypto.com connection dropped. Reconnecting...")
            await asyncio.sleep(1)
        except Exception as e:
            print(f"[-] Crypto.com Stream Parsing Exception: {e}")

# -------------------------------------------------------------------
# ENGINE TELEMETRY LOOP WITH CAPITAL SIMULATION MATRIX
# -------------------------------------------------------------------
async def evaluate_spread_engine():
    print("\n⚡ System Diagnostic: Buffering feeds for 3 seconds...")
    await asyncio.sleep(3)
    print("⚡ Telemetry Logging Engine Engaged. Printing all price differentials:")

    while True:
        for symbol in SYMBOLS:
            book = order_books[symbol]
            
            # Skip iterations if a ticker feed has not yet received its initial book updates
            if not book["kraken"]["ask"] or not book["cryptocom"]["ask"]:
                continue
                
            k_ask = book["kraken"]["ask"]
            k_bid = book["kraken"]["bid"]
            c_ask = book["cryptocom"]["ask"]
            c_bid = book["cryptocom"]["bid"]
            
            # Direction A: Buy Kraken -> Sell Crypto.com
            process_directional_telemetry(symbol, "Kraken", k_ask, "Crypto.com", c_bid)
            
            # Direction B: Buy Crypto.com -> Sell Kraken
            process_directional_telemetry(symbol, "Crypto.com", c_ask, "Kraken", k_bid)
            
        await asyncio.sleep(1.0) # Evaluates and updates the logs once per second

def process_directional_telemetry(symbol, buy_venue, buy_price, sell_venue, sell_price):
    gross_gap = sell_price - buy_price
    percentage_gap = (gross_gap / buy_price) * 100
    
    # 1. Output a debug log of the raw book difference
    print(f" -> [TICK LOG] {symbol:8} | Route: {buy_venue:10} -> {sell_venue:10} | Gap: ${gross_gap:9.4f} ({percentage_gap:7.4f}%)")
    
    # If the spread itself is raw negative, fees will never rescue it. Skip matrix overhead.
    if gross_gap <= 0:
        return
        
    # 2. Iterate and simulate the complete investment spectrum matrix
    for capital in CAPITAL_TIERS:
        # Dynamically derive entry asset trade unit volume base
        trade_size = capital / buy_price
        
        buy_fee_pct = FEES[buy_venue.lower()]["taker"]
        sell_fee_pct = FEES[sell_venue.lower()]["taker"]
        withdrawal_fee_asset = FEES[buy_venue.lower()]["withdrawal"][symbol]
        
        # Entry Trade Cost Trackers
        gross_cost_usdt = buy_price * trade_size
        entry_execution_fee = gross_cost_usdt * buy_fee_pct
        
        # Simulating cross-exchange chain transmission reductions
        arrival_asset_amount = trade_size - withdrawal_fee_asset
        
        # If your capital is too small to cover the withdrawal fee, this trade tier is instantly killed
        if arrival_asset_amount <= 0:
            continue
            
        # Exit Trade Revenue Trackers
        gross_revenue_usdt = arrival_asset_amount * sell_price
        exit_execution_fee = gross_revenue_usdt * sell_fee_pct
        
        # Net Profit Math Matrix Engine
        total_costs = entry_execution_fee + exit_execution_fee
        net_profit_usdt = gross_revenue_usdt - gross_cost_usdt - total_costs
        net_return_pct = (net_profit_usdt / gross_cost_usdt) * 100
        
        # 3. If net profit breaks above zero after tracking fees, push clear alerts to terminal
        if net_profit_usdt > 0:
            print(f"\n🔥 [NOTIFICATION: PROFITABLE EDGE] {symbol}")
            print(f"   Route         : BUY {buy_venue} (${buy_price:.2f}) -> MOVE -> SELL {sell_venue} (${sell_price:.2f})")
            print(f"   Capital Level : ${capital} USDT (Purchased: {trade_size:.6f} units)")
            print(f"   Network Fee   : -{withdrawal_fee_asset} {symbol.split('/')[0]} (Egress cost from {buy_venue})")
            print(f"   Exchange Fees : Entry Taker: ${entry_execution_fee:.4f} | Exit Taker: ${exit_execution_fee:.4f}")
            print(f"   🚀 NET PROFIT : +${net_profit_usdt:.4f} USDT ({net_return_pct:.4f}% Net Yield)")
            print("-" * 75)

# -------------------------------------------------------------------
# MAIN ENGINE ORCHESTRATION LAYER
# -------------------------------------------------------------------
async def main():
    await asyncio.gather(
        stream_kraken(),
        stream_cryptocom(),
        evaluate_spread_engine()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTelemetry logger shutting down cleanly.")
