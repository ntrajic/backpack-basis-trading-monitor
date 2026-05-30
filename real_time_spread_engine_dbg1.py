# real_time_spread_engine_dbg1.py
import asyncio
import json
import websockets
from decimal import Decimal

# -------------------------------------------------------------------
# CONFIGURATION & REVENUE COST MATRICES
# -------------------------------------------------------------------
SYMBOLS = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
CAPITAL_TIERS = [Decimal("100"), Decimal("1000"), Decimal("2000"), Decimal("3000")]

FEES = {
    "kraken": {
        "taker": Decimal("0.0026"),
        "maker": Decimal("0.0016"),
        "withdrawal": {
            "BTC/USDT": Decimal("0.0004"),
            "ETH/USDT": Decimal("0.0035"),
            "SOL/USDT": Decimal("0.005")
        }
    },
    "cryptocom": {
        "taker": Decimal("0.0026"),
        "maker": Decimal("0.0000"),  # Configured to 0% per your account settings
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
            print("[✓] Kraken Subscription Sent for: ", SYMBOLS)
            
            async for message in ws:
                data = json.loads(message)
                if data.get("channel") == "ticker" and "data" in data:
                    packet = data["data"][0]
                    symbol = packet["symbol"] # Always returns clean 'BTC/USDT' format
                    
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
            print("[✓] Crypto.com Subscription Sent for: ", list(symbol_map.keys()))
            
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
    print("\n⚡ System Diagnostic: Buffering streams. Waiting for data allocations...")
    
    # Simple console visualization mechanism to confirm loop alive state
    dots = 0
    while True:
        ready = True
        for symbol in SYMBOLS:
            if not order_books[symbol]["kraken"]["ask"] or not order_books[symbol]["cryptocom"]["ask"]:
                ready = False
                
        if not ready:
            dots = (dots + 1) % 4
            print(f"\r⌛ Waiting for initial orderbook updates from both exchanges{'.' * dots}   ", end="", flush=True)
            await asyncio.sleep(0.5)
            continue
            
        break

    print("\n⚡ Telemetry Logging Engine Engaged. Printing all price differentials:\n")

    while True:
        for symbol in SYMBOLS:
            book = order_books[symbol]
            
            k_ask = book["kraken"]["ask"]
            k_bid = book["kraken"]["bid"]
            c_ask = book["cryptocom"]["ask"]
            c_bid = book["cryptocom"]["bid"]
            
            # Direction 1: Buy Kraken (Ask) -> Sell Crypto.com (Bid)
            process_directional_telemetry(symbol, "Kraken", k_ask, "Crypto.com", c_bid)
            
            # Direction 2: Buy Crypto.com (Ask) -> Sell Kraken (Bid)
            process_directional_telemetry(symbol, "Crypto.com", c_ask, "Kraken", k_bid)
            
        await asyncio.sleep(1.0)

def process_directional_telemetry(symbol, buy_venue, buy_price, sell_venue, sell_price):
    gross_gap = sell_price - buy_price
    percentage_gap = (gross_gap / buy_price) * 100
    
    # 1. Continuous Live Gap Logging output
    print(f" -> [TICK LOG] {symbol:8} | Route: {buy_venue:10} -> {sell_venue:10} | Gap: ${gross_gap:9.4f} ({percentage_gap:7.4f}%)")
    
    # Skip calculations if the raw gap is flat or upside down
    if gross_gap <= 0:
        return
        
    # 2. Iterate and simulate the complete investment matrix
    for capital in CAPITAL_TIERS:
        trade_size = capital / buy_price
        
        buy_fee_pct = FEES[buy_venue.lower()]["taker"]
        sell_fee_pct = FEES[sell_venue.lower()]["taker"]
        withdrawal_fee_asset = FEES[buy_venue.lower()]["withdrawal"][symbol]
        
        # Entry Costs
        gross_cost_usdt = buy_price * trade_size
        entry_execution_fee = gross_cost_usdt * buy_fee_pct
        
        # Cross-Exchange Transfer Deductions
        arrival_asset_amount = trade_size - withdrawal_fee_asset
        if arrival_asset_amount <= 0:
            continue  # Withdrawal fee exceeds purchase amount
            
        # Exit Costs
        gross_revenue_usdt = arrival_asset_amount * sell_price
        exit_execution_fee = gross_revenue_usdt * sell_fee_pct
        
        # Net Profit Math
        total_costs = entry_execution_fee + exit_execution_fee
        net_profit_usdt = gross_revenue_usdt - gross_cost_usdt - total_costs
        net_return_pct = (net_profit_usdt / gross_cost_usdt) * 100
        
        # 3. Notification Trigger when fees do not eat the spread
        if net_profit_usdt > 0:
            print(f"\n🔥 [NOTIFICATION: PROFITABLE EDGE] {symbol}")
            print(f"   Route         : BUY {buy_venue} (${buy_price:.2f}) -> MOVE -> SELL {sell_venue} (${sell_price:.2f})")
            print(f"   Capital Level : ${capital} USDT (Purchased: {trade_size:.6f} units)")
            print(f"   Network Fee   : -{withdrawal_fee_asset} {symbol.split('/')[0]} (Egress from {buy_venue})")
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
