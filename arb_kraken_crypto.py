# real_time_spread_engine.py
import asyncio
import json
import websockets
from decimal import Decimal

# -------------------------------------------------------------------
# CONFIGURATION & SIMULATED COST MATRICES
# -------------------------------------------------------------------
# Target Pairs mapped uniformly
SYMBOLS = ["BTC/USDC", "ETH/USDC", "SOL/USDC"]

# Fee structures (Adjust these based on your exchange volume tiers)
FEES = {
    "kraken": {
        "taker": Decimal("0.0026"),     # 0.26% Taker Fee
        "maker": Decimal("0.0016"),     # 0.16% Maker Fee
        "withdrawal": {                 # Fixed network egress costs
            "BTC/USDC": Decimal("0.0004"),
            "ETH/USDC": Decimal("0.0035"),
            "SOL/USDC": Decimal("0.005")
        }
    },
    "cryptocom": {
        "taker": Decimal("0.0026"),     # 0.26% Taker Fee
        "maker": Decimal("0.0016"),     # 0.16% Maker Fee
        "withdrawal": {
            "BTC/USDC": Decimal("0.0005"),
            "ETH/USDC": Decimal("0.004"),
            "SOL/USDC": Decimal("0.008")
        }
    }
}

# Live Global Order Book Memory Matrix
order_books = {
    "BTC/USDC": {"kraken": {"bid": None, "ask": None}, "cryptocom": {"bid": None, "ask": None}},
    "ETH/USDC": {"kraken": {"bid": None, "ask": None}, "cryptocom": {"bid": None, "ask": None}},
    "SOL/USDC": {"kraken": {"bid": None, "ask": None}, "cryptocom": {"bid": None, "ask": None}}
}

# -------------------------------------------------------------------
# KRAKEN WEBSOCKETS LAYER (V2 Engine)
# -------------------------------------------------------------------
async def stream_kraken():
    # Kraken V2 uses clean standardized ticker routing layouts
    url = "wss://ws.kraken.com/v2" #
    
    # Mapping normalized tracking keys to Kraken-specific pair variations
    symbol_map = {"BTC/USDC": "BTC/USDC", "ETH/USDC": "ETH/USDC", "SOL/USDC": "SOL/USDC"}
    
    async for ws in websockets.connect(url):
        try:
            # Subscribe to the ticker channel for target pairs
            subscribe_msg = {
                "method": "subscribe",
                "params": {
                    "channel": "ticker",
                    "symbol": list(symbol_map.keys())
                }
            }
            await ws.send(json.dumps(subscribe_msg))
            
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
            continue

# -------------------------------------------------------------------
# CRYPTO.COM WEBSOCKETS LAYER
# -------------------------------------------------------------------
async def stream_cryptocom():
    url = "wss://stream.crypto.com/v2/market"
    
    # Crypto.com uses underscores for instrument keys internally
    symbol_map = {"BTC_USDC": "BTC/USDC", "ETH_USDC": "ETH/USDC", "SOL_USDC": "SOL/USDC"}
    
    async for ws in websockets.connect(url):
        try:
            # Heartbeat handler required by Crypto.com's gateway to avoid termination
            async def respond_heartbeat(msg_id):
                await ws.send(json.dumps({"id": msg_id, "method": "public/respond-heartbeat"}))

            subscribe_msg = {
                "id": 1,
                "method": "subscribe",
                "params": {
                    "channels": [f"ticker.{k}" for k in symbol_map.keys()]
                },
                "nonce": 1
            }
            await ws.send(json.dumps(subscribe_msg))
            
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
                        order_books[normalized_symbol]["cryptocom"]["ask"] = Decimal(str(packet["k"]))
        except websockets.ConnectionClosed:
            print("[!] Crypto.com connection dropped. Reconnecting...")
            continue

# -------------------------------------------------------------------
# REAL-TIME SPREAD EVALUATION ENGINE (Event-Driven Loop)
# -------------------------------------------------------------------
async def evaluate_spread_engine():
    print("\n⚡ Real-Time Arbitrage Spread Engine Live... Listening for book fills.")
    await asyncio.sleep(4) # Allow buffers to fill
    
    # Static test size parameter (Change to scale risk profile modeling)
    test_trade_sizes = {
        "BTC/USDC": Decimal("0.01"),
        "ETH/USDC": Decimal("0.15"),
        "SOL/USDC": Decimal("2.0")
    }

    while True:
        for symbol in SYMBOLS:
            book = order_books[symbol]
            size = test_trade_sizes[symbol]
            
            if not book["kraken"]["bid"] or not book["cryptocom"]["bid"]:
                continue
                
            # Direct Scenario A: Buy Kraken -> Sell Crypto.com
            # To execute instantly, buy at Ask, sell at Bid
            ask_k = book["kraken"]["ask"]
            bid_c = book["cryptocom"]["bid"]
            
            # Direct Scenario B: Buy Crypto.com -> Sell Kraken
            ask_c = book["cryptocom"]["ask"]
            bid_k = book["kraken"]["bid"]
            
            # Run calculations for both directional paths
            calculate_arbitrage_math(symbol, "Kraken", ask_k, "Crypto.com", bid_c, size)
            calculate_arbitrage_math(symbol, "Crypto.com", ask_c, "Kraken", bid_k, size)
            
        await asyncio.sleep(0.5) # Fast 500ms ticker scanning frequency

def calculate_arbitrage_math(symbol, buy_venue, buy_price, sell_venue, sell_price, trade_size):
    # Gross Delta Raw Edge
    gross_spread = sell_price - buy_price
    if gross_spread <= 0:
        return # Skip if negative or dead spread
        
    # Execution costs calculated down to the decimal
    buy_fee_pct = FEES[buy_venue.lower()]["taker"]
    sell_fee_pct = FEES[sell_venue.lower()]["taker"]
    withdrawal_fee_asset = FEES[buy_venue.lower()]["withdrawal"][symbol]
    
    # Cash Capital required on entry leg
    gross_cost_usdc = buy_price * trade_size
    entry_execution_fee = gross_cost_usdc * buy_fee_pct
    
    # Sell Leg Egress Calculations
    arrival_asset_amount = trade_size - withdrawal_fee_asset
    gross_revenue_usdc = arrival_asset_amount * sell_price
    exit_execution_fee = gross_revenue_usdc * sell_fee_pct
    
    # Net yield final summary after all fees
    net_profit_usdc = gross_revenue_usdc - gross_cost_usdc - entry_execution_fee - exit_execution_fee
    net_percentage = (net_profit_usdc / gross_cost_usdc) * 100
    
    if net_profit_usdc > 0:
        print(f"\n🔥 [OPPORTUNITY FOUND] {symbol} | Size: {trade_size}")
        print(f"   Route      : BUY on {buy_venue} (${buy_price}) → SELL on {sell_venue} (${sell_price})")
        print(f"   Gross Delta: ${gross_spread:.4f}")
        print(f"   Fees Deducted: Entry Taker: ${entry_execution_fee:.4f} | Exit Taker: ${exit_execution_fee:.4f}")
        print(f"   Network Cost : Withdrawal fee paid on {buy_venue}: {withdrawal_fee_asset} {symbol.split('/')[0]}")
        print(f"   🚀 NET PROFIT: +${net_profit_usdc:.2f} USDC ({net_percentage:.4f}%)")
        print("-" * 65)

# -------------------------------------------------------------------
# MAIN RUNNER ASYNC COROUTINE ORCHESTRATION
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
        print("\nStopping real-time feed engines.")
