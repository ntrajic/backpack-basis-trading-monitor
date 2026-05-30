import os
import time
import hmac
import base64
import hashlib
import json
import csv
import logging
import asyncio
from decimal import Decimal
from pathlib import Path
from typing import Dict, Tuple, Optional
import websockets
import httpx
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# ==========================================
# ENV SECURITY CORE RESOLUTION
# ==========================================
KRAKEN_API_KEY = os.getenv("KRAKEN_API_KEY")
KRAKEN_SECRET = os.getenv("KRAKEN_SECRET")
CRYPTOCOM_API_KEY = os.getenv("CRYPTOCOM_API_KEY")
CRYPTOCOM_SECRET = os.getenv("CRYPTOCOM_SECRET")


SPREAD_THRESHOLD = Decimal(os.getenv("SPREAD", "150.0"))

# Baseline safe transaction assumptions
FEE_BPS_KRAKEN = Decimal("26")      
FEE_BPS_CRYPTOCOM = Decimal("26")    

WITHDRAW_FEES = {
    "BTCUSDC": Decimal("0.0004"),
    "ETHUSDC": Decimal("0.005"),
    "SOLUSDC": Decimal("0.005"),
}

# ==========================================
# STRICT CRYPTO/USDC NETWORK MATRIX
# ==========================================
SYMBOLS = {
    "BTCUSDC": {
        "kraken_pair": "XBTUSDC",  # Adjusted to native USDC pairs
        "cryptocom_pair": "BTC_USDC",
        "asset": "BTC",
        "network": "BTC",
        "kraken_wallet": os.getenv("KRAKEN_WALLET_BTC"),
        "cryptocom_wallet": os.getenv("CRYPTOCOM_WALLET_BTC")
    },
    "ETHUSDC": {
        "kraken_pair": "ETHUSDC",
        "cryptocom_pair": "ETH_USDC",
        "asset": "ETH",
        "network": "ETH",
        "kraken_wallet": os.getenv("KRAKEN_WALLET_ETH"),
        "cryptocom_wallet": os.getenv("CRYPTOCOM_WALLET_ETH")
    },
    "SOLUSDC": {
        "kraken_pair": "SOLUSDC",
        "cryptocom_pair": "SOL_USDC",
        "asset": "SOL",
        "network": "SOL",
        "kraken_wallet": os.getenv("KRAKEN_WALLET_SOL"),
        "cryptocom_wallet": os.getenv("CRYPTOCOM_WALLET_SOL")
    },
}

state: Dict[str, Dict[str, Decimal]] = {"kraken": {}, "cryptocom": {}}
LOG_PATH = Path("arbitrage_execution_audit.csv")

if not LOG_PATH.exists():
    with LOG_PATH.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp", "symbol", "kraken_price", "cryptocom_price", 
            "raw_spread", "cheaper_venue", "expensive_venue", "effective_net_spread", "status"
        ])

# ==========================================
# REST PRIVATE UTILITIES: KRAKEN
# ==========================================
def sign_kraken(path: str, data: Dict[str, str]) -> Dict[str, str]:
    if not KRAKEN_API_KEY or not KRAKEN_SECRET:
        raise ValueError("Missing Kraken keys in local runtime footprint.")
    nonce = data["nonce"]
    postdata = data["nonce"] + "&".join(f"{k}={v}" for k, v in data.items() if k != "nonce")
    encoded = (nonce + postdata).encode()
    message = path.encode() + hashlib.sha256(encoded).digest()
    mac = hmac.new(base64.b64decode(KRAKEN_SECRET), message, hashlib.sha512)
    return {
        "API-Key": KRAKEN_API_KEY,
        "API-Sign": base64.b64encode(mac.digest()).decode(),
        "Content-Type": "application/x-www-form-urlencoded"
    }

async def kraken_rest_call(endpoint: str, data: Dict[str, str]) -> dict:
    path = f"/0/private/{endpoint}"
    url = f"https://api.kraken.com{path}"
    data["nonce"] = str(int(time.time() * 1000000))
    headers = sign_kraken(path, data)
    async with httpx.AsyncClient() as client:
        r = await client.post(url, data=data, headers=headers, timeout=10)
        res = r.json()
        if res.get("error"):
            logging.error(f"Kraken REST Node Error: {res['error']}")
        return res

# ==========================================
# REST PRIVATE UTILITIES: CRYPTO.COM
# ==========================================
def sign_cryptocom(method: str, params: Dict) -> Dict:
    if not CRYPTOCOM_API_KEY or not CRYPTOCOM_SECRET:
        raise ValueError("Missing Crypto.com keys in local runtime footprint.")
    nonce = str(int(time.time() * 1000))
    payload = {
        "id": int(nonce),
        "method": method,
        "api_key": CRYPTOCOM_API_KEY,
        "params": params,
        "nonce": nonce
    }
    param_str = ""
    if "params" in payload and payload["params"]:
        for k in sorted(payload["params"].keys()):
            param_str += f"{k}{payload['params'][k]}"
    sig_payload = f"{method}{payload['id']}{CRYPTOCOM_API_KEY}{nonce}{param_str}"
    payload["sig"] = hmac.new(
        CRYPTOCOM_SECRET.encode(),
        sig_payload.encode(),
        hashlib.sha256
    ).hexdigest()
    return payload

async def cryptocom_rest_call(method: str, params: Dict) -> dict:
    url = "https://api.crypto.com/exchange/v1/private"
    payload = sign_cryptocom(method, params)
    async with httpx.AsyncClient() as client:
        r = await client.post(url, json=payload, timeout=10)
        res = r.json()
        if res.get("code") != 0:
            logging.error(f"Crypto.com REST Node Error: {res.get('message')}")
        return res

# ==========================================
# LIQUIDITY AND BALANCES SCANNER
# ==========================================
async def get_kraken_stable_balance() -> Decimal:
    """Queries Kraken account balance and returns aggregate spending power across USD/USDC."""
    res = await kraken_rest_call("Balance", {})
    if not res or "result" not in res:
        return Decimal("0")
    balances = res["result"]
    # Handle variations in naming paradigms across account tiers
    usd = Decimal(balances.get("ZUSD", balances.get("USD", "0")))
    usdc = Decimal(balances.get("USDC", "0"))
    return usd + usdc

async def get_cryptocom_stable_balance() -> Decimal:
    """Queries Crypto.com account balance and returns aggregate spending power across USD/USDC."""
    res = await cryptocom_rest_call("private/user-balance", {})
    if not res or "result" not in res or "data" not in res["result"]:
        return Decimal("0")
    
    total_stable = Decimal("0")
    for position in res["result"]["data"][0].get("position_balances", []):
        if position.get("currency") in ["USD", "USDC"]:
            total_stable += Decimal(str(position.get("quantity", "0")))
    return total_stable

# ==========================================
# ASSET-ISOLATED SIMULTANEOUS EXECUTION
# ==========================================
async def execute_simultaneous_arbitrage(symbol: str, cheap: str, expensive: str, entry_price: Decimal):
    k_pair = SYMBOLS[symbol]["kraken_pair"]
    c_pair = SYMBOLS[symbol]["cryptocom_pair"]
    target_asset = SYMBOLS[symbol]["asset"]
    network_id = SYMBOLS[symbol]["network"]
    k_wallet_key = SYMBOLS[symbol]["kraken_wallet"]
    c_wallet_address = SYMBOLS[symbol]["cryptocom_wallet"]

    # Dynamic Capital Verification Layer
    if cheap == "kraken":
        stable_liquidity = await get_kraken_stable_balance()
    else:
        stable_liquidity = await get_cryptocom_stable_balance()

    if stable_liquidity <= 0:
        logging.warning(f"Execution Aborted for {symbol}: Cheaper exchange [{cheap.upper()}] has zero liquid spending power.")
        return

    # Calculate safe maximum purchasing volume based on balance constraints (leaving a 1% cushion for fees)
    trade_size = (stable_liquidity * Decimal("0.99")) / entry_price
    
    # Floor sanity checks for micro sizes
    if trade_size <= Decimal("0.0001"):
        logging.warning(f"Execution Aborted: Calculated volume size {trade_size} is too small for order book guidelines.")
        return

    logging.warning(f"🚨 ORDER TRIGGER DISPATCHED FOR {symbol} | Allocated Size: {trade_size:.6f}")

    # Concurrent Order Execution to mitigate execution lag
    if cheap == "kraken":
        logging.info(f"[ORDER] Market Buy Kraken {trade_size} {k_pair} | Market Sell Crypto.com {trade_size} {c_pair}")
        t1 = kraken_rest_call("AddOrder", {"ordertype": "market", "type": "buy", "volume": str(trade_size), "pair": k_pair})
        t2 = cryptocom_rest_call("private/create-order", {"instrument_name": c_pair, "side": "SELL", "type": "MARKET", "quantity": str(trade_size)})
        await asyncio.gather(t1, t2, return_exceptions=True)

        if not k_wallet_key:
            logging.error(f"Kraken routing failure: KRAKEN_WALLET_{target_asset} environment target missing.")
            return
            
        withdraw_amount = trade_size - WITHDRAW_FEES[symbol]
        logging.info(f"[ON-CHAIN ROUTE] Routing {withdraw_amount} {target_asset} from Kraken using whitelist key: [{k_wallet_key}]")
        await kraken_rest_call("Withdraw", {"asset": target_asset, "key": k_wallet_key, "amount": str(withdraw_amount)})

    else:
        logging.info(f"[ORDER] Market Buy Crypto.com {trade_size} {c_pair} | Market Sell Kraken {trade_size} {k_pair}")
        t1 = cryptocom_rest_call("private/create-order", {"instrument_name": c_pair, "side": "BUY", "type": "MARKET", "quantity": str(trade_size)})
        t2 = kraken_rest_call("AddOrder", {"ordertype": "market", "type": "sell", "volume": str(trade_size), "pair": k_pair})
        await asyncio.gather(t1, t2, return_exceptions=True)

        if not c_wallet_address:
            logging.error(f"Crypto.com routing failure: CRYPTOCOM_WALLET_{target_asset} environment target missing.")
            return

        withdraw_amount = trade_size - WITHDRAW_FEES[symbol]
        logging.info(f"[ON-CHAIN ROUTE] Routing {withdraw_amount} {target_asset} from Crypto.com to address: [{c_wallet_address}] via {network_id}")
        await cryptocom_rest_call("private/create-withdrawal", {"currency": target_asset, "amount": str(withdraw_amount), "address": c_wallet_address, "network_id": network_id})

# ==========================================
# PUBLIC WEBSOCKET TELEMETRY CHANNELS
# ==========================================
async def stream_kraken():
    url = "wss://ws.kraken.com"
    pairs = [cfg["kraken_pair"] for cfg in SYMBOLS.values()]
    payload = {"event": "subscribe", "pair": pairs, "subscription": {"name": "ticker"}}
    async for ws in websockets.connect(url):
        try:
            await ws.send(json.dumps(payload))
            async for msg in ws:
                data = json.loads(msg)
                if isinstance(data, list) and len(data) >= 4:
                    pair_raw = data[-1]
                    ticker = data[1]
                    mid = (Decimal(str(ticker["b"][0])) + Decimal(str(ticker["a"][0]))) / Decimal("2")
                    for sym, cfg in SYMBOLS.items():
                        if cfg["kraken_pair"] == pair_raw:
                            state["kraken"][sym] = mid
        except Exception:
            await asyncio.sleep(2)

async def stream_cryptocom():
    url = "wss://stream.crypto.com/v2/market"
    channels = [f"ticker.{cfg['cryptocom_pair']}" for cfg in SYMBOLS.values()]
    payload = {"id": 1, "method": "subscribe", "params": {"channels": channels}, "nonce": 1}
    async for ws in websockets.connect(url):
        try:
            await ws.send(json.dumps(payload))
            async for msg in ws:
                data = json.loads(msg)
                if data.get("method") == "ticker.update":
                    for packet in data["params"]["data"]:
                        inst = packet["i"]
                        mid = (Decimal(str(packet["b"])) + Decimal(str(packet["k"]))) / Decimal("2")
                        for sym, cfg in SYMBOLS.items():
                            if cfg["cryptocom_pair"] == inst:
                                state["cryptocom"][sym] = mid
        except Exception:
            await asyncio.sleep(2)

# ==========================================
# RUNTIME ENGINE & MONITORING INTERFACE
# ==========================================
async def core_processing_loop():
    await asyncio.sleep(4) 
    while True:
        print("\033[H\033[J", end="") 
        print("=========================================================================")
        print(f" DYNAMIC CAPITAL SCALE ENGINE (USDC) | ACTIVE TARGET SPREAD: ${SPREAD_THRESHOLD}")
        print("=========================================================================")
        
        for sym in SYMBOLS.keys():
            k_mid = state["kraken"].get(sym)
            c_mid = state["cryptocom"].get(sym)
            
            if k_mid is None or c_mid is None:
                print(f" [📡] {sym} -> Compiling real-time data streams...")
                continue
                
            raw_diff = k_mid - c_mid
            abs_spread = abs(raw_diff)
            cheap = "kraken" if k_mid < c_mid else "cryptocom"
            expensive = "cryptocom" if cheap == "kraken" else "kraken"
            entry_price = k_mid if cheap == "kraken" else c_mid
            
            # Use unit size for dashboard projection references
            sim_size = get_symbol_size(sym)
            notional = entry_price * sim_size
            k_fees = notional * (FEE_BPS_KRAKEN / Decimal("10000"))
            c_fees = notional * (FEE_BPS_CRYPTOCOM / Decimal("10000"))
            w_fees = WITHDRAW_FEES[sym] * entry_price
            
            net_effective_spread = (abs_spread * sim_size) - (k_fees + c_fees + w_fees)
            action_status = "SCANNING"
            
            if abs_spread >= SPREAD_THRESHOLD and net_effective_spread > 0:
                action_status = "TRIGGERED"
                await execute_simultaneous_arbitrage(sym, cheap, expensive, entry_price)
                
            print(f" [{action_status}] {sym}:")
            print(f"    ├─ Kraken Mid: ${k_mid:.2f}  | Whitelist Target: {SYMBOLS[sym]['kraken_wallet']}")
            print(f"    ├─ Crypto Mid: ${c_mid:.2f}  | Whitelist Target: {SYMBOLS[sym]['cryptocom_wallet']}")
            print(f"    └─ Projection per unit: ${net_effective_spread:.4f} USDC")
            print("-------------------------------------------------------------------------")
            
            with LOG_PATH.open("a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    int(time.time()), sym, float(k_mid), float(c_mid), 
                    float(abs_spread), cheap, expensive, float(net_effective_spread), action_status
                ])
                
        await asyncio.sleep(1)

async def main():
    await asyncio.gather(stream_kraken(), stream_cryptocom(), core_processing_loop())

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Terminating capital sizing monitoring matrix.")
