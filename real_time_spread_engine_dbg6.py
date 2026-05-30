# real_time_spread_engine_dbg6.py (msft)
#
# EXECUTION ADVISORY & ORDER COORDINATION LAYER — FULLY WIRED
# - Real HMAC signing for Kraken & Crypto.com
# - Real HTTP requests using aiohttp
# - Fill-polling state machine with timeout / imbalance handling
# - Real withdrawal API calls
# - Live balance pre-flight check for pre-positioned float
#
# READ THIS BEFORE ENABLING semi_auto / full_auto:
# - Test in EXECUTION_MODE="advisory" first.
# - Start with tiny sizes.
# - Never use funds you cannot afford to lose.

import asyncio
import base64
import hashlib
import hmac
import json
import os
import time
from decimal import Decimal, ROUND_DOWN
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, Any

import aiohttp

# ───────────────────────────────────────────────────────────────────
# EXECUTION CONFIGURATION
# ───────────────────────────────────────────────────────────────────

EXECUTION_MODE = os.getenv("EXECUTION_MODE", "advisory")  # "advisory" | "semi_auto" | "full_auto"

CAPITAL        = Decimal("1000")     # USDT per cycle
MAX_CYCLES     = 20
MIN_NET_YIELD_PCT         = Decimal("0.05")    # 0.05%
MIN_ALERT_INTERVAL_SEC    = 10
SPREAD_DECAY_PER_TRANSFER = Decimal("0.30")

TARGET_FLOAT = {
    "BTC": Decimal("0.02"),
    "ETH": Decimal("0.3"),
    "SOL": Decimal("5.0"),
}

TRANSFER_MINUTES = {"BTC": 30, "ETH": 5, "SOL": 1}

FEES = {
    "kraken": {
        "taker":      Decimal("0.0026"),
        "maker":      Decimal("0.0016"),
        "withdrawal": {
            "BTC": Decimal("0.0004"),
            "ETH": Decimal("0.0035"),
            "SOL": Decimal("0.005"),
        },
    },
    "cryptocom": {
        "taker":      Decimal("0.0026"),
        "maker":      Decimal("0.0000"),
        "withdrawal": {
            "BTC": Decimal("0.0005"),
            "ETH": Decimal("0.004"),
            "SOL": Decimal("0.008"),
        },
    },
}

KRAKEN_SYMBOL_MAP = {"XBT": "BTC", "ETH": "ETH", "SOL": "SOL"}
SYMBOLS = ["BTC", "ETH", "SOL"]

order_books: dict = {
    sym: {"kraken": {"bid": None, "ask": None}, "cryptocom": {"bid": None, "ask": None}}
    for sym in SYMBOLS
}
_last_alert: dict = {}

# ───────────────────────────────────────────────────────────────────
# CYCLE DATA STRUCTURES
# ───────────────────────────────────────────────────────────────────

class StepStatus(Enum):
    PENDING    = "PENDING"
    IN_FLIGHT  = "IN_FLIGHT"
    FILLED     = "FILLED"
    FAILED     = "FAILED"
    SKIPPED    = "SKIPPED"

@dataclass
class CycleResult:
    cycle_num:      int
    capital_in:     Decimal
    buy_price:      Decimal
    sell_price:     Decimal
    trade_size:     Decimal
    arrival_amount: Decimal
    entry_fee:      Decimal
    exit_fee:       Decimal
    withdrawal_fee: Decimal
    gross_revenue:  Decimal
    net_profit:     Decimal
    net_pct:        Decimal
    capital_out:    Decimal

@dataclass
class ExecutionPlan:
    symbol:           str
    buy_venue:        str
    sell_venue:       str
    cycle:            CycleResult
    cycle_index:      int
    total_cycles:     int

    buy_order_id:     Optional[str] = None
    sell_order_id:    Optional[str] = None
    withdrawal_id:    Optional[str] = None

    buy_status:       StepStatus = StepStatus.PENDING
    sell_status:      StepStatus = StepStatus.PENDING
    withdraw_status:  StepStatus = StepStatus.PENDING

    actual_buy_price:  Optional[Decimal] = None
    actual_sell_price: Optional[Decimal] = None
    actual_fill_qty:   Optional[Decimal] = None
    realised_pnl:      Optional[Decimal] = None

# ───────────────────────────────────────────────────────────────────
# HTTP SESSION
# ───────────────────────────────────────────────────────────────────

_session: Optional[aiohttp.ClientSession] = None

async def get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession()
    return _session

# ───────────────────────────────────────────────────────────────────
# CYCLE MATH
# ───────────────────────────────────────────────────────────────────

def _compute_single_cycle(
    symbol, buy_venue, sell_venue, buy_price, sell_price, capital
) -> Optional[CycleResult]:
    bk = buy_venue.lower().replace(".", "")
    sk = sell_venue.lower().replace(".", "")
    tb = FEES[bk]["taker"]
    ts = FEES[sk]["taker"]
    wf = FEES[bk]["withdrawal"][symbol]

    trade_size    = capital / buy_price
    gross_cost    = capital
    entry_fee     = gross_cost * tb
    arrival       = trade_size - wf
    if arrival <= 0:
        return None
    gross_rev     = arrival * sell_price
    exit_fee      = gross_rev * ts
    net_profit    = gross_rev - gross_cost - entry_fee - exit_fee
    if net_profit <= 0:
        return None
    net_pct = (net_profit / gross_cost) * 100
    if net_pct < MIN_NET_YIELD_PCT:
        return None
    return CycleResult(
        cycle_num=0, capital_in=capital,
        buy_price=buy_price, sell_price=sell_price,
        trade_size=trade_size, arrival_amount=arrival,
        entry_fee=entry_fee, exit_fee=exit_fee,
        withdrawal_fee=wf, gross_revenue=gross_rev,
        net_profit=net_profit, net_pct=net_pct,
        capital_out=capital + net_profit,
    )

def project_cycles(symbol, buy_venue, sell_venue, buy_price, sell_price) -> list[CycleResult]:
    results, capital = [], CAPITAL
    initial_spread   = sell_price - buy_price
    for n in range(MAX_CYCLES):
        decay    = (1 - SPREAD_DECAY_PER_TRANSFER) ** n
        proj_sell = buy_price + initial_spread * decay
        r = _compute_single_cycle(symbol, buy_venue, sell_venue, buy_price, proj_sell, capital)
        if r is None:
            break
        r.cycle_num = n
        results.append(r)
        capital = r.capital_out
    return results

# ───────────────────────────────────────────────────────────────────
# FORMATTING / ADVISORY
# ───────────────────────────────────────────────────────────────────

def _fmt(d: Decimal, p: int = 4) -> str:
    return str(d.quantize(Decimal(10) ** -p, rounding=ROUND_DOWN))

def print_advisory(symbol: str, buy_venue: str, sell_venue: str, cycles: list[CycleResult]) -> None:
    c0 = cycles[0]
    total_pnl = sum(c.net_profit for c in cycles)
    n = len(cycles)
    tm = TRANSFER_MINUTES[symbol]
    bar = "═" * 80

    print(f"\n{bar}", flush=True)
    print(f"  🔥 PROFITABLE SPREAD  {symbol}  BUY {buy_venue} → SELL {sell_venue}", flush=True)
    print(f"  {n} viable cycle(s)  ·  ~{tm*n} min window  ·  total projected P&L: +${_fmt(total_pnl, 4)}", flush=True)
    print(bar, flush=True)

    print(f"  {'Cy':>3}  {'Capital In':>12}  {'Buy$':>10}  {'Sell$':>10}  {'Profit':>10}  {'%':>6}  {'Capital Out':>12}", flush=True)
    print(f"  {'─'*76}", flush=True)
    for c in cycles:
        print(
            f"  {c.cycle_num:>3}  ${_fmt(c.capital_in,2):>11}  "
            f"${_fmt(c.buy_price,2):>9}  ${_fmt(c.sell_price,2):>9}  "
            f"+${_fmt(c.net_profit,4):>9}  {_fmt(c.net_pct,4):>5}%  "
            f"${_fmt(c.capital_out,2):>11}",
            flush=True,
        )
    print(f"  {'─'*76}", flush=True)

    float_needed = c0.trade_size
    float_avail  = TARGET_FLOAT.get(symbol, Decimal("0"))
    float_ok     = float_avail >= float_needed

    print(f"\n  ── PRE-POSITIONING CHECK ─────────────────────────────────────────", flush=True)
    print(f"  Float needed on {sell_venue}: {_fmt(float_needed, 6)} {symbol}", flush=True)
    print(f"  Float configured           : {_fmt(float_avail, 6)} {symbol}  {'✅ OK' if float_ok else '❌ INSUFFICIENT — top up before trading'}", flush=True)
    if not float_ok:
        shortage = float_needed - float_avail
        print(f"  ⚠  Shortage: {_fmt(shortage, 6)} {symbol}  — cannot execute sell leg without topping up float first.", flush=True)

    for c in cycles:
        buy_sym_str  = f"XBT/USD" if (symbol == "BTC" and buy_venue == "Kraken") else f"{symbol}/USD" if buy_venue == "Kraken" else f"{symbol}_USDT"
        sell_sym_str = f"XBT/USD" if (symbol == "BTC" and sell_venue == "Kraken") else f"{symbol}/USD" if sell_venue == "Kraken" else f"{symbol}_USDT"

        print(f"\n  {'━'*78}", flush=True)
        print(f"  CYCLE {c.cycle_num}  |  Capital: ${_fmt(c.capital_in, 2)}  →  Expected out: ${_fmt(c.capital_out, 2)}", flush=True)
        print(f"  {'━'*78}", flush=True)

        print(f"""
  STEP A — SELL (on {sell_venue}) ← execute FIRST or simultaneously with B
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Instrument  : {sell_sym_str}
  │  Side        : SELL
  │  Quantity    : {_fmt(c.arrival_amount, 6)} {symbol}  (from your pre-positioned float)
  │  Order type  : LIMIT at ${_fmt(c.sell_price, 2)} or BETTER
  │  Time-in-force: IOC (Immediate or Cancel)
  └─────────────────────────────────────────────────────────────────────────┘

  STEP B — BUY (on {buy_venue}) ← execute simultaneously with A
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Instrument  : {buy_sym_str}
  │  Side        : BUY
  │  Quantity    : {_fmt(c.trade_size, 6)} {symbol}
  │  Order type  : LIMIT at ${_fmt(c.buy_price, 2)} or BETTER
  │  Time-in-force: IOC
  └─────────────────────────────────────────────────────────────────────────┘

  STEP C — VERIFY FILLS (before withdrawal)
  STEP D — WITHDRAW from {buy_venue} → {sell_venue}  (float replenishment)
  STEP E — PROFIT IS LOCKED ✅
""", flush=True)

    print(f"\n  {'─'*78}", flush=True)
    print(f"  EXECUTION MODE: {EXECUTION_MODE.upper()}", flush=True)
    if EXECUTION_MODE == "advisory":
        print(f"  ℹ  No orders placed. Switch EXECUTION_MODE to 'semi_auto' to enable API calls.", flush=True)
    elif EXECUTION_MODE == "semi_auto":
        print(f"  ℹ  Orders will be placed via API. Withdrawal requires manual confirmation.", flush=True)
    elif EXECUTION_MODE == "full_auto":
        print(f"  ⚡ Full automation enabled. Steps A–D execute without human confirmation.", flush=True)
    print(bar, flush=True)

# ───────────────────────────────────────────────────────────────────
# KRAKEN AUTH + REST
# ───────────────────────────────────────────────────────────────────

KRAKEN_API_KEY    = os.getenv("KRAKEN_API_KEY", "")
KRAKEN_API_SECRET = os.getenv("KRAKEN_API_SECRET", "")
KRAKEN_BASE       = "https://api.kraken.com"

def _kraken_sign(path: str, data: Dict[str, str]) -> Dict[str, str]:
    if not KRAKEN_API_KEY or not KRAKEN_API_SECRET:
        raise RuntimeError("Kraken API keys missing")

    postdata = "&".join(f"{k}={v}" for k, v in data.items())
    nonce = data["nonce"].encode()
    sha256 = hashlib.sha256(nonce + postdata.encode()).digest()
    message = path.encode() + sha256
    secret = base64.b64decode(KRAKEN_API_SECRET)
    sig = hmac.new(secret, message, hashlib.sha512).digest()
    sig_b64 = base64.b64encode(sig).decode()

    return {
        "API-Key": KRAKEN_API_KEY,
        "API-Sign": sig_b64,
        "Content-Type": "application/x-www-form-urlencoded",
    }

async def _kraken_private_post(endpoint: str, data: Dict[str, str]) -> Dict[str, Any]:
    path = f"/0/private/{endpoint}"
    url = KRAKEN_BASE + path
    data = dict(data)
    data["nonce"] = str(int(time.time() * 1000))
    headers = _kraken_sign(path, data)
    session = await get_session()
    async with session.post(url, data=data, headers=headers, timeout=10) as resp:
        resp.raise_for_status()
        return await resp.json()

async def kraken_place_limit_order(
    symbol: str, side: str, price: Decimal, qty: Decimal
) -> Optional[str]:
    if EXECUTION_MODE == "advisory":
        return None
    if not KRAKEN_API_KEY or not KRAKEN_API_SECRET:
        print("[!] Kraken API keys not set in environment.", flush=True)
        return None

    kraken_pair = {"BTC": "XBTUSD", "ETH": "ETHUSD", "SOL": "SOLUSD"}[symbol]
    data = {
        "pair":      kraken_pair,
        "type":      side,
        "ordertype": "limit",
        "price":     str(price),
        "volume":    str(qty),
        "timeinforce": "ioc",
    }
    try:
        res = await _kraken_private_post("AddOrder", data)
        if res.get("error"):
            print(f"[Kraken] AddOrder error: {res['error']}", flush=True)
            return None
        txid = res.get("result", {}).get("txid")
        if isinstance(txid, list) and txid:
            oid = txid[0]
        else:
            oid = txid
        print(f"[Kraken] Order placed: {oid}", flush=True)
        return oid
    except Exception as e:
        print(f"[Kraken] AddOrder exception: {e}", flush=True)
        return None

async def kraken_get_order_status(order_id: str) -> Optional[Dict[str, Any]]:
    data = {"txid": order_id}
    try:
        res = await _kraken_private_post("QueryOrders", data)
        if res.get("error"):
            print(f"[Kraken] QueryOrders error: {res['error']}", flush=True)
            return None
        orders = res.get("result", {})
        return orders.get(order_id)
    except Exception as e:
        print(f"[Kraken] QueryOrders exception: {e}", flush=True)
        return None

async def kraken_get_balance() -> Dict[str, Decimal]:
    try:
        res = await _kraken_private_post("Balance", {})
        if res.get("error"):
            print(f"[Kraken] Balance error: {res['error']}", flush=True)
            return {}
        out = {}
        for k, v in res.get("result", {}).items():
            canon = KRAKEN_SYMBOL_MAP.get(k, k)
            out[canon] = Decimal(v)
        return out
    except Exception as e:
        print(f"[Kraken] Balance exception: {e}", flush=True)
        return {}

async def kraken_withdraw(
    symbol: str, qty: Decimal, dest_address_key: str
) -> Optional[str]:
    if EXECUTION_MODE not in ("semi_auto", "full_auto"):
        return None
    if EXECUTION_MODE == "semi_auto":
        ans = input(f"\n  ⚠  CONFIRM withdrawal of {qty} {symbol} from Kraken? [yes/no]: ").strip()
        if ans.lower() != "yes":
            print("  Withdrawal skipped by operator.", flush=True)
            return None
    asset = "XBT" if symbol == "BTC" else symbol
    data = {
        "asset": asset,
        "key": dest_address_key,
        "amount": str(qty),
    }
    try:
        res = await _kraken_private_post("Withdraw", data)
        if res.get("error"):
            print(f"[Kraken] Withdraw error: {res['error']}", flush=True)
            return None
        wid = res.get("result", {}).get("refid")
        print(f"[Kraken] Withdraw initiated: {wid}", flush=True)
        return wid
    except Exception as e:
        print(f"[Kraken] Withdraw exception: {e}", flush=True)
        return None

# ───────────────────────────────────────────────────────────────────
# CRYPTO.COM AUTH + REST
# ───────────────────────────────────────────────────────────────────

CDC_API_KEY    = os.getenv("CDC_API_KEY", "")
CDC_API_SECRET = os.getenv("CDC_API_SECRET", "")
CDC_BASE       = "https://api.crypto.com/exchange/v1"

def _cryptocom_sign(method: str, params: Dict[str, Any]) -> Dict[str, Any]:
    if not CDC_API_KEY or not CDC_API_SECRET:
        raise RuntimeError("Crypto.com API keys missing")

    nonce = int(time.time() * 1000)
    payload = {
        "id": nonce,
        "method": method,
        "api_key": CDC_API_KEY,
        "params": params or {},
        "nonce": nonce,
    }

    param_items = []
    for k in sorted(payload["params"].keys()):
        param_items.append(f"{k}{payload['params'][k]}")
    param_str = "".join(param_items)

    msg = f"{payload['method']}{payload['id']}{payload['api_key']}{payload['nonce']}{param_str}"
    sig = hmac.new(
        CDC_API_SECRET.encode(),
        msg.encode(),
        hashlib.sha256
    ).hexdigest()

    payload["sig"] = sig
    return payload

async def _cryptocom_private_post(method: str, params: Dict[str, Any]) -> Dict[str, Any]:
    payload = _cryptocom_sign(method, params)
    session = await get_session()
    async with session.post(
        CDC_BASE + "/private",
        json=payload,
        timeout=10,
    ) as resp:
        resp.raise_for_status()
        return await resp.json()

async def cryptocom_place_limit_order(
    symbol: str, side: str, price: Decimal, qty: Decimal
) -> Optional[str]:
    if EXECUTION_MODE == "advisory":
        return None
    if not CDC_API_KEY or not CDC_API_SECRET:
        print("[!] Crypto.com API keys not set in environment.", flush=True)
        return None

    inst_id = f"{symbol}_USDT"
    client_oid = f"arb_{symbol}_{int(time.time()*1000)}"
    params = {
        "instrument_name": inst_id,
        "side": side.upper(),
        "type": "LIMIT",
        "price": str(price),
        "quantity": str(qty),
        "time_in_force": "IOC",
        "client_oid": client_oid,
    }
    try:
        res = await _cryptocom_private_post("private/create-order", params)
        if res.get("code") != 0:
            print(f"[Crypto.com] create-order error: {res}", flush=True)
            return None
        oid = res.get("result", {}).get("order_id") or client_oid
        print(f"[Crypto.com] Order placed: {oid}", flush=True)
        return oid
    except Exception as e:
        print(f"[Crypto.com] create-order exception: {e}", flush=True)
        return None

async def cryptocom_get_order_status(order_id: str) -> Optional[Dict[str, Any]]:
    params = {"order_id": order_id}
    try:
        res = await _cryptocom_private_post("private/get-order-detail", params)
        if res.get("code") != 0:
            print(f"[Crypto.com] get-order-detail error: {res}", flush=True)
            return None
        return res.get("result", {})
    except Exception as e:
        print(f"[Crypto.com] get-order-detail exception: {e}", flush=True)
        return None

async def cryptocom_get_balance() -> Dict[str, Decimal]:
    try:
        res = await _cryptocom_private_post("private/get-account-summary", {})
        if res.get("code") != 0:
            print(f"[Crypto.com] get-account-summary error: {res}", flush=True)
            return {}
        out: Dict[str, Decimal] = {}
        for acct in res.get("result", {}).get("accounts", []):
            cur = acct["currency"]
            bal = Decimal(str(acct["available"]))
            out[cur] = bal
        return out
    except Exception as e:
        print(f"[Crypto.com] get-account-summary exception: {e}", flush=True)
        return {}

async def cryptocom_withdraw(
    symbol: str, qty: Decimal, dest_address: str, network: str
) -> Optional[str]:
    if EXECUTION_MODE not in ("semi_auto", "full_auto"):
        return None
    if EXECUTION_MODE == "semi_auto":
        ans = input(f"\n  ⚠  CONFIRM withdrawal of {qty} {symbol} from Crypto.com? [yes/no]: ").strip()
        if ans.lower() != "yes":
            print("  Withdrawal skipped by operator.", flush=True)
            return None
    params = {
        "currency": symbol,
        "amount": str(qty),
        "address": dest_address,
        "network": network,
    }
    try:
        res = await _cryptocom_private_post("private/create-withdrawal", params)
        if res.get("code") != 0:
            print(f"[Crypto.com] create-withdrawal error: {res}", flush=True)
            return None
        wid = res.get("result", {}).get("id")
        print(f"[Crypto.com] Withdraw initiated: {wid}", flush=True)
        return wid
    except Exception as e:
        print(f"[Crypto.com] create-withdrawal exception: {e}", flush=True)
        return None

# ───────────────────────────────────────────────────────────────────
# LIVE BALANCE PRE-FLIGHT CHECK
# ───────────────────────────────────────────────────────────────────

async def preflight_check_float(symbol: str, sell_venue: str, needed: Decimal) -> bool:
    if EXECUTION_MODE == "advisory":
        return True

    if sell_venue == "Kraken":
        bal = await kraken_get_balance()
        avail = bal.get(symbol, Decimal("0"))
    else:
        bal = await cryptocom_get_balance()
        # Crypto.com uses "BTC", "ETH", "SOL" as currency codes
        avail = bal.get(symbol, Decimal("0"))

    ok = avail >= needed
    if not ok:
        print(f"[Preflight] INSUFFICIENT FLOAT on {sell_venue} for {symbol}: "
              f"needed={needed}, available={avail}", flush=True)
    else:
        print(f"[Preflight] Float OK on {sell_venue} for {symbol}: {avail} ≥ {needed}", flush=True)
    return ok

# ───────────────────────────────────────────────────────────────────
# FILL-POLLING STATE MACHINE
# ───────────────────────────────────────────────────────────────────

async def poll_fills(
    buy_venue: str,
    sell_venue: str,
    buy_id: str,
    sell_id: str,
    timeout_sec: float = 10.0,
    poll_interval: float = 0.5,
) -> Tuple[bool, bool]:
    start = time.time()
    buy_filled = False
    sell_filled = False

    while time.time() - start < timeout_sec:
        # SELL status
        if sell_venue == "Kraken":
            s = await kraken_get_order_status(sell_id)
            if s:
                status = s.get("status")
                vol_exec = Decimal(s.get("vol_exec", "0"))
                if status == "closed" and vol_exec > 0:
                    sell_filled = True
                elif status in ("canceled", "expired", "rejected"):
                    print(f"[State] Sell leg {sell_id} {status}", flush=True)
                    return buy_filled, sell_filled
        else:
            s = await cryptocom_get_order_status(sell_id)
            if s:
                status = s.get("status")
                if status == "FILLED":
                    sell_filled = True
                elif status in ("CANCELED", "REJECTED", "EXPIRED"):
                    print(f"[State] Sell leg {sell_id} {status}", flush=True)
                    return buy_filled, sell_filled

        # BUY status
        if buy_venue == "Kraken":
            b = await kraken_get_order_status(buy_id)
            if b:
                status = b.get("status")
                vol_exec = Decimal(b.get("vol_exec", "0"))
                if status == "closed" and vol_exec > 0:
                    buy_filled = True
                elif status in ("canceled", "expired", "rejected"):
                    print(f"[State] Buy leg {buy_id} {status}", flush=True)
                    return buy_filled, sell_filled
        else:
            b = await cryptocom_get_order_status(buy_id)
            if b:
                status = b.get("status")
                if status == "FILLED":
                    buy_filled = True
                elif status in ("CANCELED", "REJECTED", "EXPIRED"):
                    print(f"[State] Buy leg {buy_id} {status}", flush=True)
                    return buy_filled, sell_filled

        if buy_filled and sell_filled:
            return True, True

        await asyncio.sleep(poll_interval)

    print("[State] Fill polling timeout reached", flush=True)
    return buy_filled, sell_filled

# ───────────────────────────────────────────────────────────────────
# CYCLE EXECUTOR
# ───────────────────────────────────────────────────────────────────

async def execute_cycle(plan: ExecutionPlan) -> None:
    c   = plan.cycle
    sym = plan.symbol

    if EXECUTION_MODE == "advisory":
        return

    print(f"\n  ⚡ EXECUTING Cycle {plan.cycle_index+1}/{plan.total_cycles}  {sym}  "
          f"BUY {plan.buy_venue} → SELL {plan.sell_venue}", flush=True)

    # Pre-flight float check on sell venue
    float_ok = await preflight_check_float(sym, plan.sell_venue, c.arrival_amount)
    if not float_ok:
        print("  [!] Aborting cycle due to insufficient float.", flush=True)
        return

    async def do_sell():
        fn = cryptocom_place_limit_order if plan.sell_venue == "Crypto.com" else kraken_place_limit_order
        oid = await fn(sym, "sell", c.sell_price, c.arrival_amount)
        plan.sell_order_id = oid
        plan.sell_status   = StepStatus.IN_FLIGHT if oid else StepStatus.FAILED
        return oid

    async def do_buy():
        fn = kraken_place_limit_order if plan.buy_venue == "Kraken" else cryptocom_place_limit_order
        oid = await fn(sym, "buy", c.buy_price, c.trade_size)
        plan.buy_order_id = oid
        plan.buy_status   = StepStatus.IN_FLIGHT if oid else StepStatus.FAILED
        return oid

    sell_id, buy_id = await asyncio.gather(do_sell(), do_buy())

    if not sell_id or not buy_id:
        print(f"  [!] Order placement failed — aborting cycle. "
              f"sell_id={sell_id} buy_id={buy_id}", flush=True)
        if sell_id and not buy_id:
            print(f"  [!] WARNING: sell leg placed but buy leg failed. "
                  f"Manual intervention required to cancel {sell_id}.", flush=True)
        return

    print(f"  ✓ Orders placed: sell={sell_id}  buy={buy_id}  — waiting for fills...", flush=True)

    buy_filled, sell_filled = await poll_fills(
        plan.buy_venue, plan.sell_venue, buy_id, sell_id, timeout_sec=15.0, poll_interval=0.5
    )

    if not buy_filled or not sell_filled:
        print(f"  [!] Imbalance detected. buy_filled={buy_filled}, sell_filled={sell_filled}", flush=True)
        print("  [!] Manual intervention required to flatten exposure.", flush=True)
        return

    plan.buy_status  = StepStatus.FILLED
    plan.sell_status = StepStatus.FILLED
    print(f"  ✓ Both legs filled.", flush=True)

    # Withdraw to replenish float
    if plan.buy_venue == "Kraken":
        wid = await kraken_withdraw(sym, c.arrival_amount, f"cryptocom_{sym.lower()}_deposit")
    else:
        network_map = {"BTC": "BTC", "ETH": "ETH", "SOL": "SOL"}
        dest_addr = os.getenv("KRAKEN_DEPOSIT_ADDRESS", "")
        if not dest_addr:
            print("[!] KRAKEN_DEPOSIT_ADDRESS not set in env; skipping withdrawal.", flush=True)
            wid = None
        else:
            wid = await cryptocom_withdraw(sym, c.arrival_amount, dest_addr, network_map[sym])

    plan.withdrawal_id   = wid
    plan.withdraw_status = StepStatus.IN_FLIGHT if wid else StepStatus.SKIPPED

    print("  ✓ Profit logically locked; withdrawal is float housekeeping.", flush=True)

# ───────────────────────────────────────────────────────────────────
# CLEANUP
# ───────────────────────────────────────────────────────────────────

async def shutdown():
    global _session
    if _session and not _session.closed:
        await _session.close()

# You’ll wire this into your existing real-time spread engine main loop.
# Example
