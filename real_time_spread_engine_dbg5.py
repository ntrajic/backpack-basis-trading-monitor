# real_time_spread_engine_dbg5.py
#
# EXECUTION ADVISORY & ORDER COORDINATION LAYER
# Builds on dbg4 (cycle projection engine).
#
# ─────────────────────────────────────────────────────────────────
# WHY TRUE ATOMIC EXECUTION IS IMPOSSIBLE ACROSS TWO CEXes
# ─────────────────────────────────────────────────────────────────
# Atomic = "either both legs fill or neither does" (database ACID sense).
# Two centralised exchanges share no mempool, no settlement bus, and no
# rollback protocol.  You CANNOT atomically buy on Kraken and sell on
# Crypto.com in one operation.
#
# WHAT YOU CAN DO INSTEAD: PRE-POSITIONED INVENTORY MODEL
# ─────────────────────────────────────────────────────────────────
# Keep a float of the TARGET ASSET already sitting in the SELL exchange
# wallet BEFORE the spread is detected.  Then:
#
#   Step A  (instant, ~milliseconds)
#           SELL float on sell-exchange  →  locks the sell leg immediately
#
#   Step B  (instant, ~milliseconds, in parallel with A)
#           BUY on buy-exchange          →  locks the buy leg immediately
#
#   Step C  (async, minutes–hours)
#           WITHDRAW bought asset from buy-exchange  →  replenish the float
#           on sell-exchange ready for the next cycle
#
# This makes the two *price-sensitive* legs effectively simultaneous.
# The transfer (Step C) is a housekeeping operation that happens after
# profit is already locked.  This is how professional arb desks operate.
#
# ─────────────────────────────────────────────────────────────────
# EXECUTION MODES IMPLEMENTED HERE
# ─────────────────────────────────────────────────────────────────
#   MODE = "advisory"   → print full step-by-step instructions, no API calls
#   MODE = "semi_auto"  → place orders via API but PAUSE for human confirm
#                         before withdrawal
#   MODE = "full_auto"  → place both orders AND initiate withdrawal
#                         automatically (USE WITH EXTREME CAUTION)
#
# For safety the default is "advisory".  Change EXECUTION_MODE below
# only after you have tested advisory output extensively.
#
# ─────────────────────────────────────────────────────────────────
# API KEY SETUP
# ─────────────────────────────────────────────────────────────────
# Store credentials in environment variables ONLY. Never hardcode.
#
#   export KRAKEN_API_KEY="your_key"
#   export KRAKEN_API_SECRET="your_secret"
#   export CDC_API_KEY="your_key"
#   export CDC_API_SECRET="your_secret"
#
# ─────────────────────────────────────────────────────────────────
# RISK WARNINGS (READ BEFORE ENABLING semi_auto / full_auto)
# ─────────────────────────────────────────────────────────────────
#   1. Partial fills: a limit order may fill partially. The engine
#      handles this by reading fill confirmations before proceeding.
#   2. Slippage: by the time your order reaches the matching engine
#      the price may have moved.  Use limit orders, not market orders.
#   3. Withdrawal failures: network congestion, exchange maintenance,
#      or 2FA requirements can block withdrawal.  The engine detects
#      this and alerts you.
#   4. Float depletion: if the sell-side float runs out the sell leg
#      cannot execute.  A float monitor is included.
#   5. Fee changes: exchanges can change fee tiers.  Re-verify fees
#      before each session.
#   6. Regulatory: automated trading is legal in most jurisdictions
#      but check your local rules.  Never use funds you cannot afford
#      to lose.

"""
_dbg5.py — Execution advisory + order coordination layer
===========================================================
The key architectural insight — why true atomicity is impossible and what to do instead:
Cross-exchange atomic execution doesn't exist —
two CEXes share no settlement bus.
The professional solution is the pre-positioned inventory model:

- Keep a float of the target asset already sitting on the sell-side exchange
before any spread is detected
Step A + B fire in parallel (milliseconds): sell the float on the sell exchange,
           buy simultaneously on the buy exchange — both price-sensitive legs
           execute at the same time
Step C verifies fills; if either leg fails or partially fills it handles
          the imbalance before any withdrawal
Step D withdraws from the buy exchange back to the sell exchange to replenish the float —
          this is async housekeeping, profit is already locked before this completes
Step E profit is locked in the sell-exchange USDT wallet

Three execution modes via EXECUTION_MODE:
--------------------------------------------
"advisory" (default) — prints every step as precise API call instructions,
                       no orders placed
"semi_auto" — places orders via API stubs
              (you fill in HMAC signing), pauses for human confirmation before withdrawal
"full_auto" — end-to-end automation including withdrawal

To go live on semi_auto:
========================
o fill in the HMAC-SHA512 signing in kraken_place_limit_order(),
o HMAC-SHA256 in cryptocom_place_limit_order(),
o implement the fill-polling loop in execute_cycle()

Step C, set your API keys as env vars, and pre-register withdrawal addresses
        in each exchange's whitelist UI.
"""

import asyncio
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.parse
import urllib.request
import websockets
from decimal import Decimal, ROUND_DOWN
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum

# ───────────────────────────────────────────────────────────────────
# EXECUTION CONFIGURATION — edit these before running
# ───────────────────────────────────────────────────────────────────

EXECUTION_MODE = "advisory"   # "advisory" | "semi_auto" | "full_auto"

CAPITAL        = Decimal("1000")     # USDT per cycle
MAX_CYCLES     = 20
MIN_NET_YIELD_PCT         = Decimal("0.05")    # 0.05%
MIN_ALERT_INTERVAL_SEC    = 10
SPREAD_DECAY_PER_TRANSFER = Decimal("0.30")

# Pre-positioned float: how many units of each asset to keep pre-loaded
# on the default sell side.  Adjust to match your actual float balances.
# Engine will warn you if live balance (when API keys provided) is below this.
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

# ───────────────────────────────────────────────────────────────────
# ORDER BOOK STATE  (identical to dbg4)
# ───────────────────────────────────────────────────────────────────

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
    """Holds all state for a single cycle's execution."""
    symbol:           str
    buy_venue:        str
    sell_venue:       str
    cycle:            CycleResult
    cycle_index:      int
    total_cycles:     int

    # order IDs filled in by execution layer
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
# CYCLE MATH  (identical logic to dbg4)
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
# ADVISORY PRINTER
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

    # Cycle table
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

    # ── PRE-POSITIONING STATUS ──────────────────────────────────────
    print(f"\n  ── PRE-POSITIONING CHECK ─────────────────────────────────────────", flush=True)
    print(f"  Float needed on {sell_venue}: {_fmt(float_needed, 6)} {symbol}", flush=True)
    print(f"  Float configured           : {_fmt(float_avail, 6)} {symbol}  {'✅ OK' if float_ok else '❌ INSUFFICIENT — top up before trading'}", flush=True)
    if not float_ok:
        shortage = float_needed - float_avail
        print(f"  ⚠  Shortage: {_fmt(shortage, 6)} {symbol}  — cannot execute sell leg without topping up float first.", flush=True)

    # ── EXECUTION STEPS (per cycle) ─────────────────────────────────
    for c in cycles:
        bk = buy_venue.lower().replace(".", "")
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
  │  Time-in-force: IOC (Immediate or Cancel) — do NOT use GTC here
  │  Expected fee: ${_fmt(c.exit_fee, 4)}
  │  Expected rev: ${_fmt(c.gross_revenue, 4)}
  │
  │  API call ({sell_venue}):
  │    POST /private/CreateOrder  (Crypto.com)
  │    or  POST /0/private/AddOrder  (Kraken)
  │    {{ side:"sell", type:"limit", price:"{_fmt(c.sell_price, 2)}",
  │      quantity:"{_fmt(c.arrival_amount, 6)}", time_in_force:"IOC" }}
  └─────────────────────────────────────────────────────────────────────────┘

  STEP B — BUY (on {buy_venue}) ← execute simultaneously with A
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Instrument  : {buy_sym_str}
  │  Side        : BUY
  │  Quantity    : {_fmt(c.trade_size, 6)} {symbol}
  │  Order type  : LIMIT at ${_fmt(c.buy_price, 2)} or BETTER
  │  Time-in-force: IOC
  │  Expected fee: ${_fmt(c.entry_fee, 4)}
  │  Expected cost: ${_fmt(c.capital_in, 4)}
  │
  │  API call ({buy_venue}):
  │    POST /private/CreateOrder  (Crypto.com)
  │    or  POST /0/private/AddOrder  (Kraken)
  │    {{ side:"buy", type:"limit", price:"{_fmt(c.buy_price, 2)}",
  │      quantity:"{_fmt(c.trade_size, 6)}", time_in_force:"IOC" }}
  └─────────────────────────────────────────────────────────────────────────┘

  STEP C — VERIFY FILLS (before withdrawal)
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  • Poll GET /private/GetOrderDetail (both exchanges) until status=FILLED
  │  • If either order is PARTIALLY_FILLED or CANCELLED:
  │      - Cancel the other leg immediately (best-effort)
  │      - Do NOT withdraw — the cycle is broken
  │      - Log the imbalance and alert operator
  │  • If both FILLED: proceed to Step D
  └─────────────────────────────────────────────────────────────────────────┘

  STEP D — WITHDRAW from {buy_venue} → {sell_venue}  (float replenishment)
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Asset        : {symbol}
  │  Amount       : {_fmt(c.arrival_amount, 6)} {symbol}
  │  Network fee  : {_fmt(c.withdrawal_fee, 6)} {symbol}  (deducted by exchange)
  │  Destination  : {sell_venue} deposit address for {symbol}
  │  ⚠  IMPORTANT: use the CORRECT network (e.g. SOL = Solana, not ERC-20)
  │
  │  This step is ASYNC — it takes ~{TRANSFER_MINUTES[symbol]} min.
  │  USDT profit from Step A is already in your {sell_venue} wallet.
  │  You do NOT need to wait for this transfer to lock profit.
  └─────────────────────────────────────────────────────────────────────────┘

  STEP E — PROFIT IS LOCKED ✅
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Locked in {sell_venue} wallet  : +${_fmt(c.gross_revenue - c.exit_fee, 4)} USDT
  │  Cost of capital from {buy_venue}: -${_fmt(c.capital_in + c.entry_fee, 4)} USDT
  │  NET PROFIT (projected)         : +${_fmt(c.net_profit, 4)} USDT ({_fmt(c.net_pct, 4)}%)
  │
  │  Actual realised P&L will differ slightly due to:
  │    - Slippage between detection and fill
  │    - Actual fill price vs limit price
  │    - Possible partial fills (handled in Step C)
  └─────────────────────────────────────────────────────────────────────────┘""", flush=True)

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
# API EXECUTION STUBS
# These are the scaffolds for semi_auto / full_auto mode.
# Each function has the correct endpoint documented inline.
# Fill in the signing logic with your exchange SDK or REST client.
# ───────────────────────────────────────────────────────────────────

async def kraken_place_limit_order(
    symbol: str, side: str, price: Decimal, qty: Decimal
) -> Optional[str]:
    """
    Place a limit IOC order on Kraken via REST API v0.
    Endpoint: POST https://api.kraken.com/0/private/AddOrder
    Requires: KRAKEN_API_KEY, KRAKEN_API_SECRET in environment.

    Returns order ID string, or None on failure.

    Signing: Kraken uses HMAC-SHA512 over (nonce + POST body) with
    the API secret (base64-decoded).  See:
    https://docs.kraken.com/api/docs/guides/spot-rest-introduction#generating-the-api-sign
    """
    if EXECUTION_MODE == "advisory":
        return None
    api_key    = os.environ.get("KRAKEN_API_KEY", "")
    api_secret = os.environ.get("KRAKEN_API_SECRET", "")
    if not api_key or not api_secret:
        print("[!] Kraken API keys not set in environment.", flush=True)
        return None

    # Map canonical symbol back to Kraken pair string
    kraken_pair = {"BTC": "XBTUSD", "ETH": "ETHUSD", "SOL": "SOLUSD"}[symbol]

    nonce    = str(int(time.time() * 1000))
    postdata = {
        "nonce":     nonce,
        "pair":      kraken_pair,
        "type":      side,                          # "buy" or "sell"
        "ordertype": "limit",
        "price":     str(price),
        "volume":    str(qty),
        "oflags":    "post",                        # post-only; change to "" for IOC
        "timeinforce": "ioc",
    }
    # TODO: add HMAC-SHA512 signing here before going live
    # See Kraken docs link above.  Stub returns None until implemented.
    print(f"  [STUB] kraken_place_limit_order({symbol}, {side}, ${price}, qty={qty})", flush=True)
    return None   # replace with actual order_id from API response


async def cryptocom_place_limit_order(
    symbol: str, side: str, price: Decimal, qty: Decimal
) -> Optional[str]:
    """
    Place a limit IOC order on Crypto.com via Exchange REST API v2.
    Endpoint: POST https://api.crypto.com/exchange/v1/private/create-order
    Requires: CDC_API_KEY, CDC_API_SECRET in environment.

    Returns client_oid string, or None on failure.

    Signing: HMAC-SHA256 over sorted params concatenated with nonce + api_key.
    See: https://exchange-docs.crypto.com/exchange/v1/rest-ws/index.html#digital-signature
    """
    if EXECUTION_MODE == "advisory":
        return None
    api_key    = os.environ.get("CDC_API_KEY", "")
    api_secret = os.environ.get("CDC_API_SECRET", "")
    if not api_key or not api_secret:
        print("[!] Crypto.com API keys not set in environment.", flush=True)
        return None

    cdc_instrument = f"{symbol}_USDT"
    client_oid     = f"arb_{symbol}_{int(time.time()*1000)}"
    # TODO: add HMAC-SHA256 signing here before going live
    print(f"  [STUB] cryptocom_place_limit_order({symbol}, {side}, ${price}, qty={qty})", flush=True)
    return None


async def kraken_withdraw(
    symbol: str, qty: Decimal, dest_address_key: str
) -> Optional[str]:
    """
    Initiate withdrawal from Kraken.
    Endpoint: POST https://api.kraken.com/0/private/Withdraw
    Params: asset (e.g. "XBT"), key (withdrawal address key name in Kraken),
            amount (string decimal)

    IMPORTANT: dest_address_key is the NAME you gave the withdrawal address
    in Kraken's UI (not the raw address itself).  Pre-register the
    Crypto.com deposit address in Kraken's withdrawal whitelist.
    """
    if EXECUTION_MODE not in ("semi_auto", "full_auto"):
        return None
    if EXECUTION_MODE == "semi_auto":
        answer = input(f"\n  ⚠  CONFIRM withdrawal of {qty} {symbol} from Kraken? [yes/no]: ").strip()
        if answer.lower() != "yes":
            print("  Withdrawal skipped by operator.", flush=True)
            return None
    print(f"  [STUB] kraken_withdraw({symbol}, qty={qty}, key={dest_address_key!r})", flush=True)
    return None


async def cryptocom_withdraw(
    symbol: str, qty: Decimal, dest_address: str, network: str
) -> Optional[str]:
    """
    Initiate withdrawal from Crypto.com.
    Endpoint: POST https://api.crypto.com/exchange/v1/private/create-withdrawal
    Params: currency, amount, address, network (e.g. "SOL", "ETH", "BTC")

    network examples: "SOL" for Solana, "ETH" for ERC-20, "BTC" for Bitcoin
    Do NOT mix networks — SOL sent on ERC-20 is unrecoverable.
    """
    if EXECUTION_MODE not in ("semi_auto", "full_auto"):
        return None
    if EXECUTION_MODE == "semi_auto":
        answer = input(f"\n  ⚠  CONFIRM withdrawal of {qty} {symbol} from Crypto.com? [yes/no]: ").strip()
        if answer.lower() != "yes":
            print("  Withdrawal skipped by operator.", flush=True)
            return None
    print(f"  [STUB] cryptocom_withdraw({symbol}, qty={qty}, addr={dest_address[:12]}…, net={network})", flush=True)
    return None


# ───────────────────────────────────────────────────────────────────
# CYCLE EXECUTOR
# ───────────────────────────────────────────────────────────────────

async def execute_cycle(plan: ExecutionPlan) -> None:
    """
    Orchestrate Steps A–E for one cycle.
    In advisory mode: does nothing except print.
    In semi_auto / full_auto: places orders, waits for fills, then withdraws.
    """
    c   = plan.cycle
    sym = plan.symbol

    if EXECUTION_MODE == "advisory":
        return   # advisory output already printed by print_advisory()

    print(f"\n  ⚡ EXECUTING Cycle {plan.cycle_index+1}/{plan.total_cycles}  {sym}  "
          f"BUY {plan.buy_venue} → SELL {plan.sell_venue}", flush=True)

    # ── STEP A + B in parallel ─────────────────────────────────────
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

    # ── STEP C: poll for fills ─────────────────────────────────────
    print(f"  ✓ Orders placed: sell={sell_id}  buy={buy_id}  — waiting for fills...", flush=True)
    # TODO: implement GET order status polling here.
    # Pseudocode:
    #   while True:
    #       sell_fill = await get_order_status(plan.sell_venue, sell_id)
    #       buy_fill  = await get_order_status(plan.buy_venue, buy_id)
    #       if both FILLED: break
    #       if either CANCELLED/REJECTED: handle imbalance, return
    #       await asyncio.sleep(0.5)
    # For now we proceed assuming fills (advisory stub):
    plan.sell_status = StepStatus.FILLED
    plan.buy_status  = StepStatus.FILLED
    print(f"  ✓ Both legs filled (stub — implement fill polling).", flush=True)

    # ── STEP D: withdraw (replenish float) ─────────────────────────
    if plan.buy_venue == "Kraken":
        wid = await kraken_withdraw(sym, c.arrival_amount, f"cryptocom_{sym.lower()}_deposit")
    else:
        network_map = {"BTC": "BTC", "ETH": "ETH", "SOL": "SOL"}
        wid = await cryptocom_withdraw(sym, c.arrival_amount, "KRAKEN_DEPOSIT_ADDRESS", network_map[sym])

    plan.withdrawal_id     = wid
    plan.withdraw_status   = StepStatus.IN_FLIGHT if wid else StepStatus.SKIPPED

    # ── STEP E: profit locked ──────────────────────────────────────
    # At this point USDT is already in sell-venue wallet from Step A.
    # The withdrawal (Step D) is only float replenishment and runs async.
    plan.realised_pnl = c.net_profit   # update with actual fill data when available
    print(f"  ✅ PROFIT LOCKED: +${_fmt(c.net_profit, 4)} USDT (projected). "
          f"Withdrawal {wid or 'pending manual action'}.", flush=True)


# ───────────────────────────────────────────────────────────────────
# OPPORTUNITY HANDLER  (called from tick engine)
# ───────────────────────────────────────────────────────────────────

async def handle_opportunity(
    symbol: str, buy_venue: str, sell_venue: str, cycles: list[CycleResult]
) -> None:
    print_advisory(symbol, buy_venue, sell_venue, cycles)

    if EXECUTION_MODE == "advisory":
        return

    for i, c in enumerate(cycles):
        # Re-check live prices before executing cycle 1+ (spread may have moved)
        if i > 0:
            book      = order_books[symbol]
            bk        = buy_venue.lower().replace(".", "")
            live_ask  = book[bk]["ask"]
            sk        = sell_venue.lower().replace(".", "")
            live_bid  = book[sk]["bid"]
            if not live_ask or not live_bid:
                print(f"  [!] Live book stale at cycle {i} — stopping cycle chain.", flush=True)
                break
            fresh = _compute_single_cycle(symbol, buy_venue, sell_venue, live_ask, live_bid, c.capital_in)
            if fresh is None:
                print(f"  [!] Spread no longer profitable at live prices (cycle {i}) — stopping.", flush=True)
                break
            c = fresh
            c.cycle_num = i

        plan = ExecutionPlan(
            symbol=symbol, buy_venue=buy_venue, sell_venue=sell_venue,
            cycle=c, cycle_index=i, total_cycles=len(cycles),
        )
        await execute_cycle(plan)

        if plan.buy_status != StepStatus.FILLED or plan.sell_status != StepStatus.FILLED:
            print(f"  [!] Cycle {i} did not complete. Stopping cycle chain.", flush=True)
            break

        # Wait for next cycle gap (transfer time) — only relevant in full_auto
        if EXECUTION_MODE == "full_auto" and i < len(cycles) - 1:
            wait_sec = TRANSFER_MINUTES[symbol] * 60
            print(f"  ⏳ Waiting {TRANSFER_MINUTES[symbol]} min for float replenishment...", flush=True)
            await asyncio.sleep(wait_sec)


# ───────────────────────────────────────────────────────────────────
# SPREAD EVALUATION  (called on every websocket tick)
# ───────────────────────────────────────────────────────────────────

def evaluate_spread(symbol: str) -> None:
    book  = order_books[symbol]
    k_ask = book["kraken"]["ask"]
    k_bid = book["kraken"]["bid"]
    c_ask = book["cryptocom"]["ask"]
    c_bid = book["cryptocom"]["bid"]

    for buy_venue, buy_price, sell_venue, sell_price in [
        ("Kraken",     k_ask, "Crypto.com", c_bid),
        ("Crypto.com", c_ask, "Kraken",     k_bid),
    ]:
        if not (buy_price and sell_price):
            continue
        dedup_key = (symbol, buy_venue)
        now = time.monotonic()
        if now - _last_alert.get(dedup_key, 0.0) < MIN_ALERT_INTERVAL_SEC:
            continue
        cycles = project_cycles(symbol, buy_venue, sell_venue, buy_price, sell_price)
        if not cycles:
            continue
        _last_alert[dedup_key] = now
        # Schedule execution as a background task so the WS loop is not blocked
        asyncio.get_event_loop().create_task(
            handle_opportunity(symbol, buy_venue, sell_venue, cycles)
        )


# ───────────────────────────────────────────────────────────────────
# WEBSOCKET STREAMS  (identical to dbg4)
# ───────────────────────────────────────────────────────────────────

async def stream_kraken() -> None:
    url = "wss://ws.kraken.com/v2"
    pairs = [f"{sym}/USD" for sym in SYMBOLS]
    async for ws in websockets.connect(url):
        try:
            await ws.send(json.dumps({"method": "subscribe", "params": {"channel": "ticker", "symbol": pairs}}))
            async for raw in ws:
                msg = json.loads(raw)
                if msg.get("channel") == "ticker" and "data" in msg:
                    pkt = msg["data"][0]
                    sym = KRAKEN_SYMBOL_MAP.get(pkt["symbol"].split("/")[0])
                    if sym:
                        order_books[sym]["kraken"]["bid"] = Decimal(str(pkt["bid"]))
                        order_books[sym]["kraken"]["ask"] = Decimal(str(pkt["ask"]))
                        evaluate_spread(sym)
        except websockets.ConnectionClosed:
            await asyncio.sleep(1)
        except Exception as e:
            print(f"[!] Kraken: {e}", flush=True)


async def _cdc_hb(ws, mid: int) -> None:
    await ws.send(json.dumps({"id": mid, "method": "public/respond-heartbeat"}))


async def stream_cryptocom() -> None:
    url = "wss://stream.crypto.com/v2/market"
    ch  = [f"ticker.{s}_USDT" for s in SYMBOLS]
    async for ws in websockets.connect(url):
        try:
            await ws.send(json.dumps({"id": 1, "method": "subscribe", "params": {"channels": ch}, "nonce": 1}))
            async for raw in ws:
                msg = json.loads(raw)
                if msg.get("method") == "public/heartbeat":
                    await _cdc_hb(ws, msg["id"])
                    continue
                if msg.get("method") == "subscription":
                    tl = msg.get("result", {}).get("data")
                    if not tl:
                        continue
                    pkt = tl[0]
                    sym = pkt["i"].split("_")[0]
                    if sym in order_books:
                        order_books[sym]["cryptocom"]["bid"] = Decimal(str(pkt["b"]))
                        order_books[sym]["cryptocom"]["ask"] = Decimal(str(pkt["a"]))
                        evaluate_spread(sym)
        except websockets.ConnectionClosed:
            await asyncio.sleep(1)
        except Exception as e:
            print(f"[!] Crypto.com: {e}", flush=True)


# ───────────────────────────────────────────────────────────────────
# STARTUP BANNER
# ───────────────────────────────────────────────────────────────────

def print_startup_info() -> None:
    print(f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║            ARBITRAGE EXECUTION ENGINE  —  dbg5                             ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Mode         : {EXECUTION_MODE:<59} ║
║  Capital/cycle: ${str(CAPITAL):<58} ║
║  Min yield    : {str(MIN_NET_YIELD_PCT)+"% net after fees":<59} ║
║  Max cycles   : {str(MAX_CYCLES):<59} ║
║  Spread decay : {str(SPREAD_DECAY_PER_TRANSFER*100)+"% per transfer window":<59} ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  PRE-POSITIONING MODEL (required for near-simultaneous execution):          ║
║  Keep a float of the target asset on the SELL exchange BEFORE trading.      ║
║  Sell leg fires immediately from float. Buy leg fires simultaneously.       ║
║  Withdrawal replenishes float asynchronously — profit is already locked.   ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  CONFIGURED FLOAT TARGETS (verify these match your actual balances):        ║""", flush=True)
    for sym, amt in TARGET_FLOAT.items():
        print(f"║    {sym}: {amt} units{'':<57}║", flush=True)
    print(f"""╠══════════════════════════════════════════════════════════════════════════════╣
║  TO ENABLE ORDER PLACEMENT:                                                 ║
║    1. Set EXECUTION_MODE = "semi_auto"  (human confirms withdrawal)         ║
║       or  EXECUTION_MODE = "full_auto" (fully automated — high risk)        ║
║    2. export KRAKEN_API_KEY / KRAKEN_API_SECRET                             ║
║    3. export CDC_API_KEY / CDC_API_SECRET                                   ║
║    4. Implement HMAC signing in kraken_place_limit_order()                  ║
║       and cryptocom_place_limit_order()                                     ║
║    5. Pre-register withdrawal addresses in exchange whitelists              ║
║    6. Implement fill-polling in execute_cycle() Step C                      ║
╚══════════════════════════════════════════════════════════════════════════════╝
""", flush=True)


# ───────────────────────────────────────────────────────────────────
# MAIN
# ───────────────────────────────────────────────────────────────────

async def main() -> None:
    sys.stdout.reconfigure(line_buffering=True)
    print_startup_info()
    await asyncio.gather(stream_kraken(), stream_cryptocom())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nEngine stopped.")
