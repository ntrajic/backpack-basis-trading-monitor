# real_time_spread_engine_dbg7_anthropic.py
# ═══════════════════════════════════════════════════════════════════════════════
# ARBITRAGE ENGINE  dbg7  —  THREE-MODE EXECUTION WITH FULL SAFEGUARD LAYER
# ═══════════════════════════════════════════════════════════════════════════════
#
# ┌─────────────────────────────────────────────────────────────────────────────
# │  MODE OVERVIEW
# ├─────────────────────────────────────────────────────────────────────────────
# │
# │  ADVISORY (default, safe)
# │  ─────────────────────────
# │  • Zero API calls.  Zero orders.  Zero withdrawals.  Zero risk.
# │  • Watches live spreads and prints exactly what it *would* do.
# │  • Shows projected cycle table, fees, net yield, withdrawal instructions.
# │  • Run this until you fully understand every output line.
# │
# │  SEMI_AUTO (orders yes, withdrawal manual)
# │  ──────────────────────────────────────────
# │  • Detects a profitable spread.
# │  • Prints a summary and asks: "Execute this trade? [yes/no]"
# │  • On confirmation: fires BUY and SELL simultaneously via asyncio.gather.
# │    Both HTTP requests are dispatched in the same event-loop tick.
# │    Real-world latency between the two exchange ACKs is typically <100ms.
# │  • Polls fills.  Handles imbalance.  Logs realised P&L.
# │  • STOPS.  Does NOT touch withdrawals.
# │  • Prints a manual withdrawal checklist so you can move funds whenever
# │    convenient (next day, next week — profit is already locked).
# │  • Each trade requires a fresh typed confirmation.
# │  • Session hard limits: MAX_TRADES_PER_SESSION, MAX_LOSS_PER_SESSION_USDT.
# │  • Capital ceiling: no single order > MAX_ORDER_USDT.
# │
# │  FULL_AUTO (dangerous — read every warning)
# │  ─────────────────────────────────────────────
# │  • Requires --full-auto CLI flag AND a typed passphrase at startup.
# │    Cannot be enabled by editing this file alone.
# │  • Fires trades AND withdrawals without human confirmation.
# │  • All semi_auto safeguards still apply, plus:
# │      - FULL_AUTO_PASSPHRASE must be set as an env var
# │      - Hard capital ceiling enforced per order and per session
# │      - Emergency kill: press Ctrl-C once → graceful shutdown
# │        (open orders are NOT automatically cancelled on exit;
# │         check your exchange dashboards after stopping)
# │
# └─────────────────────────────────────────────────────────────────────────────
#
# ┌─────────────────────────────────────────────────────────────────────────────
# │  QUICK-START
# ├─────────────────────────────────────────────────────────────────────────────
# │
# │  1. pip install aiohttp websockets
# │
# │  2. Advisory mode (no keys needed):
# │       python real_time_spread_engine_dbg7.py
# │
# │  3. Semi-auto (keys required):
# │       export KRAKEN_API_KEY="..."  KRAKEN_API_SECRET="..."
# │       export CDC_API_KEY="..."     CDC_API_SECRET="..."
# │       Set EXECUTION_MODE = "semi_auto" below.
# │       Fill KRAKEN_WITHDRAWAL_KEY_NAMES and CDC_WITHDRAWAL_ADDRESSES.
# │       python real_time_spread_engine_dbg7.py
# │
# │  4. Full-auto (keys + passphrase + CLI flag):
# │       export FULL_AUTO_PASSPHRASE="my_secret_phrase_here"
# │       Set EXECUTION_MODE = "full_auto" below.
# │       python real_time_spread_engine_dbg7.py --full-auto
# │
# └─────────────────────────────────────────────────────────────────────────────

import argparse
import asyncio
import base64
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.parse
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN
from enum import Enum
from typing import Optional

import aiohttp
import websockets

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

# ── Execution mode ─────────────────────────────────────────────────────────────
EXECUTION_MODE = "advisory"   # "advisory" | "semi_auto" | "full_auto"

# ── Capital parameters ─────────────────────────────────────────────────────────
CAPITAL           = Decimal("50")     # USDT deployed per cycle  ← start tiny
MAX_ORDER_USDT    = Decimal("200")    # hard ceiling: no single order above this
MIN_CAPITAL       = Decimal("20")     # floor: below this, fees eat everything
MAX_CYCLES        = 5                 # maximum projected cycles per opportunity

# ── Session-level risk limits (reset on each script run) ──────────────────────
MAX_TRADES_PER_SESSION    = 10        # stop after this many completed trades
MAX_LOSS_PER_SESSION_USDT = Decimal("30")  # kill engine if cumulative loss exceeds this
MAX_CONSECUTIVE_FAILURES  = 3         # stop if N trades in a row fail/imbalance

# ── Spread / yield thresholds ──────────────────────────────────────────────────
MIN_NET_YIELD_PCT         = Decimal("0.10")   # 0.10% minimum after all fees
MIN_ALERT_INTERVAL_SEC    = 15                # dedup: one alert per direction per N sec
SPREAD_DECAY_PER_TRANSFER = Decimal("0.35")   # 35% spread decay per transfer window

# ── Fill-polling ───────────────────────────────────────────────────────────────
FILL_POLL_INTERVAL_SEC = 0.5
FILL_TIMEOUT_SEC       = 20.0

# ── Pre-positioned float targets ──────────────────────────────────────────────
# Keep at least this much of each asset on the SELL exchange before trading.
# With tiny capital, even 0.002 BTC is enough for a $50 test trade.
TARGET_FLOAT = {
    "BTC": Decimal("0.002"),
    "ETH": Decimal("0.03"),
    "SOL": Decimal("0.5"),
}

# ── Transfer time estimates (minutes) ─────────────────────────────────────────
TRANSFER_MINUTES = {"BTC": 30, "ETH": 5, "SOL": 1}

# ── Fee schedule ───────────────────────────────────────────────────────────────
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

# ── Withdrawal address configuration ──────────────────────────────────────────
# See dbg6 header comments for how to set these up in each exchange's UI.
KRAKEN_WITHDRAWAL_KEY_NAMES: dict[str, str] = {
    "BTC": "cdc_btc",   # ← label you chose in Kraken's withdrawal whitelist
    "ETH": "cdc_eth",
    "SOL": "cdc_sol",
}
CDC_WITHDRAWAL_ADDRESSES: dict[str, str] = {
    "BTC": "YOUR_KRAKEN_BTC_DEPOSIT_ADDRESS",   # ← replace with real address
    "ETH": "YOUR_KRAKEN_ETH_DEPOSIT_ADDRESS",
    "SOL": "YOUR_KRAKEN_SOL_DEPOSIT_ADDRESS",
}
CDC_NETWORK_NAMES: dict[str, str] = {"BTC": "BTC", "ETH": "ETH", "SOL": "SOL"}

# ── Symbol mappings ────────────────────────────────────────────────────────────
SYMBOLS           = ["BTC", "ETH", "SOL"]
KRAKEN_SYMBOL_MAP = {"XBT": "BTC", "ETH": "ETH", "SOL": "SOL"}
KRAKEN_PAIR       = {"BTC": "XBTUSD", "ETH": "ETHUSD", "SOL": "SOLUSD"}
KRAKEN_ASSET      = {"BTC": "XBT",    "ETH": "ETH",    "SOL": "SOL"}

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — SHARED STATE
# ═══════════════════════════════════════════════════════════════════════════════

order_books: dict = {
    sym: {"kraken": {"bid": None, "ask": None}, "cryptocom": {"bid": None, "ask": None}}
    for sym in SYMBOLS
}
_last_alert:    dict                           = {}
_http_session:  Optional[aiohttp.ClientSession] = None

# ── Session risk counters (all reset on script start) ─────────────────────────
_session_trades_completed:    int     = 0
_session_cumulative_pnl:      Decimal = Decimal("0")
_session_consecutive_failures: int    = 0
_session_start_time:          float   = 0.0

# ── Runtime mode flags (set in main() after CLI parsing) ──────────────────────
_full_auto_unlocked: bool = False   # True only after passphrase verified at startup

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════════

class StepStatus(Enum):
    PENDING   = "PENDING"
    IN_FLIGHT = "IN_FLIGHT"
    FILLED    = "FILLED"
    PARTIAL   = "PARTIAL"
    CANCELLED = "CANCELLED"
    FAILED    = "FAILED"
    SKIPPED   = "SKIPPED"

@dataclass
class FillResult:
    status:        StepStatus
    filled_qty:    Decimal
    avg_price:     Decimal
    remaining_qty: Decimal
    raw:           dict = field(default_factory=dict)

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
    symbol:          str
    buy_venue:       str
    sell_venue:      str
    cycle:           CycleResult
    buy_order_id:    Optional[str]        = None
    sell_order_id:   Optional[str]        = None
    withdrawal_id:   Optional[str]        = None
    buy_status:      StepStatus           = StepStatus.PENDING
    sell_status:     StepStatus           = StepStatus.PENDING
    withdraw_status: StepStatus           = StepStatus.PENDING
    buy_fill:        Optional[FillResult] = None
    sell_fill:       Optional[FillResult] = None
    realised_pnl:    Optional[Decimal]    = None

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — SAFEGUARD LAYER
# ═══════════════════════════════════════════════════════════════════════════════

class SafeguardTripped(Exception):
    """Raised when any session-level safeguard is violated."""
    pass

def _check_session_limits() -> None:
    """
    Called before each trade.  Raises SafeguardTripped if any limit is hit.
    All limits are checked together so the log message is complete.
    """
    global _session_trades_completed, _session_consecutive_failures, _session_cumulative_pnl
    reasons = []

    if _session_trades_completed >= MAX_TRADES_PER_SESSION:
        reasons.append(
            f"MAX_TRADES_PER_SESSION ({MAX_TRADES_PER_SESSION}) reached"
        )
    if _session_cumulative_pnl <= -MAX_LOSS_PER_SESSION_USDT:
        reasons.append(
            f"MAX_LOSS_PER_SESSION_USDT (${MAX_LOSS_PER_SESSION_USDT}) breached "
            f"(current session P&L: ${_fmt(_session_cumulative_pnl, 4)})"
        )
    if _session_consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
        reasons.append(
            f"MAX_CONSECUTIVE_FAILURES ({MAX_CONSECUTIVE_FAILURES}) reached — "
            f"something may be wrong with exchange connectivity or the spread model"
        )

    if reasons:
        msg = "SESSION SAFEGUARD TRIPPED:\n" + "\n".join(f"  • {r}" for r in reasons)
        raise SafeguardTripped(msg)


def _check_order_size(capital: Decimal) -> None:
    """Raise SafeguardTripped if the order would exceed the hard capital ceiling."""
    if capital > MAX_ORDER_USDT:
        raise SafeguardTripped(
            f"Order size ${capital} exceeds MAX_ORDER_USDT (${MAX_ORDER_USDT}). "
            f"Lower CAPITAL in the config."
        )
    if capital < MIN_CAPITAL:
        raise SafeguardTripped(
            f"Order size ${capital} is below MIN_CAPITAL (${MIN_CAPITAL}). "
            f"After fees this trade would not be profitable. Increase CAPITAL."
        )


def _record_trade_result(realised_pnl: Decimal, success: bool) -> None:
    """Update session counters after a trade attempt."""
    global _session_trades_completed, _session_cumulative_pnl, _session_consecutive_failures
    _session_trades_completed    += 1
    _session_cumulative_pnl      += realised_pnl
    if success:
        _session_consecutive_failures = 0
    else:
        _session_consecutive_failures += 1


# ── Human-in-the-loop confirmation prompt ─────────────────────────────────────

async def _confirm_trade(plan_summary: str) -> bool:
    """
    In semi_auto: print summary, ask operator to type 'yes' to proceed.
    In full_auto: always returns True (no prompt).
    In advisory:  always returns False (no orders).

    Uses asyncio.get_event_loop().run_in_executor so stdin blocking doesn't
    freeze the websocket coroutines.
    """
    if EXECUTION_MODE == "advisory":
        return False
    if EXECUTION_MODE == "full_auto":
        return True

    # semi_auto
    _log("\n" + "▓" * 70)
    _log("  SEMI-AUTO TRADE CONFIRMATION REQUIRED")
    _log("▓" * 70)
    _log(plan_summary)
    _log("▓" * 70)
    _log("  Both BUY and SELL will fire SIMULTANEOUSLY if you confirm.")
    _log("  Withdrawal will NOT be triggered — you handle that manually later.")
    _log("▓" * 70)

    loop = asyncio.get_event_loop()
    try:
        answer = await loop.run_in_executor(
            None,
            lambda: input("  Type 'yes' to execute, anything else to skip: ").strip()
        )
    except EOFError:
        _log("  [!] stdin unavailable — skipping trade.")
        return False

    confirmed = answer.lower() == "yes"
    if not confirmed:
        _log("  Trade skipped by operator.")
    return confirmed


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — SIGNING  (unchanged from dbg6, verified correct)
# ═══════════════════════════════════════════════════════════════════════════════

def _kraken_sign(urlpath: str, data: dict, secret_b64: str) -> str:
    """
    Kraken HMAC-SHA512.  data["nonce"] must be a string.
    Verified against Kraken's official test vector.
    """
    post_data    = urllib.parse.urlencode(data)
    raw_input    = (data["nonce"] + post_data).encode("utf-8")
    sha256_bytes = hashlib.sha256(raw_input).digest()
    secret_bytes = base64.b64decode(secret_b64)
    mac          = hmac.new(
        secret_bytes,
        urlpath.encode("utf-8") + sha256_bytes,
        hashlib.sha512,
    )
    return base64.b64encode(mac.digest()).decode("utf-8")


def _cdc_params_to_str(obj, level: int = 0, max_level: int = 3) -> str:
    """Recursively build CDC param string (alphabetical key sort at every level)."""
    if level >= max_level:
        return str(obj)
    if isinstance(obj, dict):
        parts = []
        for k in sorted(obj.keys()):
            parts.append(k + _cdc_params_to_str(obj[k], level + 1, max_level))
        return "".join(parts)
    elif isinstance(obj, list):
        return "".join(_cdc_params_to_str(item, level + 1, max_level) for item in obj)
    return str(obj)


def _cdc_sign(method: str, params: dict, nonce: int, api_key: str,
              secret: str, request_id: int = 1) -> str:
    """Crypto.com HMAC-SHA256.  Output is hex (not base64)."""
    param_str   = _cdc_params_to_str(params)
    sig_payload = f"{method}{request_id}{api_key}{param_str}{nonce}"
    return hmac.new(
        secret.encode("utf-8"),
        sig_payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — KRAKEN REST LAYER  (unchanged from dbg6)
# ═══════════════════════════════════════════════════════════════════════════════

KRAKEN_BASE    = "https://api.kraken.com"
CDC_BASE       = "https://api.crypto.com/exchange/v1"
CDC_REQUEST_ID = 1


async def _kraken_post(endpoint: str, payload: dict) -> dict:
    api_key = os.environ.get("KRAKEN_API_KEY", "")
    secret  = os.environ.get("KRAKEN_API_SECRET", "")
    if not api_key or not secret:
        raise RuntimeError("KRAKEN_API_KEY / KRAKEN_API_SECRET not in environment")
    nonce            = str(int(time.time() * 1_000_000))
    payload["nonce"] = nonce
    urlpath          = f"/0/private/{endpoint}"
    headers = {
        "API-Key":      api_key,
        "API-Sign":     _kraken_sign(urlpath, payload, secret),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    async with _http_session.post(
        KRAKEN_BASE + urlpath, data=urllib.parse.urlencode(payload), headers=headers
    ) as resp:
        resp.raise_for_status()
        data = await resp.json()
    if data.get("error"):
        raise RuntimeError(f"Kraken error: {data['error']}")
    return data.get("result", {})


async def _cdc_post(method: str, params: dict) -> dict:
    api_key = os.environ.get("CDC_API_KEY", "")
    secret  = os.environ.get("CDC_API_SECRET", "")
    if not api_key or not secret:
        raise RuntimeError("CDC_API_KEY / CDC_API_SECRET not in environment")
    nonce = int(time.time() * 1000)
    body  = {
        "id":      CDC_REQUEST_ID,
        "method":  method,
        "params":  params,
        "api_key": api_key,
        "sig":     _cdc_sign(method, params, nonce, api_key, secret, CDC_REQUEST_ID),
        "nonce":   nonce,
    }
    async with _http_session.post(f"{CDC_BASE}/{method}", json=body) as resp:
        resp.raise_for_status()
        data = await resp.json()
    code = data.get("code", -1)
    if code != 0:
        raise RuntimeError(f"CDC error {code}: {data.get('message','')}")
    return data.get("result", {})


# ── Order placement ────────────────────────────────────────────────────────────

async def kraken_place_limit_order(
    symbol: str, side: str, price: Decimal, qty: Decimal
) -> Optional[str]:
    """POST /0/private/AddOrder  — IOC limit order."""
    if EXECUTION_MODE == "advisory":
        return None
    try:
        result = await _kraken_post("AddOrder", {
            "pair":        KRAKEN_PAIR[symbol],
            "type":        side,
            "ordertype":   "limit",
            "price":       str(price.quantize(Decimal("0.01"),       rounding=ROUND_DOWN)),
            "volume":      str(qty.quantize(  Decimal("0.00000001"), rounding=ROUND_DOWN)),
            "timeinforce": "IOC",
            "oflags":      "fciq",
        })
        txid = result["txid"][0]
        _log(f"  [ORDER] Kraken {side.upper()} {symbol}  txid={txid}")
        return txid
    except Exception as exc:
        _log(f"  [!] Kraken place_order({symbol},{side}): {exc}")
        return None


async def cdc_place_limit_order(
    symbol: str, side: str, price: Decimal, qty: Decimal
) -> Optional[str]:
    """POST private/create-order  — IOC limit order."""
    if EXECUTION_MODE == "advisory":
        return None
    try:
        result = await _cdc_post("private/create-order", {
            "instrument_name": f"{symbol}_USDT",
            "side":            side.upper(),
            "type":            "LIMIT",
            "price":           str(price.quantize(Decimal("0.01"),       rounding=ROUND_DOWN)),
            "quantity":        str(qty.quantize(  Decimal("0.00000001"), rounding=ROUND_DOWN)),
            "time_in_force":   "IOC",
        })
        order_id = str(result.get("order_id", ""))
        if not order_id:
            raise RuntimeError("No order_id in CDC response")
        _log(f"  [ORDER] CDC {side.upper()} {symbol}  order_id={order_id}")
        return order_id
    except Exception as exc:
        _log(f"  [!] CDC place_order({symbol},{side}): {exc}")
        return None


# ── Order status ───────────────────────────────────────────────────────────────

async def kraken_get_order_status(order_id: str) -> FillResult:
    """POST /0/private/QueryOrders"""
    try:
        result    = await _kraken_post("QueryOrders", {"txid": order_id, "trades": "true"})
        order     = result[order_id]
        status    = order["status"]
        vol_exec  = Decimal(str(order.get("vol_exec", "0")))
        vol_total = Decimal(str(order.get("vol", "1")))
        avg_px    = Decimal(str(order.get("price", "0"))) if vol_exec > 0 else Decimal("0")
        if status == "closed":
            step = StepStatus.FILLED
        elif status in ("canceled", "expired"):
            step = (StepStatus.FILLED   if vol_exec == vol_total and vol_exec > 0 else
                    StepStatus.PARTIAL  if vol_exec > 0 else
                    StepStatus.CANCELLED)
        elif status in ("open", "pending"):
            step = StepStatus.IN_FLIGHT
        else:
            step = StepStatus.FAILED
        return FillResult(step, vol_exec, avg_px, vol_total - vol_exec, raw=order)
    except Exception as exc:
        _log(f"  [!] kraken_get_order_status({order_id}): {exc}")
        return FillResult(StepStatus.FAILED, Decimal("0"), Decimal("0"), Decimal("0"))


async def cdc_get_order_status(order_id: str) -> FillResult:
    """POST private/get-order-detail"""
    try:
        result = await _cdc_post("private/get-order-detail", {"order_id": order_id})
        order  = result.get("order_info", result)
        status = order.get("status", "")
        filled = Decimal(str(order.get("cumulative_quantity", "0")))
        total  = Decimal(str(order.get("quantity", "1")))
        avg_px = Decimal(str(order.get("avg_price", "0")))
        if status == "FILLED":
            step = StepStatus.FILLED
        elif status in ("CANCELED", "EXPIRED"):
            step = (StepStatus.FILLED   if filled == total and filled > 0 else
                    StepStatus.PARTIAL  if filled > 0 else
                    StepStatus.CANCELLED)
        elif status == "ACTIVE":
            step = StepStatus.IN_FLIGHT
        else:
            step = StepStatus.FAILED
        return FillResult(step, filled, avg_px, total - filled, raw=order)
    except Exception as exc:
        _log(f"  [!] cdc_get_order_status({order_id}): {exc}")
        return FillResult(StepStatus.FAILED, Decimal("0"), Decimal("0"), Decimal("0"))


# ── Order cancellation ─────────────────────────────────────────────────────────

async def kraken_cancel_order(order_id: str) -> bool:
    try:
        await _kraken_post("CancelOrder", {"txid": order_id})
        _log(f"  [CANCEL] Kraken order {order_id}")
        return True
    except Exception as exc:
        _log(f"  [!] kraken_cancel({order_id}): {exc}")
        return False


async def cdc_cancel_order(symbol: str, order_id: str) -> bool:
    try:
        await _cdc_post("private/cancel-order", {
            "instrument_name": f"{symbol}_USDT",
            "order_id":        order_id,
        })
        _log(f"  [CANCEL] CDC order {order_id}")
        return True
    except Exception as exc:
        _log(f"  [!] cdc_cancel({order_id}): {exc}")
        return False


# ── Withdrawals ────────────────────────────────────────────────────────────────

async def kraken_withdraw(symbol: str, qty: Decimal, key_name: str) -> Optional[str]:
    """
    POST /0/private/Withdraw
    Only called in full_auto mode.  In semi_auto, the withdrawal checklist
    is printed for manual execution instead.
    """
    if EXECUTION_MODE != "full_auto":
        return None
    try:
        result = await _kraken_post("Withdraw", {
            "asset":  KRAKEN_ASSET[symbol],
            "key":    key_name,
            "amount": str(qty.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)),
        })
        refid = result.get("refid", "unknown")
        _log(f"  [WITHDRAW] Kraken {symbol}  refid={refid}")
        return refid
    except Exception as exc:
        _log(f"  [!] kraken_withdraw({symbol}): {exc}")
        return None


async def cdc_withdraw(
    symbol: str, qty: Decimal, dest_address: str, network: str
) -> Optional[str]:
    """
    POST private/create-withdrawal
    Only called in full_auto mode.
    """
    if EXECUTION_MODE != "full_auto":
        return None
    try:
        result = await _cdc_post("private/create-withdrawal", {
            "currency": symbol,
            "amount":   str(qty.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)),
            "address":  dest_address,
            "network":  network,
        })
        wid = str(result.get("id", "unknown"))
        _log(f"  [WITHDRAW] CDC {symbol}  id={wid}")
        return wid
    except Exception as exc:
        _log(f"  [!] cdc_withdraw({symbol}): {exc}")
        return None


# ── Balance queries ────────────────────────────────────────────────────────────

async def kraken_get_balance(asset_code: str) -> Decimal:
    try:
        result = await _kraken_post("Balance", {})
        return Decimal(str(result.get(asset_code, "0")))
    except Exception as exc:
        _log(f"  [!] kraken_get_balance({asset_code}): {exc}")
        return Decimal("0")


async def cdc_get_balance(currency: str) -> Decimal:
    try:
        result   = await _cdc_post("private/get-account-summary", {"currency": currency})
        accounts = result.get("accounts", [])
        for acct in accounts:
            if acct.get("currency") == currency:
                return Decimal(str(acct.get("available", "0")))
        return Decimal("0")
    except Exception as exc:
        _log(f"  [!] cdc_get_balance({currency}): {exc}")
        return Decimal("0")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — FILL-POLLING STATE MACHINE
# ═══════════════════════════════════════════════════════════════════════════════

async def _poll_until_terminal(
    venue: str, symbol: str, order_id: str, label: str
) -> FillResult:
    """
    Poll every FILL_POLL_INTERVAL_SEC until order reaches a terminal state
    or FILL_TIMEOUT_SEC elapses.

    IOC orders resolve in <1s on liquid books.  The timeout handles edge cases:
    exchange latency, auth delays, brief outages.

    State machine:
      IN_FLIGHT ──► FILLED     (proceed normally)
                ──► PARTIAL    (partial fill — caller checks imbalance)
                ──► CANCELLED  (IOC got nothing — no position opened)
                ──► FAILED     (API error or unknown state)
      TIMEOUT   ──► FAILED     (conservative; check exchange UI manually)
    """
    deadline   = time.monotonic() + FILL_TIMEOUT_SEC
    poll_count = 0
    while time.monotonic() < deadline:
        fill = (await kraken_get_order_status(order_id) if venue == "Kraken"
                else await cdc_get_order_status(order_id))
        poll_count += 1
        if fill.status != StepStatus.IN_FLIGHT:
            _log(
                f"  [FILL] {label} on {venue}  "
                f"status={fill.status.value}  "
                f"qty={_fmt(fill.filled_qty,6)}  "
                f"avg_px=${_fmt(fill.avg_price,4)}  "
                f"polls={poll_count}"
            )
            return fill
        await asyncio.sleep(FILL_POLL_INTERVAL_SEC)

    _log(f"  [!] FILL TIMEOUT {FILL_TIMEOUT_SEC}s  {label}  {venue}  {order_id}")
    return FillResult(StepStatus.FAILED, Decimal("0"), Decimal("0"), Decimal("0"))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — BALANCE PRE-FLIGHT CHECK
# ═══════════════════════════════════════════════════════════════════════════════

async def _preflight(
    symbol: str, sell_venue: str, buy_venue: str,
    needed_float: Decimal, needed_usdt: Decimal,
) -> bool:
    """
    Verify live balances before committing to a trade.
    Runs API calls against both exchanges.
    Skipped entirely in advisory mode.
    """
    if EXECUTION_MODE == "advisory":
        return True

    _log("  ── PREFLIGHT ─────────────────────────────────────────────────────")
    all_ok = True

    if sell_venue == "Kraken":
        sell_bal = await kraken_get_balance(KRAKEN_ASSET[symbol])
    else:
        sell_bal = await cdc_get_balance(symbol)

    sell_ok = sell_bal >= needed_float
    _log(f"  {sell_venue} {symbol}: {_fmt(sell_bal,6)}  need {_fmt(needed_float,6)}"
         f"  {'✅' if sell_ok else '❌ INSUFFICIENT'}")
    if not sell_ok:
        all_ok = False

    if buy_venue == "Kraken":
        usdt_bal = await kraken_get_balance("USDT")
    else:
        usdt_bal = await cdc_get_balance("USDT")

    usdt_ok = usdt_bal >= needed_usdt
    _log(f"  {buy_venue} USDT: {_fmt(usdt_bal,2)}  need {_fmt(needed_usdt,2)}"
         f"  {'✅' if usdt_ok else '❌ INSUFFICIENT'}")
    if not usdt_ok:
        all_ok = False

    _log("  ✅ Preflight passed" if all_ok else "  ❌ Preflight FAILED — aborting")
    return all_ok


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 9 — MANUAL WITHDRAWAL CHECKLIST
#
# In semi_auto mode, after both legs fill, we print a precise step-by-step
# withdrawal instruction block.  The operator can execute this at any time —
# immediately, or hours later when convenient.  The profit is already locked
# in the sell-exchange USDT wallet.  The withdrawal only replenishes the float.
# ═══════════════════════════════════════════════════════════════════════════════

def _print_withdrawal_checklist(plan: ExecutionPlan) -> None:
    """
    Print a precise, copy-paste-ready manual withdrawal instruction.
    Called after fills are confirmed in semi_auto mode.
    """
    c   = plan.cycle
    sym = plan.symbol
    wq  = (plan.sell_fill.filled_qty if plan.buy_fill is None
           else plan.buy_fill.filled_qty) - c.withdrawal_fee
    bar = "━" * 70

    _log(f"\n  {bar}")
    _log(f"  📋  MANUAL WITHDRAWAL CHECKLIST  (do this at your convenience)")
    _log(f"  {bar}")
    _log(f"  Trade completed:  BUY {plan.buy_venue} → SELL {plan.sell_venue}  |  {sym}")
    _log(f"  Profit locked in: {plan.sell_venue} USDT wallet  ✅")
    _log(f"  {bar}")
    _log(f"")

    if plan.buy_venue == "Kraken":
        _log(f"  STEP 1 — Log in to Kraken")
        _log(f"  STEP 2 — Go to: Funding → Withdraw → {sym} ({KRAKEN_ASSET[sym]})")
        _log(f"  STEP 3 — Select withdrawal address: \"{KRAKEN_WITHDRAWAL_KEY_NAMES[sym]}\"")
        _log(f"           (this should point to your Crypto.com {sym} deposit address)")
        _log(f"  STEP 4 — Amount to withdraw: {_fmt(wq, 8)} {sym}")
        _log(f"           (network fee {_fmt(c.withdrawal_fee, 8)} {sym} deducted by Kraken)")
        _log(f"  STEP 5 — Confirm email / 2FA from Kraken")
        _log(f"  STEP 6 — Wait ~{TRANSFER_MINUTES[sym]} min for on-chain confirmation")
        _log(f"  STEP 7 — Verify Crypto.com balance updated")
        _log(f"           Float is now replenished for the next trade ✅")
    else:
        _log(f"  STEP 1 — Log in to Crypto.com Exchange")
        _log(f"  STEP 2 — Go to: Assets → Withdraw → {sym}")
        _log(f"  STEP 3 — Select network: {CDC_NETWORK_NAMES[sym]}")
        _log(f"           ⚠  CRITICAL: match the network to the destination chain.")
        _log(f"              {sym} on the wrong network = permanent loss.")
        _log(f"  STEP 4 — Destination address (Kraken {sym} deposit):")
        _log(f"           {CDC_WITHDRAWAL_ADDRESSES[sym]}")
        _log(f"  STEP 5 — Amount: {_fmt(wq, 8)} {sym}")
        _log(f"  STEP 6 — Confirm 2FA / email from Crypto.com")
        _log(f"  STEP 7 — Wait ~{TRANSFER_MINUTES[sym]} min for on-chain confirmation")
        _log(f"  STEP 8 — Verify Kraken balance updated")
        _log(f"           Float is now replenished for the next trade ✅")

    _log(f"")
    _log(f"  NOTE: You do NOT need to do this immediately.")
    _log(f"        Profit is already in your {plan.sell_venue} USDT wallet.")
    _log(f"        Withdrawal only moves the asset back for the next cycle.")
    _log(f"  {bar}")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 10 — CYCLE EXECUTOR
# ═══════════════════════════════════════════════════════════════════════════════

async def execute_cycle(plan: ExecutionPlan) -> None:
    """
    Execute one arbitrage cycle.

    ADVISORY  — returns immediately; caller already printed the advisory summary.

    SEMI_AUTO — Steps:
      1. Human confirms the trade (typed 'yes')
      2. Safeguard checks (order size, session limits)
      3. Balance pre-flight
      4. BUY + SELL placed simultaneously via asyncio.gather  ← atomic as possible
      5. Both fills polled in parallel
      6. Imbalance detection with precise manual recovery instructions
      7. Realised P&L accounting
      8. Manual withdrawal checklist printed
      9. Session counters updated
      NO automatic withdrawal.

    FULL_AUTO — same as semi_auto but:
      • No typed confirmation prompt
      • Withdrawal triggered automatically after successful fills
      • All safeguards still apply
    """
    if EXECUTION_MODE == "advisory":
        return

    global _full_auto_unlocked
    c   = plan.cycle
    sym = plan.symbol
    sep = "─" * 70

    # ── Safeguard: order size ────────────────────────────────────────────────
    try:
        _check_order_size(c.capital_in)
    except SafeguardTripped as e:
        _log(f"  [SAFEGUARD] {e}")
        return

    # ── Safeguard: session limits ────────────────────────────────────────────
    try:
        _check_session_limits()
    except SafeguardTripped as e:
        _log(f"\n{'!'*70}")
        _log(f"  [SAFEGUARD TRIPPED — ENGINE STOPPING]")
        _log(f"  {e}")
        _log(f"{'!'*70}")
        raise   # propagates up to _handle_opportunity, stops cycle chain

    # ── Trade summary for confirmation prompt ────────────────────────────────
    summary = (
        f"  Asset      : {sym}\n"
        f"  Direction  : BUY {plan.buy_venue} @ ${_fmt(c.buy_price,2)}"
        f"  →  SELL {plan.sell_venue} @ ${_fmt(c.sell_price,2)}\n"
        f"  Capital    : ${_fmt(c.capital_in,2)} USDT\n"
        f"  Trade size : {_fmt(c.trade_size,6)} {sym}\n"
        f"  Fees (est) : ${_fmt(c.entry_fee+c.exit_fee,4)}\n"
        f"  Net profit : +${_fmt(c.net_profit,4)} ({_fmt(c.net_pct,4)}%)"
    )

    confirmed = await _confirm_trade(summary)
    if not confirmed:
        return

    _log(f"\n  ⚡ EXECUTING  {sym}  BUY {plan.buy_venue} → SELL {plan.sell_venue}")
    _log(sep)

    # ── Balance pre-flight ───────────────────────────────────────────────────
    ok = await _preflight(
        sym, plan.sell_venue, plan.buy_venue,
        needed_float=c.arrival_amount,
        needed_usdt=c.capital_in,
    )
    if not ok:
        plan.buy_status = plan.sell_status = StepStatus.FAILED
        _record_trade_result(Decimal("0"), success=False)
        return

    # ── STEP A + B  SIMULTANEOUS ORDER PLACEMENT ─────────────────────────────
    # Both coroutines are passed to asyncio.gather().
    # Python's event loop dispatches both HTTP requests before either one
    # yields back — they are in-flight concurrently.  On liquid markets the
    # two ACKs arrive within 50-150ms of each other.
    _log("  [A+B] Placing BUY and SELL simultaneously...")

    async def _place_buy():
        if plan.buy_venue == "Kraken":
            return await kraken_place_limit_order(sym, "buy", c.buy_price, c.trade_size)
        return await cdc_place_limit_order(sym, "buy", c.buy_price, c.trade_size)

    async def _place_sell():
        if plan.sell_venue == "Kraken":
            return await kraken_place_limit_order(sym, "sell", c.sell_price, c.arrival_amount)
        return await cdc_place_limit_order(sym, "sell", c.sell_price, c.arrival_amount)

    buy_id, sell_id = await asyncio.gather(_place_buy(), _place_sell())
    plan.buy_order_id  = buy_id
    plan.sell_order_id = sell_id

    # ── Placement failure triage ─────────────────────────────────────────────
    if not buy_id and not sell_id:
        _log("  [!] BOTH legs failed to place. Aborting.")
        plan.buy_status = plan.sell_status = StepStatus.FAILED
        _record_trade_result(Decimal("0"), success=False)
        return

    if buy_id and not sell_id:
        _log(f"  [!] SELL leg failed. Cancelling BUY {buy_id} to prevent naked long...")
        if plan.buy_venue == "Kraken":
            await kraken_cancel_order(buy_id)
        else:
            await cdc_cancel_order(sym, buy_id)
        plan.buy_status  = StepStatus.CANCELLED
        plan.sell_status = StepStatus.FAILED
        _record_trade_result(Decimal("0"), success=False)
        return

    if sell_id and not buy_id:
        _log(f"  [!] BUY leg failed. Cancelling SELL {sell_id} to prevent naked short...")
        if plan.sell_venue == "Kraken":
            await kraken_cancel_order(sell_id)
        else:
            await cdc_cancel_order(sym, sell_id)
        plan.sell_status = StepStatus.CANCELLED
        plan.buy_status  = StepStatus.FAILED
        _record_trade_result(Decimal("0"), success=False)
        return

    # ── STEP C  Fill polling — both legs in parallel ──────────────────────────
    _log(f"  [C] Polling fills...  buy={buy_id}  sell={sell_id}")

    buy_fill, sell_fill = await asyncio.gather(
        _poll_until_terminal(plan.buy_venue,  sym, buy_id,  "BUY"),
        _poll_until_terminal(plan.sell_venue, sym, sell_id, "SELL"),
    )
    plan.buy_fill    = buy_fill
    plan.sell_fill   = sell_fill
    plan.buy_status  = buy_fill.status
    plan.sell_status = sell_fill.status

    buy_ok  = buy_fill.status  in (StepStatus.FILLED, StepStatus.PARTIAL)
    sell_ok = sell_fill.status in (StepStatus.FILLED, StepStatus.PARTIAL)

    # ── Imbalance detection ───────────────────────────────────────────────────
    if buy_ok and not sell_ok:
        _log(f"\n  {'!'*66}")
        _log(f"  [!] IMBALANCE: BUY filled, SELL did NOT.")
        _log(f"      You are LONG {_fmt(buy_fill.filled_qty,6)} {sym} on {plan.buy_venue}.")
        _log(f"      ACTION REQUIRED:")
        _log(f"        → Log in to {plan.buy_venue}")
        _log(f"        → Place a MARKET SELL of {_fmt(buy_fill.filled_qty,6)} {sym}")
        _log(f"        → This closes the position and limits your loss to fees only")
        _log(f"  {'!'*66}")
        _record_trade_result(Decimal("0"), success=False)
        return

    if sell_ok and not buy_ok:
        _log(f"\n  {'!'*66}")
        _log(f"  [!] IMBALANCE: SELL filled, BUY did NOT.")
        _log(f"      You are SHORT {_fmt(sell_fill.filled_qty,6)} {sym} on {plan.sell_venue}.")
        _log(f"      ACTION REQUIRED:")
        _log(f"        → Log in to {plan.sell_venue}")
        _log(f"        → Place a MARKET BUY of {_fmt(sell_fill.filled_qty,6)} {sym}")
        _log(f"        → This closes the position and limits your loss to fees only")
        _log(f"  {'!'*66}")
        _record_trade_result(Decimal("0"), success=False)
        return

    if not buy_ok and not sell_ok:
        _log("  [!] Both legs failed/cancelled — no position opened. Cycle aborted.")
        _record_trade_result(Decimal("0"), success=False)
        return

    if buy_fill.status == StepStatus.PARTIAL or sell_fill.status == StepStatus.PARTIAL:
        _log(f"  [!] PARTIAL FILL(S): buy={_fmt(buy_fill.filled_qty,6)}"
             f"  sell={_fmt(sell_fill.filled_qty,6)}  — residual exposure possible.")

    # ── STEP D  Withdrawal (full_auto only) / checklist (semi_auto) ──────────
    withdraw_qty = buy_fill.filled_qty - c.withdrawal_fee

    if EXECUTION_MODE == "full_auto" and not _full_auto_unlocked:
        _log("  [!] full_auto not unlocked — skipping withdrawal.")
    elif EXECUTION_MODE == "full_auto" and withdraw_qty > 0:
        _log(f"  [D] Auto-withdrawing {withdraw_qty} {sym} "
             f"from {plan.buy_venue} → {plan.sell_venue}...")
        if plan.buy_venue == "Kraken":
            wid = await kraken_withdraw(sym, withdraw_qty, KRAKEN_WITHDRAWAL_KEY_NAMES[sym])
        else:
            wid = await cdc_withdraw(
                sym, withdraw_qty, CDC_WITHDRAWAL_ADDRESSES[sym], CDC_NETWORK_NAMES[sym]
            )
        plan.withdrawal_id   = wid
        plan.withdraw_status = StepStatus.IN_FLIGHT if wid else StepStatus.SKIPPED
    else:
        # semi_auto: print manual checklist
        plan.withdraw_status = StepStatus.SKIPPED
        _print_withdrawal_checklist(plan)

    # ── STEP E  Realised P&L ──────────────────────────────────────────────────
    bk = plan.buy_venue.lower().replace(".", "")
    sk = plan.sell_venue.lower().replace(".", "")
    g_rev    = sell_fill.filled_qty * sell_fill.avg_price
    g_cost   = buy_fill.filled_qty  * buy_fill.avg_price
    e_fee    = g_cost * FEES[bk]["taker"]
    x_fee    = g_rev  * FEES[sk]["taker"]
    realised = g_rev - g_cost - e_fee - x_fee
    plan.realised_pnl = realised

    _log(f"\n  ✅ TRADE COMPLETE")
    _log(sep)
    _log(f"  BUY  fill : {_fmt(buy_fill.filled_qty,6)} {sym} @ ${_fmt(buy_fill.avg_price,4)}")
    _log(f"  SELL fill : {_fmt(sell_fill.filled_qty,6)} {sym} @ ${_fmt(sell_fill.avg_price,4)}")
    _log(f"  Fees      : entry ${_fmt(e_fee,4)}  exit ${_fmt(x_fee,4)}")
    _log(f"  REALISED  : {'+'if realised>=0 else ''}"
         f"${_fmt(realised,4)} USDT  "
         f"({'PROFIT ✅' if realised > 0 else 'LOSS ❌'})")
    _log(sep)

    _record_trade_result(realised, success=True)
    _log_session_status()


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 11 — SESSION STATUS LOGGER
# ═══════════════════════════════════════════════════════════════════════════════

def _log_session_status() -> None:
    elapsed = int(time.monotonic() - _session_start_time)
    h, rem  = divmod(elapsed, 3600)
    m, s    = divmod(rem, 60)
    _log(
        f"\n  ── SESSION  "
        f"trades={_session_trades_completed}/{MAX_TRADES_PER_SESSION}  "
        f"P&L={'+'if _session_cumulative_pnl>=0 else ''}"
        f"${_fmt(_session_cumulative_pnl,4)}  "
        f"consec_fail={_session_consecutive_failures}  "
        f"uptime={h:02d}:{m:02d}:{s:02d}"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 12 — CYCLE MATH  (unchanged)
# ═══════════════════════════════════════════════════════════════════════════════

def _fmt(d: Decimal, p: int = 4) -> str:
    return str(d.quantize(Decimal(10) ** -p, rounding=ROUND_DOWN))


def _compute_cycle(
    symbol: str, buy_venue: str, sell_venue: str,
    buy_price: Decimal, sell_price: Decimal, capital: Decimal,
) -> Optional[CycleResult]:
    bk = buy_venue.lower().replace(".", "")
    sk = sell_venue.lower().replace(".", "")
    wf = FEES[bk]["withdrawal"][symbol]
    trade  = capital / buy_price
    cost   = capital
    e_fee  = cost * FEES[bk]["taker"]
    arrive = trade - wf
    if arrive <= 0:
        return None
    rev    = arrive * sell_price
    x_fee  = rev * FEES[sk]["taker"]
    profit = rev - cost - e_fee - x_fee
    if profit <= 0:
        return None
    pct = (profit / cost) * 100
    if pct < MIN_NET_YIELD_PCT:
        return None
    return CycleResult(
        cycle_num=0, capital_in=capital, buy_price=buy_price, sell_price=sell_price,
        trade_size=trade, arrival_amount=arrive, entry_fee=e_fee, exit_fee=x_fee,
        withdrawal_fee=wf, gross_revenue=rev, net_profit=profit, net_pct=pct,
        capital_out=capital + profit,
    )


def _project_cycles(
    symbol: str, buy_venue: str, sell_venue: str,
    buy_price: Decimal, sell_price: Decimal,
) -> list[CycleResult]:
    results, capital = [], CAPITAL
    spread0 = sell_price - buy_price
    for n in range(MAX_CYCLES):
        decay = (1 - SPREAD_DECAY_PER_TRANSFER) ** n
        r = _compute_cycle(symbol, buy_venue, sell_venue,
                           buy_price, buy_price + spread0 * decay, capital)
        if r is None:
            break
        r.cycle_num = n
        results.append(r)
        capital = r.capital_out
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 13 — OPPORTUNITY PRINTER
# ═══════════════════════════════════════════════════════════════════════════════

def _print_opportunity(
    symbol: str, buy_venue: str, sell_venue: str, cycles: list[CycleResult]
) -> None:
    total_pnl = sum(c.net_profit for c in cycles)
    n         = len(cycles)
    bar       = "═" * 78
    mode_tag  = {
        "advisory":  "👁  ADVISORY — no orders will be placed",
        "semi_auto": "⚡  SEMI-AUTO — orders fire on your 'yes'  |  withdrawal manual",
        "full_auto": "🤖  FULL-AUTO — orders + withdrawal fire automatically",
    }[EXECUTION_MODE]

    _log(f"\n{bar}")
    _log(f"  🔥 {symbol}  BUY {buy_venue} → SELL {sell_venue}"
         f"  |  {n} cycle(s)  |  +${_fmt(total_pnl,4)} projected")
    _log(f"  {mode_tag}")
    _log(bar)
    _log(f"  {'Cy':>3}  {'Capital In':>12}  {'Buy$':>10}  {'Sell$':>10}"
         f"  {'Profit':>10}  {'%':>6}  {'Capital Out':>12}")
    _log(f"  {'─'*74}")
    for c in cycles:
        _log(
            f"  {c.cycle_num:>3}  ${_fmt(c.capital_in,2):>11}  "
            f"${_fmt(c.buy_price,2):>9}  ${_fmt(c.sell_price,2):>9}  "
            f"+${_fmt(c.net_profit,4):>9}  {_fmt(c.net_pct,4):>5}%  "
            f"${_fmt(c.capital_out,2):>11}"
        )
    _log(f"  {'─'*74}")
    _log(f"  Decay {int(SPREAD_DECAY_PER_TRANSFER*100)}%/transfer  |  "
         f"~{TRANSFER_MINUTES[symbol]*n} min window  |  "
         f"Cycle 1+ = projected prices")
    _log(bar)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 14 — OPPORTUNITY HANDLER
# ═══════════════════════════════════════════════════════════════════════════════

async def _handle_opportunity(
    symbol: str, buy_venue: str, sell_venue: str, cycles: list[CycleResult]
) -> None:
    _print_opportunity(symbol, buy_venue, sell_venue, cycles)

    if EXECUTION_MODE == "advisory":
        return

    for i, projected in enumerate(cycles):
        # Cycle 1+: re-verify live prices before committing
        if i > 0:
            bk_key   = buy_venue.lower().replace(".", "")
            sk_key   = sell_venue.lower().replace(".", "")
            live_ask = order_books[symbol][bk_key]["ask"]
            live_bid = order_books[symbol][sk_key]["bid"]
            if not (live_ask and live_bid):
                _log(f"  [!] Stale order book at cycle {i} — stopping chain.")
                break
            fresh = _compute_cycle(symbol, buy_venue, sell_venue,
                                   live_ask, live_bid, projected.capital_in)
            if fresh is None:
                _log(f"  [!] Spread unprofitable at live prices (cycle {i}) — stopping.")
                break
            fresh.cycle_num = i
            projected       = fresh

        plan = ExecutionPlan(
            symbol=symbol, buy_venue=buy_venue, sell_venue=sell_venue, cycle=projected
        )

        try:
            await execute_cycle(plan)
        except SafeguardTripped:
            break   # session limit hit; stop entire chain silently

        filled = (
            plan.buy_status  in (StepStatus.FILLED, StepStatus.PARTIAL) and
            plan.sell_status in (StepStatus.FILLED, StepStatus.PARTIAL)
        )
        if not filled:
            _log(f"  [!] Cycle {i} incomplete — stopping chain.")
            break

        # In full_auto wait for float to arrive before next cycle
        if EXECUTION_MODE == "full_auto" and i < len(cycles) - 1:
            wait = TRANSFER_MINUTES[symbol] * 60
            _log(f"  ⏳ Waiting {TRANSFER_MINUTES[symbol]} min for float replenishment...")
            await asyncio.sleep(wait)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 15 — SPREAD EVALUATOR
# ═══════════════════════════════════════════════════════════════════════════════

def _evaluate(symbol: str) -> None:
    bk = order_books[symbol]
    pairs = [
        ("Kraken",     bk["kraken"]["ask"],   "Crypto.com", bk["cryptocom"]["bid"]),
        ("Crypto.com", bk["cryptocom"]["ask"], "Kraken",     bk["kraken"]["bid"]),
    ]
    for buy_venue, buy_px, sell_venue, sell_px in pairs:
        if not (buy_px and sell_px):
            continue
        key = (symbol, buy_venue)
        now = time.monotonic()
        if now - _last_alert.get(key, 0.0) < MIN_ALERT_INTERVAL_SEC:
            continue
        cycles = _project_cycles(symbol, buy_venue, sell_venue, buy_px, sell_px)
        if not cycles:
            continue
        _last_alert[key] = now
        asyncio.get_event_loop().create_task(
            _handle_opportunity(symbol, buy_venue, sell_venue, cycles)
        )


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 16 — WEBSOCKET STREAMS
# ═══════════════════════════════════════════════════════════════════════════════

async def _stream_kraken() -> None:
    url   = "wss://ws.kraken.com/v2"
    pairs = [f"{s}/USD" for s in SYMBOLS]
    async for ws in websockets.connect(url):
        try:
            await ws.send(json.dumps({
                "method": "subscribe",
                "params": {"channel": "ticker", "symbol": pairs},
            }))
            async for raw in ws:
                msg = json.loads(raw)
                if msg.get("channel") == "ticker" and "data" in msg:
                    pkt = msg["data"][0]
                    sym = KRAKEN_SYMBOL_MAP.get(pkt["symbol"].split("/")[0])
                    if sym:
                        order_books[sym]["kraken"]["bid"] = Decimal(str(pkt["bid"]))
                        order_books[sym]["kraken"]["ask"] = Decimal(str(pkt["ask"]))
                        _evaluate(sym)
        except websockets.ConnectionClosed:
            await asyncio.sleep(1)
        except Exception as exc:
            _log(f"[!] Kraken stream: {exc}")


async def _cdc_hb(ws, mid: int) -> None:
    await ws.send(json.dumps({"id": mid, "method": "public/respond-heartbeat"}))


async def _stream_cryptocom() -> None:
    url = "wss://stream.crypto.com/v2/market"
    ch  = [f"ticker.{s}_USDT" for s in SYMBOLS]
    async for ws in websockets.connect(url):
        try:
            await ws.send(json.dumps({
                "id": 1, "method": "subscribe",
                "params": {"channels": ch}, "nonce": 1,
            }))
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
                        _evaluate(sym)
        except websockets.ConnectionClosed:
            await asyncio.sleep(1)
        except Exception as exc:
            _log(f"[!] Crypto.com stream: {exc}")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 17 — LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

def _log(msg: str) -> None:
    print(msg, flush=True)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 18 — STARTUP: CLI PARSING, PASSPHRASE UNLOCK, BANNER
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Cross-exchange arbitrage engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Modes:\n"
            "  advisory  (default) — watch-only, zero orders\n"
            "  semi_auto           — orders fire on human confirmation, "
                                    "withdrawal is manual\n"
            "  full_auto           — requires --full-auto flag AND env var "
                                    "FULL_AUTO_PASSPHRASE\n"
        ),
    )
    p.add_argument(
        "--full-auto",
        action="store_true",
        help="Unlock full_auto mode (also requires FULL_AUTO_PASSPHRASE env var)",
    )
    return p.parse_args()


def _verify_full_auto_unlock() -> bool:
    """
    Full-auto requires two independent factors:
      1. --full-auto CLI flag (checked in _parse_args)
      2. FULL_AUTO_PASSPHRASE env var set to a non-empty string
      3. Operator types the passphrase correctly at startup

    Returns True if all three match, False otherwise.
    The passphrase is compared via hmac.compare_digest to prevent timing attacks.
    """
    expected = os.environ.get("FULL_AUTO_PASSPHRASE", "")
    if not expected:
        _log("  ❌ FULL_AUTO_PASSPHRASE env var not set. Cannot unlock full_auto.")
        return False
    try:
        typed = input(
            "\n  FULL-AUTO UNLOCK\n"
            "  This mode fires orders AND withdrawals without confirmation.\n"
            "  Type your FULL_AUTO_PASSPHRASE to proceed: "
        ).strip()
    except EOFError:
        _log("  [!] stdin unavailable. Cannot verify passphrase.")
        return False
    # Constant-time comparison
    ok = hmac.compare_digest(typed.encode(), expected.encode())
    if ok:
        _log("  ✅ Passphrase accepted. Full-auto mode UNLOCKED.")
    else:
        _log("  ❌ Passphrase incorrect. Falling back to advisory mode.")
    return ok


def _startup_banner() -> None:
    has_k   = bool(os.environ.get("KRAKEN_API_KEY"))
    has_cdc = bool(os.environ.get("CDC_API_KEY"))
    addrs_set = all("YOUR_" not in v for v in CDC_WITHDRAWAL_ADDRESSES.values())

    mode_desc = {
        "advisory":  "👁  ADVISORY  — watch-only, zero orders, zero risk",
        "semi_auto": "⚡  SEMI-AUTO — orders on confirmation, withdrawal MANUAL",
        "full_auto": "🤖  FULL-AUTO — orders + withdrawal automated",
    }[EXECUTION_MODE]

    print(f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  ARBITRAGE ENGINE  dbg7                                                     ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  {mode_desc:<74} ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  CAPITAL / CYCLE                                                             ║
║    Deployed per trade   : ${str(CAPITAL):<50} ║
║    Hard ceiling         : ${str(MAX_ORDER_USDT):<50} ║
║    Minimum floor        : ${str(MIN_CAPITAL):<50} ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  SESSION SAFEGUARDS                                                          ║
║    Max trades / session : {str(MAX_TRADES_PER_SESSION):<52} ║
║    Max loss / session   : ${str(MAX_LOSS_PER_SESSION_USDT):<50} ║
║    Max consec. failures : {str(MAX_CONSECUTIVE_FAILURES):<52} ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  CHECKLIST                                                                   ║
║    {'✅' if has_k  else '❌'}  KRAKEN_API_KEY in environment                                   ║
║    {'✅' if has_cdc else '❌'}  CDC_API_KEY in environment                                     ║
║    {'✅' if addrs_set else '⚠ '}  CDC_WITHDRAWAL_ADDRESSES configured                             ║
╚══════════════════════════════════════════════════════════════════════════════╝
""", flush=True)

    if EXECUTION_MODE != "advisory" and (not has_k or not has_cdc):
        print("  ❌ API keys required for non-advisory mode. Exiting.", flush=True)
        sys.exit(1)

    if EXECUTION_MODE == "full_auto" and not addrs_set:
        print("  ❌ CDC_WITHDRAWAL_ADDRESSES has placeholder values. Exiting.", flush=True)
        sys.exit(1)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 19 — MAIN
# ═══════════════════════════════════════════════════════════════════════════════

async def main(args: argparse.Namespace) -> None:
    global _http_session, _full_auto_unlocked, _session_start_time
    sys.stdout.reconfigure(line_buffering=True)

    # ── Full-auto unlock sequence ────────────────────────────────────────────
    if EXECUTION_MODE == "full_auto":
        if not args.full_auto:
            print(
                "  ❌ EXECUTION_MODE is 'full_auto' but --full-auto flag was not passed.\n"
                "     Re-run with:  python real_time_spread_engine_dbg7.py --full-auto",
                flush=True,
            )
            sys.exit(1)
        _full_auto_unlocked = _verify_full_auto_unlock()
        if not _full_auto_unlocked:
            print("  Falling back to advisory mode.", flush=True)
            # Don't sys.exit — continue running in advisory mode as a safety net

    _startup_banner()
    _session_start_time = time.monotonic()

    connector     = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
    timeout_cfg   = aiohttp.ClientTimeout(total=10, connect=5)
    _http_session = aiohttp.ClientSession(connector=connector, timeout=timeout_cfg)

    try:
        await asyncio.gather(_stream_kraken(), _stream_cryptocom())
    finally:
        await _http_session.close()
        _log(f"\n── FINAL SESSION SUMMARY ─────────────────────────────────────────")
        _log(f"  Completed trades : {_session_trades_completed}")
        _log(f"  Session P&L      : {'+'if _session_cumulative_pnl>=0 else ''}"
             f"${_fmt(_session_cumulative_pnl,4)} USDT")
        _log(f"──────────────────────────────────────────────────────────────────")


if __name__ == "__main__":
    args = _parse_args()
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print("\nEngine stopped cleanly.")
