"""
augmented_ema_scanner_rest_ohlc.py

EMA + S/R + VOLUME + RR SCANNER (REST OHLC, Kraken/Crypto.com)
==============================================================

Conceptual stack (EmperorBTC-style):

1) Trend filter via EMAs (20/50)
2) Structure: HH/HL or LH/LL
3) S/R confluence (recent swing highs/lows)
4) Volume confirmation (avoid weak fakeouts)
5) RR estimation (entry → target vs entry → stop)
6) Asset-specific behavior (BTC, ETH, SOL)
7) REST OHLC from Kraken + Crypto.com (near real-time)
8) Telegram alerts for clean signals

THEORY NOTES (embedded):

- BTC:
  Often respects higher timeframes (4H, 1D).
  EMA 20/50 on 4H is strong for swing trend.
  Use wider stops (volatility) and higher RR (2R+).

- ETH:
  Slightly more volatile than BTC.
  EMA 20/50 on 4H and 1H both useful.
  Ideally confirm ETH/BTC relative strength (not implemented here, but recommended).

- SOL:
  Much more volatile.
  EMA 20/50 on 1H or even 15m for active trading.
  You must demand higher RR (3R+) and be stricter with volume confirmation.

- Fakeouts:
  Common when EMAs are flat, volume is weak, and structure (HH/HL or LH/LL) is missing.
  We explicitly detect these conditions and flag high fakeout risk.
"""

import pandas as pd
import numpy as np
import requests
import math
import time
import asyncio
from dataclasses import dataclass
from typing import Optional, Dict, Tuple, List

# ─────────────────────────────────────────────────────────────
# PER-SYMBOL CONFIG (TIMEFRAME, RR, VOLUME)
# ─────────────────────────────────────────────────────────────

"""
We encode the theory directly into per-symbol config:

- BTC: higher TF (e.g., 240m = 4H), RR >= 2, moderate volume multiplier
- ETH: similar, but can also use 60m; here we keep 240m for swing
- SOL: more volatile, we use 60m and demand RR >= 3 and stronger volume
"""

SYMBOL_CONFIG = {
    "BTC": {
        "kraken_pair": "XBTUSD",
        "crypto_inst": "BTC_USDT",
        "interval_minutes": 240,   # 4H
        "rr_min": 2.0,
        "vol_multiplier": 1.5,
    },
    "ETH": {
        "kraken_pair": "ETHUSD",
        "crypto_inst": "ETH_USDT",
        "interval_minutes": 240,   # 4H (could also run a 60m scanner separately)
        "rr_min": 2.0,
        "vol_multiplier": 1.5,
    },
    "SOL": {
        "kraken_pair": "SOLUSD",
        "crypto_inst": "SOL_USDT",
        "interval_minutes": 60,    # 1H (more reactive for volatile SOL)
        "rr_min": 3.0,
        "vol_multiplier": 2.0,     # stricter volume requirement
    },
}

FAST_EMA = 20
SLOW_EMA = 50

VOL_LOOKBACK = 20
SWING_LOOKBACK = 20
SR_PROXIMITY_PCT = 0.5  # 0.5%

TELEGRAM_BOT_TOKEN = "YOUR_BOT_TOKEN"
TELEGRAM_CHAT_ID = "YOUR_CHAT_ID"

# ─────────────────────────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────────────────────────

@dataclass
class EMASignal:
    symbol: str
    direction: str  # "bull" or "bear"
    entry_price: float
    stop_price: float
    target_price: float
    rr: float
    at_sr: bool
    vol_ok: bool
    fakeout_risk: bool
    comment: str


# ─────────────────────────────────────────────────────────────
# INDICATORS + THEORY
# ─────────────────────────────────────────────────────────────

def compute_ema(df: pd.DataFrame, fast: int = FAST_EMA, slow: int = SLOW_EMA) -> pd.DataFrame:
    """
    EMA (Exponential Moving Average):

    - Gives more weight to recent candles.
    - Reacts faster than SMA.
    - Used here as:
        * Trend filter (20/50 alignment)
        * Dynamic support/resistance (price respecting EMAs).
    """
    df = df.copy()
    df["ema_fast"] = df["close"].ewm(span=fast, adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=slow, adjust=False).mean()
    return df


def detect_ema_crossovers(df: pd.DataFrame) -> pd.DataFrame:
    """
    EMA crossovers:

    - Bullish: fast crosses ABOVE slow → recent momentum stronger than older.
    - Bearish: fast crosses BELOW slow → recent weakness vs older.

    We detect the actual cross event by comparing previous vs current EMA relationship.
    """
    df = df.copy()
    df["prev_fast"] = df["ema_fast"].shift(1)
    df["prev_slow"] = df["ema_slow"].shift(1)

    df["bull_cross"] = (df["prev_fast"] < df["prev_slow"]) & (df["ema_fast"] > df["ema_slow"])
    df["bear_cross"] = (df["prev_fast"] > df["prev_slow"]) & (df["ema_fast"] < df["ema_slow"])
    return df


def compute_volume_filter(df: pd.DataFrame, vol_multiplier: float) -> pd.DataFrame:
    """
    Volume filter:

    - Fakeouts often occur on weak volume.
    - We require current volume >= vol_multiplier * average(volume over last VOL_LOOKBACK bars).
    - For SOL (more volatile), we use a higher multiplier to demand stronger confirmation.
    """
    df = df.copy()
    df["vol_ma"] = df["volume"].rolling(VOL_LOOKBACK).mean()
    df["vol_ok"] = df["volume"] >= (vol_multiplier * df["vol_ma"])
    return df


def find_swing_levels(df: pd.DataFrame, lookback: int = SWING_LOOKBACK) -> Tuple[float, float]:
    """
    Simple S/R approximation:

    - Support: recent swing low (min low over lookback).
    - Resistance: recent swing high (max high over lookback).

    This encodes the idea of trading EMA signals at meaningful levels,
    not in the middle of nowhere.
    """
    recent = df.tail(lookback)
    support = recent["low"].min()
    resistance = recent["high"].max()
    return support, resistance


def is_near_level(price: float, level: float, pct: float = SR_PROXIMITY_PCT) -> bool:
    """
    Check if price is within pct% of a given level.

    This enforces S/R proximity:
    - For longs, we want entries near support.
    - For shorts, near resistance.
    """
    if level == 0:
        return False
    diff_pct = abs(price - level) / level * 100
    return diff_pct <= pct


def estimate_rr(
    direction: str,
    entry: float,
    support: float,
    resistance: float,
) -> Tuple[float, float, float]:
    """
    RR estimation based on recent swing high/low:

    - LONG:
        stop below support
        target near resistance
        RR = (target - entry) / (entry - stop)

    - SHORT:
        stop above resistance
        target near support
        RR = (entry - target) / (stop - entry)

    This encodes the EmperorBTC idea:
    "Do not enter without knowing RR; high RR can compensate for lower win-rate."
    """
    if direction == "bull":
        stop = support
        target = resistance
        if entry <= stop or target <= entry:
            return math.nan, stop, target
        rr = (target - entry) / (entry - stop)
    else:
        stop = resistance
        target = support
        if entry >= stop or target >= entry:
            return math.nan, stop, target
        rr = (entry - target) / (stop - entry)
    return rr, stop, target


def detect_fakeout_risk(df: pd.DataFrame, idx: int, direction: str) -> bool:
    """
    Fakeout detection heuristic:

    Fakeouts are common when:
    - EMAs are flat (no real trend).
    - Volume is weak (no participation).
    - Structure is not aligned (no HH/HL for bull, no LH/LL for bear).

    We approximate:
    - Flat EMAs via small EMA slope.
    - Structure via recent highs/lows.
    - Volume via vol_ok flag.
    """
    if idx < 2:
        return True  # not enough history

    row = df.iloc[idx]
    prev = df.iloc[idx - 1]
    prev2 = df.iloc[idx - 2]

    slope_fast = row["ema_fast"] - prev["ema_fast"]
    slope_slow = row["ema_slow"] - prev["ema_slow"]

    flat_fast = abs(slope_fast) < (row["ema_fast"] * 0.0005)
    flat_slow = abs(slope_slow) < (row["ema_slow"] * 0.0005)

    vol_ok = bool(row.get("vol_ok", False))

    if direction == "bull":
        hh = (row["high"] > prev["high"]) and (prev["high"] > prev2["high"])
        hl = (row["low"] > prev["low"]) and (prev["low"] > prev2["low"])
        structure_ok = hh or hl
    else:
        lh = (row["high"] < prev["high"]) and (prev["high"] < prev2["high"])
        ll = (row["low"] < prev["low"]) and (prev["low"] < prev2["low"])
        structure_ok = lh or ll

    fakeout = (flat_fast and flat_slow) or (not vol_ok) or (not structure_ok)
    return fakeout


# ─────────────────────────────────────────────────────────────
# SIGNAL GENERATION
# ─────────────────────────────────────────────────────────────

def generate_ema_signals(df: pd.DataFrame, symbol: str) -> List[EMASignal]:
    """
    Full EMA strategy for a given symbol:

    1) Compute EMAs (20/50).
    2) Detect crossovers (bull/bear).
    3) Apply volume filter (symbol-specific multiplier).
    4) Find S/R levels (recent swings).
    5) Estimate RR (entry vs stop vs target).
    6) Enforce symbol-specific RR minimum:
        - BTC, ETH: RR >= 2
        - SOL: RR >= 3 (more volatile, demand more edge).
    7) Flag fakeout risk (flat EMAs, weak volume, bad structure).

    BTC:
      - Higher TF (4H) EMA 20/50.
      - Wider stops, RR >= 2.

    ETH:
      - Similar to BTC, slightly more volatile.

    SOL:
      - 1H EMA 20/50.
      - RR >= 3, strong volume required.
    """
    cfg = SYMBOL_CONFIG[symbol]
    rr_min = cfg["rr_min"]
    vol_mult = cfg["vol_multiplier"]

    df = compute_ema(df)
    df = detect_ema_crossovers(df)
    df = compute_volume_filter(df, vol_mult)

    support, resistance = find_swing_levels(df)

    signals: List[EMASignal] = []

    for i in range(len(df)):
        row = df.iloc[i]
        if not row["bull_cross"] and not row["bear_cross"]:
            continue

        direction = "bull" if row["bull_cross"] else "bear"
        entry = row["close"]

        rr, stop, target = estimate_rr(direction, entry, support, resistance)
        if math.isnan(rr) or rr < rr_min:
            # RR too low → skip (theory: don't take low-RR trades)
            continue

        if direction == "bull":
            at_sr = is_near_level(entry, support, SR_PROXIMITY_PCT)
        else:
            at_sr = is_near_level(entry, resistance, SR_PROXIMITY_PCT)

        vol_ok = bool(row.get("vol_ok", False))
        fakeout_risk = detect_fakeout_risk(df, i, direction)

        comment_parts = []
        comment_parts.append(f"{symbol} {direction.upper()} EMA {FAST_EMA}/{SLOW_EMA} crossover")
        comment_parts.append(f"RR={rr:.2f} (min required {rr_min:.1f})")
        if at_sr:
            comment_parts.append("Near S/R (good confluence)")
        else:
            comment_parts.append("Not near S/R (weaker confluence)")
        if vol_ok:
            comment_parts.append("Volume OK (strong move)")
        else:
            comment_parts.append("Volume weak (possible fakeout)")
        if fakeout_risk:
            comment_parts.append("FAKEOUT RISK HIGH")
        else:
            comment_parts.append("Structure aligned (trend-confirmed)")

        signal = EMASignal(
            symbol=symbol,
            direction=direction,
            entry_price=float(entry),
            stop_price=float(stop),
            target_price=float(target),
            rr=float(rr),
            at_sr=at_sr,
            vol_ok=vol_ok,
            fakeout_risk=fakeout_risk,
            comment=" | ".join(comment_parts),
        )
        signals.append(signal)

    return signals


# ─────────────────────────────────────────────────────────────
# TELEGRAM ALERTS
# ─────────────────────────────────────────────────────────────

def send_telegram_alert(signal: EMASignal):
    """
    Telegram alerts for clean EMA + S/R + Volume setups.

    We only alert when:
    - RR >= symbol-specific minimum.
    - at_sr == True (near S/R).
    - vol_ok == True (volume confirmation).
    - fakeout_risk == False (trend + structure aligned).

    This encodes the idea of trading only high-confluence setups.
    """
    if TELEGRAM_BOT_TOKEN == "YOUR_BOT_TOKEN":
        return  # not configured

    if not (signal.at_sr and signal.vol_ok and not signal.fakeout_risk):
        return

    text = (
        f"{signal.symbol} {signal.direction.upper()} EMA {FAST_EMA}/{SLOW_EMA} setup\n"
        f"Entry: {signal.entry_price}\n"
        f"Stop: {signal.stop_price}\n"
        f"Target: {signal.target_price}\n"
        f"RR: {signal.rr:.2f}\n"
        f"{signal.comment}"
    )

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
    try:
        requests.post(url, data=payload, timeout=5)
    except Exception as e:
        print(f"[Telegram] Error sending alert: {e}")


# ─────────────────────────────────────────────────────────────
# REST OHLC FETCHERS (KRAKEN + CRYPTO.COM)
# ─────────────────────────────────────────────────────────────

def fetch_ohlcv_kraken(pair: str, interval_minutes: int, limit: int = 200) -> pd.DataFrame:
    """
    Fetch OHLC from Kraken REST:

    Endpoint: https://api.kraken.com/0/public/OHLC
    Params:
      - pair: e.g., XBTUSD, ETHUSD, SOLUSD
      - interval: minutes (1, 5, 15, 60, 240, 1440, etc.)

    Returns a DataFrame with columns:
      ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    """
    url = "https://api.kraken.com/0/public/OHLC"
    params = {"pair": pair, "interval": interval_minutes}
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    if data.get("error"):
        raise RuntimeError(f"Kraken error: {data['error']}")

    result = list(data["result"].values())[0]  # first key is pair, second is 'last'
    rows = result[-limit:]

    records = []
    for r in rows:
        # r: [time, open, high, low, close, vwap, volume, count]
        ts, o, h, l, c, vwap, vol, count = r
        records.append(
            {
                "timestamp": int(ts),
                "open": float(o),
                "high": float(h),
                "low": float(l),
                "close": float(c),
                "volume": float(vol),
            }
        )

    df = pd.DataFrame(records)
    return df


def fetch_ohlcv_crypto(instrument: str, timeframe: str = "1h", limit: int = 200) -> pd.DataFrame:
    """
    Fetch OHLC from Crypto.com REST:

    Endpoint: https://api.crypto.com/v2/public/get-candlestick
    Params:
      - instrument_name: e.g., BTC_USDT
      - timeframe: e.g., 1m, 5m, 15m, 1h, 4h, 1d

    Returns a DataFrame with columns:
      ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    """
    url = "https://api.crypto.com/v2/public/get-candlestick"
    params = {"instrument_name": instrument, "timeframe": timeframe}
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    if data.get("code") != 0:
        raise RuntimeError(f"Crypto.com error: {data}")

    rows = data["result"]["data"]
    rows = sorted(rows, key=lambda x: x["t"])[-limit:]

    records = []
    for r in rows:
        # r: { "t": ts_ms, "o":..., "h":..., "l":..., "c":..., "v":... }
        records.append(
            {
                "timestamp": int(r["t"] // 1000),
                "open": float(r["o"]),
                "high": float(r["h"]),
                "low": float(r["l"]),
                "close": float(r["c"]),
                "volume": float(r["v"]),
            }
        )

    df = pd.DataFrame(records)
    return df


# ─────────────────────────────────────────────────────────────
# LIVE SCANNER LOOP (REST-BASED, NEAR REAL-TIME)
# ─────────────────────────────────────────────────────────────

def timeframe_to_kraken_interval(minutes: int) -> int:
    """
    Map generic minutes to Kraken interval codes.
    Kraken supports: 1,5,15,30,60,240,1440,10080,21600
    """
    if minutes in [1, 5, 15, 30, 60, 240, 1440, 10080, 21600]:
        return minutes
    # Fallback: round to nearest supported
    candidates = [1, 5, 15, 30, 60, 240, 1440, 10080, 21600]
    return min(candidates, key=lambda x: abs(x - minutes))


def timeframe_to_crypto_tf(minutes: int) -> str:
    """
    Map minutes to Crypto.com timeframe strings.
    Common: 1m, 5m, 15m, 30m, 1h, 4h, 1d
    """
    mapping = {
        1: "1m",
        5: "5m",
        15: "15m",
        30: "30m",
        60: "1h",
        240: "4h",
        1440: "1d",
    }
    if minutes in mapping:
        return mapping[minutes]
    # Fallback: nearest
    candidates = list(mapping.keys())
    best = min(candidates, key=lambda x: abs(x - minutes))
    return mapping[best]


async def live_scanner_loop(poll_seconds: int = 60):
    """
    Live EMA scanner loop (REST-based, near real-time):

    - For each symbol (BTC, ETH, SOL):
        - Fetch OHLC from Kraken (primary).
        - Optionally cross-check with Crypto.com (not required for EMA logic).
        - Run EMA + S/R + Volume + RR + fakeout logic.
        - Emit Telegram alerts for clean setups.

    This is NOT tick-level HFT; it's near real-time swing/intraday scanning,
    which is perfectly fine for EMA-based strategies.
    """
    while True:
        for symbol, cfg in SYMBOL_CONFIG.items():
            try:
                kraken_pair = cfg["kraken_pair"]
                interval_min = cfg["interval_minutes"]

                kraken_interval = timeframe_to_kraken_interval(interval_min)
                df = fetch_ohlcv_kraken(kraken_pair, kraken_interval, limit=200)

                if df is None or df.empty:
                    print(f"[{symbol}] No data from Kraken.")
                    continue

                signals = generate_ema_signals(df, symbol)
                for sig in signals:
                    print(f"[SIGNAL] {sig.comment}")
                    send_telegram_alert(sig)

            except Exception as e:
                print(f"[ERROR] live_scanner_loop {symbol}: {e}")

        await asyncio.sleep(poll_seconds)


if __name__ == "__main__":
    # Offline example:
    # df_btc = pd.read_csv("btc_ohlcv.csv")
    # signals_btc = generate_ema_signals(df_btc, "BTC")
    # for s in signals_btc:
    #     print(s.comment)

    # Live REST-based scanner:
    asyncio.run(live_scanner_loop(poll_seconds=60))
