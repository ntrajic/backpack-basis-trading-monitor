"""
file: sugmented_ema_scanner.py

EMA + S/R + VOLUME + RR SCANNER
================================

Conceptual stack (EmperorBTC-style):

1) Trend filter via EMAs (20/50)
2) Structure: HH/HL or LH/LL
3) S/R confluence (recent swing highs/lows)
4) Volume confirmation (avoid weak fakeouts)
5) RR estimation (entry → target vs entry → stop)
6) Asset-specific behavior (BTC, ETH, SOL)
7) Ready to plug into live feeds (Kraken/Crypto)
8) Telegram alerts for clean signals

This script is written to be run on OHLCV dataframes for BTC/ETH/SOL,
but the structure is ready to be wired to live data (Kraken/Crypto).

"""

import pandas as pd
import numpy as np
import requests
import time
from dataclasses import dataclass
from typing import Optional, Dict, Tuple, List
import math
import asyncio
import aiohttp

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

FAST_EMA = 20
SLOW_EMA = 50

# Volume filter: require current volume to be at least X * average of last N bars
VOL_LOOKBACK = 20
VOL_MULTIPLIER = 1.5

# S/R detection: lookback for swing highs/lows
SWING_LOOKBACK = 20

# Proximity threshold: how close price must be to S/R to count as "at level"
SR_PROXIMITY_PCT = 0.5  # 0.5% from level

# RR minimums per asset (reflecting volatility)
RR_MIN = {
    "BTC": 2.0,   # BTC: higher TF, wider stops, 2R+
    "ETH": 2.0,   # ETH: similar, but more volatile
    "SOL": 3.0,   # SOL: much more volatile → demand 3R+
}

# Telegram (optional)
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
# CORE INDICATOR LOGIC
# ─────────────────────────────────────────────────────────────

def compute_ema(df: pd.DataFrame, fast: int = FAST_EMA, slow: int = SLOW_EMA) -> pd.DataFrame:
    """
    Compute fast and slow EMAs.

    EMA is an Exponential Moving Average:
    - Gives more weight to recent candles
    - Reacts faster than SMA
    - Used here as trend filter + dynamic S/R
    """
    df = df.copy()
    df["ema_fast"] = df["close"].ewm(span=fast, adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=slow, adjust=False).mean()
    return df


def detect_ema_crossovers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect EMA crossovers:
    - Bullish: fast crosses ABOVE slow
    - Bearish: fast crosses BELOW slow

    We also keep previous values to detect the actual cross event.
    """
    df = df.copy()
    df["prev_fast"] = df["ema_fast"].shift(1)
    df["prev_slow"] = df["ema_slow"].shift(1)

    df["bull_cross"] = (df["prev_fast"] < df["prev_slow"]) & (df["ema_fast"] > df["ema_slow"])
    df["bear_cross"] = (df["prev_fast"] > df["prev_slow"]) & (df["ema_fast"] < df["ema_slow"])
    return df


def compute_volume_filter(df: pd.DataFrame) -> pd.DataFrame:
    """
    Volume filter:
    - We want to avoid fakeouts where price crosses EMAs on weak volume.
    - We require current volume >= VOL_MULTIPLIER * average(volume over last VOL_LOOKBACK bars).
    """
    df = df.copy()
    df["vol_ma"] = df["volume"].rolling(VOL_LOOKBACK).mean()
    df["vol_ok"] = df["volume"] >= (VOL_MULTIPLIER * df["vol_ma"])
    return df


def find_swing_levels(df: pd.DataFrame, lookback: int = SWING_LOOKBACK) -> Tuple[float, float]:
    """
    Simple S/R approximation:
    - Support: recent swing low (min low over lookback)
    - Resistance: recent swing high (max high over lookback)

    This is a crude but effective way to approximate S/R zones.
    """
    recent = df.tail(lookback)
    support = recent["low"].min()
    resistance = recent["high"].max()
    return support, resistance


def is_near_level(price: float, level: float, pct: float = SR_PROXIMITY_PCT) -> bool:
    """
    Check if price is within pct% of a given level.
    Used to ensure entries are near meaningful S/R, not in the middle of nowhere.
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
    Estimate RR based on recent swing high/low:

    - For a LONG:
        stop below support
        target near resistance
        RR = (target - entry) / (entry - stop)

    - For a SHORT:
        stop above resistance
        target near support
        RR = (entry - target) / (stop - entry)
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
    Fakeout detection (high-level heuristic):

    - EMAs flat → no real trend
    - No structure shift (no HH/HL for bull, no LH/LL for bear)
    - Volume not confirmed (vol_ok == False)

    Here we approximate:
    - Flat EMAs: slope of ema_fast small
    - Use vol_ok flag from volume filter
    """
    if idx < 2:
        return True  # not enough history

    row = df.iloc[idx]
    prev = df.iloc[idx - 1]
    prev2 = df.iloc[idx - 2]

    # EMA slope: difference between current and previous
    slope_fast = row["ema_fast"] - prev["ema_fast"]
    slope_slow = row["ema_slow"] - prev["ema_slow"]

    # Flat EMAs → small slope
    flat_fast = abs(slope_fast) < (row["ema_fast"] * 0.0005)  # 0.05% per bar
    flat_slow = abs(slope_slow) < (row["ema_slow"] * 0.0005)

    # Volume weak?
    vol_ok = bool(row.get("vol_ok", False))

    # Structure: very rough check using closes
    if direction == "bull":
        # Want higher lows and higher highs
        hh = (row["high"] > prev["high"]) and (prev["high"] > prev2["high"])
        hl = (row["low"] > prev["low"]) and (prev["low"] > prev2["low"])
        structure_ok = hh or hl
    else:
        # Want lower highs and lower lows
        lh = (row["high"] < prev["high"]) and (prev["high"] < prev2["high"])
        ll = (row["low"] < prev["low"]) and (prev["low"] < prev2["low"])
        structure_ok = lh or ll

    # Fakeout risk if:
    # - EMAs flat OR
    # - Volume not ok OR
    # - Structure not aligned
    fakeout = (flat_fast and flat_slow) or (not vol_ok) or (not structure_ok)
    return fakeout


# ─────────────────────────────────────────────────────────────
# SIGNAL GENERATION
# ─────────────────────────────────────────────────────────────

def generate_ema_signals(df: pd.DataFrame, symbol: str) -> List[EMASignal]:
    """
    Full EMA strategy:

    1) Compute EMAs
    2) Detect crossovers
    3) Apply volume filter
    4) Find S/R levels
    5) Estimate RR
    6) Filter by asset-specific RR minimum
    7) Flag fakeout risk

    BTC:
      - Often respects 4H/1D
      - EMA 20/50 strong for swing trend
      - Use wider stops, demand 2R+

    ETH:
      - More volatile than BTC
      - EMA 20/50 on 4H and 1H both useful
      - Ideally confirm ETH/BTC strength (not done here, but recommended)

    SOL:
      - Much more volatile
      - EMA 20/50 on 1H/15m for active trading
      - Demand 3R+ and strong volume confirmation
    """
    df = compute_ema(df)
    df = detect_ema_crossovers(df)
    df = compute_volume_filter(df)

    support, resistance = find_swing_levels(df)

    signals: List[EMASignal] = []
    rr_min = RR_MIN.get(symbol, 2.0)

    for i in range(len(df)):
        row = df.iloc[i]
        if not row["bull_cross"] and not row["bear_cross"]:
            continue

        direction = "bull" if row["bull_cross"] else "bear"
        entry = row["close"]

        rr, stop, target = estimate_rr(direction, entry, support, resistance)
        if math.isnan(rr) or rr < rr_min:
            # RR too low → skip
            continue

        # S/R proximity: for longs, we want entry near support; for shorts, near resistance
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
    Send a Telegram alert when a clean EMA + S/R + Volume setup appears.

    You should only alert on:
    - RR >= RR_MIN
    - at_sr == True
    - vol_ok == True
    - fakeout_risk == False
    """
    if TELEGRAM_BOT_TOKEN == "YOUR_BOT_TOKEN":
        return  # not configured

    if not (signal.at_sr and signal.vol_ok and not signal.fakeout_risk):
        return  # only alert on high-quality setups

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
# LIVE FEED SCANNER (KRaken/Crypto-ready structure)
# ─────────────────────────────────────────────────────────────

"""
NEXT STEP: Turn this into a live feed scanner (Kraken/Crypto).

Here we define an async loop that:
- Periodically fetches recent OHLCV for BTC/ETH/SOL from your chosen venue(s)
- Runs the EMA scanner
- Emits Telegram alerts for clean setups

You can plug in:
- Kraken REST OHLC endpoints
- Crypto.com REST OHLC endpoints
- Or your existing WS → OHLC aggregator
"""

async def fetch_ohlcv_kraken(session: aiohttp.ClientSession, pair: str, interval: int = 60) -> pd.DataFrame:
    """
    Placeholder for Kraken OHLC fetch.
    You would call Kraken's /0/public/OHLC endpoint here.

    For simplicity, this is left as a stub.
    """
    raise NotImplementedError("Wire this to Kraken OHLC REST or your own data source.")


async def fetch_ohlcv_crypto(session: aiohttp.ClientSession, instrument: str, interval: int = 60) -> pd.DataFrame:
    """
    Placeholder for Crypto.com OHLC fetch.
    You would call their market data endpoint or use your own aggregator.
    """
    raise NotImplementedError("Wire this to Crypto.com OHLC REST or your own data source.")


async def live_scanner_loop():
    """
    Live scanner loop:

    - For each symbol (BTC, ETH, SOL):
        - Fetch latest OHLCV (e.g., last 200 candles)
        - Run EMA + S/R + Volume + RR scanner
        - For each signal, send Telegram alert if clean

    This is time-based (polling). For true HFT, you'd use WS + rolling OHLC.
    """
    symbols = ["BTC", "ETH", "SOL"]

    async with aiohttp.ClientSession() as session:
        while True:
            for sym in symbols:
                try:
                    # Map symbol to venue-specific pair/instrument
                    # Example: Kraken XBTUSD, Crypto BTC_USDT, etc.
                    # Here we just call a placeholder.
                    df = await fetch_ohlcv_kraken(session, pair=f"{sym}USD", interval=60)

                    if df is None or df.empty:
                        continue

                    signals = generate_ema_signals(df, sym)
                    for sig in signals:
                        send_telegram_alert(sig)
                        print(f"[SIGNAL] {sig.comment}")
                except NotImplementedError:
                    print(f"[WARN] OHLC fetch not implemented for {sym}.")
                except Exception as e:
                    print(f"[ERROR] live_scanner_loop {sym}: {e}")

            # Sleep between scans (e.g., 60 seconds for 1m, 300 for 5m, etc.)
            await asyncio.sleep(60)


if __name__ == "__main__":
    # Example offline usage:
    # df_btc = pd.read_csv("btc_ohlcv.csv")
    # signals_btc = generate_ema_signals(df_btc, "BTC")
    # for s in signals_btc:
    #     print(s.comment)
    #
    # For live:
    # asyncio.run(live_scanner_loop())
    pass
