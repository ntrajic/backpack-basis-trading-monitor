# audit_logger_dbg7.py
#
# JSONL + CSV audit logging for each executed cycle.

import csv
import json
import os
from datetime import datetime
from decimal import Decimal
from typing import Optional

LOG_DIR = os.getenv("ARB_LOG_DIR", "./logs")
JSONL_PATH = os.path.join(LOG_DIR, "cycles_dbg7.jsonl")
CSV_PATH = os.path.join(LOG_DIR, "cycles_dbg7.csv")

os.makedirs(LOG_DIR, exist_ok=True)

CSV_HEADERS = [
    "timestamp",
    "symbol",
    "buy_venue",
    "sell_venue",
    "buy_price",
    "sell_price",
    "trade_size",
    "arrival_amount",
    "entry_fee",
    "exit_fee",
    "withdrawal_fee",
    "gross_revenue",
    "net_profit",
    "net_pct",
    "capital_in",
    "capital_out",
    "buy_order_id",
    "sell_order_id",
    "withdrawal_id",
    "buy_status",
    "sell_status",
    "withdraw_status",
]


def _dec(v: Optional[Decimal]) -> Optional[str]:
    if v is None:
        return None
    return str(v)


def log_cycle(plan, cycle):
    """
    plan: ExecutionPlan
    cycle: CycleResult
    """
    ts = datetime.utcnow().isoformat()

    record = {
        "timestamp": ts,
        "symbol": plan.symbol,
        "buy_venue": plan.buy_venue,
        "sell_venue": plan.sell_venue,
        "buy_price": _dec(cycle.buy_price),
        "sell_price": _dec(cycle.sell_price),
        "trade_size": _dec(cycle.trade_size),
        "arrival_amount": _dec(cycle.arrival_amount),
        "entry_fee": _dec(cycle.entry_fee),
        "exit_fee": _dec(cycle.exit_fee),
        "withdrawal_fee": _dec(cycle.withdrawal_fee),
        "gross_revenue": _dec(cycle.gross_revenue),
        "net_profit": _dec(cycle.net_profit),
        "net_pct": _dec(cycle.net_pct),
        "capital_in": _dec(cycle.capital_in),
        "capital_out": _dec(cycle.capital_out),
        "buy_order_id": plan.buy_order_id,
        "sell_order_id": plan.sell_order_id,
        "withdrawal_id": plan.withdrawal_id,
        "buy_status": plan.buy_status.value,
        "sell_status": plan.sell_status.value,
        "withdraw_status": plan.withdraw_status.value,
    }

    # JSONL
    with open(JSONL_PATH, "a", encoding="utf-8") as jf:
        jf.write(json.dumps(record) + "\n")

    # CSV
    file_exists = os.path.exists(CSV_PATH)
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as cf:
        writer = csv.DictWriter(cf, fieldnames=CSV_HEADERS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(record)
