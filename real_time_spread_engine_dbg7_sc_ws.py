# real_time_spread_engine_dbg7_sc_ws.py
#
# Wrapper around real_time_spread_engine_dbg7.py that adds audit logging.
# Core risk logic remains in real_time_spread_engine_dbg7.py.

from real_time_spread_engine_dbg7 import (
    ExecutionPlan,
    execute_cycle as _execute_cycle_core,
)
from audit_logger_dbg7 import log_cycle


async def execute_cycle_with_logging(plan: ExecutionPlan) -> None:
    """
    Thin wrapper: calls core execute_cycle, then logs the cycle.
    Assumes plan.cycle is the CycleResult used for the cycle.
    """
    await _execute_cycle_core(plan)
    # After core execution, log whatever happened
    log_cycle(plan, plan.cycle)
