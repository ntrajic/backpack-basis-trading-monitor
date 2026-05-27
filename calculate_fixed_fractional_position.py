def calculate_fixed_fractional_position(
    balance: float,
    remaining_heat: float,
    risk_pct: float,
    entry_price: float,
    stop_loss_price: float,
    is_long: bool = True,
) -> dict:
    """
    Calculates position sizing, nominal value, margin, and required leverage
    using the Fixed Fractional (Fixed Risk) method.

    Parameters:
    - balance (float): Current available cash balance on the exchange.
    - remaining_heat (float): Total remaining risk budget in absolute dollars.
    - risk_pct (float): Percentage of the current balance to risk on this trade (e.g., 1.5 for 1.5%).
    - entry_price (float): Execution entry price of the token.
    - stop_loss_price (float): Technical invalidation price.
    - is_long (bool): True for Long/Buy trades, False for Short/Sell trades.
    """
    # 1. Calculate ideal risk amount based on current liquid balance
    ideal_risk_amount = balance * (risk_pct / 100.0)

    # Cap the risk amount by the remaining macro risk heat budget
    actual_risk_amount = min(ideal_risk_amount, remaining_heat)

    # 2. Calculate risk per unit (absolute delta between entry and stop loss)
    if is_long:
        if stop_loss_price >= entry_price:
            raise ValueError("For a Long trade, Stop Loss must be below Entry Price.")
        risk_per_unit = entry_price - stop_loss_price
    else:
        if stop_loss_price <= entry_price:
            raise ValueError("For a Short trade, Stop Loss must be above Entry Price.")
        risk_per_unit = stop_loss_price - entry_price

    # 3. Calculate position size (units/tokens to buy/short)
    position_size_tokens = actual_risk_amount / risk_per_unit

    # 4. Calculate Nominal Position Value (notional size of the trade in fiat/stablecoin)
    nominal_position_value = position_size_tokens * entry_price

    # 5. Leverage and Margin calculation
    # Since we are deploying this out of our current available cash balance:
    required_margin = min(balance, nominal_position_value)

    # Leverage needed = Nominal Value / Available Liquid Balance used as margin
    # If nominal value is less than balance, leverage is 1x (no leverage needed, spot trade)
    required_leverage = (
        nominal_position_value / balance if nominal_position_value > balance else 1.0
    )

    return {
        "status": (
            "Success"
            if ideal_risk_amount <= remaining_heat
            else "Risk Capped by Max Heat"
        ),
        "allowed_risk_dollars": round(actual_risk_amount, 2),
        "position_size_tokens": round(position_size_tokens, 4),
        "nominal_position_value": round(nominal_position_value, 2),
        "required_margin_deployed": round(required_margin, 2),
        "suggested_exchange_leverage": round(required_leverage, 2),
    }


# --- SIMULATION RUN (Matching your Token DAB example) ---
if __name__ == "__main__":
    # Initial Setup
    account_balance = 10000.0
    max_heat_pct = 6.0  # 6% total risk budget allowed across the book
    max_heat_dollars = account_balance * (max_heat_pct / 100.0)  # $600

    print(f"--- Account Genesis ---")
    print(
        f"Balance: ${account_balance} | Total Risk Heat Capacity: ${max_heat_dollars}\n"
    )

    # Trade 1: Shorting Token DAB
    # Entry: $230, SL: $240, Risk: 1.5%
    trade_1 = calculate_fixed_fractional_position(
        balance=account_balance,
        remaining_heat=max_heat_dollars,
        risk_pct=1.5,
        entry_price=230.0,
        stop_loss_price=240.0,
        is_long=False,
    )

    print(">>> TRADE 1 EXECUTION: Short DAB")
    for key, val in trade_1.items():
        print(f"  {key}: {val}")

    # Update portfolio state after deploying Trade 1
    # Liquid balance drops by the nominal size if spot, or margin if using leverage.
    # Let's assume we use the full liquid cash balance to back it without leverage first:
    new_balance = account_balance - trade_1["nominal_position_value"]
    updated_heat = max_heat_dollars - trade_1["allowed_risk_dollars"]

    print(f"\n--- Updated Portfolio State ---")
    print(f"Remaining Liquid Balance: ${round(new_balance, 2)}")
    print(f"Remaining Risk Heat Budget: ${round(updated_heat, 2)}\n")

    # Trade 2: Longing Token CAF with dynamic remaining parameters
    # Entry: $15, SL: $13, Risk: 1.5% of *new* balance
    trade_2 = calculate_fixed_fractional_position(
        balance=new_balance,
        remaining_heat=updated_heat,
        risk_pct=1.5,
        entry_price=15.0,
        stop_loss_price=13.0,
        is_long=True,
    )

    print(">>> TRADE 2 EXECUTION: Long CAF")
    for key, val in trade_2.items():
        print(f"  {key}: {val}")
