"""
Example: How to integrate PostgreSQL into kalshi_ws.py

Add these changes to your existing kalshi_ws.py
"""

# ===== AT THE TOP OF FILE (after other imports) =====
from db_postgres import (
    initialize_database,
    shutdown_database, 
    batch_manager,
    get_minutes_to_expiration,
    upsert_market_metadata
)

# ===== IN MAIN() FUNCTION, AFTER init_db() =====
# Initialize PostgreSQL (in addition to SQLite for now)
initialize_database()

# ===== MODIFY signal_handler() =====
def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    global ws_instance
    print("\n\n⚠️  Interrupt received, shutting down...")
    if ws_instance:
        ws_instance.close()
    shutdown_database()  # <-- ADD THIS LINE
    sys.exit(0)

# ===== IN on_open(), AFTER SUBSCRIBING TO MARKETS =====
# Store market metadata for tick-tock calculations
for market in markets:
    ticker = market.get('ticker')
    event_ticker = market.get('event_ticker', event_ticker)
    title = market.get('title', '')
    close_time_str = market.get('close_time')
    status = market.get('status', 'active')
    
    # Parse close_time if available
    close_time = None
    if close_time_str:
        from dateutil.parser import parse
        close_time = parse(close_time_str)
    
    upsert_market_metadata(
        ticker=ticker,
        event_ticker=event_ticker,
        title=title,
        close_time=close_time,
        status=status
    )

# ===== IN on_message(), AFTER PROCESSING TRADE =====
# Inside the `if msg_type == "trade":` block, after getting ticker/price/count:

# Record every trade to PostgreSQL
batch_manager.add_snapshot(
    ticker=ticker,
    price=price,
    volume=count,
    trade_id=data.get("msg", {}).get("trade_id"),
    taker_side=data.get("msg", {}).get("taker_side", "unknown"),
    timestamp=data.get("msg", {}).get("ts")
)

# ===== IN send_alert() FUNCTION =====
# Add these lines at the end, before the Slack message:

# Calculate minutes to expiration
minutes_to_exp = get_minutes_to_expiration(ticker)

# Record spike event
batch_manager.add_spike(
    ticker=ticker,
    old_price=old_price,
    new_price=new_price,
    pct_change=change_pct,
    volume=volume,
    minutes_to_exp=minutes_to_exp,
    threshold=get_threshold_for_price(new_price),  # Pass the threshold used
    context=context
)

# ===== EXAMPLE: Enhanced alert message with expiration time =====
if minutes_to_exp:
    print(f"Time left: {minutes_to_exp} minutes until close")
    if ENABLE_SLACK_ALERTS and minutes_to_exp:
        message += f"⏰ {minutes_to_exp} min until close\n"
