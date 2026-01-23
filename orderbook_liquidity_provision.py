#!/usr/bin/env python3
"""
Kalshi Liquidity Provision Bot - "Catch the Knife" Strategy

CONCEPT:
When panic sellers hit "Sell All", they eat through orderbook bids down to absurdly low prices.
This bot detects those opportunities and alerts you to place lowball bids for instant arbitrage.

STRATEGY:
1. Monitor all trades in real-time via WebSocket
2. Detect "panic sell" pattern: large price drop (>30%) in short time (<60 seconds)
3. Alert you with: current price, suggested bid prices, expected profit
4. You manually review and place bids if opportunity looks real
5. Bot alerts when your bid gets filled
6. You immediately sell at recovery price

RISKS:
- False positives: What if the drop is justified? (bad news, event resolved, etc.)
- Speed: Other bots may be faster
- Capital: Bids tie up money while waiting for fills

TODO (future enhancements):
- Auto-place bids (requires trading integration)
- Historical recovery analysis (what % of panics recover?)
- News integration (filter out justified drops)
"""

import websocket
import json
import time
import sqlite3
import base64
import signal
import sys
from datetime import datetime, timezone, timedelta
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from typing import Dict, List, Optional, Tuple
from collections import defaultdict, deque
from slack_notify import send_slack_message

# Global WebSocket instance
ws_instance = None

# ===== CONFIGURATION =====

WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
PRIVATE_KEY_PATH = "private_key.pem"
KEY_ID = "cc76eee9-dba9-4bf2-a06f-eddf6a44a8e1"

# Target markets to monitor
TARGET_MARKETS = [
    "KXTRUMPSAY-26JAN26",
    "KXBERNIEMENTION-26JAN20",
    "KXTRUMPMENTION-26JAN21",
    "KXTRUMPMENTIONB-26JAN21"
]

# Panic detection thresholds
PANIC_DROP_THRESHOLD = 0.30  # 30%+ drop = potential panic
PANIC_TIME_WINDOW = 60  # Must happen within 60 seconds
MIN_PRICE_FOR_ARBIT = 0.05  # Only care about drops if price > $0.05 (filters penny stocks)

# Suggested bid discount (how far below panic price to bid)
BID_DISCOUNT_TIERS = [
    (0.05, 0.15, 0.30),  # If price drops to $0.05-$0.15, bid 30% below
    (0.15, 0.30, 0.20),  # If price drops to $0.15-$0.30, bid 20% below
    (0.30, 999, 0.15),   # If price drops to $0.30+, bid 15% below
]

# Alert cooldown (avoid spam)
ALERT_COOLDOWN_SECONDS = 300  # 5 minutes between alerts per ticker

ENABLE_SLACK = True
DB_PATH = "kalshi_liquidity.db"

# ===== TRADE HISTORY TRACKER =====

# Store recent trades per ticker: {ticker: deque([(timestamp, price), ...])}
trade_history = defaultdict(lambda: deque(maxlen=100))

def record_trade(ticker: str, price: float):
    """Record a trade in the history."""
    now = datetime.now(timezone.utc)
    trade_history[ticker].append((now, price))

def get_price_at_time(ticker: str, seconds_ago: int) -> Optional[float]:
    """Get price from N seconds ago."""
    if ticker not in trade_history or len(trade_history[ticker]) == 0:
        return None
    
    cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)
    
    # Find oldest trade within time window
    for timestamp, price in trade_history[ticker]:
        if timestamp >= cutoff_time:
            return price
    
    # If no trades in window, return oldest available
    return trade_history[ticker][0][1] if trade_history[ticker] else None

def get_recent_high(ticker: str, seconds: int) -> Optional[float]:
    """Get highest price in last N seconds."""
    if ticker not in trade_history:
        return None
    
    cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=seconds)
    recent_prices = [price for timestamp, price in trade_history[ticker] if timestamp >= cutoff_time]
    
    return max(recent_prices) if recent_prices else None

# ===== DATABASE (for cooldowns) =====

def init_db():
    """Initialize SQLite database for alert cooldowns."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS alert_cooldowns (
            ticker TEXT PRIMARY KEY,
            last_alert_time TEXT
        )
    """)
    conn.commit()
    conn.close()

def is_in_cooldown(ticker: str) -> bool:
    """Check if ticker is in alert cooldown."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT last_alert_time FROM alert_cooldowns WHERE ticker = ?", (ticker,))
    result = cursor.fetchone()
    conn.close()
    
    if not result:
        return False
    
    last_alert = datetime.fromisoformat(result[0])
    cooldown_until = last_alert + timedelta(seconds=ALERT_COOLDOWN_SECONDS)
    return datetime.now(timezone.utc) < cooldown_until

def set_alert_cooldown(ticker: str):
    """Set alert cooldown for ticker."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    cursor.execute("""
        INSERT INTO alert_cooldowns (ticker, last_alert_time)
        VALUES (?, ?)
        ON CONFLICT(ticker) DO UPDATE SET last_alert_time = excluded.last_alert_time
    """, (ticker, now))
    conn.commit()
    conn.close()

# ===== PANIC DETECTION LOGIC =====

def calculate_suggested_bid(panic_price: float) -> float:
    """Calculate suggested bid price based on panic price."""
    for min_price, max_price, discount in BID_DISCOUNT_TIERS:
        if min_price <= panic_price < max_price:
            return panic_price * (1 - discount)
    return panic_price * 0.85  # Default 15% discount

def detect_panic_sell(ticker: str, current_price: float):
    """
    Detect if current trade is part of a panic sell.
    
    Logic:
    1. Get price from PANIC_TIME_WINDOW seconds ago
    2. Calculate % drop
    3. If drop >= PANIC_DROP_THRESHOLD, it's a panic
    4. Alert user with arbitrage opportunity
    """
    if current_price < MIN_PRICE_FOR_ARBIT:
        return  # Ignore penny stocks
    
    if is_in_cooldown(ticker):
        return  # Already alerted recently
    
    # Get recent high price
    recent_high = get_recent_high(ticker, PANIC_TIME_WINDOW)
    
    if not recent_high or recent_high <= 0:
        return  # Not enough history
    
    # Calculate drop percentage
    drop_pct = (recent_high - current_price) / recent_high
    
    if drop_pct >= PANIC_DROP_THRESHOLD:
        # PANIC DETECTED!
        suggested_bid = calculate_suggested_bid(current_price)
        expected_recovery = recent_high * 0.8  # Conservative: assume 80% recovery
        potential_profit_pct = (expected_recovery - suggested_bid) / suggested_bid
        
        send_panic_alert(
            ticker=ticker,
            recent_high=recent_high,
            panic_price=current_price,
            drop_pct=drop_pct,
            suggested_bid=suggested_bid,
            expected_recovery=expected_recovery,
            potential_profit_pct=potential_profit_pct
        )
        
        set_alert_cooldown(ticker)

def send_panic_alert(
    ticker: str,
    recent_high: float,
    panic_price: float,
    drop_pct: float,
    suggested_bid: float,
    expected_recovery: float,
    potential_profit_pct: float
):
    """Send panic sell alert to terminal and Slack."""
    print("\n" + "="*70)
    print("🔥 PANIC SELL DETECTED - ARBITRAGE OPPORTUNITY 🔥")
    print("="*70)
    print(f"Ticker:           {ticker}")
    print(f"Recent High:      ${recent_high:.2f}")
    print(f"Panic Price:      ${panic_price:.2f}")
    print(f"Drop:             {drop_pct:.1%}")
    print(f"\n💰 SUGGESTED ACTION:")
    print(f"   Place bid at:  ${suggested_bid:.2f}")
    print(f"   Expected recovery: ${expected_recovery:.2f}")
    print(f"   Potential profit: {potential_profit_pct:.1%}")
    print(f"\n⚠️  CAUTION: Verify this isn't justified by news/events!")
    print("="*70 + "\n")
    
    if ENABLE_SLACK:
        message = (
            f"🔥 *PANIC SELL OPPORTUNITY* 🔥\n"
            f"*{ticker}*\n\n"
            f"📉 Drop: ${recent_high:.2f} → ${panic_price:.2f} ({drop_pct:.1%})\n"
            f"💰 Suggested bid: ${suggested_bid:.2f}\n"
            f"📈 Expected recovery: ${expected_recovery:.2f}\n"
            f"💵 Potential profit: {potential_profit_pct:.1%}\n\n"
            f"⚠️ Verify before placing bid!"
        )
        send_slack_message(message)

# ===== AUTHENTICATION =====

def load_private_key(key_path: str):
    """Load RSA private key from PEM file."""
    with open(key_path, 'rb') as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    return private_key

def sign_message(private_key, message: str) -> str:
    """Sign a message using RSA-PSS."""
    message_bytes = message.encode('utf-8')
    signature = private_key.sign(
        message_bytes,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH
        ),
        hashes.SHA256()
    )
    return base64.b64encode(signature).decode('utf-8')

def get_ws_auth_headers(private_key, key_id: str) -> Dict[str, str]:
    """Generate authentication headers for WebSocket connection."""
    timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
    timestamp_str = str(timestamp)
    
    message = f"{timestamp_str}GET/trade-api/ws/v2"
    signature = sign_message(private_key, message)
    
    return {
        "KALSHI-ACCESS-KEY": key_id,
        "KALSHI-ACCESS-SIGNATURE": signature,
        "KALSHI-ACCESS-TIMESTAMP": timestamp_str
    }

# ===== WEBSOCKET HANDLERS =====

def on_message(ws, message):
    """Handle incoming WebSocket messages."""
    try:
        data = json.loads(message)
        msg_type = data.get("type", "")
        
        # Only process actual trades
        if msg_type == "trade":
            ticker = data.get("msg", {}).get("ticker")
            price = data.get("msg", {}).get("yes_price")
            
            if ticker and price is not None:
                price = price / 100.0  # Convert cents to dollars
                
                # Record trade
                record_trade(ticker, price)
                
                # Check for panic sell
                detect_panic_sell(ticker, price)
    
    except Exception as e:
        print(f"Error processing message: {e}")

def on_error(ws, error):
    """Handle WebSocket errors."""
    print(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    """Handle WebSocket connection close."""
    print(f"\nWebSocket closed: {close_status_code} - {close_msg}")

def on_open(ws):
    """Handle WebSocket connection open."""
    print("✓ WebSocket connected")
    print("Discovering contracts and subscribing...")
    
    from p import ExchangeClient
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend
    
    with open(PRIVATE_KEY_PATH, 'rb') as f:
        private_key_rest = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())
    
    client = ExchangeClient(
        exchange_api_base="https://api.elections.kalshi.com/trade-api/v2",
        key_id=KEY_ID,
        private_key=private_key_rest
    )
    
    total_contracts = 0
    for event_ticker in TARGET_MARKETS:
        try:
            response = client.get_markets(event_ticker=event_ticker, limit=500)
            markets = response.get('markets', [])
            
            if not markets:
                print(f"  ⚠️  No contracts found for {event_ticker}")
                continue
            
            for market in markets:
                ticker = market.get('ticker')
                if ticker:
                    subscribe_msg = {
                        "id": 1,
                        "cmd": "subscribe",
                        "params": {
                            "channels": ["trade"],
                            "market_ticker": ticker
                        }
                    }
                    ws.send(json.dumps(subscribe_msg))
                    total_contracts += 1
            
            print(f"  → {event_ticker}: {len(markets)} contracts")
            
        except Exception as e:
            print(f"  ⚠️  Error subscribing to {event_ticker}: {e}")
    
    print(f"\n🎯 Monitoring {total_contracts} contracts for panic sells...")
    print(f"   Panic threshold: {PANIC_DROP_THRESHOLD:.0%}+ drop in {PANIC_TIME_WINDOW}s")
    print(f"   Alert cooldown: {ALERT_COOLDOWN_SECONDS}s\n")

# ===== SIGNAL HANDLER =====

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    global ws_instance
    print("\n\n⚠️  Interrupt received, shutting down...")
    if ws_instance:
        ws_instance.close()
    sys.exit(0)

# ===== MAIN =====

def main():
    """Start liquidity provision bot."""
    global ws_instance
    
    signal.signal(signal.SIGINT, signal_handler)
    
    print("="*70)
    print("Kalshi Liquidity Provision Bot - 'Catch the Knife' Strategy")
    print("="*70)
    print("\n⚠️  This bot alerts you to potential arbitrage opportunities.")
    print("    Always verify the drop isn't justified before placing bids!\n")
    
    init_db()
    
    try:
        private_key = load_private_key(PRIVATE_KEY_PATH)
    except FileNotFoundError:
        print(f"ERROR: Private key not found at {PRIVATE_KEY_PATH}")
        return
    
    while True:
        try:
            headers = get_ws_auth_headers(private_key, KEY_ID)
            
            ws_instance = websocket.WebSocketApp(
                WS_URL,
                header=[f"{k}: {v}" for k, v in headers.items()],
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            
            ws_instance.run_forever()
            break
            
        except KeyboardInterrupt:
            print("\n\nShutting down...")
            if ws_instance:
                ws_instance.close()
            break
        except Exception as e:
            print(f"Connection error: {e}")
            print("Reconnecting in 5 seconds...")
            time.sleep(5)

if __name__ == "__main__":
    main()
