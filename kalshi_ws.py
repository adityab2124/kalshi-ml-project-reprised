#!/usr/bin/env python3
"""
Kalshi WebSocket client for real-time market price monitoring.
Connects to Kalshi's websocket feed and monitors for price spikes.
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
from typing import Dict, List, Optional
from slack_notify import send_slack_message
from google_context import get_market_context

# Global WebSocket instance for signal handler
ws_instance = None

# ===== CONFIGURATION =====

# Kalshi WebSocket URL
WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"

# API credentials (same as REST API)
PRIVATE_KEY_PATH = "private_key.pem"
KEY_ID = "cc76eee9-dba9-4bf2-a06f-eddf6a44a8e1"

# Target markets to monitor (event tickers)
TARGET_MARKETS = [
    "KXTRUMPSAY-26JAN26",
    "KXBERNIEMENTION-26JAN20",
    "KXTRUMPMENTION-26JAN21",
    "KXTRUMPMENTIONB-26JAN21"
]

# Price-tiered spike thresholds (filters penny stock noise)
PRICE_TIERS = [
    (0.00, 0.10, 1.00),   # $0.01-$0.10: Need 100%+ spike
    (0.10, 0.30, 0.30),   # $0.10-$0.30: Need 30%+ spike
    (0.30, 0.50, 0.20),   # $0.30-$0.50: Need 20%+ spike
    (0.50, 999, 0.15),    # $0.50+: Need 15%+ spike
]

# Cooldown period in seconds (30 seconds)
COOLDOWN_SECONDS = 30

# Enable Slack notifications (set SLACK_BOT_TOKEN env var)
ENABLE_SLACK_ALERTS = True

# Enable Google Search context (set GOOGLE_API_KEY env var)
ENABLE_GOOGLE_SEARCH = True
AI_CONTEXT_THRESHOLD = 0.15

# Database path
DB_PATH = "kalshi_monitor.db"

# ===== DATABASE SETUP =====

def init_db():
    """Initialize SQLite database for price caching."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_cache (
            ticker TEXT PRIMARY KEY,
            last_price REAL,
            last_updated TEXT,
            cooldown_until TEXT
        )
    """)
    conn.commit()
    conn.close()

def get_cached_price(ticker: str) -> Optional[float]:
    """Get last known price for ticker."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT last_price FROM market_cache WHERE ticker = ?", (ticker,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

def update_cached_price(ticker: str, price: float):
    """Update cached price and timestamp."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    cursor.execute("""
        INSERT INTO market_cache (ticker, last_price, last_updated, cooldown_until)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(ticker) DO UPDATE SET
            last_price = excluded.last_price,
            last_updated = excluded.last_updated
    """, (ticker, price, now, now))
    conn.commit()
    conn.close()

def is_in_cooldown(ticker: str) -> bool:
    """Check if ticker is in cooldown period."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT cooldown_until FROM market_cache WHERE ticker = ?", (ticker,))
    result = cursor.fetchone()
    conn.close()
    
    if not result or not result[0]:
        return False
    
    cooldown_until = datetime.fromisoformat(result[0])
    return datetime.now(timezone.utc) < cooldown_until

def set_cooldown(ticker: str):
    """Set cooldown period for ticker."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cooldown_until = (datetime.now(timezone.utc) + timedelta(seconds=COOLDOWN_SECONDS)).isoformat()
    cursor.execute("""
        UPDATE market_cache SET cooldown_until = ? WHERE ticker = ?
    """, (cooldown_until, ticker))
    conn.commit()
    conn.close()

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
    
    # Message to sign: timestamp + method + path
    message = f"{timestamp_str}GET/trade-api/ws/v2"
    signature = sign_message(private_key, message)
    
    return {
        "KALSHI-ACCESS-KEY": key_id,
        "KALSHI-ACCESS-SIGNATURE": signature,
        "KALSHI-ACCESS-TIMESTAMP": timestamp_str
    }

# ===== ALERT LOGIC =====

def get_threshold_for_price(price: float) -> float:
    """Get appropriate spike threshold based on price tier."""
    for min_price, max_price, threshold in PRICE_TIERS:
        if min_price <= price < max_price:
            return threshold
    return 0.15  # Default fallback

def send_alert(ticker: str, old_price: float, new_price: float, change_pct: float, context: Optional[str] = None):
    """Send spike alert to terminal and Slack."""
    print("\n" + "="*60)
    print("⚠️  SPIKE DETECTED!")
    print("="*60)
    print(f"Ticker:  {ticker}")
    print(f"Old:     ${old_price:.2f}")
    print(f"New:     ${new_price:.2f}")
    print(f"Change:  {change_pct:+.1%}")
    if context:
        print(f"Context: {context}")
    print("="*60 + "\n")
    
    if ENABLE_SLACK_ALERTS:
        message = (
            f"🚨 *SPIKE ALERT* 🚨\n"
            f"*{ticker}*\n"
            f"${old_price:.2f} → ${new_price:.2f} ({change_pct:+.1%})\n"
        )
        if context:
            message += f"\n💡 {context}"
        
        send_slack_message(message)

# ===== WEBSOCKET HANDLERS =====

def on_message(ws, message):
    """Handle incoming WebSocket messages."""
    try:
        data = json.loads(message)
        msg_type = data.get("type", "")
        
        # ONLY track actual trades (executed transactions)
        # Orderbook deltas show individual orders at ANY price level, not the real market price
        if msg_type == "trade":
            ticker = data.get("msg", {}).get("ticker")
            price = data.get("msg", {}).get("yes_price")
            
            if ticker and price is not None:
                # Convert price from cents to dollars
                price = price / 100.0
                
                print(f"[TRADE] {ticker}: ${price:.2f}")
                
                # Check for spike with price-tiered threshold
                old_price = get_cached_price(ticker)
                if old_price is not None and old_price > 0:
                    change_pct = (price - old_price) / old_price
                    threshold = get_threshold_for_price(price)
                    
                    if abs(change_pct) >= threshold and not is_in_cooldown(ticker):
                        # Get AI context if enabled
                        context = None
                        if ENABLE_GOOGLE_SEARCH and abs(change_pct) >= AI_CONTEXT_THRESHOLD:
                            context = get_market_context(ticker, f"Price: ${price:.2f}", ENABLE_GOOGLE_SEARCH)
                        
                        send_alert(ticker, old_price, price, change_pct, context)
                        set_cooldown(ticker)
                else:
                    # First time seeing this ticker - cache it
                    print(f"[CACHE] {ticker}: ${price:.2f} (first trade)")
                
                # Update cache
                update_cached_price(ticker, price)
    
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
    
    # Import REST client to discover contracts
    from p import ExchangeClient
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend
    
    # Load credentials for REST API
    with open(PRIVATE_KEY_PATH, 'rb') as f:
        private_key_rest = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())
    
    client = ExchangeClient(
        exchange_api_base="https://api.elections.kalshi.com/trade-api/v2",
        key_id=KEY_ID,
        private_key=private_key_rest
    )
    
    # Discover and subscribe to all contracts
    total_contracts = 0
    for event_ticker in TARGET_MARKETS:
        try:
            # Get all markets for this event
            response = client.get_markets(event_ticker=event_ticker, limit=500)
            markets = response.get('markets', [])
            
            if not markets:
                print(f"  ⚠️  No contracts found for {event_ticker}")
                continue
            
            # Subscribe to each contract
            for market in markets:
                ticker = market.get('ticker')
                if ticker:
                    subscribe_msg = {
                        "id": 1,
                        "cmd": "subscribe",
                        "params": {
                            "channels": ["trade"],  # Only track real trades, not orderbook noise
                            "market_ticker": ticker
                        }
                    }
                    ws.send(json.dumps(subscribe_msg))
                    total_contracts += 1
            
            print(f"  → {event_ticker}: {len(markets)} contracts")
            
        except Exception as e:
            print(f"  ⚠️  Error subscribing to {event_ticker}: {e}")
    
    print(f"\n🎯 Monitoring {total_contracts} contracts with price-tiered thresholds:")
    print(f"   $0.01-$0.10 → 100%+ | $0.10-$0.30 → 30%+ | $0.30-$0.50 → 20%+ | $0.50+ → 15%+")
    print(f"   Cooldown: {COOLDOWN_SECONDS}s | Slack: {ENABLE_SLACK_ALERTS} | AI Context: {ENABLE_GOOGLE_SEARCH}\n")

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
    """Start WebSocket monitoring."""
    global ws_instance
    
    # Set up signal handler for Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)
    
    print("="*60)
    print("Kalshi WebSocket Monitor - Delta Sniper")
    print("="*60)
    
    # Initialize database
    init_db()
    
    # Load credentials
    try:
        private_key = load_private_key(PRIVATE_KEY_PATH)
    except FileNotFoundError:
        print(f"ERROR: Private key not found at {PRIVATE_KEY_PATH}")
        return
    
    # Run forever (auto-reconnect on disconnect)
    while True:
        try:
            # Generate fresh auth headers on each connection attempt
            headers = get_ws_auth_headers(private_key, KEY_ID)
            
            # Create WebSocket connection
            ws_instance = websocket.WebSocketApp(
                WS_URL,
                header=[f"{k}: {v}" for k, v in headers.items()],
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            
            ws_instance.run_forever()
            
            # If run_forever exits normally, break (don't reconnect)
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
