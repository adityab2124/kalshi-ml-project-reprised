#!/usr/bin/env python3
"""
Delta Sniper Bot - Monitors Kalshi markets for rapid price spikes.

Polls markets every 5 seconds and alerts on 15%+ price jumps.
Uses SQLite for caching and cooldown management.
"""

import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple
from p import ExchangeClient
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from slack_notify import send_slack_message
import os

# ===== CONFIGURATION =====
EXCHANGE_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
KEY_ID = "cc76eee9-dba9-4bf2-a06f-eddf6a44a8e1"
PRIVATE_KEY_PATH = "private_key.pem"
DB_PATH = "kalshi_monitor.db"

# Target markets to monitor (event tickers or full tickers)
TARGET_MARKETS = [
    "KXTRUMPSAY-26JAN19",
    "KXBERNIEMENTION-26JAN20",
]

# Spike threshold (15% = 0.15)
SPIKE_THRESHOLD = 0.15

# Cooldown period in seconds (10 minutes = 600)
COOLDOWN_SECONDS = 600

# Polling interval in seconds
POLL_INTERVAL = 5

# Enable Slack notifications (set to False to disable)
ENABLE_SLACK_ALERTS = True


def load_private_key(key_path: str):
    """Load RSA private key from PEM file."""
    with open(key_path, 'rb') as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    return private_key


def init_database(db_path: str) -> sqlite3.Connection:
    """
    Initialize SQLite database and create market_cache table if it doesn't exist.
    
    Returns:
        SQLite connection object
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create market_cache table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_cache (
            ticker TEXT PRIMARY KEY,
            last_price REAL NOT NULL,
            last_updated TIMESTAMP NOT NULL,
            cooldown_until TIMESTAMP
        )
    """)
    
    conn.commit()
    return conn


def get_market_price(client: ExchangeClient, ticker: str) -> Optional[float]:
    """
    Fetch current price for a market ticker from Kalshi API.
    
    Args:
        client: ExchangeClient instance
        ticker: Market ticker (e.g., "KXTRUMPSAY-26JAN19-IQ" for specific contract,
                or "KXTRUMPSAY-26JAN19" for event - will use best price)
    
    Returns:
        Current price (0-1) or None if fetch fails
    """
    try:
        # If it's a full contract ticker (contains two or more dashes), get that specific market
        if ticker.count('-') >= 2:
            # Try get_market() first for specific ticker
            try:
                market_data = client.get_market(ticker=ticker)
                market = market_data.get('market', {})
                price = market.get('yes_bid') or market.get('yes_ask') or market.get('last_price') or 0
                if price > 0:
                    # Convert from cents to decimal (0-1)
                    return price / 100.0
            except:
                # Fallback to get_markets with tickers parameter
                response = client.get_markets(tickers=ticker, limit=1)
                markets = response.get('markets', [])
                
                if markets:
                    market = markets[0]
                    price = market.get('yes_bid') or market.get('yes_ask') or market.get('last_price') or 0
                    if price > 0:
                        # Convert from cents to decimal (0-1)
                        return price / 100.0
        
        # If it's an event ticker (contains one dash), get all markets for that event
        elif ticker.count('-') == 1:
            # Event ticker - get markets for this event and use best price
            response = client.get_markets(event_ticker=ticker, limit=100)
            markets = response.get('markets', [])
            
            if markets:
                # Get the highest priced contract as the "event price"
                best_price = 0
                for market in markets:
                    price = market.get('yes_bid') or market.get('yes_ask') or market.get('last_price') or 0
                    if price > best_price:
                        best_price = price
                
                if best_price > 0:
                    # Convert from cents to decimal (0-1)
                    return best_price / 100.0
        
        return None
        
    except Exception as e:
        print(f"⚠️  Error fetching price for {ticker}: {e}")
        return None


def get_cached_price(conn: sqlite3.Connection, ticker: str) -> Optional[Tuple[float, Optional[datetime]]]:
    """
    Retrieve last price and cooldown status from SQLite cache.
    
    Args:
        conn: SQLite connection
        ticker: Market ticker
    
    Returns:
        Tuple of (last_price, cooldown_until) or (None, None) if not found
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT last_price, cooldown_until 
        FROM market_cache 
        WHERE ticker = ?
    """, (ticker,))
    
    row = cursor.fetchone()
    if row:
        last_price, cooldown_until_str = row
        cooldown_until = None
        if cooldown_until_str:
            try:
                cooldown_until = datetime.fromisoformat(cooldown_until_str)
                # Ensure timezone-aware
                if cooldown_until.tzinfo is None:
                    cooldown_until = cooldown_until.replace(tzinfo=timezone.utc)
            except:
                pass
        return (last_price, cooldown_until)
    return (None, None)


def update_cache(conn: sqlite3.Connection, ticker: str, price: float, 
                 cooldown_until: Optional[datetime] = None):
    """
    Update SQLite cache with new price and optional cooldown.
    
    Args:
        conn: SQLite connection
        ticker: Market ticker
        price: Current price
        cooldown_until: Optional cooldown expiration time
    """
    cursor = conn.cursor()
    now = datetime.now(timezone.utc)
    
    cooldown_str = cooldown_until.isoformat() if cooldown_until else None
    
    cursor.execute("""
        INSERT OR REPLACE INTO market_cache 
        (ticker, last_price, last_updated, cooldown_until)
        VALUES (?, ?, ?, ?)
    """, (ticker, price, now.isoformat(), cooldown_str))
    
    conn.commit()


def send_alert(ticker: str, old_price: float, new_price: float, change_pct: float):
    """
    Send alert for price spike detection.
    
    Args:
        ticker: Market ticker
        old_price: Previous price
        new_price: Current price
        change_pct: Percentage change (0.15 = 15%)
    """
    # High-visibility terminal message
    print("\n" + "="*60)
    print("⚠️  SPIKE DETECTED ⚠️")
    print("="*60)
    print(f"Ticker: {ticker}")
    print(f"Old Price: ${old_price:.4f}")
    print(f"New Price: ${new_price:.4f}")
    print(f"Change: +{change_pct*100:.2f}%")
    print(f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("="*60 + "\n")
    
    # Send Slack notification if enabled
    if ENABLE_SLACK_ALERTS:
        try:
            slack_message = (
                f"🚨 *Price Spike Detected*\n"
                f"*Ticker:* {ticker}\n"
                f"*Old Price:* ${old_price:.4f}\n"
                f"*New Price:* ${new_price:.4f}\n"
                f"*Change:* +{change_pct*100:.2f}%\n"
                f"*Time:* {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
            )
            send_slack_message(slack_message)
        except Exception as e:
            print(f"⚠️  Failed to send Slack alert: {e}")


def is_in_cooldown(cooldown_until: Optional[datetime]) -> bool:
    """
    Check if market is currently in cooldown period.
    
    Args:
        cooldown_until: Cooldown expiration time
    
    Returns:
        True if in cooldown, False otherwise
    """
    if cooldown_until is None:
        return False
    
    now = datetime.now(timezone.utc)
    # Ensure cooldown_until is timezone-aware
    if cooldown_until.tzinfo is None:
        cooldown_until = cooldown_until.replace(tzinfo=timezone.utc)
    
    return now < cooldown_until


def monitor_market(client: ExchangeClient, conn: sqlite3.Connection, ticker: str):
    """
    Monitor a single market for price spikes.
    
    Args:
        client: ExchangeClient instance
        conn: SQLite connection
        ticker: Market ticker to monitor
    """
    try:
        # Get current price from API
        current_price = get_market_price(client, ticker)
        
        if current_price is None:
            # Price fetch failed - skip this iteration
            return
        
        # Get cached price and cooldown status
        cached_result = get_cached_price(conn, ticker)
        last_price, cooldown_until = cached_result
        
        # Check if in cooldown
        if is_in_cooldown(cooldown_until):
            # Still in cooldown - update price but don't alert
            update_cache(conn, ticker, current_price, cooldown_until)
            return
        
        # If this is the first time we're seeing this market, just cache it
        if last_price is None:
            update_cache(conn, ticker, current_price)
            return
        
        # Calculate price change
        if last_price > 0:
            change_pct = (current_price - last_price) / last_price
            
            # Check for spike (15% or more increase)
            if change_pct >= SPIKE_THRESHOLD:
                # Trigger alert
                send_alert(ticker, last_price, current_price, change_pct)
                
                # Set cooldown (10 minutes from now)
                cooldown_until = datetime.now(timezone.utc) + timedelta(seconds=COOLDOWN_SECONDS)
                update_cache(conn, ticker, current_price, cooldown_until)
            else:
                # No spike - just update price
                update_cache(conn, ticker, current_price)
        else:
            # Last price was 0 - just update
            update_cache(conn, ticker, current_price)
            
    except Exception as e:
        print(f"⚠️  Error monitoring {ticker}: {e}")
        # Continue monitoring other markets even if one fails


def test_alert():
    """Test function to verify Slack alerts work without waiting for a real spike."""
    print("="*60)
    print("TESTING SLACK ALERT")
    print("="*60)
    print("Sending test alert to Slack...\n")
    
    # Send a test alert
    test_ticker = TARGET_MARKETS[0] if TARGET_MARKETS else "TEST-TICKER"
    send_alert(test_ticker, 0.50, 0.60, 0.20)  # 20% spike
    
    print("\n✓ Test alert sent. Check your Slack channel for the message.")
    print("If you don't see it, check:")
    print("  1. SLACK_BOT_TOKEN is set in environment")
    print("  2. SLACK_CHANNEL is set (or defaults to #general)")
    print("  3. Bot is invited to the channel")


def main():
    """Main monitoring loop."""
    print("="*60)
    print("DELTA SNIPER BOT - Kalshi Market Monitor")
    print("="*60)
    print(f"Monitoring {len(TARGET_MARKETS)} markets")
    print(f"Spike threshold: {SPIKE_THRESHOLD*100}%")
    print(f"Polling interval: {POLL_INTERVAL} seconds")
    print(f"Cooldown period: {COOLDOWN_SECONDS} seconds ({COOLDOWN_SECONDS//60} minutes)")
    print(f"Slack alerts: {'ENABLED' if ENABLE_SLACK_ALERTS else 'DISABLED'}")
    print("="*60)
    print("\nPress Ctrl+C to stop monitoring\n")
    
    # Initialize database
    conn = init_database(DB_PATH)
    
    # Load private key and create client
    try:
        private_key = load_private_key(PRIVATE_KEY_PATH)
        client = ExchangeClient(
            exchange_api_base=EXCHANGE_API_BASE,
            key_id=KEY_ID,
            private_key=private_key
        )
        print("✓ Connected to Kalshi API\n")
    except Exception as e:
        print(f"✗ Failed to connect to Kalshi API: {e}")
        return
    
    # Main monitoring loop
    iteration = 0
    try:
        while True:
            iteration += 1
            print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}] Poll #{iteration} - Checking {len(TARGET_MARKETS)} markets...")
            
            for ticker in TARGET_MARKETS:
                monitor_market(client, conn, ticker)
            
            # Wait 5 seconds before next poll
            time.sleep(POLL_INTERVAL)
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Monitoring stopped by user")
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
    finally:
        conn.close()
        print("✓ Database connection closed")


if __name__ == "__main__":
    import sys
    
    # Check for test flag
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_alert()
    else:
        main()
