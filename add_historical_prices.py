#!/usr/bin/env python3
"""
Add historical prices to already-collected finalized markets.
Fetches the last price before each market closed.
"""

from p import ExchangeClient
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import pandas as pd
import time
from datetime import datetime, timedelta
from dateutil import parser

# ===== CONFIGURATION =====
EXCHANGE_API_BASE = "https://demo-api.kalshi.co/trade-api/v2"
KEY_ID = "cc76eee9-dba9-4bf2-a06f-eddf6a44a8e1"
PRIVATE_KEY_PATH = "private_key.pem"

def load_private_key(key_path: str):
    """Load RSA private key from PEM file"""
    with open(key_path, 'rb') as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    return private_key

def get_last_price_before_close(client: ExchangeClient, ticker: str, close_time_str: str):
    """
    Get the last price before market closed
    Returns (yes_price, no_price) in cents, or (None, None) if not available
    """
    try:
        # Parse close time
        close_dt = parser.parse(close_time_str)
        end_ts = int(close_dt.timestamp())
        start_ts = end_ts - 7200  # 2 hours before close
        
        # Extract series ticker (first part)
        parts = ticker.split('-', 1)
        if len(parts) < 1:
            return None, None
        
        series_ticker = parts[0]
        
        # Get historical prices
        history = client.get_market_history(
            series_ticker=series_ticker,
            market_ticker=ticker,
            period_interval=300,  # 5 min intervals
            end_ts=end_ts,
            start_ts=start_ts
        )
        
        # Get last price before close
        candlesticks = history.get('candlesticks', [])
        if candlesticks:
            last_candle = candlesticks[-1]
            yes_price = last_candle.get('yes_close') or last_candle.get('yes_price')
            no_price = last_candle.get('no_close') or last_candle.get('no_price')
            return yes_price, no_price
        
    except Exception as e:
        pass
    
    return None, None

def add_historical_prices(input_file="kalshi_markets.csv", output_file="kalshi_markets_with_prices.csv"):
    """Add historical prices to finalized markets"""
    print("="*60)
    print("ADDING HISTORICAL PRICES TO FINALIZED MARKETS")
    print("="*60)
    
    # Load data
    df = pd.read_csv(input_file)
    print(f"\nLoaded {len(df)} markets from {input_file}")
    
    # Filter to finalized markets without prices
    finalized = df[df['status'].isin(['settled', 'finalized'])].copy()
    no_prices = finalized[finalized['yes_probability'].isna() | (finalized['yes_price_cents'] == 0)].copy()
    
    print(f"Found {len(no_prices)} finalized markets without prices")
    
    if len(no_prices) == 0:
        print("All markets already have prices!")
        return
    
    # Load client
    private_key = load_private_key(PRIVATE_KEY_PATH)
    client = ExchangeClient(
        exchange_api_base=EXCHANGE_API_BASE,
        key_id=KEY_ID,
        private_key=private_key
    )
    
    # Fetch historical prices
    print(f"\nFetching historical prices for {len(no_prices)} markets...")
    print("(This may take a while due to rate limiting)\n")
    
    updated_count = 0
    for idx, row in no_prices.iterrows():
        ticker = row['ticker']
        close_time = row.get('close_time', '')
        
        if not close_time:
            continue
        
        print(f"  [{updated_count + 1}/{len(no_prices)}] {ticker}...", end=' ')
        
        yes_price, no_price = get_last_price_before_close(client, ticker, close_time)
        
        if yes_price is not None:
            # Update the dataframe
            df.at[idx, 'yes_price_cents'] = yes_price
            df.at[idx, 'no_price_cents'] = no_price
            df.at[idx, 'yes_probability'] = yes_price / 100
            df.at[idx, 'no_probability'] = no_price / 100
            updated_count += 1
            print(f"✓ ${yes_price/100:.2f}")
        else:
            print("✗ (no data)")
        
        time.sleep(0.2)  # Rate limiting
    
    # Save updated data
    print(f"\n✓ Updated {updated_count} markets with historical prices")
    df.to_csv(output_file, index=False)
    print(f"✓ Saved to {output_file}")
    
    # Summary
    finalized_with_prices = df[
        (df['status'].isin(['settled', 'finalized'])) & 
        (df['yes_probability'].notna()) & 
        (df['yes_price_cents'] > 0)
    ]
    
    print(f"\nSummary:")
    print(f"  Total finalized markets: {len(finalized)}")
    print(f"  With prices: {len(finalized_with_prices)}")
    print(f"  With outcomes: {finalized[finalized['outcome'].notna()].shape[0]}")
    print(f"  Ready for analysis: {len(finalized_with_prices[finalized_with_prices['outcome'].notna()])}")

if __name__ == "__main__":
    add_historical_prices()

