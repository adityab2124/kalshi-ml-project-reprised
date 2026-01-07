#!/usr/bin/env python3
"""
Build historical dataset from settled Kalshi markets.

Output: dataset.csv with one row per market:
market_id, close_time, cutoff_time, price_at_cutoff, outcome, time_gap_minutes, num_trades_used
"""

from p import ExchangeClient
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import pandas as pd
import csv
from datetime import datetime, timedelta
from dateutil import parser
import time

# ===== CONFIGURATION =====
EXCHANGE_API_BASE = "https://demo-api.kalshi.co/trade-api/v2"
KEY_ID = os.getenv("KALSHI_KEY_ID")
PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH", "private_key.pem")
CUTOFF_HOURS_BEFORE_CLOSE = 6  # Cutoff = close_time - 6 hours
TRADES_WINDOW_HOURS = 24  # Fetch trades in [cutoff - 24h, close_time]
TARGET_MARKETS = 300
TEST_MODE = False  # Set to True to test with 20 markets first

def load_private_key(key_path: str):
    """Load RSA private key from PEM file"""
    with open(key_path, 'rb') as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    return private_key

def get_trades_for_market(client: ExchangeClient, ticker: str, start_ts: int, end_ts: int):
    """Get trades for a market in the specified time window"""
    try:
        # Fetch trades without ticker filter (ticker filter may not work for historical)
        # Then filter by ticker and timestamp in memory
        all_trades = []
        cursor = None
        max_iterations = 10  # Limit to prevent infinite loops
        
        for i in range(max_iterations):
            try:
                params = {'limit': 1000}
                if cursor:
                    params['cursor'] = cursor
                
                trades_response = client.get_trades(**params)
                trade_list = trades_response.get('trades', [])
                
                if not trade_list:
                    break
                
                all_trades.extend(trade_list)
                
                # Check if we have enough trades or if we've gone past our time window
                # Filter and check if we have trades in our window
                filtered = [t for t in all_trades 
                           if t.get('ticker') == ticker 
                           and start_ts <= parser.parse(t.get('created_time', '1970-01-01')).timestamp() <= end_ts]
                
                if len(filtered) >= 10:  # If we have enough, we can stop
                    break
                
                cursor = trades_response.get('cursor')
                if not cursor:
                    break
                
                time.sleep(0.2)
            except Exception as e:
                break
        
        # Filter by ticker and timestamp
        filtered_trades = []
        for trade in all_trades:
            if trade.get('ticker') != ticker:
                continue
            
            trade_time_str = trade.get('created_time', '')
            if trade_time_str:
                try:
                    trade_dt = parser.parse(trade_time_str)
                    trade_ts = int(trade_dt.timestamp())
                    if start_ts <= trade_ts <= end_ts:
                        filtered_trades.append(trade)
                except:
                    pass
        
        return filtered_trades
    except Exception as e:
        print(f"      Error fetching trades: {e}")
        return []

def find_price_at_cutoff(trades: list, cutoff_ts: int, close_ts: int):
    """
    Find price at cutoff time.
    Strategy:
    1. Find trade with latest timestamp <= cutoff_time
    2. If none, fallback to last trade before close_time
    Returns: (price, trade_timestamp, num_trades_used) or (None, None, len(trades))
    """
    if not trades:
        return None, None, 0
    
    # Sort trades by timestamp (newest first)
    sorted_trades = sorted(trades, key=lambda t: t.get('created_time', ''), reverse=True)
    
    # Convert cutoff_ts to datetime for comparison
    cutoff_dt = datetime.fromtimestamp(cutoff_ts)
    close_dt = datetime.fromtimestamp(close_ts)
    
    # Strategy 1: Find trade with latest timestamp <= cutoff_time
    for trade in sorted_trades:
        trade_time_str = trade.get('created_time', '')
        if not trade_time_str:
            continue
        
        try:
            trade_dt = parser.parse(trade_time_str)
            if trade_dt <= cutoff_dt:
                # Found trade at or before cutoff
                price = trade.get('price')
                if price is not None:
                    return price, trade_dt.timestamp(), len(trades)
        except:
            continue
    
    # Strategy 2: Fallback to last trade before close_time
    for trade in sorted_trades:
        trade_time_str = trade.get('created_time', '')
        if not trade_time_str:
            continue
        
        try:
            trade_dt = parser.parse(trade_time_str)
            if trade_dt <= close_dt:
                price = trade.get('price')
                if price is not None:
                    return price, trade_dt.timestamp(), len(trades)
        except:
            continue
    
    # No valid trade found
    return None, None, len(trades)

def build_dataset():
    """Build the historical dataset"""
    print("="*60)
    print("BUILDING HISTORICAL DATASET FROM SETTLED MARKETS")
    print("="*60)
    target = 20 if TEST_MODE else TARGET_MARKETS
    print(f"Target: {target} markets {'(TEST MODE)' if TEST_MODE else ''}")
    print(f"Cutoff: {CUTOFF_HOURS_BEFORE_CLOSE} hours before close")
    print(f"Trades window: {TRADES_WINDOW_HOURS} hours\n")
    
    # Load client
    private_key = load_private_key(PRIVATE_KEY_PATH)
    client = ExchangeClient(
        exchange_api_base=EXCHANGE_API_BASE,
        key_id=KEY_ID,
        private_key=private_key
    )
    
    # Step 1: Pull settled markets
    print("Step 1: Fetching settled markets...")
    all_markets = []
    cursor = None
    page = 1
    
    while len(all_markets) < TARGET_MARKETS:
        try:
            params = {'limit': 100, 'status': 'settled'}
            if cursor:
                params['cursor'] = params.get('cursor', cursor)
            
            response = client.get_markets(**params)
            markets = response.get('markets', [])
            
            if not markets:
                # Try finalized status
                params['status'] = 'finalized'
                response = client.get_markets(**params)
                markets = response.get('markets', [])
            
            if not markets:
                break
            
            # Filter to markets with close_time and outcome
            for m in markets:
                if len(all_markets) >= TARGET_MARKETS:
                    break
                
                close_time = m.get('close_time')
                outcome = m.get('outcome') or m.get('result')
                
                if close_time and outcome:
                    # Normalize outcome to binary
                    outcome_str = str(outcome).upper()
                    if outcome_str in ['YES', 'Y', '1', 'TRUE']:
                        binary_outcome = 1
                    elif outcome_str in ['NO', 'N', '0', 'FALSE']:
                        binary_outcome = 0
                    else:
                        continue  # Skip non-binary outcomes
                    
                    all_markets.append({
                        'ticker': m.get('ticker'),
                        'close_time': close_time,
                        'outcome': binary_outcome
                    })
            
            cursor = response.get('cursor')
            if not cursor:
                break
            
            page += 1
            time.sleep(0.2)
            
        except Exception as e:
            print(f"  Error fetching markets: {e}")
            break
    
    print(f"  Fetched {len(all_markets)} settled markets with close_time + outcome\n")
    
    # Step 2-6: Process each market
    print("Step 2-6: Processing markets to find price_at_cutoff...")
    dataset = []
    valid_count = 0
    
    for i, market in enumerate(all_markets, 1):
        ticker = market['ticker']
        close_time_str = market['close_time']
        outcome = market['outcome']
        
        print(f"  [{i}/{len(all_markets)}] {ticker}...", end=' ')
        
        try:
            # Parse close_time
            close_dt = parser.parse(close_time_str)
            close_ts = int(close_dt.timestamp())
            
            # Calculate cutoff_time (close_time - 6 hours)
            cutoff_dt = close_dt - timedelta(hours=CUTOFF_HOURS_BEFORE_CLOSE)
            cutoff_ts = int(cutoff_dt.timestamp())
            
            # Calculate trades window [cutoff - 24h, close_time]
            window_start_dt = cutoff_dt - timedelta(hours=TRADES_WINDOW_HOURS)
            window_start_ts = int(window_start_dt.timestamp())
            
            # Try to get market details for last_price as fallback
            try:
                market_details = client.get_market(ticker=ticker)
                market_data = market_details.get('market', market_details)
                last_price = market_data.get('last_price')
                previous_price = market_data.get('previous_price')
            except:
                market_data = None
                last_price = None
                previous_price = None
            
            # Fetch trades in window
            trades = get_trades_for_market(client, ticker, window_start_ts, close_ts)
            
            # Find price at cutoff
            price_at_cutoff, trade_ts, num_trades_used = find_price_at_cutoff(
                trades, cutoff_ts, close_ts
            )
            
            # Fallback: if no trades found, use last_price or previous_price
            if price_at_cutoff is None:
                if last_price and last_price > 0:
                    price_at_cutoff = last_price
                    trade_ts = cutoff_ts  # Approximate
                    num_trades_used = 0  # Indicates we used market data, not trades
                elif previous_price and previous_price > 0:
                    price_at_cutoff = previous_price
                    trade_ts = cutoff_ts
                    num_trades_used = 0
            
            if price_at_cutoff is not None:
                # Calculate time_gap_minutes
                if trade_ts:
                    time_gap_minutes = (cutoff_ts - trade_ts) / 60
                else:
                    time_gap_minutes = None
                
                dataset.append({
                    'market_id': ticker,
                    'close_time': close_time_str,
                    'cutoff_time': cutoff_dt.isoformat() + 'Z',
                    'price_at_cutoff': price_at_cutoff,
                    'outcome': outcome,
                    'time_gap_minutes': round(time_gap_minutes, 2) if time_gap_minutes is not None else None,
                    'num_trades_used': num_trades_used
                })
                valid_count += 1
                source = "trades" if num_trades_used > 0 else "market_data"
                print(f"✓ ${price_at_cutoff:.4f} (gap: {time_gap_minutes:.1f}min, {source}, trades: {num_trades_used})")
            else:
                print("✗ No valid price found")
            
            time.sleep(0.2)  # Rate limiting
            
        except Exception as e:
            print(f"✗ Error: {e}")
            continue
    
    # Step 7: Save to CSV
    print(f"\nStep 7: Saving dataset...")
    if dataset:
        df = pd.DataFrame(dataset)
        df.to_csv('dataset.csv', index=False)
        print(f"  ✓ Saved {len(dataset)} rows to dataset.csv")
    else:
        print("  ✗ No data to save")
        return
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Total markets processed: {len(all_markets)}")
    print(f"Markets with valid price_at_cutoff: {valid_count}")
    print(f"Success rate: {valid_count/len(all_markets)*100:.1f}%")
    
    if valid_count > 0:
        df = pd.DataFrame(dataset)
        print(f"\nPrice statistics:")
        print(f"  Mean: ${df['price_at_cutoff'].mean():.4f}")
        print(f"  Min: ${df['price_at_cutoff'].min():.4f}")
        print(f"  Max: ${df['price_at_cutoff'].max():.4f}")
        print(f"  Std: ${df['price_at_cutoff'].std():.4f}")
        
        print(f"\nTime gap statistics:")
        print(f"  Mean: {df['time_gap_minutes'].mean():.1f} minutes")
        print(f"  Median: {df['time_gap_minutes'].median():.1f} minutes")
        
        print(f"\nOutcome distribution:")
        print(f"  YES (1): {(df['outcome'] == 1).sum()} ({(df['outcome'] == 1).mean()*100:.1f}%)")
        print(f"  NO (0): {(df['outcome'] == 0).sum()} ({(df['outcome'] == 0).mean()*100:.1f}%)")
        
        print(f"\n✓ Dataset ready for calibration analysis!")

if __name__ == "__main__":
    build_dataset()

