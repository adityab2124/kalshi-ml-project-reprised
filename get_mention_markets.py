#!/usr/bin/env python3
"""
Fetch ALL historical settled mention markets from Kalshi API.
This gets markets you didn't trade in too - the full universe of settled mention markets.
"""

import os
import sys
from datetime import datetime, timezone
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from p import ExchangeClient
import db_postgres

PRIVATE_KEY_PATH = "private_key.pem"
KEY_ID = "cc76eee9-dba9-4bf2-a06f-eddf6a44a8e1"
EXCHANGE_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Mention market series to fetch
MENTION_SERIES = [
    "KXTRUMPMENTION",
    "KXTRUMPMENTIONB", 
    "KXTRUMPSAY",
    "KXBERNIEMENTION",
    "KXMAMDANIMENTION",
    "KXKIMMELMENTION",
    "KXNEWSNATIONMENTION"
]

def load_private_key(key_path: str):
    with open(key_path, 'rb') as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    return private_key

def get_settlement_time(market_data: dict):
    """Extract settlement timestamp from market."""
    settlement_ts = market_data.get('settlement_ts')
    if settlement_ts:
        if isinstance(settlement_ts, str):
            from dateutil import parser
            return parser.parse(settlement_ts)
        elif isinstance(settlement_ts, (int, float)):
            return datetime.fromtimestamp(settlement_ts / 1000, tz=timezone.utc)
    
    # Fallback to close_time
    close_time = market_data.get('close_time')
    if close_time:
        if isinstance(close_time, str):
            from dateutil import parser
            return parser.parse(close_time)
    
    return None

def fetch_all_mention_markets(client, start_date=None, end_date=None):
    """
    Fetch ALL settled mention markets from Kalshi.
    
    Args:
        client: ExchangeClient instance
        start_date: Optional start date (ISO string or datetime)
        end_date: Optional end date (ISO string or datetime)
    
    Returns:
        List of market dicts with ticker, result, settlement_time
    """
    print("="*70)
    print("Fetching ALL Historical Mention Markets from Kalshi")
    print("="*70)
    
    all_markets = []
    
    for series in MENTION_SERIES:
        print(f"\n📊 Fetching series: {series}")
        print("-"*70)
        
        try:
            # Get markets for this series
            params = {
                'series_ticker': series,
                'status': 'settled',  # Only settled markets
                'limit': 1000
            }
            
            if start_date:
                params['min_settled_ts'] = start_date if isinstance(start_date, str) else start_date.isoformat()
            if end_date:
                params['max_settled_ts'] = end_date if isinstance(end_date, str) else end_date.isoformat()
            
            # Note: get_markets doesn't take series_ticker directly
            # We need to use event_ticker filter or fetch all and filter
            
            # Strategy: Get all settled markets and filter by ticker pattern
            cursor = None
            page = 1
            series_count = 0
            
            while True:
                print(f"  Fetching page {page}...", end='', flush=True)
                
                fetch_params = {
                    'status': 'settled',
                    'limit': 200
                }
                
                if cursor:
                    fetch_params['cursor'] = cursor
                if start_date:
                    fetch_params['min_settled_ts'] = start_date if isinstance(start_date, str) else start_date.isoformat()
                if end_date:
                    fetch_params['max_settled_ts'] = end_date if isinstance(end_date, str) else end_date.isoformat()
                
                response = client.get_markets(**fetch_params)
                markets = response.get('markets', [])
                
                if not markets:
                    print(" no more markets")
                    break
                
                # Filter for this series
                matching_markets = [m for m in markets if m.get('ticker', '').startswith(series)]
                series_count += len(matching_markets)
                
                print(f" found {len(matching_markets)}/{len(markets)} matching")
                
                for market in matching_markets:
                    ticker = market.get('ticker')
                    result = market.get('result')
                    settlement_time = get_settlement_time(market)
                    settlement_value = market.get('settlement_value')
                    status = market.get('status')
                    
                    # Convert result to price
                    if result is not None:
                        result_str = str(result).upper()
                        final_price = 100 if result_str == 'YES' else 0 if result_str == 'NO' else None
                    elif settlement_value is not None:
                        final_price = int(settlement_value)
                    else:
                        final_price = None
                    
                    if final_price is not None and settlement_time:
                        all_markets.append({
                            'ticker': ticker,
                            'result': result,
                            'final_price': final_price,
                            'settlement_time': settlement_time,
                            'series': series
                        })
                        
                        # Print sample
                        if len(all_markets) <= 5 or len(all_markets) % 20 == 0:
                            outcome = "YES" if final_price == 100 else "NO"
                            print(f"    ✓ {ticker}: {outcome} @ {settlement_time.date()}")
                
                # Check for next page
                cursor = response.get('cursor')
                if not cursor:
                    break
                
                page += 1
                
                # Safety limit
                if page > 50:
                    print("  ⚠️  Reached page limit, stopping")
                    break
            
            print(f"  Total for {series}: {series_count} settled markets")
            
        except Exception as e:
            print(f"  ✗ Error fetching {series}: {e}")
            import traceback
            traceback.print_exc()
    
    return all_markets

def save_to_database(markets):
    """Save markets to database."""
    if not markets:
        print("\n⚠️  No markets to save")
        return
    
    print("\n" + "="*70)
    print(f"Saving {len(markets)} markets to database...")
    print("="*70)
    
    db_postgres.initialize_database()
    
    saved_count = 0
    skipped_count = 0
    
    for market in markets:
        try:
            # Check if already exists
            with db_postgres.db_pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT ticker FROM market_results WHERE ticker = %s", 
                             (market['ticker'],))
                exists = cursor.fetchone()
            
            if exists:
                skipped_count += 1
                continue
            
            # Insert
            with db_postgres.db_pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO market_results (ticker, final_price, settlement_time)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (ticker) DO NOTHING
                """, (market['ticker'], market['final_price'], market['settlement_time']))
            
            saved_count += 1
            
            if saved_count % 10 == 0:
                outcome = "YES" if market['final_price'] == 100 else "NO"
                print(f"  ✓ [{saved_count}] {market['ticker']}: {outcome}")
        
        except Exception as e:
            print(f"  ✗ Error saving {market['ticker']}: {e}")
    
    print(f"\n✅ Saved {saved_count} new markets")
    print(f"⏭️  Skipped {skipped_count} existing markets")
    
    db_postgres.shutdown_database()

if __name__ == "__main__":
    # Parse arguments
    start_date = None
    end_date = None
    save_db = True
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--2025":
            start_date = "2025-01-01T00:00:00Z"
            end_date = "2025-12-31T23:59:59Z"
            print("📅 Fetching 2025 data only\n")
        elif sys.argv[1] == "--2024":
            start_date = "2024-01-01T00:00:00Z"
            end_date = "2024-12-31T23:59:59Z"
            print("📅 Fetching 2024 data only\n")
        elif sys.argv[1] == "--all":
            print("📅 Fetching ALL historical data\n")
        elif sys.argv[1] == "--no-save":
            save_db = False
            print("⚠️  Running in preview mode (won't save to DB)\n")
    else:
        # Default: all time
        print("📅 Fetching ALL historical data (use --2025, --2024, or --all)\n")
    
    # Load credentials
    try:
        private_key = load_private_key(PRIVATE_KEY_PATH)
    except FileNotFoundError:
        print(f"ERROR: Private key not found at {PRIVATE_KEY_PATH}")
        sys.exit(1)
    
    client = ExchangeClient(
        exchange_api_base=EXCHANGE_API_BASE,
        key_id=KEY_ID,
        private_key=private_key
    )
    
    # Fetch markets
    markets = fetch_all_mention_markets(client, start_date, end_date)
    
    # Summary
    print("\n" + "="*70)
    print("Fetch Complete")
    print("="*70)
    print(f"Total markets found: {len(markets)}")
    
    if markets:
        # Group by series
        from collections import Counter
        series_counts = Counter(m['series'] for m in markets)
        print("\nBreakdown by series:")
        for series, count in series_counts.most_common():
            print(f"  {series}: {count} markets")
        
        # Date range
        dates = [m['settlement_time'] for m in markets]
        print(f"\nDate range: {min(dates).date()} to {max(dates).date()}")
        
        # Save to database
        if save_db:
            save_to_database(markets)
        else:
            print("\n⚠️  Not saving to database (use without --no-save to save)")
    else:
        print("\n⚠️  No settled mention markets found")
        print("This could mean:")
        print("  1. These markets only started recently")
        print("  2. API filters are too restrictive")
        print("  3. Series names are incorrect")