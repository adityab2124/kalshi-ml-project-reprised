#!/usr/bin/env python3
"""
Settlement Sync Script for Kalshi Trading Bot
Pulls final outcomes from Kalshi API for settled markets and stores them in PostgreSQL.
Designed to run once every 24 hours (via cron or scheduler).

FIXED VERSION:
- Uses /portfolio/settlements endpoint (more efficient)
- Correctly uses 'result' field (not 'market_result')
- Handles None results properly
- Falls back to individual market lookups only when needed
"""

import os
import sys
import re
from datetime import datetime, timezone
from dateutil import parser
from typing import Optional, List, Dict
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import psycopg2
from psycopg2.extras import RealDictCursor
from p import ExchangeClient
import db_postgres

# ===== CONFIGURATION =====

PRIVATE_KEY_PATH = "private_key.pem"
KEY_ID = "cc76eee9-dba9-4bf2-a06f-eddf6a44a8e1"
EXCHANGE_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Target markets to match against (from kalshi_ws.py)
TARGET_MARKETS = [
    "KXTRUMPSAY-26FEB02",
    "KXTRUMPMENTION-26JAN21",
    "KXTRUMPMENTIONB-26JAN21",
    "KXTRUMPMENTIONB-26JAN28",
    "KXTRUMPSAYMONTH-26FEB01"
]

# Test tickers: Add known settled market tickers here for testing
TEST_TICKERS = []

# ===== DATABASE HELPERS =====

def get_all_tracked_tickers() -> List[str]:
    """Get all unique tickers from market_history table."""
    if db_postgres.db_pool is None:
        db_postgres.init_db_pool()
    
    with db_postgres.db_pool.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT ticker FROM market_history ORDER BY ticker")
        results = cursor.fetchall()
        return [row[0] for row in results]

def upsert_settlement_result(ticker: str, final_price: int, settlement_time: datetime):
    """Insert or update settlement result in market_results table."""
    if db_postgres.db_pool is None:
        db_postgres.init_db_pool()
    
    with db_postgres.db_pool.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO market_results (ticker, final_price, settlement_time)
            VALUES (%s, %s, %s)
            ON CONFLICT (ticker) DO UPDATE SET
                final_price = EXCLUDED.final_price,
                settlement_time = EXCLUDED.settlement_time,
                updated_at = NOW()
        """, (ticker, final_price, settlement_time))

def get_already_settled_tickers() -> set:
    """Get tickers that are already in market_results (to skip if desired)."""
    if db_postgres.db_pool is None:
        db_postgres.init_db_pool()
    
    with db_postgres.db_pool.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT ticker FROM market_results")
        results = cursor.fetchall()
        return {row[0] for row in results}

# ===== KALSHI API HELPERS =====

def load_private_key(key_path: str):
    """Load RSA private key from PEM file."""
    with open(key_path, 'rb') as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    return private_key

def get_final_price_from_settlement(settlement_data: dict) -> Optional[int]:
    """
    Extract final price from SETTLEMENT object (from /portfolio/settlements).
    
    CRITICAL FIX: The raw JSON has 'market_result', but the SDK object doesn't expose it!
    Need to access the underlying dict or convert to dict first.
    """
    # If it's a pydantic model/object, convert to dict
    if hasattr(settlement_data, 'to_dict'):
        settlement_dict = settlement_data.to_dict()
    elif hasattr(settlement_data, 'dict'):
        settlement_dict = settlement_data.dict()
    elif hasattr(settlement_data, 'model_dump'):
        settlement_dict = settlement_data.model_dump()
    elif hasattr(settlement_data, '__dict__'):
        settlement_dict = settlement_data.__dict__
    else:
        settlement_dict = settlement_data
    
    # Now get market_result from the dict
    result = settlement_dict.get('market_result')
    
    if result is None:
        # Market is in limbo - settled_time exists but no outcome yet
        return None
    
    result_str = str(result).upper()
    if result_str == 'YES':
        return 100
    elif result_str == 'NO':
        return 0
    
    return None

def get_final_price_from_market(market_data: dict) -> Optional[int]:
    """
    Extract final price from MARKET object (from /markets/{ticker}).
    
    The response has structure: {"market": {...actual data...}}
    """
    # Market endpoint returns {"market": {...}}
    if 'market' in market_data:
        market = market_data['market']
    else:
        market = market_data
    
    # Check if market is actually settled/closed/determined
    status = market.get('status', '').lower()
    if status not in ['settled', 'closed', 'determined', 'finalized']:
        return None
    
    # Try the 'result' field first (from market data)
    result = market.get('result')
    
    # Fallback to settlement_value (numeric)
    if result is None:
        settlement_value = market.get('settlement_value')
        if settlement_value is not None:
            return int(settlement_value)
    
    if result is None:
        return None
    
    result_str = str(result).upper()
    if result_str == 'YES':
        return 100
    elif result_str == 'NO':
        return 0
    
    return None

def get_settlement_time(data: dict) -> Optional[datetime]:
    """Extract settlement_time from settlement or market data."""
    # Try settled_time first (from settlements endpoint)
    settlement_ts = data.get('settled_time')
    
    # Fallback to settlement_time or settlement_ts
    if settlement_ts is None:
        settlement_ts = data.get('settlement_time') or data.get('settlement_ts')
    
    if settlement_ts:
        # Could be datetime object, timestamp in milliseconds, or ISO string
        if isinstance(settlement_ts, datetime):
            return settlement_ts
        elif isinstance(settlement_ts, (int, float)):
            return datetime.fromtimestamp(settlement_ts / 1000, tz=timezone.utc)
        elif isinstance(settlement_ts, str):
            return parser.parse(settlement_ts)
    
    # Last resort: use close_time
    close_time = data.get('close_time')
    if close_time:
        if isinstance(close_time, (int, float)):
            return datetime.fromtimestamp(close_time / 1000, tz=timezone.utc)
        elif isinstance(close_time, str):
            return parser.parse(close_time)
    
    return None

# ===== NEW: BULK SETTLEMENT SYNC =====

def sync_settlements_bulk(client: ExchangeClient, skip_existing: bool = True) -> int:
    """
    NEW APPROACH: Use /portfolio/settlements endpoint to get all your settlements at once.
    This is WAY more efficient than checking markets one-by-one.
    
    Returns:
        Number of new settlements saved
    """
    print("="*60)
    print("Fetching ALL settlements from Kalshi API")
    print("="*60)
    
    already_settled = get_already_settled_tickers() if skip_existing else set()
    
    settled_count = 0
    skipped_count = 0
    pending_count = 0
    
    try:
        # Get all your settlements (paginated)
        cursor = None
        page = 1
        
        while True:
            print(f"\nFetching page {page}...")
            
            # Call /portfolio/settlements
            params = {'limit': 200}
            if cursor:
                params['cursor'] = cursor
            
            response = client.get_portfolio_settlements(**params)
            settlements = response.get('settlements', [])
            
            if not settlements:
                print("No more settlements found")
                break
            
            print(f"Processing {len(settlements)} settlements from page {page}...")
            
            for settlement in settlements:
                ticker = settlement.get('ticker')
                
                if not ticker:
                    continue
                
                # Skip if already in DB
                if skip_existing and ticker in already_settled:
                    skipped_count += 1
                    continue
                
                # Extract result and settlement time
                final_price = get_final_price_from_settlement(settlement)
                settlement_time = get_settlement_time(settlement)
                
                if final_price is None:
                    # Result is None - market settled but no outcome yet (limbo)
                    pending_count += 1
                    if pending_count % 20 == 1:  # Log occasionally
                        print(f"  ⏳ {ticker}: Settled but no result yet (pending)")
                    continue
                
                if settlement_time is None:
                    print(f"  ⚠️  {ticker}: Has result but no settlement_time")
                    continue
                
                # Save to database
                upsert_settlement_result(ticker, final_price, settlement_time)
                settled_count += 1
                
                outcome_str = "YES" if final_price == 100 else "NO"
                print(f"  ✓ {ticker}: {outcome_str} @ {settlement_time.isoformat()}")
            
            # Check for next page
            cursor = response.get('cursor')
            if not cursor:
                print("\nReached end of settlements")
                break
            
            page += 1
    
    except Exception as e:
        print(f"\n✗ Error fetching settlements: {e}")
        import traceback
        traceback.print_exc()
    
    # Summary
    print("\n" + "="*60)
    print("Bulk Settlements Sync Complete")
    print("="*60)
    print(f"New settlements saved: {settled_count}")
    print(f"Skipped (already in DB): {skipped_count}")
    print(f"Pending (no result yet): {pending_count}")
    
    return settled_count

# ===== ORIGINAL SYNC LOGIC (kept for fallback) =====

def sync_settlements_individual(skip_existing: bool = True, test_tickers: Optional[List[str]] = None, 
                               client: Optional[ExchangeClient] = None):
    """
    FALLBACK: Individual market lookups (slower, less efficient).
    Use this only if you need to check specific tickers not in your settlements.
    """
    print("="*60)
    print("Kalshi Settlement Sync (Individual Market Lookups)")
    print("="*60)
    print(f"Started at: {datetime.now(timezone.utc).isoformat()}\n")
    
    db_postgres.initialize_database()
    
    if client is None:
        try:
            private_key = load_private_key(PRIVATE_KEY_PATH)
        except FileNotFoundError:
            print(f"ERROR: Private key not found at {PRIVATE_KEY_PATH}")
            return
        
        client = ExchangeClient(
            exchange_api_base=EXCHANGE_API_BASE,
            key_id=KEY_ID,
            private_key=private_key
        )
    
    # Get tickers to check
    if test_tickers:
        print(f"🧪 TEST MODE: Checking {len(test_tickers)} specified tickers\n")
        tickers_to_check = test_tickers
    else:
        print("Fetching tracked tickers from database...")
        all_tickers = get_all_tracked_tickers()
        print(f"Found {len(all_tickers)} unique tickers in market_history\n")
        
        if skip_existing:
            already_settled = get_already_settled_tickers()
            tickers_to_check = [t for t in all_tickers if t not in already_settled]
            print(f"Skipping {len(already_settled)} already-settled markets")
            print(f"Checking {len(tickers_to_check)} markets...\n")
        else:
            tickers_to_check = all_tickers
    
    settled_count = 0
    error_count = 0
    skipped_count = 0
    
    for i, ticker in enumerate(tickers_to_check, 1):
        try:
            market_data = client.get_market(ticker)
            
            status = market_data.get('status', '').lower()
            if status not in ['settled', 'closed', 'determined']:
                skipped_count += 1
                if i % 50 == 0:
                    print(f"  [{i}/{len(tickers_to_check)}] {ticker}: {status} (skipped)")
                continue
            
            final_price = get_final_price_from_market(market_data)
            settlement_time = get_settlement_time(market_data)
            
            if final_price is None:
                print(f"  ⚠️  {ticker}: Settled but couldn't determine final_price")
                error_count += 1
                continue
            
            if settlement_time is None:
                print(f"  ⚠️  {ticker}: Settled but couldn't determine settlement_time")
                error_count += 1
                continue
            
            upsert_settlement_result(ticker, final_price, settlement_time)
            settled_count += 1
            
            outcome_str = "YES" if final_price == 100 else "NO"
            print(f"  ✓ [{i}/{len(tickers_to_check)}] {ticker}: {outcome_str} @ {settlement_time.isoformat()}")
            
        except Exception as e:
            error_count += 1
            print(f"  ✗ [{i}/{len(tickers_to_check)}] {ticker}: Error - {e}")
    
    print("\n" + "="*60)
    print("Individual Sync Complete")
    print("="*60)
    print(f"Total tickers checked: {len(tickers_to_check)}")
    print(f"Newly settled: {settled_count}")
    print(f"Skipped (not settled): {skipped_count}")
    print(f"Errors: {error_count}")
    
    db_postgres.shutdown_database()

# ===== MAIN =====

if __name__ == "__main__":
    # Parse command line arguments
    skip_existing = True
    test_tickers = None
    use_bulk = True  # NEW: Default to bulk mode
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--force":
            skip_existing = False
            print("⚠️  Running in --force mode (will re-check all markets)\n")
        elif sys.argv[1] == "--individual":
            use_bulk = False
            print("⚠️  Using individual market lookups (slower)\n")
        elif sys.argv[1] == "--test":
            use_bulk = False
            if len(sys.argv) > 2:
                test_tickers = [t.strip() for t in sys.argv[2].split(",")]
            elif TEST_TICKERS:
                test_tickers = TEST_TICKERS
            else:
                print("ERROR: --test requires comma-separated tickers")
                sys.exit(1)
            print(f"🧪 TEST MODE: Will check {len(test_tickers)} tickers\n")
    
    # Load API credentials
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
    
    # Initialize database
    db_postgres.initialize_database()
    
    try:
        if use_bulk:
            # NEW: Use bulk settlements endpoint (recommended)
            sync_settlements_bulk(client, skip_existing=skip_existing)
        else:
            # OLD: Individual market lookups
            sync_settlements_individual(skip_existing=skip_existing, test_tickers=test_tickers, client=client)
    finally:
        db_postgres.shutdown_database()
