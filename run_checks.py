#!/usr/bin/env python3
"""
Systematic checks to verify data pipeline before calibration analysis
"""

from p import ExchangeClient
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import pandas as pd
from datetime import datetime, timedelta
from dateutil import parser
import time

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

def check_1_list_markets_with_dates(client):
    """Check 1: Can you list markets with status + dates?"""
    print("="*60)
    print("CHECK 1: List markets with status + dates")
    print("="*60)
    
    try:
        markets = client.get_markets(limit=100, status='open')
        market_list = markets.get('markets', [])
        
        if len(market_list) < 100:
            # Try settled too
            settled = client.get_markets(limit=100, status='settled')
            market_list.extend(settled.get('markets', []))
        
        print(f"  Fetched {len(market_list)} markets")
        
        # Check for status and close_time
        has_status = 0
        has_close_time = 0
        clean_timestamps = 0
        
        for m in market_list[:100]:
            if m.get('status'):
                has_status += 1
            if m.get('close_time'):
                has_close_time += 1
                try:
                    parser.parse(m.get('close_time'))
                    clean_timestamps += 1
                except:
                    pass
        
        print(f"  Markets with status: {has_status}/100")
        print(f"  Markets with close_time: {has_close_time}/100")
        print(f"  Markets with clean timestamps: {clean_timestamps}/100")
        
        if clean_timestamps >= 100:
            print("  ✓ PASS: Can pull 100+ markets with clean close timestamps")
            return True, market_list[:100]
        else:
            print(f"  ✗ FAIL: Only {clean_timestamps} markets with clean timestamps")
            return False, market_list[:100]
            
    except Exception as e:
        print(f"  ✗ FAIL: Error - {e}")
        return False, []

def check_2_settled_markets_outcomes(client):
    """Check 2: Do settled markets expose outcomes?"""
    print("\n" + "="*60)
    print("CHECK 2: Settled markets expose outcomes")
    print("="*60)
    
    try:
        markets = client.get_markets(limit=50, status='settled')
        market_list = markets.get('markets', [])
        
        if len(market_list) == 0:
            # Try finalized
            markets = client.get_markets(limit=50, status='finalized')
            market_list = markets.get('markets', [])
        
        print(f"  Fetched {len(market_list)} settled/finalized markets")
        
        # Check outcomes
        has_outcome = 0
        binary_outcomes = 0
        outcomes = []
        
        for m in market_list:
            outcome = m.get('outcome') or m.get('result')
            if outcome:
                has_outcome += 1
                outcome_str = str(outcome).upper()
                if outcome_str in ['YES', 'NO', 'Y', 'N', '1', '0', 'TRUE', 'FALSE']:
                    binary_outcomes += 1
                outcomes.append(outcome)
        
        print(f"  Markets with outcome field: {has_outcome}/{len(market_list)}")
        print(f"  Markets with binary outcome: {binary_outcomes}/{len(market_list)}")
        print(f"  Sample outcomes: {set(outcomes[:10])}")
        
        if binary_outcomes >= 20:
            print("  ✓ PASS: Can extract clear binary outcomes from settled markets")
            return True
        else:
            print(f"  ✗ FAIL: Only {binary_outcomes} markets with binary outcomes")
            return False
            
    except Exception as e:
        print(f"  ✗ FAIL: Error - {e}")
        return False

def check_3_markets_expose_price(client):
    """Check 3: Do markets expose a 'last price' or 'close price'?"""
    print("\n" + "="*60)
    print("CHECK 3: Markets expose last/close price")
    print("="*60)
    
    try:
        # Check open markets
        open_markets = client.get_markets(limit=20, status='open')
        open_list = open_markets.get('markets', [])
        
        open_with_price = 0
        for m in open_list[:10]:
            ticker = m.get('ticker')
            if ticker:
                try:
                    details = client.get_market(ticker=ticker)
                    market_data = details.get('market', details)
                    yes_price = market_data.get('yes_bid') or market_data.get('yes_ask')
                    if yes_price:
                        open_with_price += 1
                    time.sleep(0.1)
                except:
                    pass
        
        print(f"  Open markets with price: {open_with_price}/10")
        
        # Check settled markets
        settled_markets = client.get_markets(limit=20, status='settled')
        settled_list = settled_markets.get('markets', [])
        
        if len(settled_list) == 0:
            settled_markets = client.get_markets(limit=20, status='finalized')
            settled_list = settled_markets.get('markets', [])
        
        settled_with_price = 0
        for m in settled_list[:10]:
            ticker = m.get('ticker')
            if ticker:
                try:
                    details = client.get_market(ticker=ticker)
                    market_data = details.get('market', details)
                    yes_price = market_data.get('yes_bid') or market_data.get('yes_ask')
                    if yes_price:
                        settled_with_price += 1
                    time.sleep(0.1)
                except:
                    pass
        
        print(f"  Settled markets with current price: {settled_with_price}/10")
        print(f"  (Note: Settled markets may not have current prices)")
        
        if open_with_price >= 8:
            print("  ✓ PASS: Can get numeric price for open markets")
            return True
        else:
            print(f"  ✗ FAIL: Only {open_with_price} open markets with prices")
            return False
            
    except Exception as e:
        print(f"  ✗ FAIL: Error - {e}")
        import traceback
        traceback.print_exc()
        return False

def check_4_historical_prices_endpoint(client):
    """Check 4: Is there a historical prices/trades endpoint?"""
    print("\n" + "="*60)
    print("CHECK 4: Historical prices/trades endpoint")
    print("="*60)
    
    try:
        # Get a sample market
        markets = client.get_markets(limit=10, status='settled')
        market_list = markets.get('markets', [])
        
        if len(market_list) == 0:
            markets = client.get_markets(limit=10, status='finalized')
            market_list = markets.get('markets', [])
        
        if len(market_list) == 0:
            print("  ✗ FAIL: No settled markets to test")
            return False
        
        test_market = market_list[0]
        ticker = test_market.get('ticker')
        close_time = test_market.get('close_time')
        
        if not ticker or not close_time:
            print("  ✗ FAIL: Test market missing ticker or close_time")
            return False
        
        print(f"  Testing with market: {ticker}")
        print(f"  Close time: {close_time}")
        
        # Try to get historical prices
        try:
            close_dt = parser.parse(close_time)
            end_ts = int(close_dt.timestamp())
            start_ts = end_ts - 86400  # 1 day before
            
            parts = ticker.split('-', 1)
            series_ticker = parts[0] if parts else ticker
            
            history = client.get_market_history(
                series_ticker=series_ticker,
                market_ticker=ticker,
                period_interval=3600,  # 1 hour
                end_ts=end_ts,
                start_ts=start_ts
            )
            
            candlesticks = history.get('candlesticks', [])
            print(f"  Retrieved {len(candlesticks)} historical price points")
            
            if len(candlesticks) > 0:
                sample = candlesticks[0]
                print(f"  Sample data keys: {list(sample.keys())}")
                print(f"  Sample: {sample}")
                
                # Check if we have timestamp and price
                has_timestamp = 'ts' in sample or 'timestamp' in sample
                has_price = 'yes_price' in sample or 'yes_close' in sample or 'price' in sample
                
                if has_timestamp and has_price:
                    print("  ✓ PASS: Can query market and get (timestamp, price) points")
                    return True
                else:
                    print(f"  ✗ FAIL: Missing timestamp or price in response")
                    return False
            else:
                print("  ⚠️  WARNING: No historical data returned (may be API limitation)")
                print("  ✗ FAIL: Cannot get historical prices")
                return False
                
        except Exception as e:
            print(f"  ✗ FAIL: Error fetching history - {e}")
            import traceback
            traceback.print_exc()
            return False
            
    except Exception as e:
        print(f"  ✗ FAIL: Error - {e}")
        return False

def check_5_price_at_cutoff(client):
    """Check 5: Can you pull 'price at a fixed cutoff' before close?"""
    print("\n" + "="*60)
    print("CHECK 5: Price at fixed cutoff before close")
    print("="*60)
    
    try:
        # Get sample markets
        markets = client.get_markets(limit=25, status='settled')
        market_list = markets.get('markets', [])
        
        if len(market_list) == 0:
            markets = client.get_markets(limit=25, status='finalized')
            market_list = markets.get('markets', [])
        
        if len(market_list) < 20:
            print(f"  ✗ FAIL: Only {len(market_list)} markets available")
            return False
        
        print(f"  Testing with {min(20, len(market_list))} markets")
        
        cutoff_hours = 1  # 1 hour before close
        success_count = 0
        
        for i, m in enumerate(market_list[:20]):
            ticker = m.get('ticker')
            close_time = m.get('close_time')
            
            if not ticker or not close_time:
                continue
            
            try:
                close_dt = parser.parse(close_time)
                cutoff_ts = int(close_dt.timestamp()) - (cutoff_hours * 3600)
                start_ts = cutoff_ts - 3600  # 1 hour window
                
                parts = ticker.split('-', 1)
                series_ticker = parts[0] if parts else ticker
                
                history = client.get_market_history(
                    series_ticker=series_ticker,
                    market_ticker=ticker,
                    period_interval=300,  # 5 min
                    end_ts=cutoff_ts,
                    start_ts=start_ts
                )
                
                candlesticks = history.get('candlesticks', [])
                if candlesticks:
                    last_price = candlesticks[-1].get('yes_close') or candlesticks[-1].get('yes_price')
                    if last_price:
                        success_count += 1
                
                time.sleep(0.2)
                
            except Exception as e:
                pass
        
        print(f"  Successfully computed price-at-cutoff for {success_count}/20 markets")
        
        if success_count >= 15:
            print("  ✓ PASS: Can compute price-at-cutoff for sample markets")
            return True
        else:
            print(f"  ✗ FAIL: Only {success_count} markets with price-at-cutoff")
            return False
            
    except Exception as e:
        print(f"  ✗ FAIL: Error - {e}")
        return False

def check_6_dataset_sanity():
    """Check 6: Dataset sanity"""
    print("\n" + "="*60)
    print("CHECK 6: Dataset sanity")
    print("="*60)
    
    try:
        # Try to load existing CSV
        try:
            df = pd.read_csv('kalshi_markets.csv')
        except:
            print("  ⚠️  No existing dataset found, will check after data collection")
            return None
        
        print(f"  Loaded {len(df)} markets from CSV")
        
        # Check prices in [0,1] or [0,100]
        if 'yes_probability' in df.columns:
            prob_col = 'yes_probability'
            prob_max = df[prob_col].max()
            prob_min = df[prob_col].min()
            print(f"  Price range: [{prob_min:.3f}, {prob_max:.3f}]")
            
            if prob_max > 1.0:
                print("  ⚠️  Prices > 1.0, may need normalization")
            elif prob_max <= 1.0 and prob_min >= 0:
                print("  ✓ Prices in [0,1] range")
            else:
                print("  ✗ Prices out of expected range")
        
        # Check outcome is binary
        if 'outcome' in df.columns:
            outcomes = df['outcome'].dropna().unique()
            binary_outcomes = [str(o).upper() in ['YES', 'NO', 'Y', 'N', '1', '0', 'TRUE', 'FALSE'] for o in outcomes]
            if all(binary_outcomes):
                print("  ✓ Outcomes are binary")
            else:
                print(f"  ⚠️  Non-binary outcomes found: {set(outcomes)}")
        
        # Check missing close_time
        if 'close_time' in df.columns:
            missing_close = df['close_time'].isna().sum()
            if missing_close == 0:
                print("  ✓ No missing close_time")
            else:
                print(f"  ✗ {missing_close} markets missing close_time")
        
        # Check duplicate market ids
        if 'ticker' in df.columns:
            duplicates = df['ticker'].duplicated().sum()
            if duplicates == 0:
                print("  ✓ No duplicate market IDs")
            else:
                print(f"  ✗ {duplicates} duplicate market IDs")
        
        # Check sample size
        settled = df[df['status'].isin(['settled', 'finalized']) if 'status' in df.columns else df]
        with_outcomes = settled[settled['outcome'].notna()] if 'outcome' in df.columns else settled
        with_prices = with_outcomes[with_outcomes['yes_probability'].notna()] if 'yes_probability' in df.columns else with_outcomes
        
        print(f"  Resolved contracts: {len(with_prices)}")
        
        if len(with_prices) >= 500:
            print("  ✓ Sample size sufficient (500+)")
            return True
        elif len(with_prices) >= 100:
            print(f"  ⚠️  Sample size: {len(with_prices)} (ideally 500+)")
            return True
        else:
            print(f"  ✗ Sample size too small: {len(with_prices)}")
            return False
            
    except Exception as e:
        print(f"  ✗ FAIL: Error - {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all checks"""
    print("="*60)
    print("SYSTEMATIC CHECKS FOR CALIBRATION ANALYSIS")
    print("="*60)
    
    # Load client
    try:
        private_key = load_private_key(PRIVATE_KEY_PATH)
        client = ExchangeClient(
            exchange_api_base=EXCHANGE_API_BASE,
            key_id=KEY_ID,
            private_key=private_key
        )
    except Exception as e:
        print(f"ERROR: Failed to initialize client - {e}")
        return
    
    results = {}
    
    # Run checks
    results['check_1'] = check_1_list_markets_with_dates(client)
    results['check_2'] = check_2_settled_markets_outcomes(client)
    results['check_3'] = check_3_markets_expose_price(client)
    results['check_4'] = check_4_historical_prices_endpoint(client)
    results['check_5'] = check_5_price_at_cutoff(client)
    results['check_6'] = check_6_dataset_sanity()
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    for check_name, result in results.items():
        if result is True:
            status = "✓ PASS"
        elif result is False:
            status = "✗ FAIL"
        else:
            status = "⚠️  SKIP"
        
        print(f"  {check_name.upper()}: {status}")
    
    passed = sum(1 for r in results.values() if r is True)
    total = sum(1 for r in results.values() if r is not None)
    
    print(f"\n  Passed: {passed}/{total} checks")
    
    if passed == total:
        print("\n  ✓ ALL CHECKS PASSED - Ready for calibration analysis!")
    else:
        print(f"\n  ⚠️  {total - passed} checks failed - Review issues above")

if __name__ == "__main__":
    main()

