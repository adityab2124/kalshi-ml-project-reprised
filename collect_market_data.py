#!/usr/bin/env python3
"""
Collect Kalshi market data for analysis:
- Current prices (probabilities)
- Historical prices
- Market outcomes (when settled)
"""

from p import ExchangeClient
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import json
import csv
from datetime import datetime, timedelta
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

def collect_market_data(client: ExchangeClient, output_file="kalshi_market_data.csv", 
                       status='settled', max_pages=10, max_markets=500):
    """
    Collect market data including:
    - Ticker
    - Current yes/no prices (probabilities)
    - Market status
    - Close time
    - Outcome (if settled)
    
    Args:
        status: 'settled' (for outcomes) or 'open' (for current prices)
        max_pages: Maximum number of pages to fetch (default 10 = ~1000 markets)
        max_markets: Maximum total markets to collect
    """
    print("="*60)
    print(f"COLLECTING KALSHI MARKET DATA (status={status})")
    print("="*60)
    
    all_markets = []
    cursor = None
    page = 1
    
    # Collect markets (with pagination)
    while page <= max_pages and len(all_markets) < max_markets:
        print(f"\nFetching page {page}...")
        try:
            params = {'limit': 100, 'status': status}
            if cursor:
                params['cursor'] = cursor
            
            markets_response = client.get_markets(**params)
            markets = markets_response.get('markets', [])
            
            if not markets:
                print("  No more markets found")
                break
            
            print(f"  Found {len(markets)} markets on this page")
            
            # Process each market
            for market in markets:
                if len(all_markets) >= max_markets:
                    print(f"  Reached max_markets limit ({max_markets})")
                    break
                
                ticker = market.get('ticker', '')
                
                # Try to use data from list response first (faster)
                # Only fetch details if needed
                market_data = market
                
                # If list response has enough data, use it; otherwise fetch details
                if not market.get('yes_bid') and not market.get('yes_ask'):
                    try:
                        market_details = client.get_market(ticker=ticker)
                        market_data = market_details.get('market', market_details)
                        time.sleep(0.1)  # Rate limiting for individual calls
                    except Exception as e:
                        print(f"    Error fetching {ticker}: {e}")
                        continue
                
                # Extract key data
                try:
                    yes_price = market_data.get('yes_bid', None)
                    no_price = market_data.get('no_bid', None)
                    
                    # If no bid, try ask prices
                    if yes_price is None:
                        yes_price = market_data.get('yes_ask', None)
                    if no_price is None:
                        no_price = market_data.get('no_ask', None)
                    
                    # For finalized markets without current prices, try to get historical price
                    if (yes_price is None or yes_price == 0) and market_data.get('status') in ['settled', 'finalized']:
                        ticker = market_data.get('ticker', ticker)
                        if ticker:
                            # Try to get last price before market closed
                            try:
                                # Extract series ticker (first part before market details)
                                parts = ticker.split('-', 1)
                                if len(parts) >= 1:
                                    series_ticker = parts[0]
                                    # Get historical prices (last hour before close)
                                    import time
                                    from datetime import datetime, timedelta
                                    
                                    # Get close time if available
                                    close_time_str = market_data.get('close_time', '')
                                    if close_time_str:
                                        try:
                                            from dateutil import parser
                                            close_dt = parser.parse(close_time_str)
                                            end_ts = int(close_dt.timestamp())
                                            start_ts = end_ts - 3600  # 1 hour before close
                                            
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
                                                yes_price = last_candle.get('yes_close', last_candle.get('yes_price'))
                                                no_price = last_candle.get('no_close', last_candle.get('no_price'))
                                        except Exception as e:
                                            pass  # If historical fetch fails, keep trying other methods
                            except Exception as e:
                                pass  # Continue with None prices if historical fetch fails
                    
                    # Convert to probability (prices are in cents, so divide by 100)
                    yes_prob = yes_price / 100 if yes_price else None
                    no_prob = no_price / 100 if no_price else None
                    
                    market_record = {
                        'ticker': ticker,
                        'title': market_data.get('title', market_data.get('event_title', '')),
                        'status': market_data.get('status', ''),
                        'yes_price_cents': yes_price,
                        'no_price_cents': no_price,
                        'yes_probability': yes_prob,
                        'no_probability': no_prob,
                        'close_time': market_data.get('close_time', ''),
                        'outcome': market_data.get('result', market_data.get('outcome', '')),
                        'category': market_data.get('category', ''),
                        'subtitle': market_data.get('subtitle', ''),
                    }
                    
                    all_markets.append(market_record)
                except Exception as e:
                    print(f"    Error processing {ticker}: {e}")
                    continue
            
            # Check for next page
            cursor = markets_response.get('cursor')
            if not cursor:
                print("  No more pages available")
                break
            
            page += 1
            time.sleep(0.2)  # Rate limiting
            
        except Exception as e:
            print(f"Error: {e}")
            break
    
    # Save to CSV
    if all_markets:
        print(f"\nSaving {len(all_markets)} markets to {output_file}...")
        fieldnames = ['ticker', 'title', 'status', 'yes_price_cents', 'no_price_cents', 
                     'yes_probability', 'no_probability', 'close_time', 'outcome', 
                     'category', 'subtitle']
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_markets)
        
        print(f"✓ Data saved to {output_file}")
        
        # Print summary
        settled = [m for m in all_markets if m['status'] == 'settled']
        open_markets = [m for m in all_markets if m['status'] == 'open']
        
        print(f"\nSummary:")
        print(f"  Total markets collected: {len(all_markets)}")
        
        if status == 'settled':
            with_outcomes = [m for m in all_markets if m.get('outcome')]
            print(f"  Markets with outcomes: {len(with_outcomes)}")
            print(f"  Markets without outcomes: {len(all_markets) - len(with_outcomes)}")
        else:
            open_markets = [m for m in all_markets if m.get('status') == 'open']
            settled = [m for m in all_markets if m.get('status') == 'settled']
            print(f"  Open markets: {len(open_markets)}")
            print(f"  Settled markets: {len(settled)}")
    else:
        print("No markets collected")

def collect_historical_prices(client: ExchangeClient, ticker: str, days_back=30):
    """
    Collect historical price data for a specific market
    Returns list of price points over time
    """
    print(f"\nCollecting historical prices for {ticker}...")
    
    # Calculate time range
    end_ts = int(time.time())
    start_ts = int((datetime.now() - timedelta(days=days_back)).timestamp())
    
    try:
        # Extract series and market ticker from full ticker
        # Format is usually: SERIES-MARKET
        parts = ticker.split('-', 1)
        if len(parts) >= 2:
            series_ticker = parts[0]
            market_ticker = ticker
            
            # Get candlestick data (price history)
            history = client.get_market_history(
                series_ticker=series_ticker,
                market_ticker=market_ticker,
                period_interval=3600,  # 1 hour intervals
                end_ts=end_ts,
                start_ts=start_ts
            )
            
            return history.get('candlesticks', [])
    except Exception as e:
        print(f"Error fetching history: {e}")
        return []

def main():
    """Main function"""
    try:
        # Load credentials
        private_key = load_private_key(PRIVATE_KEY_PATH)
        client = ExchangeClient(
            exchange_api_base=EXCHANGE_API_BASE,
            key_id=KEY_ID,
            private_key=private_key
        )
        
        # Collect market data
        # For calibration analysis, we need SETTLED markets with outcomes
        print("\nCollecting SETTLED markets (these have outcomes we can analyze)...")
        collect_market_data(
            client, 
            "kalshi_markets.csv",
            status='settled',  # Focus on settled markets with outcomes
            max_pages=5,       # Limit to 5 pages (~500 markets)
            max_markets=500    # Or max 500 markets total
        )
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

