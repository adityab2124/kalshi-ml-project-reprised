#!/usr/bin/env python3
"""
Simple demo script to fetch market data and past orders from Kalshi API
"""

from p import ExchangeClient
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import json

# ===== CONFIGURATION =====
# Replace these with your actual credentials
EXCHANGE_API_BASE = "https://demo-api.kalshi.co/trade-api/v2"  # or "https://api.elections.kalshi.com/trade-api/v2" for prod
KEY_ID = "cc76eee9-dba9-4bf2-a06f-eddf6a44a8e1"

# Load private key from PEM file
PRIVATE_KEY_PATH = "private_key.pem"  # Private key file in same directory



def load_private_key(key_path: str):
    """Load RSA private key from PEM file"""
    with open(key_path, 'rb') as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,  # Set password if your key is encrypted
            backend=default_backend()
        )
    return private_key

# ===== DEMO FUNCTIONS =====

def demo_market_data(client: ExchangeClient):
    """Demo: Get market data"""
    print("\n" + "="*50)
    print("DEMO: Fetching Market Data")
    print("="*50)
    
    # Get first 5 markets
    print("\n1. Getting list of markets (limit=5)...")
    markets = client.get_markets(limit=5)
    print(f"   Found {len(markets.get('markets', []))} markets")
    
    if markets.get('markets'):
        first_market = markets['markets'][0]
        ticker = first_market.get('ticker', '')
        print(f"\n   First market ticker: {ticker}")
        
        # Get details for first market
        if ticker:
            print(f"\n2. Getting details for market: {ticker}")
            market_details = client.get_market(ticker=ticker)
            print(f"   Market: {market_details.get('title', 'N/A')}")
            print(f"   Status: {market_details.get('status', 'N/A')}")
            print(f"   Yes bid: {market_details.get('yes_bid', 'N/A')}")
            print(f"   No bid: {market_details.get('no_bid', 'N/A')}")
            
            # Get orderbook for this market
            print(f"\n3. Getting orderbook for {ticker}...")
            orderbook = client.get_orderbook(ticker=ticker, depth=3)
            print(f"   Orderbook data retrieved")
            if 'yes' in orderbook:
                print(f"   Yes bids: {len(orderbook.get('yes', {}).get('bids', []))} levels")
                print(f"   Yes asks: {len(orderbook.get('yes', {}).get('asks', []))} levels")

def demo_past_orders(client: ExchangeClient):
    """Demo: Get past orders and trades"""
    print("\n" + "="*50)
    print("DEMO: Fetching Past Orders & Trades")
    print("="*50)
    
    # Get recent orders
    print("\n1. Getting recent orders (limit=5)...")
    try:
        orders = client.get_orders(limit=5)
        print(f"   Found {len(orders.get('orders', []))} orders")
        
        if orders.get('orders'):
            for i, order in enumerate(orders['orders'][:3], 1):
                print(f"\n   Order {i}:")
                print(f"     ID: {order.get('order_id', 'N/A')}")
                print(f"     Ticker: {order.get('ticker', 'N/A')}")
                print(f"     Side: {order.get('side', 'N/A')}")
                print(f"     Status: {order.get('status', 'N/A')}")
    except Exception as e:
        print(f"   ⚠️  Could not fetch orders: {e}")
        print("   (This may require a real account with trading permissions)")
    
    # Get recent trades
    print("\n2. Getting recent trades (limit=5)...")
    try:
        trades = client.get_trades(limit=5)
        print(f"   Found {len(trades.get('trades', []))} trades")
        
        if trades.get('trades'):
            for i, trade in enumerate(trades['trades'][:3], 1):
                print(f"\n   Trade {i}:")
                print(f"     Ticker: {trade.get('ticker', 'N/A')}")
                print(f"     Price: {trade.get('price', 'N/A')}")
                print(f"     Volume: {trade.get('count', 'N/A')}")
    except Exception as e:
        print(f"   ⚠️  Could not fetch trades: {e}")

def main():
    """Main demo function"""
    print("Kalshi API Demo - Market Data & Past Orders")
    print("="*50)
    
    # Load private key
    try:
        private_key = load_private_key(PRIVATE_KEY_PATH)
    except FileNotFoundError:
        print(f"\nERROR: Private key file not found at: {PRIVATE_KEY_PATH}")
        print("Please update PRIVATE_KEY_PATH with your actual key file path")
        return
    except Exception as e:
        print(f"\nERROR: Failed to load private key: {e}")
        return
    
    # Initialize client
    if KEY_ID == "your_key_id_here":
        print("\nERROR: Please set your KEY_ID in the script")
        return
    
    try:
        client = ExchangeClient(
            exchange_api_base=EXCHANGE_API_BASE,
            key_id=KEY_ID,
            private_key=private_key
        )
        
        # Run demos
        demo_market_data(client)
        demo_past_orders(client)
        
        print("\n" + "="*50)
        print("Demo completed successfully!")
        print("="*50)
        
    except Exception as e:
        print(f"\nERROR: {e}")
        print("Make sure your credentials are correct and you have API access")

if __name__ == "__main__":
    main()

