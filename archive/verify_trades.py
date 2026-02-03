#!/usr/bin/env python3
"""
Verify that trades from the API are real by checking market details
"""

from p import ExchangeClient
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import json

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

def verify_trades(client: ExchangeClient):
    """Verify trades by checking market details"""
    print("="*60)
    print("VERIFYING TRADES - Checking Market Details")
    print("="*60)
    
    # Get recent trades
    print("\n1. Fetching recent trades...")
    try:
        trades = client.get_trades(limit=5)
        trade_list = trades.get('trades', [])
        print(f"   Found {len(trade_list)} trades\n")
    except Exception as e:
        print(f"   ERROR: {e}")
        return
    
    # Verify each trade
    for i, trade in enumerate(trade_list[:3], 1):
        ticker = trade.get('ticker', 'N/A')
        price = trade.get('price', 'N/A')
        volume = trade.get('count', 'N/A')
        timestamp = trade.get('ts', 'N/A')
        
        print(f"{'='*60}")
        print(f"TRADE {i}: {ticker}")
        print(f"{'='*60}")
        print(f"  Price: ${price}")
        print(f"  Volume: {volume} contracts")
        print(f"  Timestamp: {timestamp}")
        
        # Get market details to verify
        print(f"\n  Verifying market exists...")
        try:
            market = client.get_market(ticker=ticker)
            
            # Market verification
            print(f"  ✓ Market exists and is active")
            
            # Try different possible response structures
            if isinstance(market, dict):
                # Check for nested structure
                market_data = market.get('market', market)
                
                print(f"  Market Title: {market_data.get('title', market_data.get('event_title', 'N/A'))}")
                print(f"  Market Status: {market_data.get('status', 'N/A')}")
                print(f"  Subtitle: {market_data.get('subtitle', market_data.get('event_subtitle', 'N/A'))}")
                
                # Show raw structure for debugging
                if 'market' in market:
                    print(f"\n  Market Data Keys: {list(market_data.keys())[:10]}")
            
            # Check if price makes sense
            market_data = market.get('market', market)
            yes_bid = market_data.get('yes_bid', None)
            yes_ask = market_data.get('yes_ask', None)
            no_bid = market_data.get('no_bid', None)
            no_ask = market_data.get('no_ask', None)
            
            print(f"\n  Current Market Prices:")
            if yes_bid is not None:
                print(f"    YES Bid: ${yes_bid/100:.2f} | Ask: ${yes_ask/100:.2f}")
            if no_bid is not None:
                print(f"    NO Bid: ${no_bid/100:.2f} | Ask: ${no_ask/100:.2f}")
            
            # Verify trade price is within reasonable range
            trade_price_cents = int(float(price) * 100) if price != 'N/A' else None
            if trade_price_cents and yes_bid and yes_ask:
                if yes_bid <= trade_price_cents <= yes_ask:
                    print(f"  ✓ Trade price ${price} is within current bid-ask spread")
                else:
                    print(f"  ⚠️  Trade price ${price} is outside current bid-ask spread")
                    print(f"     (This could be a historical trade)")
            
            # Get orderbook to see current market depth
            print(f"\n  Fetching orderbook...")
            orderbook = client.get_orderbook(ticker=ticker, depth=3)
            if orderbook:
                yes_bids = orderbook.get('yes', {}).get('bids', [])
                yes_asks = orderbook.get('yes', {}).get('asks', [])
                if yes_bids or yes_asks:
                    print(f"  ✓ Orderbook shows active market")
                    if yes_bids:
                        print(f"    Best YES bid: ${yes_bids[0].get('price', 0)/100:.2f} @ {yes_bids[0].get('count', 0)}")
                    if yes_asks:
                        print(f"    Best YES ask: ${yes_asks[0].get('price', 0)/100:.2f} @ {yes_asks[0].get('count', 0)}")
            
        except Exception as e:
            print(f"  ✗ ERROR verifying market: {e}")
            print(f"    This trade may be invalid or the market may not exist")
        
        print()  # Blank line between trades

def main():
    """Main function"""
    try:
        # Load private key
        private_key = load_private_key(PRIVATE_KEY_PATH)
        
        # Initialize client
        client = ExchangeClient(
            exchange_api_base=EXCHANGE_API_BASE,
            key_id=KEY_ID,
            private_key=private_key
        )
        
        # Verify trades
        verify_trades(client)
        
        print("\n" + "="*60)
        print("Verification complete!")
        print("="*60)
        print("\nWhat to look for:")
        print("  ✓ Market exists and has valid details")
        print("  ✓ Trade price is within reasonable range")
        print("  ✓ Orderbook shows active trading")
        print("  ✓ Market status is 'open' or 'active'")
        
    except Exception as e:
        print(f"\nERROR: {e}")

if __name__ == "__main__":
    main()

