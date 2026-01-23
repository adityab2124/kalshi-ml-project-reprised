#!/usr/bin/env python3
"""
Test script to fetch Bernie mention market contracts and send them to Slack.

This is a standalone test file - doesn't modify delta_sniper.py
"""

from p import ExchangeClient
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from slack_notify import send_slack_message
from datetime import datetime, timezone

# ===== CONFIGURATION =====
EXCHANGE_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
KEY_ID = "cc76eee9-dba9-4bf2-a06f-eddf6a44a8e1"
PRIVATE_KEY_PATH = "private_key.pem"

# Bernie event to test
BERNIE_EVENT_TICKER = "KXBERNIEMENTION-26JAN20"


def load_private_key(key_path: str):
    """Load RSA private key from PEM file."""
    with open(key_path, 'rb') as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    return private_key


def discover_contracts_for_event(client: ExchangeClient, event_ticker: str):
    """
    Discover all contracts (markets) for a given event ticker.
    
    Returns:
        List of dictionaries with 'ticker' and 'yes_sub_title' (contract name)
    """
    try:
        response = client.get_markets(event_ticker=event_ticker, limit=100)
        markets = response.get('markets', [])
        
        contracts = []
        for market in markets:
            ticker = market.get('ticker', '')
            yes_sub = market.get('yes_sub_title', 'Yes')
            if ticker:
                contracts.append({
                    'ticker': ticker,
                    'yes_sub_title': yes_sub,
                    'market': market
                })
        
        return contracts
    except Exception as e:
        print(f"⚠️  Error discovering contracts for {event_ticker}: {e}")
        return []


def get_market_price(client: ExchangeClient, ticker: str):
    """
    Fetch current price for a specific contract ticker.
    
    Returns:
        Tuple of (price, contract_name) or None if fetch fails
    """
    try:
        # Try get_market() first
        try:
            market_data = client.get_market(ticker=ticker)
            market = market_data.get('market', {})
            price = market.get('yes_bid') or market.get('yes_ask') or market.get('last_price') or 0
            contract_name = market.get('yes_sub_title', ticker.split('-')[-1] if '-' in ticker else 'Yes')
            if price > 0:
                return (price / 100.0, contract_name)
        except:
            # Fallback to get_markets
            response = client.get_markets(tickers=ticker, limit=1)
            markets = response.get('markets', [])
            
            if markets:
                market = markets[0]
                price = market.get('yes_bid') or market.get('yes_ask') or market.get('last_price') or 0
                contract_name = market.get('yes_sub_title', ticker.split('-')[-1] if '-' in ticker else 'Yes')
                if price > 0:
                    return (price / 100.0, contract_name)
        
        return None
    except Exception as e:
        print(f"⚠️  Error fetching price for {ticker}: {e}")
        return None


def main():
    """Main test function."""
    print("="*60)
    print("TESTING BERNIE CONTRACTS DISCOVERY")
    print("="*60)
    
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
    
    # Discover contracts for Bernie event
    print(f"Fetching contracts for: {BERNIE_EVENT_TICKER}\n")
    
    contracts = discover_contracts_for_event(client, BERNIE_EVENT_TICKER)
    
    if not contracts:
        print("✗ No contracts found")
        return
    
    print(f"✓ Found {len(contracts)} contracts\n")
    
    # Fetch prices for each contract
    contracts_with_prices = []
    for contract in contracts:
        ticker = contract['ticker']
        name = contract['yes_sub_title']
        
        price_result = get_market_price(client, ticker)
        if price_result:
            price, _ = price_result
            contracts_with_prices.append({
                'name': name,
                'ticker': ticker,
                'price': price
            })
    
    # Sort by price (highest first)
    contracts_with_prices.sort(key=lambda x: x['price'], reverse=True)
    
    # Build Slack message
    message = f"📊 *Bernie Mention Market Contracts*\n"
    message += f"*Event:* {BERNIE_EVENT_TICKER}\n"
    message += f"*Total Contracts:* {len(contracts)}\n"
    message += f"*Time:* {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
    message += "*Top Contracts (by price):*\n"
    
    # Show top 15 contracts
    for i, contract in enumerate(contracts_with_prices[:15], 1):
        message += f"{i}. *{contract['name']}*: ${contract['price']:.2f}\n"
        message += f"   `{contract['ticker']}`\n"
    
    if len(contracts_with_prices) > 15:
        message += f"\n... and {len(contracts_with_prices) - 15} more contracts"
    
    # Send to Slack
    print("Sending contracts list to Slack...\n")
    try:
        send_slack_message(message)
        print("✓ Slack message sent with Bernie contracts!")
    except Exception as e:
        print(f"⚠️  Failed to send Slack message: {e}")
    
    # Also print to terminal
    print("\n" + "="*60)
    print("CONTRACTS DISCOVERED:")
    print("="*60)
    for contract in contracts_with_prices:
        print(f"{contract['name']:35} ${contract['price']:.2f}  ({contract['ticker']})")
    print("="*60)
    print(f"\n✓ Total: {len(contracts_with_prices)} contracts with prices")


if __name__ == "__main__":
    main()
