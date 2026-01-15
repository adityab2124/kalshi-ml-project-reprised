#!/usr/bin/env python3
"""
Filter Kalshi markets by industry/category.

Supports three filtering methods:
1. series_ticker parameter (recommended)
2. event_ticker prefix filtering
3. category field filtering (if populated)
"""

from p import ExchangeClient
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from typing import List, Dict, Optional
from industry_mapping import get_industry, get_market_industry, get_all_industries
from semantic_tagging import get_semantic_tags, filter_markets_by_semantic_tag

# ===== CONFIGURATION =====
EXCHANGE_API_BASE ="https://api.elections.kalshi.com/trade-api/v2"
# "https://demo-api.kalshi.co/trade-api/v2"
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

def filter_by_series_ticker(client: ExchangeClient, series_ticker: str, 
                           limit: int = 100, status: Optional[str] = None) -> List[Dict]:
    """
    Option 1: Filter markets using series_ticker parameter (recommended)
    
    Args:
        client: ExchangeClient instance
        series_ticker: Series ticker to filter by (e.g., 'KXBTC15M')
        limit: Maximum number of markets to return
        status: Optional status filter ('open', 'settled', etc.)
    
    Returns:
        List of market dictionaries
    """
    print(f"Filtering by series_ticker: {series_ticker}")
    print("-" * 60)
    
    params = {'series_ticker': series_ticker, 'limit': limit}
    if status:
        params['status'] = status
    
    try:
        response = client.get_markets(**params)
        markets = response.get('markets', [])
        print(f"✓ Found {len(markets)} markets")
        return markets
    except Exception as e:
        print(f"✗ Error: {e}")
        return []

def filter_by_event_ticker_prefix(client: ExchangeClient, prefix: str,
                                  limit: int = 500, status: Optional[str] = None) -> List[Dict]:
    """
    Option 2: Filter markets by event_ticker prefix (post-processing)
    
    Args:
        client: ExchangeClient instance
        prefix: Event ticker prefix to filter by (e.g., 'KXBTC')
        limit: Maximum number of markets to fetch before filtering
        status: Optional status filter ('open', 'settled', etc.)
    
    Returns:
        List of market dictionaries matching the prefix
    """
    print(f"Filtering by event_ticker prefix: {prefix}")
    print("-" * 60)
    
    params = {'limit': limit}
    if status:
        params['status'] = status
    
    try:
        response = client.get_markets(**params)
        all_markets = response.get('markets', [])
        
        # Filter by event_ticker prefix
        filtered = [m for m in all_markets 
                   if m.get('event_ticker', '').startswith(prefix)]
        
        print(f"✓ Found {len(filtered)} markets (from {len(all_markets)} total)")
        return filtered
    except Exception as e:
        print(f"✗ Error: {e}")
        return []

def filter_by_category(client: ExchangeClient, category: str,
                      limit: int = 500, status: Optional[str] = None) -> List[Dict]:
    """
    Option 3: Filter markets by category field (if populated in production API)
    
    Args:
        client: ExchangeClient instance
        category: Category name to filter by (e.g., 'Politics', 'Sports')
        limit: Maximum number of markets to fetch before filtering
        status: Optional status filter ('open', 'settled', etc.)
    
    Returns:
        List of market dictionaries matching the category
    """
    print(f"Filtering by category: {category}")
    print("-" * 60)
    
    params = {'limit': limit}
    if status:
        params['status'] = status
    
    try:
        response = client.get_markets(**params)
        all_markets = response.get('markets', [])
        
        # Filter by category
        filtered = [m for m in all_markets 
                   if m.get('category', '').lower() == category.lower()]
        
        print(f"✓ Found {len(filtered)} markets (from {len(all_markets)} total)")
        
        if len(filtered) == 0:
            print("⚠️  Note: Category field may be empty in demo API")
            print("   Check production API or use series_ticker method instead")
        
        return filtered
    except Exception as e:
        print(f"✗ Error: {e}")
        return []

def filter_by_industry_label(client: ExchangeClient, industry: str,
                            limit: int = 1000, status: Optional[str] = None) -> List[Dict]:
    """
    Filter markets by industry label using industry mapping.
    
    Args:
        client: ExchangeClient instance
        industry: Industry label (e.g., 'Crypto', 'Sports', 'Politics')
        limit: Maximum number of markets to fetch before filtering
        status: Optional status filter ('open', 'settled', etc.)
    
    Returns:
        List of market dictionaries matching the industry
    """
    print(f"Filtering by industry label: {industry}")
    print("-" * 60)
    
    params = {'limit': limit}
    if status:
        params['status'] = status
    
    try:
        response = client.get_markets(**params)
        all_markets = response.get('markets', [])
        
        # Filter by industry using mapping
        filtered = []
        for market in all_markets:
            market_industry = get_market_industry(market)
            if market_industry and market_industry.lower() == industry.lower():
                filtered.append(market)
        
        print(f"✓ Found {len(filtered)} markets (from {len(all_markets)} total)")
        return filtered
    except Exception as e:
        print(f"✗ Error: {e}")
        return []

def filter_by_semantic_tag(client: ExchangeClient, tag: str, limit: int = 1000, status: Optional[str] = None) -> List[Dict]:
    """
    Filter markets by a semantic tag derived from the market title.

    Tags come from get_semantic_tags(title):
    - mention
    - price_direction
    - time_bucketed
    - binary_outcome
    """
    print(f"Filtering by semantic tag: {tag}")
    print("-" * 60)

    params = {"limit": limit}
    if status:
        params["status"] = status

    try:
        response = client.get_markets(**params)
        all_markets = response.get("markets", [])
        filtered = filter_markets_by_semantic_tag(all_markets, tag)
        print(f"✓ Found {len(filtered)} markets (from {len(all_markets)} total)")
        return filtered
    except Exception as e:
        print(f"✗ Error: {e}")
        return []

def display_markets(markets: List[Dict], max_display: int = 10, show_industry: bool = True):
    """Display market information"""
    if not markets:
        print("No markets found.")
        return
    
    print(f"\n{'='*60}")
    print(f"MARKETS ({len(markets)} total, showing first {min(max_display, len(markets))}):")
    print(f"{'='*60}\n")
    
    for i, market in enumerate(markets[:max_display], 1):
        print(f"{i}. {market.get('ticker', 'N/A')}")
        title = market.get('title', 'N/A')
        print(f"   Title: {title}")
        print(f"   Event Ticker: {market.get('event_ticker', 'N/A')}")
        if show_industry:
            industry = get_market_industry(market)
            print(f"   Industry: {industry or 'Unknown'}")
        print(f"   Semantic Tags: {get_semantic_tags(title)}")
        print(f"   Status: {market.get('status', 'N/A')}")
        print(f"   Category: '{market.get('category', '')}'")
        yes_price = market.get('yes_bid') or market.get('yes_ask')
        if yes_price:
            print(f"   YES Price: ${yes_price/100:.2f}")
        print()

def discover_series_tickers(client: ExchangeClient, limit: int = 500) -> Dict[str, int]:
    """
    Discover available series tickers by analyzing event_ticker prefixes
    
    Returns:
        Dictionary mapping series ticker prefixes to count of markets
    """
    print("Discovering available series tickers...")
    print("-" * 60)
    
    try:
        response = client.get_markets(limit=limit)
        markets = response.get('markets', [])
        
        series_counts = {}
        for market in markets:
            event_ticker = market.get('event_ticker', '')
            if event_ticker:
                prefix = event_ticker.split('-')[0] if '-' in event_ticker else event_ticker
                series_counts[prefix] = series_counts.get(prefix, 0) + 1
        
        print(f"Found {len(series_counts)} unique series tickers:")
        print()
        for series, count in sorted(series_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {series:30} {count:4} markets")
        
        return series_counts
    except Exception as e:
        print(f"✗ Error: {e}")
        return {}

def main():
    """Main function - example usage"""
    print("="*60)
    print("KALSHI MARKET FILTERING BY INDUSTRY")
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
    
    # Example 1: Discover available series
    print("\n" + "="*60)
    print("STEP 1: Discover Available Series")
    print("="*60)
    series_counts = discover_series_tickers(client, limit=200)
    
    # Example 2: Filter by series_ticker (recommended)
    print("\n" + "="*60)
    print("STEP 2: Filter by Series Ticker (Option 1 - Recommended)")
    print("="*60)
    # Use a larger limit and no status filter to see more markets (open + settled)
    crypto_markets = filter_by_series_ticker(client, 'KXBTC15M', limit=500, status=None)
    display_markets(crypto_markets, max_display=5)
    
    # Example 3: Filter by event_ticker prefix
    print("\n" + "="*60)
    print("STEP 3: Filter by Event Ticker Prefix (Option 2)")
    print("="*60)
    eth_markets = filter_by_event_ticker_prefix(client, 'KXETH', limit=500, status=None)
    display_markets(eth_markets, max_display=5)
    
    # Example 4: Filter by category (may not work in demo)
    print("\n" + "="*60)
    print("STEP 4: Filter by Category (Option 3)")
    print("="*60)
    politics_markets = filter_by_category(client, 'Politics', limit=500, status=None)
    display_markets(politics_markets, max_display=5)
    
    # Example 5: Filter by industry label (using mapping)
    print("\n" + "="*60)
    print("STEP 5: Filter by Industry Label (Using Mapping)")
    print("="*60)
    print(f"Available industries: {', '.join(get_all_industries())}")
    print()
    
    crypto_markets = filter_by_industry_label(client, 'Crypto', limit=500, status=None)
    display_markets(crypto_markets, max_display=5, show_industry=True)

    # Example 6: Filter by semantic tag
    print("\n" + "="*60)
    print("STEP 6: Filter by Semantic Tag (Title-only, Rule-based)")
    print("="*60)
    mention_markets = filter_by_semantic_tag(client, "mention", limit=500, status=None)
    display_markets(mention_markets, max_display=5, show_industry=True)
    
    print("\n" + "="*60)
    print("USAGE EXAMPLES:")
    print("="*60)
    print("""
# Filter by series ticker:
markets = filter_by_series_ticker(client, 'KXBTC15M', limit=100, status='open')

# Filter by event ticker prefix:
markets = filter_by_event_ticker_prefix(client, 'KXETH', limit=1000, status='open')

# Filter by category:
markets = filter_by_category(client, 'Sports', limit=1000, status='open')

# Filter by industry label (using mapping):
markets = filter_by_industry_label(client, 'Crypto', limit=1000, status='open')
markets = filter_by_industry_label(client, 'Sports', limit=1000, status='open')
markets = filter_by_industry_label(client, 'Politics', limit=1000, status='open')

# Filter by semantic tag (title-only):
markets = filter_by_semantic_tag(client, 'mention', limit=1000, status='open')
markets = filter_by_semantic_tag(client, 'price_direction', limit=1000, status='open')
markets = filter_by_semantic_tag(client, 'time_bucketed', limit=1000, status='open')
markets = filter_by_semantic_tag(client, 'binary_outcome', limit=1000, status='open')

# Get industry for a specific ticker:
from industry_mapping import get_industry
industry = get_industry(series_ticker='KXBTC15M')  # Returns: 'Crypto'

# Get industry for a market dict:
from industry_mapping import get_market_industry
industry = get_market_industry(market)  # Returns: 'Sports', 'Crypto', etc.

# Discover available series:
series = discover_series_tickers(client, limit=500)
    """)

if __name__ == "__main__":
    main()
