#!/usr/bin/env python3
"""
Industry Mapping Layer for Kalshi Markets

Maps series_ticker/event_ticker patterns to industry labels using regex.
"""

import re
from typing import Optional, Dict, List

# Industry mapping: list of (pattern, industry_label) tuples
# Patterns are regex strings that will be matched against series_ticker/event_ticker
INDUSTRY_MAPPING = [
    # Crypto
    (r'^KXBTC', 'Crypto'),           # Bitcoin
    (r'^KXETH', 'Crypto'),           # Ethereum
    (r'^KXSOL', 'Crypto'),           # Solana
    (r'^KXCRYPTO', 'Crypto'),        # General crypto
    (r'CRYPTO', 'Crypto'),           # Any crypto mention
    
    # Sports
    (r'^KXNFL', 'Sports'),           # NFL
    (r'^KXNBA', 'Sports'),           # NBA
    (r'^KXMBL', 'Sports'),           # MLB
    (r'^KXNHL', 'Sports'),           # NHL
    (r'^KXNCAA', 'Sports'),          # NCAA
    (r'^KXATP', 'Sports'),           # ATP Tennis (e.g., KXATPMATCH)
    (r'^KXWTA', 'Sports'),           # WTA Tennis (e.g., KXWTAMATCH)
    (r'^KXFOOTBALL', 'Sports'),      # Football
    (r'^KXBASKETBALL', 'Sports'),    # Basketball
    (r'^KXBASEBALL', 'Sports'),      # Baseball
    (r'^KXSOCCER', 'Sports'),        # Soccer
    (r'^KXSPORTS', 'Sports'),        # General sports
    (r'SPORTS', 'Sports'),           # Any sports mention
    
    # Esports
    (r'^KXESPORTS', 'Esports'),      # Esports
    (r'ESPORTS', 'Esports'),         # Any esports mention
    (r'GAMING', 'Esports'),          # Gaming
    (r'LEAGUE', 'Esports'),          # League (gaming)
    
    # Politics
    (r'^KXPRES', 'Politics'),        # President
    (r'^KXELECTION', 'Politics'),    # Election
    (r'^KXCONGRESS', 'Politics'),    # Congress
    (r'^KXSENATE', 'Politics'),      # Senate
    (r'^KXHOUSE', 'Politics'),       # House
    (r'^KXPOLITICS', 'Politics'),    # Politics
    (r'POLITICS', 'Politics'),       # Any politics mention
    (r'ELECTION', 'Politics'),       # Elections
    (r'PRESIDENT', 'Politics'),      # President
    
    # Finance
    (r'^KXSTOCK', 'Finance'),        # Stock market
    (r'^KXSP500', 'Finance'),        # S&P 500
    (r'^KXDOW', 'Finance'),          # Dow Jones
    (r'^KXNASDAQ', 'Finance'),       # NASDAQ
    (r'^KXFINANCE', 'Finance'),      # Finance
    (r'STOCK', 'Finance'),           # Stocks
    (r'MARKET', 'Finance'),          # Markets (financial)
    (r'ECONOMY', 'Finance'),         # Economy
    
    # Technology
    (r'^KXTECH', 'Technology'),      # Technology
    (r'^KXAAPL', 'Technology'),      # Apple
    (r'^KXMSFT', 'Technology'),      # Microsoft
    (r'^KXGOOGL', 'Technology'),     # Google
    (r'^KXMETA', 'Technology'),      # Meta
    (r'TECH', 'Technology'),         # Technology
    (r'APPLE', 'Technology'),        # Apple
    (r'MICROSOFT', 'Technology'),    # Microsoft
    
    # Mentions (Social Media)
    (r'^KXMENTIONS', 'Mentions'),    # Mentions
    (r'MENTIONS', 'Mentions'),       # Mentions
    (r'TWITTER', 'Mentions'),        # Twitter/X
    (r'^KXTWITTER', 'Mentions'),     # Twitter series
    
    # Culture/Entertainment
    (r'^KXOSCARS', 'Culture'),       # Oscars
    (r'^KXGRAMMY', 'Culture'),       # Grammys
    (r'^KXTV', 'Culture'),           # TV
    (r'^KXMOVIE', 'Culture'),        # Movies
    (r'ENTERTAINMENT', 'Culture'),   # Entertainment
]

def get_industry(series_ticker: Optional[str] = None, 
                event_ticker: Optional[str] = None) -> Optional[str]:
    """
    Get industry label for a given series_ticker or event_ticker.
    
    Args:
        series_ticker: Series ticker string (e.g., 'KXBTC15M')
        event_ticker: Event ticker string (fallback if series_ticker not provided)
    
    Returns:
        Industry label string (e.g., 'Crypto', 'Sports', 'Politics') or None if no match
    
    Examples:
        >>> get_industry(series_ticker='KXBTC15M')
        'Crypto'
        
        >>> get_industry(event_ticker='KXNFL-SUPERBOWL')
        'Sports'
        
        >>> get_industry(series_ticker='KXUNKNOWN')
        None
    """
    # Try series_ticker first (primary)
    ticker_to_match = series_ticker
    
    # Fallback to event_ticker if series_ticker not provided
    if not ticker_to_match and event_ticker:
        ticker_to_match = event_ticker
    
    if not ticker_to_match:
        return None
    
    # Extract prefix (part before first dash) if present
    # e.g., 'KXBTC15M-26JAN140015' -> 'KXBTC15M'
    prefix = ticker_to_match.split('-')[0] if '-' in ticker_to_match else ticker_to_match
    
    # Match against patterns (order matters - first match wins)
    for pattern, industry in INDUSTRY_MAPPING:
        if re.search(pattern, prefix, re.IGNORECASE):
            return industry
    
    return None

def get_all_industries() -> List[str]:
    """
    Get list of all available industry labels.
    
    Returns:
        List of unique industry labels
    """
    industries = set()
    for _, industry in INDUSTRY_MAPPING:
        industries.add(industry)
    return sorted(list(industries))

def add_industry_mapping(pattern: str, industry: str):
    """
    Add a new industry mapping pattern (for extensibility).
    
    Args:
        pattern: Regex pattern string
        industry: Industry label
    """
    INDUSTRY_MAPPING.append((pattern, industry))

def get_market_industry(market: dict) -> Optional[str]:
    """
    Get industry for a market dictionary.
    
    Args:
        market: Market dictionary from API response
    
    Returns:
        Industry label or None
    """
    # Try to extract series_ticker from event_ticker if needed
    event_ticker = market.get('event_ticker', '')
    ticker = market.get('ticker', '')
    
    # Extract series prefix from event_ticker or ticker
    series_prefix = None
    if event_ticker:
        series_prefix = event_ticker.split('-')[0] if '-' in event_ticker else event_ticker
    elif ticker:
        series_prefix = ticker.split('-')[0] if '-' in ticker else ticker
    
    return get_industry(series_ticker=series_prefix)

# Example usage and testing
if __name__ == "__main__":
    print("="*60)
    print("INDUSTRY MAPPING LAYER - TESTING")
    print("="*60)
    
    # Test cases
    test_cases = [
        ('KXBTC15M', 'Crypto'),
        ('KXETH15M-26JAN14', 'Crypto'),
        ('KXNFL-SUPERBOWL', 'Sports'),
        ('KXPRES-2024', 'Politics'),
        ('KXSTOCK-SP500', 'Finance'),
        ('KXTECH-AAPL', 'Technology'),
        ('KXMENTIONS-TWITTER', 'Mentions'),
        ('KXUNKNOWN-SERIES', None),
    ]
    
    print("\nTesting get_industry() function:")
    print("-" * 60)
    for series_ticker, expected in test_cases:
        result = get_industry(series_ticker=series_ticker)
        status = "✓" if result == expected else "✗"
        print(f"{status} {series_ticker:30} -> {result or 'None':15} (expected: {expected or 'None'})")
    
    print(f"\nAvailable industries: {', '.join(get_all_industries())}")
    
    print("\n" + "="*60)
    print("Usage Example:")
    print("="*60)
    print("""
from industry_mapping import get_industry, get_market_industry

# Get industry from series ticker
industry = get_industry(series_ticker='KXBTC15M')
# Returns: 'Crypto'

# Get industry from market dict
market = {'event_ticker': 'KXNFL-SUPERBOWL', 'ticker': '...'}
industry = get_market_industry(market)
# Returns: 'Sports'
    """)
