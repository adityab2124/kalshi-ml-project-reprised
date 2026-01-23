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
from semantic_tagging import get_semantic_tags, filter_markets_by_semantic_tag, filter_markets_by_mention_prefix, HARDCODED_MENTION_PREFIXES
from slack_notify import send_slack_message
import argparse
import time
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# ===== CONFIGURATION =====
EXCHANGE_API_BASE ="https://api.elections.kalshi.com/trade-api/v2"
# "https://demo-api.kalshi.co/trade-api/v2"
KEY_ID = "cc76eee9-dba9-4bf2-a06f-eddf6a44a8e1"
PRIVATE_KEY_PATH = "private_key.pem"

# ===== KALSHI URL MAPPING =====
# Maps event ticker prefixes to their Kalshi URL format
# Format: https://kalshi.com/markets/{series}/{slug}/{prefix}-{date}
KALSHI_URL_MAPPING = {
    'KXTRUMPSAY': {
        'series': 'kxtrumpsay',
        'slug': 'what-will-trump-say',
        'prefix': 'kxtrumpsay'
    },
    'KXTRUMPSAYMONTH': {
        'series': 'kxtrumpsaymonth',
        'slug': 'what-will-trump-say',  # May need adjustment
        'prefix': 'kxtrumpsaymonth'
    },
    'KXMAMDANIMENTION': {
        'series': 'kxmamdani',
        'slug': 'what-will-mamdani-say',  # May need adjustment
        'prefix': 'kxmamdani'
    },
    'KXMRBEASTMENTION': {
        'series': 'kxmrbeast',
        'slug': 'what-will-mrbeast-say',  # May need adjustment
        'prefix': 'kxmrbeast'
    },
    'KXBERNIEMENTION': {
        'series': 'kxbernie',
        'slug': 'what-will-bernie-say',  # May need adjustment
        'prefix': 'kxbernie'
    },
}

def get_kalshi_market_url(event_ticker: str) -> str:
    """
    Generate Kalshi market URL from event_ticker.
    
    Format: https://kalshi.com/markets/{series}/{slug}/{prefix}-{date}
    
    Example:
        KXTRUMPSAY-26JAN19 -> https://kalshi.com/markets/kxtrumpsay/what-will-trump-say/kxtrumpsay-26jan19
    """
    if not event_ticker or '-' not in event_ticker:
        return f"https://kalshi.com/markets/{event_ticker.lower()}"
    
    # Split event_ticker into prefix and date
    parts = event_ticker.split('-', 1)
    prefix = parts[0]
    date_part = parts[1] if len(parts) > 1 else ''
    
    # Look up URL mapping
    url_info = KALSHI_URL_MAPPING.get(prefix)
    if url_info:
        # Format: https://kalshi.com/markets/{series}/{slug}/{prefix}-{date}
        return f"https://kalshi.com/markets/{url_info['series']}/{url_info['slug']}/{url_info['prefix']}-{date_part.lower()}"
    else:
        # Fallback: use lowercase event_ticker
        return f"https://kalshi.com/markets/{event_ticker.lower().replace('-', '/')}"

# ===== INDUSTRY → SERIES PREFIXES MAPPING =====
INDUSTRY_SERIES_MAPPING = {
    'Crypto': ['KXBTC', 'KXETH', 'KXSOL'],
    'Sports': ['KXMV', 'KXNBA', 'KXNFL'],
    'Politics': ['KXPRES', 'KXELECTION', 'KXCONGRESS'],
}

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

# ===== LOCAL FILTER FUNCTIONS (No API calls - work on existing market list) =====

def fetch_markets_by_industry(client: ExchangeClient, industry: str, 
                              limit_per_series: int = 500, status: Optional[str] = None) -> List[Dict]:
    """
    Fetch markets by industry, looping over mapped series prefixes.
    
    Args:
        client: ExchangeClient instance
        industry: Industry name (e.g., 'Crypto', 'Sports', 'Politics')
        limit_per_series: Maximum markets per series prefix
        status: Optional status filter ('open', 'settled', etc.)
    
    Returns:
        Combined list of market dictionaries from all series prefixes
    """
    if industry not in INDUSTRY_SERIES_MAPPING:
        print(f"✗ Unknown industry: {industry}")
        print(f"  Available industries: {', '.join(INDUSTRY_SERIES_MAPPING.keys())}")
        return []
    
    series_prefixes = INDUSTRY_SERIES_MAPPING[industry]
    print(f"Fetching markets for industry: {industry}")
    print(f"  Series prefixes: {', '.join(series_prefixes)}")
    print("-" * 60)
    
    all_markets = []
    
    for prefix in series_prefixes:
        print(f"  Fetching {prefix}...", end=" ")
        try:
            params = {'series_ticker': prefix, 'limit': limit_per_series}
            if status:
                params['status'] = status
            
            response = client.get_markets(**params)
            markets = response.get('markets', [])
            all_markets.extend(markets)
            print(f"✓ {len(markets)} markets")
            
            # Rate limiting
            time.sleep(0.2)
        except Exception as e:
            print(f"✗ Error: {e}")
            continue
    
    print(f"\n✓ Total fetched: {len(all_markets)} markets from {len(series_prefixes)} series")
    return all_markets

def discover_speech_mention_prefixes(markets: List[Dict]) -> List[str]:
    """
    Discover speech/mention event ticker prefixes from a list of markets.
    
    An event is speech-related if its event_ticker prefix (before first -) contains 
    "SAY" or "MENTION" (case-insensitive).
    
    Args:
        markets: List of market dictionaries
    
    Returns:
        List of unique event ticker prefixes that are speech/mention related
    """
    prefix_set = set()
    
    for market in markets:
        event_ticker = market.get('event_ticker', '')
        if event_ticker:
            # Extract prefix (substring before first -)
            prefix = event_ticker.split('-')[0] if '-' in event_ticker else event_ticker
            
            # Check if prefix contains SAY or MENTION (case-insensitive)
            prefix_upper = prefix.upper()
            if 'SAY' in prefix_upper or 'MENTION' in prefix_upper:
                prefix_set.add(prefix)
    
    return sorted(list(prefix_set))

def fetch_markets_by_event_prefix(client: ExchangeClient, event_prefixes: List[str],
                                  limit_per_fetch: int = 500, status: Optional[str] = None) -> List[Dict]:
    """
    Fetch markets and filter locally by event_ticker prefix.
    
    Fetches /markets?limit=500, then filters locally where event_ticker.startswith(prefix).
    
    Args:
        client: ExchangeClient instance
        event_prefixes: List of event ticker prefixes to filter by
        limit_per_fetch: Maximum markets per fetch
        status: Optional status filter
    
    Returns:
        Combined list of market dictionaries matching the prefixes
    """
    if not event_prefixes:
        return []
    
    print(f"Fetching markets and filtering by {len(event_prefixes)} event ticker prefixes...")
    print("-" * 60)
    
    all_markets = []
    
    for prefix in event_prefixes:
        print(f"  Fetching markets for prefix {prefix}...", end=" ")
        try:
            params = {'limit': limit_per_fetch}
            if status:
                params['status'] = status
            
            response = client.get_markets(**params)
            markets = response.get('markets', [])
            
            # Filter locally where event_ticker.startswith(prefix)
            filtered = [m for m in markets if m.get('event_ticker', '').startswith(prefix)]
            all_markets.extend(filtered)
            print(f"✓ {len(filtered)} markets (from {len(markets)} fetched)")
            
            # Rate limiting
            time.sleep(0.2)
        except Exception as e:
            print(f"✗ Error: {e}")
            continue
    
    print(f"\n✓ Total fetched: {len(all_markets)} markets matching {len(event_prefixes)} event ticker prefixes")
    return all_markets

def fetch_markets_paginated(client: ExchangeClient, target_markets: int = 2500, 
                            status: Optional[str] = None, limit_per_page: int = 500) -> List[Dict]:
    """
    Fetch markets with pagination (for generic fetch mode).
    
    Args:
        client: ExchangeClient instance
        target_markets: Target number of markets to collect (default: 2500)
        status: Optional status filter ('open', 'settled', etc.)
        limit_per_page: Markets per page (default: 500)
    
    Returns:
        List of market dictionaries
    """
    print(f"Fetching markets with pagination (target: {target_markets}, status={status or 'all'})...")
    print("-" * 60)
    
    all_markets = []
    cursor = None
    page = 1
    
    while len(all_markets) < target_markets:
        try:
            params = {'limit': limit_per_page}
            if status:
                params['status'] = status
            if cursor:
                params['cursor'] = cursor
            
            response = client.get_markets(**params)
            markets = response.get('markets', [])
            
            if not markets:
                print(f"  No more markets found (page {page})")
                break
            
            all_markets.extend(markets)
            print(f"  Page {page}: {len(markets)} markets (total: {len(all_markets)})")
            
            # Get next cursor
            cursor = response.get('cursor')
            if not cursor:
                print(f"  No more pages available")
                break
            
            page += 1
            time.sleep(0.2)  # Rate limiting
            
        except Exception as e:
            print(f"✗ Error on page {page}: {e}")
            break
    
    print(f"\n✓ Fetched {len(all_markets)} total markets across {page} pages")
    return all_markets

def fetch_markets_once(client: ExchangeClient, limit: int = 500, status: Optional[str] = None) -> List[Dict]:
    """
    Fetch markets once from the API (generic fetch, no industry filtering).
    
    Args:
        client: ExchangeClient instance
        limit: Maximum number of markets to fetch
        status: Optional status filter ('open', 'settled', etc.)
    
    Returns:
        List of market dictionaries
    """
    print(f"Fetching markets (limit={limit}, status={status or 'all'})...")
    print("-" * 60)
    
    params = {'limit': limit}
    if status:
        params['status'] = status
    
    try:
        response = client.get_markets(**params)
        markets = response.get('markets', [])
        print(f"✓ Fetched {len(markets)} markets")
        return markets
    except Exception as e:
        print(f"✗ Error: {e}")
        return []

def filter_markets_by_series_ticker_local(markets: List[Dict], series_ticker: str) -> List[Dict]:
    """Filter markets locally by series_ticker (no API call)"""
    filtered = []
    for market in markets:
        event_ticker = market.get('event_ticker', '')
        if event_ticker:
            prefix = event_ticker.split('-')[0] if '-' in event_ticker else event_ticker
            if prefix == series_ticker:
                filtered.append(market)
    return filtered

def filter_markets_by_event_ticker_prefix_local(markets: List[Dict], prefix: str) -> List[Dict]:
    """Filter markets locally by event_ticker prefix (no API call)"""
    return [m for m in markets if m.get('event_ticker', '').startswith(prefix)]

def filter_markets_by_category_local(markets: List[Dict], category: str) -> List[Dict]:
    """Filter markets locally by category (no API call)"""
    return [m for m in markets if m.get('category', '').lower() == category.lower()]

def filter_markets_by_industry_label_local(markets: List[Dict], industry: str) -> List[Dict]:
    """Filter markets locally by industry label (no API call)"""
    filtered = []
    for market in markets:
        market_industry = get_market_industry(market)
        if market_industry and market_industry.lower() == industry.lower():
            filtered.append(market)
    return filtered

def filter_current_markets(markets: List[Dict]) -> List[Dict]:
    """
    Filter to current markets only (close_time in future and status not finalized).
    
    Uses close_time as primary condition - ignores status except for finalized markets.
    A market is current if: close_time > now AND status != "finalized"
    
    Args:
        markets: List of market dictionaries
    
    Returns:
        List of markets that are current (close_time > now and status != finalized)
    """
    now = datetime.now(timezone.utc)
    filtered = []
    
    for market in markets:
        status = market.get('status', '').lower()
        close_time_str = market.get('close_time', '')
        
        # Exclude finalized markets
        if status == 'finalized':
            continue
        
        # Check close_time is in future (primary condition)
        if not close_time_str:
            # Skip markets without close_time
            continue
        
        try:
            # Parse ISO format: 2026-01-14T05:15:00Z
            close_time_str_clean = close_time_str.replace('Z', '+00:00')
            close_time = datetime.fromisoformat(close_time_str_clean)
            # Ensure timezone-aware comparison
            if close_time.tzinfo is None:
                close_time = close_time.replace(tzinfo=timezone.utc)
            if close_time > now:
                # Market is current if close_time is in future and not finalized
                filtered.append(market)
        except (ValueError, AttributeError):
            # If parsing fails, skip this market
            continue
    
    return filtered

def filter_upcoming_markets(markets: List[Dict], days_ahead: int = 14) -> List[Dict]:
    """
    Filter to upcoming markets (open/inactive/pending status and close_time within days_ahead).
    
    Args:
        markets: List of market dictionaries
        days_ahead: Number of days to look ahead (default: 14)
    
    Returns:
        List of markets that are upcoming (now < close_time <= now + days_ahead)
    """
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(days=days_ahead)
    filtered = []
    
    for market in markets:
        status = market.get('status', '').lower()
        close_time_str = market.get('close_time', '')
        
        # Check status - allow open, inactive, pending (exclude finalized)
        if status in ['finalized', 'settled']:
            continue
        
        # Check close_time is in future and within days_ahead
        if close_time_str:
            try:
                # Parse ISO format: 2026-01-14T05:15:00Z
                close_time_str_clean = close_time_str.replace('Z', '+00:00')
                close_time = datetime.fromisoformat(close_time_str_clean)
                # Ensure timezone-aware comparison
                if close_time.tzinfo is None:
                    close_time = close_time.replace(tzinfo=timezone.utc)
                
                # Must be in future and within days_ahead
                if close_time <= now or close_time > cutoff:
                    continue
                
                filtered.append(market)
            except (ValueError, AttributeError):
                # If parsing fails, skip this market
                continue
        else:
            # If no close_time, skip this market
            continue
    
    return filtered

def group_markets_by_event(markets: List[Dict]) -> Dict[str, List[Dict]]:
    """
    Group markets by event_ticker.
    
    Args:
        markets: List of market dictionaries
    
    Returns:
        Dictionary mapping event_ticker to list of markets (contracts) for that event
    """
    events = defaultdict(list)
    
    for market in markets:
        event_ticker = market.get('event_ticker', '')
        if event_ticker:
            events[event_ticker].append(market)
    
    return dict(events)

def display_events(events: Dict[str, List[Dict]], max_display: int = 20, show_answers: bool = True):
    """
    Display events grouped by event_ticker.
    
    Args:
        events: Dictionary mapping event_ticker to list of markets (contracts)
        max_display: Maximum number of events to display
        show_answers: Whether to show all answers/contracts
    """
    if not events:
        print("No events found.")
        return
    
    print(f"\n{'='*60}")
    print(f"EVENTS ({len(events)} total, showing first {min(max_display, len(events))}):")
    print(f"{'='*60}\n")
    
    for i, (event_ticker, contracts) in enumerate(list(events.items())[:max_display], 1):
        # Use first contract's title and metadata
        first_contract = contracts[0]
        title = first_contract.get('title', 'N/A')
        close_time = first_contract.get('close_time', 'N/A')
        
        # Get industry
        industry = get_market_industry(first_contract)
        
        # Get semantic tags from title
        semantic_tags = get_semantic_tags(title)
        
        print(f"{i}. {event_ticker}")
        print(f"   Title: {title}")
        if industry:
            print(f"   Industry: {industry}")
        print(f"   Semantic Tags: {semantic_tags}")
        print(f"   Close Time: {close_time}")
        print(f"   Contracts: {len(contracts)}")
        
        # Show all answers/contracts if requested
        if show_answers and contracts:
            # Sort by price (yes_bid or yes_ask, whichever is available)
            priced_contracts = []
            unpriced_contracts = []
            
            for c in contracts:
                price = c.get('yes_bid') or c.get('yes_ask') or 0
                if price > 0:
                    priced_contracts.append((c, price))
                else:
                    unpriced_contracts.append(c)
            
            # Show priced contracts first (sorted by price)
            if priced_contracts:
                priced_contracts.sort(key=lambda x: x[1], reverse=True)
                print(f"   Answers (by price):")
                for j, (contract, price) in enumerate(priced_contracts, 1):
                    ticker = contract.get('ticker', 'N/A')
                    yes_sub = contract.get('yes_sub_title', '')
                    price_display = f"${price/100:.2f}" if price else "N/A"
                    print(f"     {j}. {ticker}: {yes_sub or 'Yes'} @ {price_display}")
            
            # Show unpriced contracts
            if unpriced_contracts:
                for j, contract in enumerate(unpriced_contracts, 1):
                    ticker = contract.get('ticker', 'N/A')
                    yes_sub = contract.get('yes_sub_title', '')
                    offset = len(priced_contracts) + 1 if priced_contracts else 1
                    print(f"     {j + len(priced_contracts)}. {ticker}: {yes_sub or 'Yes'} @ N/A")
        
        print()

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
    parser = argparse.ArgumentParser(description='Filter Kalshi markets by industry')
    parser.add_argument('--industry', type=str, help='Industry to fetch (Crypto, Sports, Politics). If not provided, uses generic fetch.')
    parser.add_argument('--semantic', type=str, help='Semantic tag to filter by (mention, speech_mention, price_direction, time_bucketed, binary_outcome). Filters already-fetched markets.')
    parser.add_argument('--current', action='store_true', help='Filter to current markets only (active/initialized, close_time > now). For use with --semantic speech_mention.')
    parser.add_argument('--upcoming', action='store_true', help='Filter to upcoming markets (open/inactive/pending, close_time within next 14 days). For use with --semantic speech_mention.')
    parser.add_argument('--limit', type=int, default=500, help='Limit per series prefix (default: 500)')
    parser.add_argument('--status', type=str, default=None, help='Status filter (open, settled, etc.). Default: None (all)')
    
    args = parser.parse_args()
    
    # Validate mutually exclusive flags
    if args.current and args.upcoming:
        print("ERROR: --current and --upcoming are mutually exclusive. Use only one.")
        return
    
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
    
    # Special handling for speech_mention: fetch directly by hardcoded prefixes → regex filter
    if args.semantic == 'speech_mention':
        print("\n" + "="*60)
        print("STEP 0: Fetch Markets by Hardcoded Mention Prefixes")
        print("="*60)
        print(f"Hardcoded prefixes: {HARDCODED_MENTION_PREFIXES}")
        print("-" * 60)
        
        # Fetch markets directly by series_ticker for each hardcoded prefix
        all_markets = []
        prefix_counts = {}
        
        for prefix in HARDCODED_MENTION_PREFIXES:
            print(f"Fetching {prefix}...", end=" ")
            try:
                params = {'series_ticker': prefix, 'limit': args.limit}
                if args.status:
                    params['status'] = args.status
                
                response = client.get_markets(**params)
                markets = response.get('markets', [])
                
                if markets:
                    all_markets.extend(markets)
                    prefix_counts[prefix] = len(markets)
                    print(f"✓ {len(markets)} markets")
                else:
                    prefix_counts[prefix] = 0
                    print("✓ 0 markets")
                
                # Rate limiting
                time.sleep(0.2)
            except Exception as e:
                prefix_counts[prefix] = 0
                print(f"✗ Error: {e}")
                continue
        
        print(f"\n✓ Total fetched: {len(all_markets)} markets from {len(HARDCODED_MENTION_PREFIXES)} prefixes")
        print("\nMarkets per prefix:")
        for prefix, count in prefix_counts.items():
            print(f"  {prefix:30} {count:4} markets")
        
        if not all_markets:
            print("\nNo markets found for any hardcoded prefix. Exiting.")
            return
        
        # Filter to current or upcoming markets based on flags
        filtered_markets = []
        
        if args.upcoming:
            print("\n" + "="*60)
            print("STEP 1: Filter to Upcoming Markets")
            print("="*60)
            days_ahead = 14
            now = datetime.now(timezone.utc)
            cutoff = now + timedelta(days=days_ahead)
            print(f"Date range: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC to {cutoff.strftime('%Y-%m-%d %H:%M:%S')} UTC (next {days_ahead} days)")
            
            filtered_markets = filter_upcoming_markets(all_markets, days_ahead=days_ahead)
            print(f"✓ Found {len(filtered_markets)} upcoming markets (from {len(all_markets)} total)")
            
            if not filtered_markets:
                print("\nNo upcoming markets found. Exiting.")
                return
        
        elif args.current:
            print("\n" + "="*60)
            print("STEP 1: Filter to Current Markets")
            print("="*60)
            filtered_markets = filter_current_markets(all_markets)
            print(f"✓ Found {len(filtered_markets)} current markets (from {len(all_markets)} total)")
            
            if not filtered_markets:
                print("\nNo current markets found. Exiting.")
                return
        
        else:
            # Default: use current markets if neither flag is provided
            print("\n" + "="*60)
            print("STEP 1: Filter to Current Markets (default)")
            print("="*60)
            filtered_markets = filter_current_markets(all_markets)
            print(f"✓ Found {len(filtered_markets)} current markets (from {len(all_markets)} total)")
            
            if not filtered_markets:
                print("\nNo current markets found. Exiting.")
                return
        
        # Group markets by event_ticker
        print("\n" + "="*60)
        print("STEP 2: Group Markets by Event")
        print("="*60)
        events = group_markets_by_event(filtered_markets)
        print(f"✓ Found {len(events)} unique events (from {len(filtered_markets)} contracts)")
        
        # Apply semantic regex filtering on event titles
        print("\n" + "="*60)
        print("STEP 3: Apply Semantic Regex Filter (speech_mention)")
        print("="*60)
        filtered_events = {}
        for event_ticker, contracts in events.items():
            first_contract = contracts[0]
            title = first_contract.get('title', '')
            tags = get_semantic_tags(title)
            if 'speech_mention' in tags:
                filtered_events[event_ticker] = contracts
        
        print(f"✓ Found {len(filtered_events)} events matching speech_mention pattern (from {len(events)} total)")
        
        print(f"\n✓ Final result: {len(filtered_events)} mention events")
        display_events(filtered_events, max_display=20, show_answers=True)
        
        # Send Slack notification
        if filtered_events:
            try:
                # Build summary message
                num_events = len(filtered_events)
                
                # Get example event (first one)
                example_event_ticker = list(filtered_events.keys())[0]
                example_contracts = filtered_events[example_event_ticker]
                example_contract = example_contracts[0]
                example_title = example_contract.get('title', 'N/A')
                example_close_time = example_contract.get('close_time', 'N/A')
                
                # Format close_time for readability
                if example_close_time and example_close_time != 'N/A':
                    try:
                        close_dt = datetime.fromisoformat(example_close_time.replace('Z', '+00:00').replace('+00:00', ''))
                        example_close_time = close_dt.strftime('%Y-%m-%d %H:%M UTC')
                    except:
                        pass
                
                # Get top 3 contracts by price
                priced_contracts = []
                for c in example_contracts:
                    price = c.get('yes_bid') or c.get('yes_ask') or 0
                    if price > 0:
                        priced_contracts.append((c, price))
                
                # Sort by price (highest first)
                priced_contracts.sort(key=lambda x: x[1], reverse=True)
                top_3 = priced_contracts[:3]
                
                # Build message (keep under ~10 lines)
                message = f"📊 *Kalshi Mention Markets*\n"
                message += f"{num_events} active event{'s' if num_events != 1 else ''}\n\n"
                message += f"*{example_title[:80]}...*\n"
                message += f"Closes: {example_close_time}\n\n"
                
                # Top 3 contracts
                if top_3:
                    message += "*Top 3 contracts:*\n"
                    for i, (contract, price) in enumerate(top_3, 1):
                        yes_sub = contract.get('yes_sub_title', 'Yes')
                        price_display = f"${price/100:.2f}"
                        
                        # Try to get price change
                        previous_price = contract.get('previous_price')
                        last_price = contract.get('last_price')
                        change_str = ""
                        if previous_price and previous_price != price:
                            change = price - previous_price
                            change_pct = (change / previous_price * 100) if previous_price > 0 else 0
                            arrow = "↑" if change > 0 else "↓"
                            change_str = f" {arrow}${abs(change)/100:.2f} ({abs(change_pct):.1f}%)"
                        elif last_price and last_price != price:
                            change = price - last_price
                            change_pct = (change / last_price * 100) if last_price > 0 else 0
                            arrow = "↑" if change > 0 else "↓"
                            change_str = f" {arrow}${abs(change)/100:.2f} ({abs(change_pct):.1f}%)"
                        
                        message += f"{i}. {yes_sub[:30]}: {price_display}{change_str}\n"
                
                # Market link (using proper Kalshi URL format)
                market_url = get_kalshi_market_url(example_event_ticker)
                message += f"\n<{market_url}|View on Kalshi>"
                
                send_slack_message(message)
            except Exception as e:
                print(f"⚠️  Warning: Failed to send Slack notification: {e}")
        
        return
    
    # Fetch markets - industry-controlled or generic
    print("\n" + "="*60)
    if args.industry:
        print(f"STEP 0: Fetch Markets by Industry ({args.industry})")
        print("="*60)
        all_markets = fetch_markets_by_industry(client, args.industry, limit_per_series=args.limit, status=args.status)
    elif args.semantic:
        # If --semantic without --industry: use pagination to find rare markets
        print("STEP 0: Fetch Markets with Pagination (Generic)")
        print("="*60)
        all_markets = fetch_markets_paginated(client, target_markets=2500, status=args.status, limit_per_page=args.limit)
    else:
        print("STEP 0: Fetch Markets Once (Generic)")
        print("="*60)
        all_markets = fetch_markets_once(client, limit=args.limit, status=args.status)
    
    if not all_markets:
        print("No markets fetched. Exiting.")
        return
    
    # If --semantic is provided, filter and display, then exit
    if args.semantic:
        print("\n" + "="*60)
        print(f"Filtering by Semantic Tag: {args.semantic}")
        print("="*60)
        filtered_markets = filter_markets_by_semantic_tag(all_markets, args.semantic)
        print(f"✓ Found {len(filtered_markets)} markets (from {len(all_markets)} total)")
        display_markets(filtered_markets, max_display=20, show_industry=True)
        return
    
    # If --industry is provided, run streamlined analysis (skip Step 5)
    if args.industry:
        # Step 1: Discover available series
        print("\n" + "="*60)
        print("STEP 1: Discover Available Series (from fetched markets)")
        print("="*60)
        series_counts = {}
        for market in all_markets:
            event_ticker = market.get('event_ticker', '')
            if event_ticker:
                prefix = event_ticker.split('-')[0] if '-' in event_ticker else event_ticker
                series_counts[prefix] = series_counts.get(prefix, 0) + 1
        
        print(f"Found {len(series_counts)} unique series tickers:")
        print()
        for series, count in sorted(series_counts.items(), key=lambda x: x[1], reverse=True)[:15]:
            print(f"  {series:30} {count:4} markets")
        
        # Step 6: Semantic tagging analysis
        print("\n" + "="*60)
        print("STEP 6: Semantic Tag Analysis")
        print("="*60)
        semantic_counts = {}
        for market in all_markets:
            title = market.get('title', '')
            tags = get_semantic_tags(title)
            for tag in tags:
                semantic_counts[tag] = semantic_counts.get(tag, 0) + 1
        
        print("Semantic tag distribution:")
        print()
        for tag, count in sorted(semantic_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {tag:20} {count:4} markets")
        
        # Display sample markets
        print("\n" + "="*60)
        print(f"SAMPLE MARKETS ({len(all_markets)} total, showing first 10):")
        print("="*60)
        display_markets(all_markets, max_display=10, show_industry=True)
        return
    
    # Exploration/demo mode (no flags provided)
    # Example 1: Discover available series (using fetched markets)
    print("\n" + "="*60)
    print("STEP 1: Discover Available Series (from fetched markets)")
    print("="*60)
    series_counts = {}
    for market in all_markets:
        event_ticker = market.get('event_ticker', '')
        if event_ticker:
            prefix = event_ticker.split('-')[0] if '-' in event_ticker else event_ticker
            series_counts[prefix] = series_counts.get(prefix, 0) + 1
    
    print(f"Found {len(series_counts)} unique series tickers:")
    print()
    for series, count in sorted(series_counts.items(), key=lambda x: x[1], reverse=True)[:15]:
        print(f"  {series:30} {count:4} markets")
    
    # Example 2: Filter by series_ticker (local)
    print("\n" + "="*60)
    print("STEP 2: Filter by Series Ticker (Local)")
    print("="*60)
    crypto_markets = filter_markets_by_series_ticker_local(all_markets, 'KXBTC15M')
    print(f"✓ Found {len(crypto_markets)} markets (from {len(all_markets)} total)")
    display_markets(crypto_markets, max_display=5)
    
    # Example 3: Filter by event_ticker prefix (local)
    print("\n" + "="*60)
    print("STEP 3: Filter by Event Ticker Prefix (Local)")
    print("="*60)
    eth_markets = filter_markets_by_event_ticker_prefix_local(all_markets, 'KXETH')
    print(f"✓ Found {len(eth_markets)} markets (from {len(all_markets)} total)")
    display_markets(eth_markets, max_display=5)
    
    # Example 4: Filter by category (local)
    print("\n" + "="*60)
    print("STEP 4: Filter by Category (Local)")
    print("="*60)
    politics_markets = filter_markets_by_category_local(all_markets, 'Politics')
    print(f"✓ Found {len(politics_markets)} markets (from {len(all_markets)} total)")
    if len(politics_markets) == 0:
        print("⚠️  Note: Category field may be empty in demo API")
    display_markets(politics_markets, max_display=5)
    
    # Example 5: Filter by industry label (local)
    print("\n" + "="*60)
    print("STEP 5: Filter by Industry Label (Local)")
    print("="*60)
    print(f"Available industries: {', '.join(get_all_industries())}")
    print()
    
    crypto_markets = filter_markets_by_industry_label_local(all_markets, 'Crypto')
    print(f"✓ Found {len(crypto_markets)} markets (from {len(all_markets)} total)")
    display_markets(crypto_markets, max_display=5, show_industry=True)

    # Example 6: Filter by semantic tag (local)
    print("\n" + "="*60)
    print("STEP 6: Filter by Semantic Tag (Local)")
    print("="*60)
    mention_markets = filter_markets_by_semantic_tag(all_markets, "mention")
    print(f"✓ Found {len(mention_markets)} markets (from {len(all_markets)} total)")
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
