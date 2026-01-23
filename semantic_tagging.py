#!/usr/bin/env python3
"""
Rule-based semantic tagging for Kalshi market titles.

Operates ONLY on the title string (no scoring, no ML, no external data).
"""

import re
from typing import List

# Hardcoded event ticker prefixes for known mention markets
# Kalshi reuses stable prefixes while rotating dates, so these are reliable identifiers
HARDCODED_MENTION_PREFIXES = [
    "KXTRUMPSAY",
    "KXTRUMPSAYMONTH",
    "KXMAMDANIMENTION",
    "KXMRBEASTMENTION",
    "KXBERNIEMENTION",
    # "KXNFLMENTION",  # Commented out for now
]


_MENTION_KEYWORDS = {
    "say",
    "says",
    "said",
    "mention",
    "mentions",
    "mentioned",
    "use",
    "uses",
    "using",
}

_PRICE_DIRECTION_KEYWORDS = {
    "price",
    "up",
    "down",
    "above",
    "below",
    "over",
    "under",
    "greater",
    "less",
    "at least",
    "at most",
}

_TIME_BUCKETED_KEYWORDS = {
    "next",
    "minute",
    "minutes",
    "hour",
    "hours",
    "today",
    "tomorrow",
    "this week",
    "this month",
    "this year",
}


def get_semantic_tags(title: str) -> List[str]:
    """
    Return a list of semantic tags based ONLY on the market title.

    Rules:
    - If contains words like say/mention/use OR includes quoted text -> 'mention'
    - If matches pattern "what will .* say" (case-insensitive) -> 'speech_mention'
    - If contains price-direction language like price/up/down/above/below -> 'price_direction'
    - If contains time-window language like next/minutes/hours/today -> 'time_bucketed'
    - If none match -> ['binary_outcome']
    """
    if not title:
        return ["binary_outcome"]

    t = title.strip().lower()
    tags: List[str] = []

    # Mention: keyword OR quoted text ("..." or '...')
    has_quotes = bool(re.search(r"['\"“”''].+?['\"“”'']", title))
    if has_quotes or any(k in t for k in _MENTION_KEYWORDS):
        tags.append("mention")

    # Speech mention: matches "what will .* say" or "will .* say" pattern (case-insensitive)
    # Must contain "say" AND match one of the patterns
    if "say" in t:
        if re.search(r"what will .* say", t, re.IGNORECASE) or re.search(r"will .* say", t, re.IGNORECASE):
            tags.append("speech_mention")

    # Price direction
    if any(k in t for k in _PRICE_DIRECTION_KEYWORDS):
        tags.append("price_direction")

    # Time bucketed
    if any(k in t for k in _TIME_BUCKETED_KEYWORDS):
        tags.append("time_bucketed")

    if not tags:
        tags.append("binary_outcome")

    # Stable ordering
    order = {"mention": 0, "speech_mention": 1, "price_direction": 2, "time_bucketed": 3, "binary_outcome": 4}
    tags = sorted(set(tags), key=lambda x: order.get(x, 999))
    return tags


def has_semantic_tag(title: str, tag: str) -> bool:
    """True if get_semantic_tags(title) contains tag."""
    return tag in get_semantic_tags(title)


def is_mention_prefix(event_ticker: str) -> bool:
    """
    Check if an event_ticker starts with any hardcoded mention prefix.
    
    Args:
        event_ticker: Event ticker string (e.g., "KXTRUMPSAY-2024-01-15")
    
    Returns:
        True if event_ticker starts with any prefix in HARDCODED_MENTION_PREFIXES
    """
    if not event_ticker:
        return False
    
    for prefix in HARDCODED_MENTION_PREFIXES:
        if event_ticker.startswith(prefix):
            return True
    
    return False

def filter_markets_by_semantic_tag(markets: List[dict], tag: str) -> List[dict]:
    """Filter a list of market dicts by semantic tag derived from title."""
    out: List[dict] = []
    for m in markets:
        title = (m.get("title") or "").strip()
        if has_semantic_tag(title, tag):
            out.append(m)
    return out

def filter_markets_by_mention_prefix(markets: List[dict]) -> List[dict]:
    """
    Filter markets by hardcoded mention event ticker prefixes.
    
    Args:
        markets: List of market dictionaries
    
    Returns:
        List of markets whose event_ticker starts with a known mention prefix
    """
    out: List[dict] = []
    for m in markets:
        event_ticker = m.get("event_ticker", "")
        if is_mention_prefix(event_ticker):
            out.append(m)
    return out
