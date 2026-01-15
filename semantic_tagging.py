#!/usr/bin/env python3
"""
Rule-based semantic tagging for Kalshi market titles.

Operates ONLY on the title string (no scoring, no ML, no external data).
"""

import re
from typing import List


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
    - If contains price-direction language like price/up/down/above/below -> 'price_direction'
    - If contains time-window language like next/minutes/hours/today -> 'time_bucketed'
    - If none match -> ['binary_outcome']
    """
    if not title:
        return ["binary_outcome"]

    t = title.strip().lower()
    tags: List[str] = []

    # Mention: keyword OR quoted text ("..." or '...')
    has_quotes = bool(re.search(r"['\"“”‘’].+?['\"“”‘’]", title))
    if has_quotes or any(k in t for k in _MENTION_KEYWORDS):
        tags.append("mention")

    # Price direction
    if any(k in t for k in _PRICE_DIRECTION_KEYWORDS):
        tags.append("price_direction")

    # Time bucketed
    if any(k in t for k in _TIME_BUCKETED_KEYWORDS):
        tags.append("time_bucketed")

    if not tags:
        tags.append("binary_outcome")

    # Stable ordering
    order = {"mention": 0, "price_direction": 1, "time_bucketed": 2, "binary_outcome": 3}
    tags = sorted(set(tags), key=lambda x: order.get(x, 999))
    return tags


def has_semantic_tag(title: str, tag: str) -> bool:
    """True if get_semantic_tags(title) contains tag."""
    return tag in get_semantic_tags(title)


def filter_markets_by_semantic_tag(markets: List[dict], tag: str) -> List[dict]:
    """Filter a list of market dicts by semantic tag derived from title."""
    out: List[dict] = []
    for m in markets:
        title = (m.get("title") or "").strip()
        if has_semantic_tag(title, tag):
            out.append(m)
    return out

