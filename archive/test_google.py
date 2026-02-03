#!/usr/bin/env python3
"""
Test script for Google Search Grounding feature.
"""

import os
from google_context import get_market_context

# Check if API key is set
api_key = os.getenv('GOOGLE_API_KEY')
if not api_key:
    print("❌ ERROR: GOOGLE_API_KEY not set!")
    print("   Set it with: export GOOGLE_API_KEY='your-key-here'")
    exit(1)

print("✓ GOOGLE_API_KEY is set")
print("🔍 Testing Google Search Grounding...\n")

# Test with a sample market
ticker = "KXBERNIEMENTION-26JAN20-BILL"
contract = "Billionaire"

print(f"Ticker: {ticker}")
print(f"Contract: {contract}\n")

result = get_market_context(ticker, contract)

if result:
    print("✓ Success! Got context:")
    print(f"  Summary: {result.get('summary', 'N/A')}")
    print(f"  URL: {result.get('url', 'N/A')}")
else:
    print("❌ Failed to get context (returned None)")
    print("   Check error messages above for details")
