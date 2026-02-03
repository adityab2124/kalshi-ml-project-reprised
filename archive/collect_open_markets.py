#!/usr/bin/env python3
"""
Collect OPEN markets (which have current prices) for calibration analysis.
These markets will need to be tracked until they close to get outcomes.
"""

from collect_market_data import load_private_key, ExchangeClient
import csv

# ===== CONFIGURATION =====
EXCHANGE_API_BASE = "https://demo-api.kalshi.co/trade-api/v2"
KEY_ID = "cc76eee9-dba9-4bf2-a06f-eddf6a44a8e1"
PRIVATE_KEY_PATH = "private_key.pem"

def collect_open_markets():
    """Collect open markets with current prices"""
    print("="*60)
    print("COLLECTING OPEN MARKETS (with current prices)")
    print("="*60)
    print("\nThese markets have current prices we can use for calibration.")
    print("Track them until they close to get outcomes.\n")
    
    # Load credentials
    private_key = load_private_key(PRIVATE_KEY_PATH)
    client = ExchangeClient(
        exchange_api_base=EXCHANGE_API_BASE,
        key_id=KEY_ID,
        private_key=private_key
    )
    
    # Import the collection function
    from collect_market_data import collect_market_data
    
    # Collect open markets
    collect_market_data(
        client,
        "kalshi_open_markets.csv",
        status='open',      # Get open markets (have prices)
        max_pages=5,
        max_markets=500
    )
    
    print("\n" + "="*60)
    print("NEXT STEPS:")
    print("="*60)
    print("1. Wait for some of these markets to close")
    print("2. Re-run collect_market_data.py with status='settled'")
    print("3. Match open market prices with settled outcomes")
    print("4. Run analyze_calibration.py")

if __name__ == "__main__":
    collect_open_markets()

