from p import ExchangeClient
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from dateutil import parser
import pandas as pd
from datetime import datetime
import time

EXCHANGE_API_BASE = "https://demo-api.kalshi.co/trade-api/v2"
KEY_ID = "cc76eee9-dba9-4bf2-a06f-eddf6a44a8e1"
PRIVATE_KEY_PATH = "private_key.pem"

TRADES_LIMIT = 500   # small on purpose → fast
MAX_MARKETS = 20     # snapshot only

def load_private_key(path):
    with open(path, "rb") as f:
        return serialization.load_pem_private_key(
            f.read(), password=None, backend=default_backend()
        )

def main():
    key = load_private_key(PRIVATE_KEY_PATH)
    client = ExchangeClient(
        exchange_api_base=EXCHANGE_API_BASE,
        key_id=KEY_ID,
        private_key=key
    )

    print("Fetching recent trades...")
    trades = client.get_trades(limit=TRADES_LIMIT)["trades"]

    # Keep latest trade per ticker
    latest_trade = {}
    for t in trades:
        ticker = t["ticker"]
        ts = parser.parse(t["created_time"])

        if ticker not in latest_trade or ts > latest_trade[ticker]["ts"]:
            latest_trade[ticker] = {
                "ts": ts,
                "price": t["price"]
            }

    rows = []
    snapshot_time = datetime.utcnow().isoformat()

    print("Fetching market titles...")
    for i, (ticker, info) in enumerate(latest_trade.items()):
        if i >= MAX_MARKETS:
            break

        try:
            market = client.get_market(ticker=ticker)["market"]

            rows.append({
                "ticker": ticker,
                "title": market.get("title"),
                "snapshot_time": snapshot_time,
                "yes_price": info["price"]
            })

            print(f"✓ {ticker} | price={info['price']}")

        except Exception:
            continue

        time.sleep(0.05)

    df = pd.DataFrame(rows)
    df.to_csv("dataset_phase1.csv", index=False)

    print(f"\nSaved dataset_phase1.csv with {len(df)} rows")

if __name__ == "__main__":
    main()
 