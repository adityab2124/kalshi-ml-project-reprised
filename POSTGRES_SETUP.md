# PostgreSQL Setup for Kalshi Trading Bot

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install psycopg2-binary
```

### 2. Start PostgreSQL with Docker

```bash
# Start the container (creates database automatically)
docker-compose up -d

# Check if it's running
docker-compose ps

# View logs
docker-compose logs -f postgres
```

### 3. Verify Connection

```bash
python3 db_postgres.py
```

You should see:
```
✓ PostgreSQL connection pool initialized
✓ Batch manager ready (size=100, timeout=5s)
✓ Test insert successful
✓ Found X recent spikes
```

---

## 📊 Database Schema

### Tables

1. **`market_snapshots`** - Every trade recorded
   - `ticker`, `price`, `volume`, `notional_value`
   - `trade_id`, `taker_side`, `timestamp`, `recorded_at`

2. **`market_metadata`** - Market lifecycle info
   - `ticker`, `event_ticker`, `title`
   - `open_time`, `close_time`, `expiration_time`, `status`

3. **`spike_events`** - Your bot's alerts
   - `ticker`, `old_price`, `new_price`, `pct_change`
   - `volume`, `notional_impact`, `minutes_to_expiration`

### Views

- `high_impact_spikes` - Pre-filtered for significant events
- `market_activity_summary` - 24h trading summary per ticker

---

## 🔧 Integration with kalshi_ws.py

Add this to the top of `kalshi_ws.py`:

```python
from db_postgres import initialize_database, shutdown_database, batch_manager, get_minutes_to_expiration, upsert_market_metadata
import signal

# Initialize DB at startup
initialize_database()

# Shutdown handler
def cleanup_handler(sig, frame):
    print("\n\nShutting down...")
    shutdown_database()
    sys.exit(0)

signal.signal(signal.SIGTERM, cleanup_handler)
```

In `on_message()` where you process trades:

```python
# Record every trade
batch_manager.add_snapshot(
    ticker=ticker,
    price=price,
    volume=count,
    trade_id=data.get("msg", {}).get("trade_id"),
    taker_side=data.get("msg", {}).get("taker_side", "unknown"),
    timestamp=data.get("msg", {}).get("ts")
)
```

In `send_alert()` when spike detected:

```python
# Record spike event
minutes_to_exp = get_minutes_to_expiration(ticker)
batch_manager.add_spike(
    ticker=ticker,
    old_price=old_price,
    new_price=new_price,
    pct_change=change_pct,
    volume=volume,
    minutes_to_exp=minutes_to_exp,
    threshold=threshold,
    context=context
)
```

---

## 📈 Sample Queries

### 1. High-Impact Spikes (> 20%, > $50, > 1hr left)

```sql
SELECT * FROM high_impact_spikes;
```

### 2. Panic Sell Opportunities

```sql
SELECT * FROM spike_events 
WHERE pct_change < -0.30 
  AND notional_impact > 100 
  AND minutes_to_expiration > 30
ORDER BY detected_at DESC
LIMIT 20;
```

### 3. Market Activity (Last 24h)

```sql
SELECT * FROM market_activity_summary;
```

### 4. Price Chart Data for Specific Ticker

```sql
SELECT 
    TO_TIMESTAMP(timestamp/1000) as time,
    price,
    volume,
    notional_value
FROM market_snapshots
WHERE ticker = 'KXMAMDANIMENTION-26JAN24-RACE'
  AND recorded_at > NOW() - INTERVAL '24 hours'
ORDER BY timestamp ASC;
```

### 5. Top Movers Today

```sql
SELECT 
    ticker,
    MIN(price) as low,
    MAX(price) as high,
    (MAX(price) - MIN(price)) / MIN(price) * 100 as pct_range,
    SUM(notional_value) as total_volume
FROM market_snapshots
WHERE recorded_at > CURRENT_DATE
GROUP BY ticker
ORDER BY pct_range DESC
LIMIT 20;
```

---

## 🔐 Environment Variables (Optional)

Create `.env` file:

```bash
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=kalshi_trading
POSTGRES_USER=kalshi
POSTGRES_PASSWORD=kalshi_secure_password
```

Then load in Python:

```python
from dotenv import load_dotenv
load_dotenv()
```

---

## 🛠 Maintenance

### View Database Size

```bash
docker exec kalshi_postgres psql -U kalshi -d kalshi_trading -c "
  SELECT 
    pg_size_pretty(pg_database_size('kalshi_trading')) as db_size;
"
```

### Backup Database

```bash
docker exec kalshi_postgres pg_dump -U kalshi kalshi_trading > backup_$(date +%Y%m%d).sql
```

### Restore Database

```bash
cat backup_20240123.sql | docker exec -i kalshi_postgres psql -U kalshi kalshi_trading
```

### Stop Container

```bash
docker-compose down  # Stop but keep data
docker-compose down -v  # Stop and delete data
```

---

## 📊 Performance Tips

1. **Batch Size**: Increase `BATCH_SIZE` in `db_postgres.py` if you have very high trade volume
2. **Indexes**: The schema includes indexes on hot columns (ticker, timestamp, etc.)
3. **Connection Pool**: Adjust `minconn`/`maxconn` based on your workload
4. **Flush Frequency**: Lower `BATCH_TIMEOUT` for more real-time data, higher for better performance

---

## 🐛 Troubleshooting

### "Connection refused"
- Check Docker is running: `docker ps`
- Check port 5432 isn't blocked: `netstat -an | grep 5432`

### "Password authentication failed"
- Check credentials in `docker-compose.yml` match `db_postgres.py`

### "Slow inserts"
- Increase `BATCH_SIZE` or `BATCH_TIMEOUT`
- Check Docker resource limits

---

## Next Steps

1. ✅ Start PostgreSQL
2. ✅ Test connection
3. ✅ Integrate into `kalshi_ws.py`
4. ✅ Run bot and collect data
5. ✅ Use sample queries for analysis
6. 🚀 Build backtesting strategies!
