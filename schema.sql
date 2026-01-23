-- Kalshi Trading Database Schema
-- High-fidelity data collection for backtesting and analysis

-- =====================================================
-- 1. MARKET SNAPSHOTS (Raw Trade Data)
-- =====================================================
CREATE TABLE IF NOT EXISTS market_snapshots (
    id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(100) NOT NULL,
    price DECIMAL(10, 4) NOT NULL,  -- Price in dollars (e.g., 0.3600)
    volume INTEGER NOT NULL,  -- Number of contracts traded
    notional_value DECIMAL(12, 2) NOT NULL,  -- price * volume
    trade_id VARCHAR(100) UNIQUE,  -- Kalshi's unique trade ID
    taker_side VARCHAR(10),  -- 'yes' or 'no'
    timestamp BIGINT NOT NULL,  -- Kalshi's timestamp (unix ms)
    recorded_at TIMESTAMPTZ DEFAULT NOW()  -- Local recording time
);

CREATE INDEX IF NOT EXISTS idx_snapshots_ticker ON market_snapshots(ticker);
CREATE INDEX IF NOT EXISTS idx_snapshots_timestamp ON market_snapshots(timestamp);
CREATE INDEX IF NOT EXISTS idx_snapshots_recorded_at ON market_snapshots(recorded_at);

-- =====================================================
-- 2. MARKET METADATA (Tick-Tock Factor)
-- =====================================================
CREATE TABLE IF NOT EXISTS market_metadata (
    ticker VARCHAR(100) PRIMARY KEY,
    event_ticker VARCHAR(100) NOT NULL,
    title TEXT,
    open_time TIMESTAMPTZ,
    close_time TIMESTAMPTZ,
    expiration_time TIMESTAMPTZ,
    status VARCHAR(50),
    last_updated TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_metadata_close_time ON market_metadata(close_time);
CREATE INDEX IF NOT EXISTS idx_metadata_event_ticker ON market_metadata(event_ticker);

-- =====================================================
-- 3. SPIKE EVENTS (Alert Records)
-- =====================================================
CREATE TABLE IF NOT EXISTS spike_events (
    id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(100) NOT NULL,
    old_price DECIMAL(10, 4) NOT NULL,
    new_price DECIMAL(10, 4) NOT NULL,
    pct_change DECIMAL(8, 4) NOT NULL,  -- Percentage change (e.g., 0.2540 = 25.4%)
    volume INTEGER NOT NULL,  -- Contracts in the triggering trade
    notional_impact DECIMAL(12, 2) NOT NULL,  -- Total $ moved (new_price * volume)
    minutes_to_expiration INTEGER,  -- Minutes left until close
    threshold_used DECIMAL(5, 2),  -- Which threshold triggered (e.g., 0.15)
    detected_at TIMESTAMPTZ DEFAULT NOW(),
    context TEXT  -- Optional: AI context or notes
);

CREATE INDEX IF NOT EXISTS idx_spikes_ticker ON spike_events(ticker);
CREATE INDEX IF NOT EXISTS idx_spikes_detected_at ON spike_events(detected_at);
CREATE INDEX IF NOT EXISTS idx_spikes_pct_change ON spike_events(pct_change);
CREATE INDEX IF NOT EXISTS idx_spikes_notional_impact ON spike_events(notional_impact);

-- =====================================================
-- HELPER VIEWS
-- =====================================================

-- View: Recent high-impact spikes
CREATE OR REPLACE VIEW high_impact_spikes AS
SELECT 
    ticker,
    old_price,
    new_price,
    pct_change,
    notional_impact,
    minutes_to_expiration,
    detected_at
FROM spike_events
WHERE pct_change > 0.20  -- > 20%
  AND notional_impact > 50.00  -- > $50
  AND minutes_to_expiration > 60  -- > 1 hour left
ORDER BY detected_at DESC;

-- View: Market activity summary
CREATE OR REPLACE VIEW market_activity_summary AS
SELECT 
    ticker,
    COUNT(*) as trade_count,
    SUM(volume) as total_volume,
    SUM(notional_value) as total_notional,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    MAX(recorded_at) as last_trade
FROM market_snapshots
WHERE recorded_at > NOW() - INTERVAL '24 hours'
GROUP BY ticker
ORDER BY total_notional DESC;

-- =====================================================
-- SAMPLE QUERIES (for reference)
-- =====================================================

-- Query 1: Show spikes > 20% with > $50 notional and > 1 hour to expiration
-- SELECT * FROM high_impact_spikes;

-- Query 2: Find panic sell opportunities (large drops)
-- SELECT * FROM spike_events 
-- WHERE pct_change < -0.30 
--   AND notional_impact > 100 
--   AND minutes_to_expiration > 30
-- ORDER BY detected_at DESC
-- LIMIT 20;

-- Query 3: Market activity during specific time window
-- SELECT ticker, COUNT(*) as trades, SUM(notional_value) as total_value
-- FROM market_snapshots
-- WHERE recorded_at BETWEEN '2024-01-23 14:00:00' AND '2024-01-23 15:00:00'
-- GROUP BY ticker
-- ORDER BY total_value DESC;

-- Query 4: Price movements over time for a specific ticker
-- SELECT 
--     ticker,
--     price,
--     volume,
--     notional_value,
--     TO_TIMESTAMP(timestamp/1000) as trade_time
-- FROM market_snapshots
-- WHERE ticker = 'KXMAMDANIMENTION-26JAN24-RACE'
-- ORDER BY timestamp ASC;
