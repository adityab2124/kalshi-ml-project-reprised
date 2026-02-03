-- Kalshi High-Fidelity Data Schema

-- 1. Raw price/volume history
CREATE TABLE IF NOT EXISTS market_history (
    id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(100) NOT NULL,
    price DECIMAL(10, 4) NOT NULL,
    quantity INTEGER NOT NULL,
    notional_value DECIMAL(12, 2) NOT NULL,
    kalshi_ts BIGINT NOT NULL,
    recorded_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2. Expiration logic (Tick-Tock Factor)
CREATE TABLE IF NOT EXISTS market_metadata (
    ticker VARCHAR(100) PRIMARY KEY,
    title VARCHAR(255),
    close_time TIMESTAMPTZ NOT NULL,
    expiration_time TIMESTAMPTZ
);

-- 3. Spike logs
CREATE TABLE IF NOT EXISTS spike_logs (
    id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(100) NOT NULL,
    start_price DECIMAL(10, 4) NOT NULL,
    end_price DECIMAL(10, 4) NOT NULL,
    pct_change DECIMAL(8, 4) NOT NULL,
    total_spike_volume_usd DECIMAL(12, 2) NOT NULL,
    detected_at TIMESTAMPTZ DEFAULT NOW()
);

-- 4. Market settlement results
CREATE TABLE IF NOT EXISTS market_results (
    ticker VARCHAR(100) PRIMARY KEY,
    final_price INTEGER NOT NULL,  -- 100 for YES, 0 for NO
    settlement_time TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_history_ticker ON market_history(ticker);
CREATE INDEX IF NOT EXISTS idx_history_ts ON market_history(kalshi_ts);
CREATE INDEX IF NOT EXISTS idx_spikes_ticker ON spike_logs(ticker);
CREATE INDEX IF NOT EXISTS idx_results_settlement_time ON market_results(settlement_time);