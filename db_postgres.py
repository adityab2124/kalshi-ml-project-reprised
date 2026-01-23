#!/usr/bin/env python3
"""
PostgreSQL Database Module for Kalshi Trading Bot
High-fidelity data collection with batch insert optimization
"""

import psycopg2
from psycopg2.extras import execute_batch
from psycopg2.pool import SimpleConnectionPool
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timezone
from contextlib import contextmanager
import os
import time
from collections import deque

# ===== CONFIGURATION =====

DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'database': os.getenv('POSTGRES_DB', 'kalshi_trading'),
    'user': os.getenv('POSTGRES_USER', 'kalshi'),
    'password': os.getenv('POSTGRES_PASSWORD', 'kalshi_secure_password')
}

# Batch insert configuration
BATCH_SIZE = 100  # Insert every 100 records
BATCH_TIMEOUT = 5  # Or every 5 seconds, whichever comes first

# ===== CONNECTION POOL =====

class DatabasePool:
    """Thread-safe connection pool for PostgreSQL."""
    
    def __init__(self, minconn=2, maxconn=10):
        self.pool = SimpleConnectionPool(minconn, maxconn, **DB_CONFIG)
    
    @contextmanager
    def get_connection(self):
        """Context manager for getting a connection from the pool."""
        conn = self.pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            self.pool.putconn(conn)
    
    def close_all(self):
        """Close all connections in the pool."""
        self.pool.closeall()

# Global pool instance
db_pool = None

def init_db_pool():
    """Initialize the database connection pool."""
    global db_pool
    if db_pool is None:
        db_pool = DatabasePool()
    return db_pool

# ===== BATCH INSERT MANAGER =====

class BatchInsertManager:
    """
    Manages batched inserts to avoid slowing down the WebSocket thread.
    Automatically flushes when batch is full or timeout expires.
    """
    
    def __init__(self, batch_size=BATCH_SIZE, timeout=BATCH_TIMEOUT):
        self.batch_size = batch_size
        self.timeout = timeout
        
        # Separate queues for each table
        self.snapshot_queue = deque()
        self.spike_queue = deque()
        
        self.last_flush = time.time()
    
    def add_snapshot(self, ticker: str, price: float, volume: int, 
                     trade_id: str, taker_side: str, timestamp: int):
        """Add a market snapshot to the batch queue."""
        notional_value = price * volume
        self.snapshot_queue.append({
            'ticker': ticker,
            'price': price,
            'volume': volume,
            'notional_value': notional_value,
            'trade_id': trade_id,
            'taker_side': taker_side,
            'timestamp': timestamp
        })
        
        self._check_flush()
    
    def add_spike(self, ticker: str, old_price: float, new_price: float,
                  pct_change: float, volume: int, minutes_to_exp: Optional[int],
                  threshold: float, context: Optional[str] = None):
        """Add a spike event to the batch queue."""
        notional_impact = new_price * volume
        self.spike_queue.append({
            'ticker': ticker,
            'old_price': old_price,
            'new_price': new_price,
            'pct_change': pct_change,
            'volume': volume,
            'notional_impact': notional_impact,
            'minutes_to_expiration': minutes_to_exp,
            'threshold_used': threshold,
            'context': context
        })
        
        self._check_flush()
    
    def _check_flush(self):
        """Check if we should flush based on size or timeout."""
        total_size = len(self.snapshot_queue) + len(self.spike_queue)
        time_elapsed = time.time() - self.last_flush
        
        if total_size >= self.batch_size or time_elapsed >= self.timeout:
            self.flush()
    
    def flush(self):
        """Force flush all queued data to database."""
        if not db_pool:
            return
        
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            
            # Flush snapshots
            if self.snapshot_queue:
                snapshot_data = [
                    (s['ticker'], s['price'], s['volume'], s['notional_value'],
                     s['trade_id'], s['taker_side'], s['timestamp'])
                    for s in self.snapshot_queue
                ]
                execute_batch(cursor, """
                    INSERT INTO market_snapshots 
                    (ticker, price, volume, notional_value, trade_id, taker_side, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trade_id) DO NOTHING
                """, snapshot_data, page_size=100)
                self.snapshot_queue.clear()
            
            # Flush spikes
            if self.spike_queue:
                spike_data = [
                    (s['ticker'], s['old_price'], s['new_price'], s['pct_change'],
                     s['volume'], s['notional_impact'], s['minutes_to_expiration'],
                     s['threshold_used'], s['context'])
                    for s in self.spike_queue
                ]
                execute_batch(cursor, """
                    INSERT INTO spike_events
                    (ticker, old_price, new_price, pct_change, volume, notional_impact,
                     minutes_to_expiration, threshold_used, context)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, spike_data, page_size=100)
                self.spike_queue.clear()
        
        self.last_flush = time.time()

# Global batch manager
batch_manager = None

def init_batch_manager():
    """Initialize the batch insert manager."""
    global batch_manager
    if batch_manager is None:
        batch_manager = BatchInsertManager()
    return batch_manager

# ===== MARKET METADATA FUNCTIONS =====

def upsert_market_metadata(ticker: str, event_ticker: str, title: str,
                           close_time: Optional[datetime] = None,
                           status: str = 'active'):
    """Insert or update market metadata."""
    if not db_pool:
        return
    
    with db_pool.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO market_metadata 
            (ticker, event_ticker, title, close_time, status, last_updated)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (ticker) DO UPDATE SET
                close_time = EXCLUDED.close_time,
                status = EXCLUDED.status,
                last_updated = NOW()
        """, (ticker, event_ticker, title, close_time, status))

def get_minutes_to_expiration(ticker: str) -> Optional[int]:
    """Calculate minutes remaining until market closes."""
    if not db_pool:
        return None
    
    with db_pool.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT EXTRACT(EPOCH FROM (close_time - NOW())) / 60
            FROM market_metadata
            WHERE ticker = %s
        """, (ticker,))
        result = cursor.fetchone()
        return int(result[0]) if result and result[0] else None

# ===== QUERY HELPERS =====

def get_recent_spikes(limit: int = 50, min_pct_change: float = 0.15) -> List[Dict]:
    """Get recent spike events above a certain threshold."""
    if not db_pool:
        return []
    
    with db_pool.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ticker, old_price, new_price, pct_change, notional_impact,
                   minutes_to_expiration, detected_at
            FROM spike_events
            WHERE ABS(pct_change) >= %s
            ORDER BY detected_at DESC
            LIMIT %s
        """, (min_pct_change, limit))
        
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

def get_market_trades(ticker: str, hours_back: int = 24) -> List[Dict]:
    """Get all trades for a specific ticker in the last N hours."""
    if not db_pool:
        return []
    
    with db_pool.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ticker, price, volume, notional_value,
                   TO_TIMESTAMP(timestamp/1000) as trade_time
            FROM market_snapshots
            WHERE ticker = %s
              AND recorded_at > NOW() - INTERVAL '%s hours'
            ORDER BY timestamp ASC
        """, (ticker, hours_back))
        
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

def get_panic_sell_opportunities(min_drop: float = 0.30, min_notional: float = 100.0) -> List[Dict]:
    """Find potential panic sell opportunities for arbitrage."""
    if not db_pool:
        return []
    
    with db_pool.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ticker, old_price, new_price, pct_change, notional_impact,
                   minutes_to_expiration, detected_at
            FROM spike_events
            WHERE pct_change < -%s
              AND notional_impact > %s
              AND minutes_to_expiration > 30
            ORDER BY detected_at DESC
            LIMIT 20
        """, (min_drop, min_notional))
        
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

# ===== INITIALIZATION =====

def initialize_database():
    """Initialize database pool and batch manager."""
    init_db_pool()
    init_batch_manager()
    print("✓ PostgreSQL connection pool initialized")
    print(f"✓ Batch manager ready (size={BATCH_SIZE}, timeout={BATCH_TIMEOUT}s)")

def shutdown_database():
    """Gracefully shutdown database connections."""
    global batch_manager, db_pool
    
    if batch_manager:
        batch_manager.flush()  # Flush any pending data
        print("✓ Flushed pending database writes")
    
    if db_pool:
        db_pool.close_all()
        print("✓ Closed database connections")

# ===== TESTING =====

if __name__ == "__main__":
    print("Testing PostgreSQL connection...")
    
    try:
        initialize_database()
        
        # Test insert
        batch_manager.add_snapshot(
            ticker="TEST-TICKER",
            price=0.50,
            volume=100,
            trade_id="test-123",
            taker_side="yes",
            timestamp=int(time.time() * 1000)
        )
        batch_manager.flush()
        
        print("✓ Test insert successful")
        
        # Test query
        spikes = get_recent_spikes(limit=5)
        print(f"✓ Found {len(spikes)} recent spikes")
        
        shutdown_database()
        
    except Exception as e:
        print(f"✗ Database test failed: {e}")
