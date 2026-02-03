#!/usr/bin/env python3
"""
PostgreSQL Database Module for Kalshi Trading Bot
Updated to match requested schema naming conventions.
"""

import psycopg2
from psycopg2.extras import execute_batch
from psycopg2.pool import SimpleConnectionPool
from typing import List, Dict, Optional
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

BATCH_SIZE = 50
BATCH_TIMEOUT = 5

# ===== CONNECTION POOL =====

class DatabasePool:
    def __init__(self, minconn=2, maxconn=10):
        self.pool = SimpleConnectionPool(minconn, maxconn, **DB_CONFIG)
    
    @contextmanager
    def get_connection(self):
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
        self.pool.closeall()

db_pool = None

def init_db_pool():
    global db_pool
    if db_pool is None:
        db_pool = DatabasePool()
    return db_pool

# ===== BATCH INSERT MANAGER =====

class BatchInsertManager:
    def __init__(self, batch_size=BATCH_SIZE, timeout=BATCH_TIMEOUT):
        self.batch_size = batch_size
        self.timeout = timeout
        self.history_queue = deque()
        self.spike_queue = deque()
        self.last_flush = time.time()
    
    def add_history(self, ticker: str, price: float, quantity: int, kalshi_ts: int):
        self.history_queue.append({
            'ticker': ticker,
            'price': price,
            'quantity': quantity,
            'notional_value': price * quantity,
            'kalshi_ts': kalshi_ts
        })
        self._check_flush()
    
    def add_spike(self, ticker: str, start_price: float, end_price: float, 
                  pct_change: float, volume: int):
        self.spike_queue.append({
            'ticker': ticker,
            'start_price': start_price,
            'end_price': end_price,
            'pct_change': pct_change,
            'total_spike_volume_usd': end_price * volume
        })
        self._check_flush()
    
    def _check_flush(self):
        if (len(self.history_queue) + len(self.spike_queue)) >= self.batch_size or \
           (time.time() - self.last_flush) >= self.timeout:
            self.flush()
    
    def flush(self):
        if not db_pool: return
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            if self.history_queue:
                data = [(h['ticker'], h['price'], h['quantity'], h['notional_value'], h['kalshi_ts']) 
                        for h in self.history_queue]
                execute_batch(cursor, """
                    INSERT INTO market_history (ticker, price, quantity, notional_value, kalshi_ts)
                    VALUES (%s, %s, %s, %s, %s)
                """, data)
                self.history_queue.clear()
            if self.spike_queue:
                data = [(s['ticker'], s['start_price'], s['end_price'], s['pct_change'], s['total_spike_volume_usd']) 
                        for s in self.spike_queue]
                execute_batch(cursor, """
                    INSERT INTO spike_logs (ticker, start_price, end_price, pct_change, total_spike_volume_usd)
                    VALUES (%s, %s, %s, %s, %s)
                """, data)
                self.spike_queue.clear()
        self.last_flush = time.time()

batch_manager = None

def init_batch_manager():
    global batch_manager
    if batch_manager is None:
        batch_manager = BatchInsertManager()
    return batch_manager

# ===== HELPERS =====

def upsert_metadata(ticker: str, event_ticker: str, close_time: datetime, title: Optional[str] = None, exp_time: Optional[datetime] = None):
    with db_pool.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO market_metadata (ticker, event_ticker, title, close_time, expiration_time)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ticker) DO UPDATE SET 
                event_ticker = EXCLUDED.event_ticker,
                title = COALESCE(EXCLUDED.title, market_metadata.title),
                close_time = EXCLUDED.close_time
        """, (ticker, event_ticker, title, close_time, exp_time))

def get_ttl_minutes(ticker: str) -> Optional[int]:
    """Calculate the 'Tick-Tock Factor'."""
    with db_pool.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT EXTRACT(EPOCH FROM (close_time - NOW())) / 60 FROM market_metadata WHERE ticker = %s", (ticker,))
        res = cursor.fetchone()
        return int(res[0]) if res and res[0] else None

def initialize_database():
    init_db_pool()
    init_batch_manager()

def shutdown_database():
    if batch_manager: batch_manager.flush()
    if db_pool: db_pool.close_all()
