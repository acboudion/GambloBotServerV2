-- Market-data database. Deliberately separate from the news database: quote
-- bursts (thousands of rows/sec) must never contend with news/alert writes,
-- and tick retention churns far more than 14-day news retention.
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA auto_vacuum=INCREMENTAL;

-- Timestamps: ts_us = epoch microseconds (integer keys keep the indices about
-- half the size of ISO strings — this matters at millions of quote rows).
-- Bars use ts = epoch seconds of the bar start.

CREATE TABLE IF NOT EXISTS stock_trades (
    symbol TEXT NOT NULL,
    ts_us INTEGER NOT NULL,
    price REAL NOT NULL,
    size INTEGER,
    exchange TEXT,
    conditions TEXT,
    tape TEXT,
    trade_id INTEGER
);
CREATE INDEX IF NOT EXISTS idx_trades_symbol_ts ON stock_trades(symbol, ts_us);
CREATE INDEX IF NOT EXISTS idx_trades_ts ON stock_trades(ts_us);

CREATE TABLE IF NOT EXISTS stock_quotes (
    symbol TEXT NOT NULL,
    ts_us INTEGER NOT NULL,
    bid_price REAL,
    bid_size INTEGER,
    bid_exchange TEXT,
    ask_price REAL,
    ask_size INTEGER,
    ask_exchange TEXT,
    conditions TEXT,
    tape TEXT
);
CREATE INDEX IF NOT EXISTS idx_quotes_symbol_ts ON stock_quotes(symbol, ts_us);
CREATE INDEX IF NOT EXISTS idx_quotes_ts ON stock_quotes(ts_us);

CREATE TABLE IF NOT EXISTS stock_bars (
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,          -- '1min' | '1day'
    ts INTEGER NOT NULL,              -- bar start, epoch seconds
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume INTEGER,
    trade_count INTEGER,
    vwap REAL,
    updated_at TEXT NOT NULL,
    seq INTEGER,
    PRIMARY KEY (symbol, timeframe, ts)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_bars_seq ON stock_bars(seq);
CREATE INDEX IF NOT EXISTS idx_bars_ts ON stock_bars(ts);

CREATE TABLE IF NOT EXISTS stock_statuses (
    symbol TEXT NOT NULL,
    ts_us INTEGER NOT NULL,
    status_code TEXT,
    status_msg TEXT,
    reason_code TEXT,
    reason_msg TEXT,
    tape TEXT
);
CREATE INDEX IF NOT EXISTS idx_statuses_symbol_ts ON stock_statuses(symbol, ts_us);
CREATE INDEX IF NOT EXISTS idx_statuses_ts ON stock_statuses(ts_us);

CREATE TABLE IF NOT EXISTS stock_lulds (
    symbol TEXT NOT NULL,
    ts_us INTEGER NOT NULL,
    limit_up REAL,
    limit_down REAL,
    indicator TEXT,
    tape TEXT
);
CREATE INDEX IF NOT EXISTS idx_lulds_symbol_ts ON stock_lulds(symbol, ts_us);
CREATE INDEX IF NOT EXISTS idx_lulds_ts ON stock_lulds(ts_us);

-- Corrections, cancelErrors, malformed frames, unknown message types.
CREATE TABLE IF NOT EXISTS market_raw_events (
    received_at TEXT NOT NULL,
    message_type TEXT,
    raw_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_market_raw_received ON market_raw_events(received_at);
