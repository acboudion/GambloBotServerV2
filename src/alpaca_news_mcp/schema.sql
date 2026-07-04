PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS news_articles (
    id INTEGER PRIMARY KEY,
    headline TEXT NOT NULL,
    summary TEXT,
    author TEXT,
    created_at TEXT,
    updated_at TEXT,
    content_html TEXT,
    content_text TEXT,
    url TEXT,
    source TEXT,
    symbols_json TEXT NOT NULL DEFAULT '[]',
    raw_json TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    last_seen_source TEXT NOT NULL,
    update_count INTEGER NOT NULL DEFAULT 0,
    latency_ms INTEGER,
    is_content_present INTEGER NOT NULL DEFAULT 0,
    seq INTEGER
);
-- NOTE: idx_articles_seq is created by migration m1 (migrations.py), which also
-- backfills seq for databases created before the column existed. schema.sql runs
-- before migrations, so the index cannot live here without breaking old DBs.

-- External-content FTS index over article text. Synced explicitly in
-- Store.upsert_article / prune_retention; rebuilt for old DBs by migration m2.
CREATE VIRTUAL TABLE IF NOT EXISTS news_fts USING fts5(
    headline, summary, content_text,
    content='news_articles', content_rowid='id',
    tokenize='porter unicode61'
);

CREATE INDEX IF NOT EXISTS idx_articles_created_at ON news_articles(created_at);
CREATE INDEX IF NOT EXISTS idx_articles_updated_at ON news_articles(updated_at);
CREATE INDEX IF NOT EXISTS idx_articles_first_seen_at ON news_articles(first_seen_at);
CREATE INDEX IF NOT EXISTS idx_articles_source ON news_articles(source);

CREATE TABLE IF NOT EXISTS news_article_versions (
    version_id TEXT PRIMARY KEY,
    article_id INTEGER NOT NULL,
    updated_at TEXT,
    received_at TEXT NOT NULL,
    source TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    headline TEXT,
    summary TEXT,
    content_hash TEXT,
    FOREIGN KEY(article_id) REFERENCES news_articles(id)
);

CREATE INDEX IF NOT EXISTS idx_versions_article_id ON news_article_versions(article_id);
CREATE INDEX IF NOT EXISTS idx_versions_received_at ON news_article_versions(received_at);

CREATE TABLE IF NOT EXISTS news_symbol_index (
    symbol TEXT NOT NULL,
    article_id INTEGER NOT NULL,
    created_at TEXT,
    updated_at TEXT,
    received_at TEXT NOT NULL,
    source TEXT,
    PRIMARY KEY(symbol, article_id),
    FOREIGN KEY(article_id) REFERENCES news_articles(id)
);

CREATE INDEX IF NOT EXISTS idx_symbol_index_received_at ON news_symbol_index(received_at);
CREATE INDEX IF NOT EXISTS idx_symbol_index_symbol_received ON news_symbol_index(symbol, received_at);

CREATE TABLE IF NOT EXISTS raw_events (
    event_id TEXT PRIMARY KEY,
    received_at TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    message_type TEXT,
    raw_json TEXT NOT NULL,
    replayed INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_raw_events_received_at ON raw_events(received_at);
CREATE INDEX IF NOT EXISTS idx_raw_events_endpoint ON raw_events(endpoint);

CREATE TABLE IF NOT EXISTS stream_status (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS alerts (
    alert_id TEXT PRIMARY KEY,
    article_id INTEGER,
    created_at TEXT NOT NULL,
    severity TEXT NOT NULL,
    category TEXT NOT NULL,
    symbols_json TEXT NOT NULL DEFAULT '[]',
    headline TEXT,
    reason TEXT NOT NULL,
    acknowledged INTEGER NOT NULL DEFAULT 0,
    raw_json TEXT NOT NULL,
    content_hash TEXT,
    direction TEXT NOT NULL DEFAULT 'neutral',
    -- Monotonic delta-poll cursor. Like news_articles.seq, the unique index
    -- is created by its migration (m5), which also backfills old rows.
    seq INTEGER,
    FOREIGN KEY(article_id) REFERENCES news_articles(id)
);

CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts(created_at);
CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts(severity);
CREATE INDEX IF NOT EXISTS idx_alerts_category ON alerts(category);
CREATE INDEX IF NOT EXISTS idx_alerts_article_id ON alerts(article_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_alerts_article_category ON alerts(article_id, category) WHERE article_id IS NOT NULL;
