-- Calendar - SQLite schema
-- Migration 002: Calendar events table and indexes

CREATE TABLE IF NOT EXISTS calendar_events (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    date INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    stock_footage TEXT DEFAULT '[]',
    initial_clips TEXT DEFAULT '[]',
    intermediate_clips TEXT DEFAULT '[]',
    final_clips TEXT DEFAULT '[]',
    created_at TEXT,
    updated_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_calendar_events_date ON calendar_events(year, month, date);
CREATE INDEX IF NOT EXISTS idx_calendar_events_title ON calendar_events(title);
