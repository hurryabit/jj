CREATE TABLE IF NOT EXISTS views (
    row_id INTEGER PRIMARY KEY,
    id BLOB NOT NULL UNIQUE,
    data BLOB NOT NULL,
    __last_written_ms INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
);
CREATE TABLE IF NOT EXISTS operations (
    row_id INTEGER PRIMARY KEY,
    id BLOB NOT NULL UNIQUE,
    view_id BLOB NOT NULL,
    parents BLOB NOT NULL,
    metadata BLOB NOT NULL,
    commit_predecessors BLOB,
    __last_written_ms INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
);
