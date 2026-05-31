CREATE TABLE IF NOT EXISTS files (
    row_id INTEGER PRIMARY KEY,
    id BLOB NOT NULL UNIQUE,
    size INTEGER NOT NULL,
    simhash INTEGER,
    compression_mode INTEGER NOT NULL,
    compression_base_id INTEGER,
    compressed_data BLOB NOT NULL,
    __last_written_ms INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
);
CREATE TABLE IF NOT EXISTS symlinks (
    row_id INTEGER PRIMARY KEY,
    id BLOB NOT NULL UNIQUE,
    target TEXT NOT NULL,
    __last_written_ms INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
);
CREATE TABLE IF NOT EXISTS trees (
    row_id INTEGER PRIMARY KEY,
    id BLOB NOT NULL UNIQUE,
    entries BLOB NOT NULL,
    __last_written_ms INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
);
CREATE TABLE IF NOT EXISTS commits (
    row_id INTEGER PRIMARY KEY,
    id BLOB NOT NULL UNIQUE,
    parents BLOB NOT NULL,
    predecessors BLOB NOT NULL,
    root_trees BLOB NOT NULL,
    conflict_labels BLOB NOT NULL,
    change_id BLOB NOT NULL,
    description TEXT NOT NULL,
    author_name TEXT NOT NULL,
    author_email TEXT NOT NULL,
    author_timestamp INTEGER NOT NULL,
    author_tz_offset INTEGER NOT NULL,
    committer_name TEXT NOT NULL,
    committer_email TEXT NOT NULL,
    committer_timestamp INTEGER NOT NULL,
    committer_tz_offset INTEGER NOT NULL,
    secure_sig_data BLOB,
    secure_sig_sig BLOB,
    __last_written_ms INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
);
CREATE INDEX IF NOT EXISTS idx_commits_change_id ON commits (change_id);
CREATE TABLE IF NOT EXISTS copies (
    row_id INTEGER PRIMARY KEY,
    id BLOB NOT NULL UNIQUE,
    generation INTEGER NOT NULL,
    current_path TEXT NOT NULL,
    parents BLOB NOT NULL,
    salt BLOB NOT NULL,
    __last_written_ms INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
);
