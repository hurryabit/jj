use std::fs;

use pollster::FutureExt as _;

use crate::error::SqlBackendError;


/// Repository statistics gathered from the SQL store.
#[derive(Debug, Clone)]
pub struct Stats {
    /// Number of commit objects.
    pub commits: i64,
    /// Number of tree objects.
    pub trees: i64,
    /// Number of file (blob) objects.
    pub blobs: i64,
    /// Total zstd-compressed size of all file objects, in bytes.
    pub blob_compressed_bytes: i64,
    /// Total uncompressed size of all file objects, in bytes.
    pub blob_uncompressed_bytes: i64,
    /// Total size of the database files on disk (main + WAL + SHM), in bytes.
    pub db_size_bytes: u64,
}

/// Per-table storage statistics from the SQLite `dbstat` virtual table.
#[derive(Debug, Clone)]
pub struct DbTableStats {
    /// Table or index name.
    pub name: String,
    /// Total bytes of user payload stored in this table/index.
    pub payload_bytes: i64,
    /// Number of rows, counted from leaf pages only.
    pub rows: i64,
    /// Total number of B-tree cells across all pages (leaf + interior).
    pub cells: i64,
}

impl super::SqlBackend {
    pub fn stats(&self) -> Result<Stats, SqlBackendError> {
        let conn = self.db.lock().block_on();
        let commits = conn.query_row("SELECT COUNT(*) FROM commits", (), |r| r.get(0))?;
        let trees = conn.query_row("SELECT COUNT(*) FROM trees", (), |r| r.get(0))?;
        #[rustfmt::skip]
        let (blobs, blob_compressed_bytes, blob_uncompressed_bytes) = conn.query_row(
            "SELECT COUNT(*), \
                    COALESCE(SUM(LENGTH(content)), 0), \
                    COALESCE(SUM(uncompressed_size), 0) \
             FROM files",
            (),
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        let db_size_bytes = ["backend.db3", "backend.db3-wal", "backend.db3-shm"]
            .iter()
            .filter_map(|name| fs::metadata(self.path.join(name)).ok())
            .map(|m| m.len())
            .sum();
        Ok(Stats {
            commits,
            trees,
            blobs,
            blob_compressed_bytes,
            blob_uncompressed_bytes,
            db_size_bytes,
        })
    }

    pub fn db_stats(&self) -> Result<Vec<DbTableStats>, SqlBackendError> {
        let conn = self.db.lock().block_on();
        let mut stmt = conn.prepare(
            "SELECT name, \
                    SUM(payload) AS payload_bytes, \
                    SUM(CASE WHEN pagetype = 'leaf' THEN ncell ELSE 0 END) AS rows, \
                    SUM(ncell) AS cells \
             FROM dbstat \
             GROUP BY name \
             ORDER BY payload_bytes DESC",
        )?;
        let rows = stmt
            .query_map((), |r| {
                Ok(DbTableStats {
                    name: r.get(0)?,
                    payload_bytes: r.get(1)?,
                    rows: r.get(2)?,
                    cells: r.get(3)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }
}
