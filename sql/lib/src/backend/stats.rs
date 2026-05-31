use std::fs;

use jj_sql_macro::sql;
use sqlx::Row as _;
use sqlx::sqlite::SqliteRow;

use crate::error::SqlBackendResult;

/// Repository statistics gathered from the SQL store.
#[derive(Debug, Clone)]
pub struct FilesStats {
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
#[derive(Debug, Clone, sqlx::FromRow)]
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
    pub async fn files_stats(&self) -> SqlBackendResult<FilesStats> {
        let mut conn = self.conn().await?;
        let commits = sqlx::query_scalar!("SELECT COUNT(*) FROM commits")
            .fetch_one(&mut *conn)
            .await?;
        let trees = sqlx::query_scalar!("SELECT COUNT(*) FROM trees")
            .fetch_one(&mut *conn)
            .await?;
        let (blobs, blob_compressed_bytes, blob_uncompressed_bytes) = sqlx::query(sql!(
            "
            SELECT
                COUNT(*),
                COALESCE(SUM(LENGTH(compressed_data)), 0),
                COALESCE(SUM(size), 0)
            FROM files
            "
        ))
        .try_map(|row: SqliteRow| Ok((row.try_get(0)?, row.try_get(1)?, row.try_get(2)?)))
        .fetch_one(&mut *conn)
        .await?;
        let db_size_bytes = ["backend.db3", "backend.db3-wal", "backend.db3-shm"]
            .iter()
            .filter_map(|name| fs::metadata(self.path.join(name)).ok())
            .map(|m| m.len())
            .sum();
        Ok(FilesStats {
            commits,
            trees,
            blobs,
            blob_compressed_bytes,
            blob_uncompressed_bytes,
            db_size_bytes,
        })
    }

    pub async fn db_table_stats(&self) -> SqlBackendResult<Vec<DbTableStats>> {
        let mut conn = self.conn().await?;
        let rows = sqlx::query_as(sql!(
            "
            SELECT
                name,
                SUM(payload) AS payload_bytes,
                SUM(CASE WHEN pagetype = 'leaf' THEN ncell ELSE 0 END) AS rows,\
                SUM(ncell) AS cells
            FROM dbstat
            GROUP BY name
            ORDER BY payload_bytes DESC
            "
        ))
        .fetch_all(&mut *conn)
        .await?;
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use crate::SqlBackend;

    #[tokio::test]
    async fn test_stats_dont_crash() -> anyhow::Result<()> {
        let backend = SqlBackend::init_in_memory().await?;
        backend.files_stats().await?;
        backend.db_table_stats().await?;
        Ok(())
    }
}
