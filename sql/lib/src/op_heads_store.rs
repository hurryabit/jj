use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use jj_lib::op_heads_store::OpHeadsStore;
use jj_lib::op_heads_store::OpHeadsStoreError;
use jj_lib::op_heads_store::OpHeadsStoreLock;
use jj_lib::op_store as jj;
use jj_sql_macro::sql;
use sqlx::Connection;
use sqlx::Row as _;
use sqlx::Sqlite;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::sqlite::SqliteJournalMode;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::sqlite::SqliteRow;
use sqlx::sqlite::SqliteSynchronous;

use crate::convert::JjExt as _;
use crate::convert::ModelExt as _;
use crate::error::SqlBackendError;
use crate::error::SqlBackendResult;
use crate::hash::Hash;
use crate::op_store::model::OperationId;

#[derive(Debug)]
pub struct SqlOpHeadsStore {
    path: PathBuf,
    pool: sqlx::Pool<Sqlite>,
}

impl SqlOpHeadsStore {
    pub fn name() -> &'static str {
        "sql"
    }

    pub fn store_path(&self) -> &Path {
        &self.path
    }

    fn connect_options() -> SqliteConnectOptions {
        SqliteConnectOptions::new()
            .busy_timeout(Duration::from_millis(5000))
            .pragma("encoding", "'UTF-8'")
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
    }

    fn pool_options() -> SqlitePoolOptions {
        SqlitePoolOptions::new().max_connections(1)
    }

    async fn connect(
        store_path: &Path,
        connect_options: SqliteConnectOptions,
        pool_options: SqlitePoolOptions,
    ) -> SqlBackendResult<Self> {
        let pool = pool_options
            .connect_with(connect_options.filename(store_path.join("op_heads.db3")))
            .await?;
        Ok(Self {
            path: store_path.to_path_buf(),
            pool,
        })
    }

    async fn initialize(&self, root_op_id: &jj::OperationId) -> Result<(), SqlBackendError> {
        {
            // NOTE: We need to return the connection to the pool before the writes below.
            let mut conn = self.pool.acquire().await?;
            sqlx::raw_sql(include_str!("../sql/op_heads.sql"))
                .execute(&mut *conn)
                .await?;
        }
        self.update_op_heads(&[], root_op_id).await
    }

    pub async fn init_in_memory() -> SqlBackendResult<Self> {
        let backend = Self::connect(
            Path::new(":memory:"),
            Self::connect_options().in_memory(true),
            Self::pool_options(),
        )
        .await?;
        let root_op_id = OperationId(Hash([0xf1; _])).into_jj();
        backend.initialize(&root_op_id).await?;
        Ok(backend)
    }

    pub async fn init(store_path: &Path, root_op_id: &jj::OperationId) -> SqlBackendResult<Self> {
        let backend = Self::connect(
            store_path,
            Self::connect_options().create_if_missing(true),
            Self::pool_options(),
        )
        .await?;
        backend.initialize(root_op_id).await?;
        Ok(backend)
    }

    pub async fn load(store_path: &Path) -> SqlBackendResult<Self> {
        let backend = Self::connect(
            store_path,
            Self::connect_options().create_if_missing(false),
            Self::pool_options(),
        )
        .await?;
        Ok(backend)
    }

    async fn get_op_heads(&self) -> SqlBackendResult<Vec<jj::OperationId>> {
        let mut conn = self.pool.acquire().await?;
        let ids = sqlx::query(sql!("SELECT id FROM op_heads"))
            .try_map(|row: SqliteRow| Ok(row.try_get::<OperationId, _>(0)?.into_jj()))
            .fetch_all(&mut *conn)
            .await?;
        Ok(ids)
    }

    async fn update_op_heads(
        &self,
        old_ids: &[jj::OperationId],
        new_id: &jj::OperationId,
    ) -> SqlBackendResult<()> {
        assert!(!old_ids.contains(new_id));
        let mut conn = self.pool.acquire().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query!(
            "INSERT OR IGNORE INTO op_heads VALUES (?1)",
            new_id.to_model()?
        )
        .execute(&mut *tx)
        .await?;
        for old_id in old_ids {
            sqlx::query!("DELETE FROM op_heads WHERE id = ?1", old_id.to_model()?)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

/// A no-op lock.  SQLite's `BEGIN IMMEDIATE` in `update_op_heads` already
/// provides atomicity; the advisory lock is best-effort (jj does not require
/// it for correctness).
struct NoOpLock;
impl OpHeadsStoreLock for NoOpLock {}

#[async_trait]
impl OpHeadsStore for SqlOpHeadsStore {
    fn name(&self) -> &str {
        Self::name()
    }

    async fn get_op_heads(&self) -> Result<Vec<jj::OperationId>, OpHeadsStoreError> {
        self.get_op_heads()
            .await
            .map_err(|e| OpHeadsStoreError::Read(e.into()))
    }

    async fn update_op_heads(
        &self,
        old_ids: &[jj::OperationId],
        new_id: &jj::OperationId,
    ) -> Result<(), OpHeadsStoreError> {
        assert!(!old_ids.is_empty());
        self.update_op_heads(old_ids, new_id)
            .await
            .map_err(|e| OpHeadsStoreError::Read(e.into()))
    }

    async fn lock(&self) -> Result<Box<dyn OpHeadsStoreLock + '_>, OpHeadsStoreError> {
        Ok(Box::new(NoOpLock))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::cloned_ref_to_slice_refs)]
    use jj_lib::op_heads_store::OpHeadsStore as _;
    use jj_lib::op_store::OperationId;

    use super::*;

    /// Pads `prefix` with trailing zeros to produce a full-length
    /// `OperationId`.
    fn op_id(prefix: &str) -> OperationId {
        use crate::op_store::model::OPERATION_ID_LENGTH;
        let hex = format!("{:0<width$}", prefix, width = OPERATION_ID_LENGTH * 2);
        OperationId::try_from_hex(&hex).unwrap()
    }

    async fn make_store() -> (SqlOpHeadsStore, tempfile::TempDir, OperationId) {
        let dir = tempfile::tempdir().unwrap();
        let root_op_id = op_id("0000");
        let store = SqlOpHeadsStore::init(dir.path(), &root_op_id)
            .await
            .unwrap();
        (store, dir, root_op_id)
    }

    #[tokio::test]
    async fn test_init_only_root() {
        let (store, _dir, root_id) = make_store().await;
        let heads = store.get_op_heads().await.unwrap();
        assert_eq!(heads, vec![root_id]);
    }

    #[tokio::test]
    async fn test_advance_from_root() {
        let (store, _dir, root_id) = make_store().await;
        let head_id = op_id("aaaa");
        store.update_op_heads(&[root_id], &head_id).await.unwrap();
        let heads = store.get_op_heads().await.unwrap();
        assert_eq!(heads, vec![head_id]);
    }

    #[tokio::test]
    async fn test_advance_from_normal() {
        let (store, _dir, root_id) = make_store().await;
        let base_id = op_id("aaaa");
        let head_id = op_id("bbbb");
        store.update_op_heads(&[root_id], &base_id).await.unwrap();
        store.update_op_heads(&[base_id], &head_id).await.unwrap();
        let heads = store.get_op_heads().await.unwrap();
        assert_eq!(heads, vec![head_id]);
    }

    #[tokio::test]
    async fn test_branch_from_root() {
        let (store, _dir, root_id) = make_store().await;
        let left_id = op_id("aaaa");
        let right_id = op_id("bbbb");
        // Simulate two concurrent ops both starting from root_id.
        store
            .update_op_heads(&[root_id.clone()], &left_id)
            .await
            .unwrap();
        store.update_op_heads(&[root_id], &right_id).await.unwrap();
        let heads = store.get_op_heads().await.unwrap();
        assert_eq!(heads, vec![left_id, right_id]);
    }

    #[tokio::test]
    async fn test_branch_from_normal() {
        let (store, _dir, root_id) = make_store().await;
        let base_id = op_id("aaaa");
        let left_id = op_id("bbbb");
        let right_id = op_id("cccc");
        store.update_op_heads(&[root_id], &base_id).await.unwrap();

        // Simulate two concurrent ops both starting from root_op_id.
        store
            .update_op_heads(&[base_id.clone().clone()], &left_id)
            .await
            .unwrap();
        store.update_op_heads(&[base_id], &right_id).await.unwrap();
        let heads = store.get_op_heads().await.unwrap();
        assert_eq!(heads, vec![left_id, right_id]);
    }

    #[tokio::test]
    async fn test_merge_two_heads() {
        let (store, _dir, root_id) = make_store().await;
        let left_id = op_id("aaaa");
        let right_id = op_id("bbbb");
        let merge_id = op_id("cccc");

        // Simulate two concurrent ops both starting from root_id.
        store
            .update_op_heads(&[root_id.clone()], &left_id)
            .await
            .unwrap();
        store.update_op_heads(&[root_id], &right_id).await.unwrap();
        // Merge left_id and right_id.
        store
            .update_op_heads(&[left_id, right_id], &merge_id)
            .await
            .unwrap();
        let heads = store.get_op_heads().await.unwrap();
        assert_eq!(heads, vec![merge_id]);
    }

    #[tokio::test]
    async fn test_idempotent_insert() {
        let (store, _dir, root_id) = make_store().await;
        let id = op_id("aabb");
        store
            .update_op_heads(&[root_id.clone()], &id)
            .await
            .unwrap();
        store.update_op_heads(&[root_id], &id).await.unwrap();
        let heads = store.get_op_heads().await.unwrap();
        assert_eq!(heads, vec![id]);
    }

    #[tokio::test]
    async fn test_lock_returns_without_error() {
        let (store, _dir, _root_id) = make_store().await;
        let _lock = store.lock().await.unwrap();
    }

    #[tokio::test]
    async fn test_persist_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let root_id = op_id("0000");
        let id = op_id("ccddee");
        {
            let store = SqlOpHeadsStore::init(dir.path(), &root_id).await.unwrap();
            store.update_op_heads(&[root_id], &id).await.unwrap();
        }
        let store2 = SqlOpHeadsStore::load(dir.path()).await.unwrap();
        let heads = store2.get_op_heads().await.unwrap();
        assert_eq!(heads, vec![id]);
    }
}
