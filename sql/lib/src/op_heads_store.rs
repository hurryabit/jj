use std::fmt;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use balsaq::ConnectionExt as _;
use futures::lock::Mutex;
use jj_lib::op_heads_store::OpHeadsStore;
use jj_lib::op_heads_store::OpHeadsStoreError;
use jj_lib::op_heads_store::OpHeadsStoreLock;
use jj_lib::op_store::OperationId;
use rusqlite::Connection;

use crate::convert::JjExt as _;
use crate::convert::ModelExt as _;
use crate::error::SqlBackendError;

#[balsaq::schema]
mod model {
    use balsaq::ConnectionExt as _;
    use balsaq::Model as _;
    use rusqlite::Connection;

    #[balsaq::table("op_heads")]
    pub struct OpHead {
        #[primary_key]
        pub id: crate::op_store::model::OperationId,
    }

    impl OpHead {
        pub fn get_all(conn: &Connection) -> rusqlite::Result<Vec<Self>> {
            conn.get_all(Self::SELECT, ())
        }
    }
}

pub struct SqlOpHeadsStore {
    #[allow(dead_code)]
    path: PathBuf,
    db: Mutex<Connection>,
}

impl fmt::Debug for SqlOpHeadsStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlOpHeadsStore")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl SqlOpHeadsStore {
    pub fn name() -> &'static str {
        "sql"
    }

    fn connect(store_path: &Path) -> Result<Connection, SqlBackendError> {
        let conn = Connection::open(store_path.join("op_heads.db3"))?;
        conn.busy_timeout(Duration::from_millis(5000))?;
        conn.pragma_update(None, "encoding", "UTF-8")?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        // The entire op-heads DB fits in a single page; only synchronous matters.
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        Ok(conn)
    }

    fn from_parts(store_path: &Path, conn: Connection) -> Self {
        Self {
            path: store_path.to_path_buf(),
            db: Mutex::new(conn),
        }
    }

    pub fn init(store_path: &Path, root_op_id: &OperationId) -> Result<Self, SqlBackendError> {
        let conn = Self::connect(store_path)?;
        conn.execute_batch(model::SCHEMA)?;
        conn.insert(model::OpHead {
            id: root_op_id.to_model()?,
        })?;
        Ok(Self::from_parts(store_path, conn))
    }

    pub fn load(store_path: &Path) -> Result<Self, SqlBackendError> {
        let conn = Self::connect(store_path)?;
        Ok(Self::from_parts(store_path, conn))
    }

    async fn write_conn<T>(
        &self,
        new_op_id: &OperationId,
        f: impl AsyncFnOnce(&mut Connection) -> Result<T, SqlBackendError>,
    ) -> Result<T, OpHeadsStoreError> {
        let mut conn = self.db.lock().await;
        let res = f(&mut conn).await.map_err(|e| OpHeadsStoreError::Write {
            new_op_id: new_op_id.clone(),
            source: e.into(),
        })?;
        Ok(res)
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

    async fn get_op_heads(&self) -> Result<Vec<OperationId>, OpHeadsStoreError> {
        let conn = self.db.lock().await;
        let rows = model::OpHead::get_all(&conn).map_err(|e| OpHeadsStoreError::Read(e.into()))?;
        Ok(rows.into_iter().map(|r| r.id.into_jj()).collect())
    }

    async fn update_op_heads(
        &self,
        old_ids: &[OperationId],
        new_id: &OperationId,
    ) -> Result<(), OpHeadsStoreError> {
        assert!(!old_ids.is_empty());
        assert!(!old_ids.contains(new_id));
        self.write_conn(new_id, async |conn| {
            // Use IMMEDIATE to acquire the write lock upfront and avoid TOCTOU
            // between reading and writing the heads pointer.
            let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            tx.insert(model::OpHead {
                id: new_id.to_model()?,
            })?;
            {
                let mut stmt = tx.prepare_cached("DELETE FROM op_heads WHERE id = ?1")?;
                for old_id in old_ids {
                    stmt.execute((old_id.to_model()?,))?;
                }
            }
            tx.commit()?;
            Ok(())
        })
        .await
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
    use pollster::FutureExt as _;

    use super::*;

    /// Pads `prefix` with trailing zeros to produce a full-length
    /// `OperationId`.
    fn op_id(prefix: &str) -> OperationId {
        use crate::op_store::model::OPERATION_ID_LENGTH;
        let hex = format!("{:0<width$}", prefix, width = OPERATION_ID_LENGTH * 2);
        OperationId::try_from_hex(&hex).unwrap()
    }

    fn make_store() -> (SqlOpHeadsStore, tempfile::TempDir, OperationId) {
        let dir = tempfile::tempdir().unwrap();
        let root_op_id = op_id("0000");
        let store = SqlOpHeadsStore::init(dir.path(), &root_op_id).unwrap();
        (store, dir, root_op_id)
    }

    #[test]
    fn test_init_only_root() {
        let (store, _dir, root_id) = make_store();
        let heads = store.get_op_heads().block_on().unwrap();
        assert_eq!(heads, vec![root_id]);
    }

    #[test]
    fn test_advance_from_root() {
        let (store, _dir, root_id) = make_store();
        let head_id = op_id("aaaa");
        store
            .update_op_heads(&[root_id], &head_id)
            .block_on()
            .unwrap();
        let heads = store.get_op_heads().block_on().unwrap();
        assert_eq!(heads, vec![head_id]);
    }

    #[test]
    fn test_advance_from_normal() {
        let (store, _dir, root_id) = make_store();
        let base_id = op_id("aaaa");
        let head_id = op_id("bbbb");
        store
            .update_op_heads(&[root_id], &base_id)
            .block_on()
            .unwrap();
        store
            .update_op_heads(&[base_id], &head_id)
            .block_on()
            .unwrap();
        let heads = store.get_op_heads().block_on().unwrap();
        assert_eq!(heads, vec![head_id]);
    }

    #[test]
    fn test_branch_from_root() {
        let (store, _dir, root_id) = make_store();
        let left_id = op_id("aaaa");
        let right_id = op_id("bbbb");
        // Simulate two concurrent ops both starting from root_id.
        store
            .update_op_heads(&[root_id.clone()], &left_id)
            .block_on()
            .unwrap();
        store
            .update_op_heads(&[root_id], &right_id)
            .block_on()
            .unwrap();
        let heads = store.get_op_heads().block_on().unwrap();
        assert_eq!(heads, vec![left_id, right_id]);
    }

    #[test]
    fn test_branch_from_normal() {
        let (store, _dir, root_id) = make_store();
        let base_id = op_id("aaaa");
        let left_id = op_id("bbbb");
        let right_id = op_id("cccc");
        store
            .update_op_heads(&[root_id], &base_id)
            .block_on()
            .unwrap();

        // Simulate two concurrent ops both starting from root_op_id.
        store
            .update_op_heads(&[base_id.clone().clone()], &left_id)
            .block_on()
            .unwrap();
        store
            .update_op_heads(&[base_id], &right_id)
            .block_on()
            .unwrap();
        let heads = store.get_op_heads().block_on().unwrap();
        assert_eq!(heads, vec![left_id, right_id]);
    }

    #[test]
    fn test_merge_two_heads() {
        let (store, _dir, root_id) = make_store();
        let left_id = op_id("aaaa");
        let right_id = op_id("bbbb");
        let merge_id = op_id("cccc");

        // Simulate two concurrent ops both starting from root_id.
        store
            .update_op_heads(&[root_id.clone()], &left_id)
            .block_on()
            .unwrap();
        store
            .update_op_heads(&[root_id], &right_id)
            .block_on()
            .unwrap();
        // Merge left_id and right_id.
        store
            .update_op_heads(&[left_id, right_id], &merge_id)
            .block_on()
            .unwrap();
        let heads = store.get_op_heads().block_on().unwrap();
        assert_eq!(heads, vec![merge_id]);
    }

    #[test]
    fn test_idempotent_insert() {
        let (store, _dir, root_id) = make_store();
        let id = op_id("aabb");
        store
            .update_op_heads(&[root_id.clone()], &id)
            .block_on()
            .unwrap();
        store.update_op_heads(&[root_id], &id).block_on().unwrap();
        let heads = store.get_op_heads().block_on().unwrap();
        assert_eq!(heads, vec![id]);
    }

    #[test]
    fn test_lock_returns_without_error() {
        let (store, _dir, _root_id) = make_store();
        let _lock = store.lock().block_on().unwrap();
    }

    #[test]
    fn test_persist_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let root_id = op_id("0000");
        let id = op_id("ccddee");
        {
            let store = SqlOpHeadsStore::init(dir.path(), &root_id).unwrap();
            store.update_op_heads(&[root_id], &id).block_on().unwrap();
        }
        let store2 = SqlOpHeadsStore::load(dir.path()).unwrap();
        let heads = store2.get_op_heads().block_on().unwrap();
        assert_eq!(heads, vec![id]);
    }
}
