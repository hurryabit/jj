mod convert;
pub mod model;

use std::fmt::Debug;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::SystemTime;

use async_trait::async_trait;
use balsaq::ConnectionExt as _;
use balsaq::Model as _;
use futures::lock::Mutex;
use jj_lib::content_hash::blake2b_hash;
use jj_lib::object_id::HexPrefix;
use jj_lib::object_id::ObjectId;
use jj_lib::object_id::PrefixResolution;
use jj_lib::op_store::OpStore;
use jj_lib::op_store::OpStoreError;
use jj_lib::op_store::OpStoreResult;
use jj_lib::op_store::Operation;
use jj_lib::op_store::OperationId;
use jj_lib::op_store::RootOperationData;
use jj_lib::op_store::View;
use jj_lib::op_store::ViewId;
use rusqlite::Connection;
use rusqlite::OptionalExtension as _;

use crate::backend::model::CommitId as ModelCommitId;
use crate::convert::JjExt;
use crate::convert::ModelExt as _;
use crate::error::SqlBackendError;

#[derive(Debug)]
pub struct SqlOpStore {
    #[allow(dead_code)]
    path: PathBuf,
    db: Mutex<Connection>,
    root_data: RootOperationData,
    root_operation_id: OperationId,
    root_view_id: ViewId,
}

impl SqlOpStore {
    pub fn name() -> &'static str {
        "sql"
    }

    fn connect(store_path: &Path) -> Result<Connection, SqlBackendError> {
        let conn = Connection::open(store_path.join("op_store.db3"))?;
        conn.busy_timeout(Duration::from_millis(5000))?;
        conn.pragma_update(None, "encoding", "UTF-8")?;
        // page_size must precede journal_mode; ignored on existing databases.
        conn.pragma_update(None, "page_size", 8 * 1024)?; // 8 KiB
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "cache_size", -32 * 1024)?; // 32 MiB (!)
        conn.pragma_update(None, "mmap_size", 32 * 1024 * 1024)?; // 32 MiB
        conn.pragma_update(None, "temp_store", "MEMORY")?;
        Ok(conn)
    }

    fn from_parts(store_path: &Path, conn: Connection, root_data: RootOperationData) -> Self {
        Self {
            path: store_path.to_path_buf(),
            db: Mutex::new(conn),
            root_operation_id: OperationId::from_bytes(&[0; model::OPERATION_ID_LENGTH]),
            root_view_id: ViewId::from_bytes(&[0; model::VIEW_ID_LENGTH]),
            root_data,
        }
    }

    pub fn init(store_path: &Path, root_data: RootOperationData) -> Result<Self, SqlBackendError> {
        let conn = Self::connect(store_path)?;
        conn.execute_batch(model::SCHEMA)?;
        Ok(Self::from_parts(store_path, conn, root_data))
    }

    pub fn load(store_path: &Path, root_data: RootOperationData) -> Result<Self, SqlBackendError> {
        let conn = Self::connect(store_path)?;
        Ok(Self::from_parts(store_path, conn, root_data))
    }

    async fn read_object<T, Id>(
        &self,
        id: &Id,
        f: impl AsyncFnOnce(&mut Connection, &Id::Model) -> Result<T, SqlBackendError>,
    ) -> OpStoreResult<T>
    where
        Id: JjExt + ObjectId,
    {
        let mut conn = self.db.lock().await;
        let model_id = id.to_model().map_err(|e| e.with_read_context(id))?;
        let res = f(&mut conn, &model_id)
            .await
            .map_err(|e| e.with_read_context(id))?;
        Ok(res)
    }

    async fn write_object<T>(
        &self,
        object_type: &'static str,
        f: impl AsyncFnOnce(&mut Connection) -> Result<T, SqlBackendError>,
    ) -> OpStoreResult<T> {
        let mut conn = self.db.lock().await;
        let res = f(&mut conn)
            .await
            .map_err(|e| e.with_write_context(object_type))?;
        Ok(res)
    }
}

#[async_trait]
impl OpStore for SqlOpStore {
    fn name(&self) -> &str {
        Self::name()
    }

    fn root_operation_id(&self) -> &OperationId {
        &self.root_operation_id
    }

    async fn read_view(&self, id: &ViewId) -> OpStoreResult<View> {
        if *id == self.root_view_id {
            return Ok(View::make_root(self.root_data.root_commit_id.clone()));
        }

        self.read_object(id, async |conn, id| {
            let row = conn.get::<model::ViewRow>(id)?;
            let m: model::View = serde_json::from_str(&row.data)?;
            Ok(m.into_jj())
        })
        .await
    }

    async fn write_view(&self, view: &View) -> OpStoreResult<ViewId> {
        self.write_object("view", async |conn| {
            let id = model::ViewId::from(blake2b_hash(view));
            let data = serde_json::to_string(&view.to_model()?)?;
            conn.insert(model::ViewRow { id, data })?;
            Ok(id.into_jj())
        })
        .await
    }

    async fn read_operation(&self, id: &OperationId) -> OpStoreResult<Operation> {
        if *id == self.root_operation_id {
            return Ok(Operation::make_root(self.root_view_id.clone()));
        }
        self.read_object(id, async |conn, id| {
            let row = conn.get::<model::OperationRow>(id)?;
            let metadata: model::OperationMetadata = serde_json::from_str(&row.metadata)?;
            let parents = model::OperationParent::get_all_for_operation(conn, id)?
                .into_iter()
                .map(|r| r.parent_id.into_jj())
                .collect();
            let commit_predecessors = row
                .commit_predecessors
                .map(|s| {
                    serde_json::from_str::<
                        std::collections::BTreeMap<ModelCommitId, Vec<ModelCommitId>>,
                    >(&s)
                })
                .transpose()?
                .into_jj();
            Ok(Operation {
                view_id: row.view_id.into_jj(),
                parents,
                metadata: metadata.into_jj(),
                commit_predecessors,
            })
        })
        .await
    }

    async fn write_operation(&self, op: &Operation) -> OpStoreResult<OperationId> {
        self.write_object("operation", async |conn| {
            if op.parents.is_empty() {
                return Err(SqlBackendError::InternalError(String::from(
                    "cannot write an operation with no parents",
                )));
            }
            let id = model::OperationId::from(blake2b_hash(op));
            let view_id = op.view_id.to_model()?;
            let metadata = serde_json::to_string(&op.metadata.to_model()?)?;
            let commit_predecessors = op
                .commit_predecessors
                .to_model()?
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?;
            let tx = conn.transaction()?;
            tx.insert(model::OperationRow {
                id,
                view_id,
                metadata,
                commit_predecessors,
            })?;
            for (pos, parent_id) in op.parents.iter().enumerate() {
                tx.insert(model::OperationParent {
                    operation_id: id,
                    position: pos.try_into().map_err(SqlBackendError::len_too_large)?,
                    parent_id: parent_id.to_model()?,
                })?;
            }
            tx.commit()?;
            Ok(id.into_jj())
        })
        .await
    }

    async fn resolve_operation_id_prefix(
        &self,
        prefix: &HexPrefix,
    ) -> OpStoreResult<PrefixResolution<OperationId>> {
        let hex_prefix = prefix.hex();

        // Fast path: full-length prefix → single lookup.
        if hex_prefix.len() == model::OPERATION_ID_LENGTH * 2 {
            let full_id = OperationId::from_bytes(prefix.as_full_bytes().unwrap());
            let exists = if full_id == self.root_operation_id {
                true
            } else {
                let model_full_id = full_id
                    .to_model()
                    .map_err(|e| OpStoreError::Other(e.into()))?;
                let conn = self.db.lock().await;
                conn.get::<model::OperationRow>(&model_full_id)
                    .optional()
                    .map_err(|e| OpStoreError::Other(e.into()))?
                    .is_some()
            };
            let res = if exists {
                PrefixResolution::SingleMatch(full_id)
            } else {
                PrefixResolution::NoMatch
            };
            return Ok(res);
        }

        // Scan all stored IDs for a prefix match.
        // TODO: See if the database can help us and avoid allocating the Vec.
        let conn = self.db.lock().await;
        let all_ids: Vec<OperationId> = conn
            .get_all::<model::OperationRow, _>(model::OperationRow::SELECT, ())
            .map_err(|e| OpStoreError::Other(e.into()))?
            .into_iter()
            .map(|r| r.id.into_jj())
            .collect();

        let mut matched: Option<OperationId> = prefix
            .matches(&self.root_operation_id)
            .then(|| self.root_operation_id.clone());
        for id in all_ids {
            if prefix.matches(&id) {
                if matched.is_some() {
                    return Ok(PrefixResolution::AmbiguousMatch);
                }
                matched = Some(id);
            }
        }
        Ok(matched.map_or(PrefixResolution::NoMatch, PrefixResolution::SingleMatch))
    }

    async fn gc(&self, head_ids: &[OperationId], keep_newer: SystemTime) -> OpStoreResult<()> {
        let keep_newer_ms = keep_newer
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let model_head_ids = head_ids
            .iter()
            .filter(|id| **id != self.root_operation_id)
            .map(|id| id.to_model())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| OpStoreError::Other(e.into()))?;
        let mut conn = self.db.lock().await;
        gc_impl(&mut conn, &model_head_ids, keep_newer_ms)
            .map_err(|e| OpStoreError::Other(e.into()))
    }
}

fn gc_impl(
    conn: &mut Connection,
    head_ids: &[model::OperationId],
    keep_newer_ms: i64,
) -> Result<(), SqlBackendError> {
    let tx = conn.transaction()?;

    tx.execute_batch(
        "DROP TABLE IF EXISTS _gc_live_ops;
         CREATE TEMP TABLE _gc_live_ops (id BLOB PRIMARY KEY);",
    )?;

    {
        let mut stmt = tx.prepare("INSERT OR IGNORE INTO _gc_live_ops VALUES (?1)")?;
        for id in head_ids {
            stmt.execute((id,))?;
        }
    }

    tx.execute_batch(
        "WITH RECURSIVE live(id) AS (
             SELECT id FROM _gc_live_ops
             UNION
             SELECT op.parent_id FROM operation_parents op
                 JOIN live ON op.operation_id = live.id
         )
         INSERT OR IGNORE INTO _gc_live_ops SELECT id FROM live;",
    )?;

    tx.execute(
        "DELETE FROM operations
         WHERE id NOT IN (SELECT id FROM _gc_live_ops)
           AND __last_written_ms <= ?1",
        (keep_newer_ms,),
    )?;

    tx.execute_batch(
        "DELETE FROM operation_parents
         WHERE operation_id NOT IN (SELECT id FROM operations);",
    )?;

    tx.execute(
        "DELETE FROM views
         WHERE id NOT IN (SELECT view_id FROM operations)
           AND __last_written_ms <= ?1",
        (keep_newer_ms,),
    )?;

    tx.execute_batch("DROP TABLE _gc_live_ops;")?;

    tx.commit()?;
    conn.execute_batch("VACUUM;")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::cloned_ref_to_slice_refs)]
    use std::collections::BTreeMap;
    use std::collections::HashSet;

    use jj_lib::backend::CommitId;
    use jj_lib::backend::MillisSinceEpoch;
    use jj_lib::backend::Timestamp;
    use jj_lib::op_store::OpStore as _;
    use jj_lib::op_store::Operation;
    use jj_lib::op_store::OperationId;
    use jj_lib::op_store::OperationMetadata;
    use jj_lib::op_store::RefTarget;
    use jj_lib::op_store::RemoteRef;
    use jj_lib::op_store::RemoteRefState;
    use jj_lib::op_store::RemoteView;
    use jj_lib::op_store::RootOperationData;
    use jj_lib::op_store::TimestampRange;
    use jj_lib::op_store::View;
    use jj_lib::op_store::ViewId;
    use jj_lib::ref_name::RefName;
    use jj_lib::ref_name::WorkspaceNameBuf;
    use pollster::FutureExt as _;

    use super::*;

    /// Pads `prefix` with trailing zeros to produce a full-length `CommitId`.
    fn commit_id(prefix: &str) -> CommitId {
        use crate::backend::model::COMMIT_ID_LENGTH;
        let hex = format!("{:0<width$}", prefix, width = COMMIT_ID_LENGTH * 2);
        CommitId::try_from_hex(&hex).unwrap()
    }

    fn make_store() -> (SqlOpStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let root_data = RootOperationData {
            root_commit_id: commit_id("aabbcc"),
        };
        let store = SqlOpStore::init(dir.path(), root_data).unwrap();
        (store, dir)
    }

    fn make_view() -> View {
        let head1 = commit_id("1111");
        let head2 = commit_id("2222");
        let bm_target = RefTarget::normal(commit_id("3333"));
        let tag_target = RefTarget::normal(commit_id("4444"));
        let remote_bm_target = RefTarget::normal(commit_id("5555"));
        let wc_commit = commit_id("6666");
        View {
            head_ids: HashSet::from([head1, head2]),
            local_bookmarks: BTreeMap::from([("main".into(), bm_target)]),
            local_tags: BTreeMap::from([("v1.0".into(), tag_target)]),
            remote_views: BTreeMap::from([(
                "origin".into(),
                RemoteView {
                    bookmarks: BTreeMap::from([(
                        "main".into(),
                        RemoteRef {
                            target: remote_bm_target,
                            state: RemoteRefState::Tracked,
                        },
                    )]),
                    tags: BTreeMap::new(),
                },
            )]),
            git_refs: BTreeMap::new(),
            git_head: RefTarget::absent(),
            wc_commit_ids: BTreeMap::from([("default".into(), wc_commit)]),
        }
    }

    fn make_operation(view_id: ViewId, parent_id: OperationId) -> Operation {
        Operation {
            view_id,
            parents: vec![parent_id],
            metadata: OperationMetadata {
                time: TimestampRange {
                    start: Timestamp {
                        timestamp: MillisSinceEpoch(1_000_000),
                        tz_offset: 0,
                    },
                    end: Timestamp {
                        timestamp: MillisSinceEpoch(1_001_000),
                        tz_offset: 0,
                    },
                },
                description: "test op".to_owned(),
                hostname: "localhost".to_owned(),
                username: "alice".to_owned(),
                is_snapshot: false,
                workspace_name: Some(WorkspaceNameBuf::from("default")),
                attributes: BTreeMap::from([("key".to_owned(), "val".to_owned())]),
            },
            commit_predecessors: Some(BTreeMap::from([(
                commit_id("aaaa"),
                vec![commit_id("bbbb")],
            )])),
        }
    }

    // ── view roundtrip ────────────────────────────────────────────────────────

    #[test]
    fn test_write_read_view_roundtrip() {
        let (store, _dir) = make_store();
        let view = make_view();
        let view_id = store.write_view(&view).block_on().unwrap();
        let read_back = store.read_view(&view_id).block_on().unwrap();
        assert_eq!(read_back, view);
    }

    #[test]
    fn test_write_read_view_idempotent() {
        let (store, _dir) = make_store();
        let view = make_view();
        let id1 = store.write_view(&view).block_on().unwrap();
        let id2 = store.write_view(&view).block_on().unwrap();
        assert_eq!(id1, id2);
    }

    // ── absent / conflict RefTarget ───────────────────────────────────────────

    #[test]
    fn test_absent_ref_target_roundtrip() {
        let (store, _dir) = make_store();
        let view = View {
            head_ids: HashSet::from([commit_id("1234")]),
            local_bookmarks: BTreeMap::from([("gone".into(), RefTarget::absent())]),
            local_tags: BTreeMap::new(),
            remote_views: BTreeMap::new(),
            git_refs: BTreeMap::new(),
            git_head: RefTarget::absent(),
            wc_commit_ids: BTreeMap::new(),
        };
        let id = store.write_view(&view).block_on().unwrap();
        let back = store.read_view(&id).block_on().unwrap();
        assert_eq!(
            back.local_bookmarks[RefName::new("gone")],
            RefTarget::absent()
        );
    }

    #[test]
    fn test_conflict_ref_target_roundtrip() {
        let (store, _dir) = make_store();
        let conflict = RefTarget::from_legacy_form(
            [commit_id("1111")],
            [commit_id("2222"), commit_id("3333")],
        );
        let view = View {
            head_ids: HashSet::from([commit_id("ffff")]),
            local_bookmarks: BTreeMap::from([("conflict".into(), conflict.clone())]),
            local_tags: BTreeMap::new(),
            remote_views: BTreeMap::new(),
            git_refs: BTreeMap::new(),
            git_head: RefTarget::absent(),
            wc_commit_ids: BTreeMap::new(),
        };
        let id = store.write_view(&view).block_on().unwrap();
        let back = store.read_view(&id).block_on().unwrap();
        assert_eq!(back.local_bookmarks[RefName::new("conflict")], conflict);
    }

    // ── operation roundtrip ───────────────────────────────────────────────────

    #[test]
    fn test_write_read_operation_roundtrip() {
        let (store, _dir) = make_store();
        let view = make_view();
        let view_id = store.write_view(&view).block_on().unwrap();
        let op = make_operation(view_id, store.root_operation_id().clone());
        let op_id = store.write_operation(&op).block_on().unwrap();
        let read_back = store.read_operation(&op_id).block_on().unwrap();
        assert_eq!(read_back, op);
    }

    #[test]
    fn test_write_read_operation_no_predecessors() {
        let (store, _dir) = make_store();
        let view = make_view();
        let view_id = store.write_view(&view).block_on().unwrap();
        let mut op = make_operation(view_id, store.root_operation_id().clone());
        op.commit_predecessors = None;
        let op_id = store.write_operation(&op).block_on().unwrap();
        let back = store.read_operation(&op_id).block_on().unwrap();
        assert_eq!(back.commit_predecessors, None);
    }

    #[test]
    fn test_write_read_operation_empty_predecessors() {
        let (store, _dir) = make_store();
        let view = make_view();
        let view_id = store.write_view(&view).block_on().unwrap();
        let mut op = make_operation(view_id, store.root_operation_id().clone());
        op.commit_predecessors = Some(BTreeMap::new());
        let op_id = store.write_operation(&op).block_on().unwrap();
        let back = store.read_operation(&op_id).block_on().unwrap();
        assert_eq!(back.commit_predecessors, Some(BTreeMap::new()));
    }

    // ── root objects ──────────────────────────────────────────────────────────

    #[test]
    fn test_read_root_operation() {
        let (store, _dir) = make_store();
        let op = store
            .read_operation(store.root_operation_id())
            .block_on()
            .unwrap();
        assert!(op.parents.is_empty());
    }

    // ── prefix resolution ─────────────────────────────────────────────────────

    #[test]
    fn test_resolve_operation_id_prefix() {
        let (store, _dir) = make_store();
        let view = make_view();
        let view_id = store.write_view(&view).block_on().unwrap();
        let op = make_operation(view_id, store.root_operation_id().clone());
        let op_id = store.write_operation(&op).block_on().unwrap();

        // Full prefix resolves to single match.
        let full_hex = op_id.hex();
        let prefix = HexPrefix::try_from_hex(full_hex.as_str()).unwrap();
        let result = store
            .resolve_operation_id_prefix(&prefix)
            .block_on()
            .unwrap();
        assert_eq!(result, PrefixResolution::SingleMatch(op_id.clone()));

        // Short prefix that matches only this op.
        let short_prefix = HexPrefix::try_from_hex(&full_hex[..4]).unwrap();
        let result = store
            .resolve_operation_id_prefix(&short_prefix)
            .block_on()
            .unwrap();
        assert_eq!(result, PrefixResolution::SingleMatch(op_id));
    }

    #[test]
    fn test_resolve_operation_id_prefix_no_match() {
        let (store, _dir) = make_store();
        // All-zeros prefix matches root_operation_id, not our custom 0xdeadbeef.
        let prefix = HexPrefix::try_from_hex("deadbeef").unwrap();
        let result = store
            .resolve_operation_id_prefix(&prefix)
            .block_on()
            .unwrap();
        assert_eq!(result, PrefixResolution::NoMatch);
    }

    // ── gc ────────────────────────────────────────────────────────────────────

    /// Writes a view whose `wc_commit_ids` are keyed by `tag`, making each
    /// call with a distinct `tag` produce a distinct content-addressed ID.
    fn write_tagged_view_and_op(
        store: &SqlOpStore,
        parent_id: OperationId,
        tag: &str,
    ) -> (ViewId, OperationId) {
        let mut view = make_view();
        let wc = view.wc_commit_ids.values().next().cloned().unwrap();
        view.wc_commit_ids.clear();
        view.wc_commit_ids.insert(WorkspaceNameBuf::from(tag), wc);
        let view_id = store.write_view(&view).block_on().unwrap();
        let op_id = store
            .write_operation(&make_operation(view_id.clone(), parent_id))
            .block_on()
            .unwrap();
        (view_id, op_id)
    }

    #[test]
    fn test_gc_keeps_reachable_ops_and_views() {
        let (store, _dir) = make_store();
        let root = store.root_operation_id().clone();
        let (view_id, op_id) = write_tagged_view_and_op(&store, root, "a");

        // GC with op_id as the sole head and keep_newer in the past: nothing
        // should be deleted because op_id is reachable.
        store
            .gc(&[op_id.clone()], SystemTime::UNIX_EPOCH)
            .block_on()
            .unwrap();

        store.read_operation(&op_id).block_on().unwrap();
        store.read_view(&view_id).block_on().unwrap();
    }

    #[test]
    fn test_gc_deletes_unreachable_old_ops_and_views() {
        let (store, _dir) = make_store();
        let root = store.root_operation_id().clone();
        let (old_view_id, old_op_id) = write_tagged_view_and_op(&store, root.clone(), "old");
        let (live_view_id, live_op_id) = write_tagged_view_and_op(&store, root, "live");

        // GC with only live_op_id as head and keep_newer far in the future (so
        // nothing is protected by recency). old_op and old_view should be gone.
        store
            .gc(&[live_op_id.clone()], SystemTime::now())
            .block_on()
            .unwrap();

        store.read_operation(&live_op_id).block_on().unwrap();
        store.read_view(&live_view_id).block_on().unwrap();
        assert!(store.read_operation(&old_op_id).block_on().is_err());
        assert!(store.read_view(&old_view_id).block_on().is_err());
    }

    #[test]
    fn test_gc_keeps_recent_unreachable_ops_and_views() {
        let (store, _dir) = make_store();
        let root = store.root_operation_id().clone();
        let (recent_view_id, recent_op_id) =
            write_tagged_view_and_op(&store, root.clone(), "recent");
        let (live_view_id, live_op_id) = write_tagged_view_and_op(&store, root, "live");

        // keep_newer = UNIX_EPOCH means "delete everything older than epoch",
        // which is nothing (all created_ms values are positive).
        store
            .gc(&[live_op_id.clone()], SystemTime::UNIX_EPOCH)
            .block_on()
            .unwrap();

        // recent_op and recent_view are unreachable but protected by recency.
        store.read_operation(&recent_op_id).block_on().unwrap();
        store.read_view(&recent_view_id).block_on().unwrap();
        store.read_operation(&live_op_id).block_on().unwrap();
        store.read_view(&live_view_id).block_on().unwrap();
    }

    #[test]
    fn test_gc_keeps_ancestor_ops() {
        let (store, _dir) = make_store();
        let root = store.root_operation_id().clone();
        let (_, op1_id) = write_tagged_view_and_op(&store, root, "a");
        let (view2_id, op2_id) = write_tagged_view_and_op(&store, op1_id.clone(), "b");

        // op2 is the head; op1 is its ancestor and must be kept.
        store
            .gc(&[op2_id.clone()], SystemTime::now())
            .block_on()
            .unwrap();

        store.read_operation(&op1_id).block_on().unwrap();
        store.read_operation(&op2_id).block_on().unwrap();
        store.read_view(&view2_id).block_on().unwrap();
    }

    #[test]
    fn test_gc_keeps_view_shared_by_surviving_op() {
        let (store, _dir) = make_store();
        let root = store.root_operation_id().clone();

        // Write a single view and two operations that both reference it.
        // op_a and op_b have different parents so they get distinct IDs.
        let view = make_view();
        let view_id = store.write_view(&view).block_on().unwrap();
        let op_a_id = store
            .write_operation(&make_operation(view_id.clone(), root))
            .block_on()
            .unwrap();
        let op_b_id = store
            .write_operation(&make_operation(view_id.clone(), op_a_id.clone()))
            .block_on()
            .unwrap();

        // GC keeps op_a, deletes op_b (unreachable from op_a and old enough).
        // The shared view must survive because op_a still references it.
        store
            .gc(&[op_a_id.clone()], SystemTime::now())
            .block_on()
            .unwrap();

        store.read_operation(&op_a_id).block_on().unwrap();
        store.read_view(&view_id).block_on().unwrap();
        assert!(store.read_operation(&op_b_id).block_on().is_err());
    }

    #[test]
    fn test_gc_deletes_long_unreachable_chain() {
        let (store, _dir) = make_store();
        let root = store.root_operation_id().clone();

        // Build a chain: root → op1 → op2 → op3 → op4.
        let (_, op1_id) = write_tagged_view_and_op(&store, root, "1");
        let (_, op2_id) = write_tagged_view_and_op(&store, op1_id.clone(), "2");
        let (_, op3_id) = write_tagged_view_and_op(&store, op2_id.clone(), "3");
        let (_, op4_id) = write_tagged_view_and_op(&store, op3_id.clone(), "4");

        // GC with only op1 as head; op2, op3, op4 are all unreachable.
        store
            .gc(&[op1_id.clone()], SystemTime::now())
            .block_on()
            .unwrap();

        store.read_operation(&op1_id).block_on().unwrap();
        assert!(store.read_operation(&op2_id).block_on().is_err());
        assert!(store.read_operation(&op3_id).block_on().is_err());
        assert!(store.read_operation(&op4_id).block_on().is_err());
    }
}
