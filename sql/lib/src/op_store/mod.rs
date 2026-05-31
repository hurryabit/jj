mod convert;
pub mod model;

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::SystemTime;

use async_trait::async_trait;
use jj_lib::content_hash::blake2b_hash;
use jj_lib::object_id::HexPrefix;
use jj_lib::object_id::PrefixResolution;
use jj_lib::op_store as jj;
use jj_lib::op_store::OpStore;
use jj_lib::op_store::OpStoreError;
use jj_lib::op_store::OpStoreResult;
use jj_sql_macro::sql;
use sqlx::Connection as _;
use sqlx::Sqlite;
use sqlx::SqliteConnection;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::sqlite::SqliteJournalMode;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::sqlite::SqliteSynchronous;

use crate::backend::model::CommitId;
use crate::model::Model as _;
use crate::model::SqliteConnectionExt as _;
use crate::convert::JjExt;
use crate::convert::ModelExt as _;
use crate::error::SqlBackendError;
use crate::op_store::model::OPERATION_ID_LENGTH;
use crate::op_store::model::OperationId;
use crate::op_store::model::OperationRow;
use crate::op_store::model::ViewId;
use crate::op_store::model::ViewRow;
use crate::postcard::Postcard;

#[derive(Debug)]
pub struct SqlOpStore {
    #[allow(dead_code)]
    path: PathBuf,
    pool: sqlx::Pool<Sqlite>,
    root_data: jj::RootOperationData,
    root_operation_id: jj::OperationId,
}

impl SqlOpStore {
    pub fn name() -> &'static str {
        "sql"
    }

    fn connect_options() -> SqliteConnectOptions {
        SqliteConnectOptions::new()
            .busy_timeout(Duration::from_millis(5000))
            .pragma("encoding", "'UTF-8'")
            .page_size(8 * 1024) // 8 KiB
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .pragma("cache_size", (-32 * 1024).to_string()) // 32 MiB
            .pragma("mmap_size", (32 * 1024 * 1024).to_string()) // 32 MiB
            .pragma("temp_store", "MEMORY")
    }

    fn pool_options() -> SqlitePoolOptions {
        SqlitePoolOptions::new()
            .max_connections(1)
            .after_connect(|conn, _meta| Box::pin(crate::backend::vtab::sqlx_load_module(conn)))
    }

    fn root_operation_id(root_data: &jj::RootOperationData) -> jj::OperationId {
        let root_view = jj::View::make_root(root_data.root_commit_id.clone());
        let root_view_id = ViewId::from(blake2b_hash(&root_view)).into_jj();
        let root_op = jj::Operation::make_root(root_view_id);
        OperationId::from(blake2b_hash(&root_op)).into_jj()
    }

    async fn connect(
        store_path: &Path,
        connect_options: SqliteConnectOptions,
        pool_options: SqlitePoolOptions,
        root_data: jj::RootOperationData,
    ) -> Result<Self, SqlBackendError> {
        let pool = pool_options
            .connect_with(connect_options.filename(store_path.join("op_store.db3")))
            .await?;
        let root_operation_id = Self::root_operation_id(&root_data);
        Ok(Self {
            path: store_path.to_path_buf(),
            pool,
            root_operation_id,
            root_data,
        })
    }

    async fn initialize(&self) -> Result<(), SqlBackendError> {
        {
            // NOTE: We need to return the connection to the pool before the writes below.
            let mut conn = self.pool.acquire().await?;
            sqlx::raw_sql(include_str!("../../sql/op_store.sql"))
                .execute(&mut *conn)
                .await?;
        }
        let root_view = jj::View::make_root(self.root_data.root_commit_id.clone());
        let root_view_id = self.write_view(&root_view).await?;
        let root_op = jj::Operation::make_root(root_view_id);
        self.write_operation(&root_op).await?;
        Ok(())
    }

    pub async fn init_in_memory() -> Result<Self, SqlBackendError> {
        let store = Self::connect(
            Path::new(":memory:"),
            Self::connect_options().in_memory(true),
            Self::pool_options(),
            jj::RootOperationData {
                root_commit_id: CommitId::ZERO.into_jj(),
            },
        )
        .await?;
        store.initialize().await?;
        Ok(store)
    }

    pub async fn init(
        store_path: &Path,
        root_data: jj::RootOperationData,
    ) -> Result<Self, SqlBackendError> {
        let store = Self::connect(
            store_path,
            Self::connect_options().create_if_missing(true),
            Self::pool_options(),
            root_data,
        )
        .await?;
        store.initialize().await?;
        Ok(store)
    }

    pub async fn load(
        store_path: &Path,
        root_data: jj::RootOperationData,
    ) -> Result<Self, SqlBackendError> {
        Self::connect(
            store_path,
            Self::connect_options().create_if_missing(false),
            Self::pool_options(),
            root_data,
        )
        .await
    }

    async fn read_view(&self, id: &jj::ViewId) -> Result<jj::View, SqlBackendError> {
        let id = id.to_model()?;
        let mut conn = self.pool.acquire().await?;
        let (_, view) = conn.fetch_by_hash_id::<ViewRow>(&id).await?;
        Ok(view.data.decode()?.into_jj())
    }

    async fn write_view(&self, view: &jj::View) -> Result<jj::ViewId, SqlBackendError> {
        let id = ViewId::from(blake2b_hash(view));
        let data = Postcard::encode(&view.to_model()?)?;
        let mut conn = self.pool.acquire().await?;
        conn.insert(&ViewRow { id, data }).await?;
        Ok(id.into_jj())
    }

    async fn read_operation(&self, id: &jj::OperationId) -> Result<jj::Operation, SqlBackendError> {
        let id = id.to_model()?;
        let mut conn = self.pool.acquire().await?;
        let (_, op) = conn.fetch_by_hash_id::<OperationRow>(&id).await?;
        Ok(jj::Operation {
            view_id: op.view_id.into_jj(),
            parents: op.parents.decode()?.into_jj(),
            metadata: op.metadata.decode()?.into_jj(),
            commit_predecessors: op
                .commit_predecessors
                .map(|p| p.decode())
                .transpose()?
                .into_jj(),
        })
    }

    async fn write_operation(
        &self,
        op: &jj::Operation,
    ) -> Result<jj::OperationId, SqlBackendError> {
        let id = OperationId::from(blake2b_hash(op));
        let mut conn = self.pool.acquire().await?;
        conn.insert(&OperationRow {
            id,
            view_id: op.view_id.to_model()?,
            parents: Postcard::encode(&op.parents.to_model()?)?,
            metadata: Postcard::encode(&op.metadata.to_model()?)?,
            commit_predecessors: op
                .commit_predecessors
                .to_model()?
                .as_ref()
                .map(Postcard::encode)
                .transpose()?,
        })
        .await?;
        Ok(id.into_jj())
    }

    async fn resolve_operation_id_prefix(
        &self,
        prefix: &HexPrefix,
    ) -> Result<PrefixResolution<jj::OperationId>, SqlBackendError> {
        let mut conn = self.pool.acquire().await?;

        // Fast path: full-length prefix → single lookup.
        if prefix.hex().len() == OPERATION_ID_LENGTH * 2 {
            let full_id = jj::OperationId::from_bytes(prefix.as_full_bytes().unwrap());
            let exists = OperationRow::fetch_by_hash_id_query(&full_id.to_model()?)
                .fetch_optional(&mut *conn)
                .await?
                .is_some();
            return Ok(if exists {
                PrefixResolution::SingleMatch(full_id)
            } else {
                PrefixResolution::NoMatch
            });
        }

        // Scan all stored IDs for a prefix match.
        let all_ids: Vec<jj::OperationId> =
            sqlx::query_scalar::<_, OperationId>(sql!("SELECT id FROM operations"))
                .fetch_all(&mut *conn)
                .await?
                .into_jj();

        let mut matched: Option<jj::OperationId> = None;
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

    async fn gc(
        &self,
        head_ids: &[jj::OperationId],
        keep_newer: SystemTime,
    ) -> Result<(), SqlBackendError> {
        let keep_newer_ms = keep_newer
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let head_ids = head_ids
            .iter()
            .map(|id| id.to_model())
            .collect::<Result<Vec<_>, _>>()?;
        let mut conn = self.pool.acquire().await?;
        gc_impl(&mut conn, &head_ids, keep_newer_ms).await
    }
}

#[async_trait]
impl OpStore for SqlOpStore {
    fn name(&self) -> &str {
        Self::name()
    }

    fn root_operation_id(&self) -> &jj::OperationId {
        &self.root_operation_id
    }

    async fn read_view(&self, id: &jj::ViewId) -> OpStoreResult<jj::View> {
        self.read_view(id)
            .await
            .map_err(|e| e.with_read_context(id).into())
    }

    async fn write_view(&self, view: &jj::View) -> OpStoreResult<jj::ViewId> {
        self.write_view(view)
            .await
            .map_err(|e| e.with_write_context("view").into())
    }

    async fn read_operation(&self, id: &jj::OperationId) -> OpStoreResult<jj::Operation> {
        self.read_operation(id)
            .await
            .map_err(|e| e.with_read_context(id).into())
    }

    async fn write_operation(&self, op: &jj::Operation) -> OpStoreResult<jj::OperationId> {
        if op.parents.is_empty() {
            return Err(SqlBackendError::InternalError(String::from(
                "cannot write an operation with no parents",
            ))
            .into());
        }
        self.write_operation(op)
            .await
            .map_err(|e| e.with_write_context("operation").into())
    }

    async fn resolve_operation_id_prefix(
        &self,
        prefix: &HexPrefix,
    ) -> OpStoreResult<PrefixResolution<jj::OperationId>> {
        self.resolve_operation_id_prefix(prefix)
            .await
            .map_err(|e| OpStoreError::Other(e.into()))
    }

    async fn gc(&self, head_ids: &[jj::OperationId], keep_newer: SystemTime) -> OpStoreResult<()> {
        self.gc(head_ids, keep_newer)
            .await
            .map_err(|e| OpStoreError::Other(e.into()))
    }
}

#[derive(sqlx::FromRow)]
struct OpForGc {
    id: OperationId,
    parents: Postcard<Vec<OperationId>>,
    last_written_ms: i64,
}

async fn gc_impl(
    conn: &mut SqliteConnection,
    head_ids: &[OperationId],
    keep_newer_ms: i64,
) -> Result<(), SqlBackendError> {
    let mut tx = conn.begin_with("BEGIN IMMEDIATE").await?;

    // Fetch all operations with their parent lists and timestamps.
    let all_ops: Vec<OpForGc> = sqlx::query_as(sql!(
        "SELECT id, parents, __last_written_ms AS last_written_ms FROM operations"
    ))
    .fetch_all(&mut *tx)
    .await?;

    // BFS from head_ids to find the reachable set.
    let parent_map: HashMap<OperationId, Vec<OperationId>> = all_ops
        .iter()
        .map(|row| Ok((row.id, row.parents.decode()?)))
        .collect::<Result<_, SqlBackendError>>()?;

    let mut reachable: HashSet<OperationId> = HashSet::new();
    let mut queue: VecDeque<OperationId> = head_ids.iter().cloned().collect();
    while let Some(id) = queue.pop_front() {
        if reachable.insert(id)
            && let Some(parents) = parent_map.get(&id)
        {
            for &parent in parents {
                if !reachable.contains(&parent) {
                    queue.push_back(parent);
                }
            }
        }
    }

    // Collect ops that are unreachable AND old enough to delete.
    let to_delete: Vec<OperationId> = all_ops
        .into_iter()
        .filter(|row| !reachable.contains(&row.id) && row.last_written_ms <= keep_newer_ms)
        .map(|row| row.id)
        .collect();

    if !to_delete.is_empty() {
        for id in &to_delete {
            sqlx::query!("DELETE FROM operations WHERE id = ?", id)
                .execute(&mut *tx)
                .await?;
        }
    }

    // Remove views that are no longer referenced by any operation.
    sqlx::query!(
        "
        DELETE FROM views
        WHERE id NOT IN (SELECT view_id FROM operations) AND __last_written_ms <= ?
        ",
        keep_newer_ms
    )
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    sqlx::raw_sql("VACUUM").execute(&mut *conn).await?;
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
    use jj_lib::object_id::ObjectId as _;
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

    use super::*;
    use crate::backend::model::COMMIT_ID_LENGTH;

    /// Pads `prefix` with trailing zeros to produce a full-length `CommitId`.
    fn commit_id(prefix: &str) -> CommitId {
        let hex = format!("{:0<width$}", prefix, width = COMMIT_ID_LENGTH * 2);
        CommitId::try_from_hex(&hex).unwrap()
    }

    async fn make_store() -> (SqlOpStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let root_data = RootOperationData {
            root_commit_id: commit_id("aabbcc"),
        };
        let store = SqlOpStore::init(dir.path(), root_data).await.unwrap();
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

    #[tokio::test]
    async fn test_write_read_view_roundtrip() {
        let (store, _dir) = make_store().await;
        let view = make_view();
        let view_id = store.write_view(&view).await.unwrap();
        let read_back = store.read_view(&view_id).await.unwrap();
        assert_eq!(read_back, view);
    }

    #[tokio::test]
    async fn test_write_read_view_idempotent() {
        let (store, _dir) = make_store().await;
        let view = make_view();
        let id1 = store.write_view(&view).await.unwrap();
        let id2 = store.write_view(&view).await.unwrap();
        assert_eq!(id1, id2);
    }

    #[tokio::test]
    async fn test_absent_ref_target_roundtrip() {
        let (store, _dir) = make_store().await;
        let view = View {
            head_ids: HashSet::from([commit_id("1234")]),
            local_bookmarks: BTreeMap::from([("gone".into(), RefTarget::absent())]),
            local_tags: BTreeMap::new(),
            remote_views: BTreeMap::new(),
            git_refs: BTreeMap::new(),
            git_head: RefTarget::absent(),
            wc_commit_ids: BTreeMap::new(),
        };
        let id = store.write_view(&view).await.unwrap();
        let back = store.read_view(&id).await.unwrap();
        assert_eq!(
            back.local_bookmarks[RefName::new("gone")],
            RefTarget::absent()
        );
    }

    #[tokio::test]
    async fn test_conflict_ref_target_roundtrip() {
        let (store, _dir) = make_store().await;
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
        let id = store.write_view(&view).await.unwrap();
        let back = store.read_view(&id).await.unwrap();
        assert_eq!(back.local_bookmarks[RefName::new("conflict")], conflict);
    }

    #[tokio::test]
    async fn test_write_read_operation_roundtrip() {
        let (store, _dir) = make_store().await;
        let view = make_view();
        let view_id = store.write_view(&view).await.unwrap();
        let op = make_operation(view_id, store.root_operation_id().clone());
        let op_id = store.write_operation(&op).await.unwrap();
        let read_back = store.read_operation(&op_id).await.unwrap();
        assert_eq!(read_back, op);
    }

    #[tokio::test]
    async fn test_write_read_operation_no_predecessors() {
        let (store, _dir) = make_store().await;
        let view = make_view();
        let view_id = store.write_view(&view).await.unwrap();
        let mut op = make_operation(view_id, store.root_operation_id().clone());
        op.commit_predecessors = None;
        let op_id = store.write_operation(&op).await.unwrap();
        let back = store.read_operation(&op_id).await.unwrap();
        assert_eq!(back.commit_predecessors, None);
    }

    #[tokio::test]
    async fn test_write_read_operation_empty_predecessors() {
        let (store, _dir) = make_store().await;
        let view = make_view();
        let view_id = store.write_view(&view).await.unwrap();
        let mut op = make_operation(view_id, store.root_operation_id().clone());
        op.commit_predecessors = Some(BTreeMap::new());
        let op_id = store.write_operation(&op).await.unwrap();
        let back = store.read_operation(&op_id).await.unwrap();
        assert_eq!(back.commit_predecessors, Some(BTreeMap::new()));
    }

    #[tokio::test]
    async fn test_read_root_operation() {
        let (store, _dir) = make_store().await;
        let op = store
            .read_operation(store.root_operation_id())
            .await
            .unwrap();
        assert!(op.parents.is_empty());
    }

    #[tokio::test]
    async fn test_resolve_operation_id_prefix() {
        let (store, _dir) = make_store().await;
        let view = make_view();
        let view_id = store.write_view(&view).await.unwrap();
        let op = make_operation(view_id, store.root_operation_id().clone());
        let op_id = store.write_operation(&op).await.unwrap();

        let full_hex = op_id.hex();
        let prefix = HexPrefix::try_from_hex(full_hex.as_str()).unwrap();
        let result = store.resolve_operation_id_prefix(&prefix).await.unwrap();
        assert_eq!(result, PrefixResolution::SingleMatch(op_id.clone()));

        let short_prefix = HexPrefix::try_from_hex(&full_hex[..4]).unwrap();
        let result = store
            .resolve_operation_id_prefix(&short_prefix)
            .await
            .unwrap();
        assert_eq!(result, PrefixResolution::SingleMatch(op_id));
    }

    #[tokio::test]
    async fn test_resolve_operation_id_prefix_no_match() {
        let (store, _dir) = make_store().await;
        let prefix = HexPrefix::try_from_hex("deadbeef").unwrap();
        let result = store.resolve_operation_id_prefix(&prefix).await.unwrap();
        assert_eq!(result, PrefixResolution::NoMatch);
    }

    async fn write_tagged_view_and_op(
        store: &SqlOpStore,
        parent_id: OperationId,
        tag: &str,
    ) -> (ViewId, OperationId) {
        let mut view = make_view();
        let wc = view.wc_commit_ids.values().next().cloned().unwrap();
        view.wc_commit_ids.clear();
        view.wc_commit_ids.insert(WorkspaceNameBuf::from(tag), wc);
        let view_id = store.write_view(&view).await.unwrap();
        let op_id = store
            .write_operation(&make_operation(view_id.clone(), parent_id))
            .await
            .unwrap();
        (view_id, op_id)
    }

    #[tokio::test]
    async fn test_gc_keeps_reachable_ops_and_views() {
        let (store, _dir) = make_store().await;
        let root = store.root_operation_id().clone();
        let (view_id, op_id) = write_tagged_view_and_op(&store, root, "a").await;

        store
            .gc(&[op_id.clone()], SystemTime::UNIX_EPOCH)
            .await
            .unwrap();

        store.read_operation(&op_id).await.unwrap();
        store.read_view(&view_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_gc_deletes_unreachable_old_ops_and_views() {
        let (store, _dir) = make_store().await;
        let root = store.root_operation_id().clone();
        let (old_view_id, old_op_id) = write_tagged_view_and_op(&store, root.clone(), "old").await;
        let (live_view_id, live_op_id) = write_tagged_view_and_op(&store, root, "live").await;

        store
            .gc(&[live_op_id.clone()], SystemTime::now())
            .await
            .unwrap();

        store.read_operation(&live_op_id).await.unwrap();
        store.read_view(&live_view_id).await.unwrap();
        assert!(store.read_operation(&old_op_id).await.is_err());
        assert!(store.read_view(&old_view_id).await.is_err());
    }

    #[tokio::test]
    async fn test_gc_keeps_recent_unreachable_ops_and_views() {
        let (store, _dir) = make_store().await;
        let root = store.root_operation_id().clone();
        let (recent_view_id, recent_op_id) =
            write_tagged_view_and_op(&store, root.clone(), "recent").await;
        let (live_view_id, live_op_id) = write_tagged_view_and_op(&store, root, "live").await;

        store
            .gc(&[live_op_id.clone()], SystemTime::UNIX_EPOCH)
            .await
            .unwrap();

        store.read_operation(&recent_op_id).await.unwrap();
        store.read_view(&recent_view_id).await.unwrap();
        store.read_operation(&live_op_id).await.unwrap();
        store.read_view(&live_view_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_gc_keeps_ancestor_ops() {
        let (store, _dir) = make_store().await;
        let root = store.root_operation_id().clone();
        let (_, op1_id) = write_tagged_view_and_op(&store, root, "a").await;
        let (view2_id, op2_id) = write_tagged_view_and_op(&store, op1_id.clone(), "b").await;

        store
            .gc(&[op2_id.clone()], SystemTime::now())
            .await
            .unwrap();

        store.read_operation(&op1_id).await.unwrap();
        store.read_operation(&op2_id).await.unwrap();
        store.read_view(&view2_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_gc_keeps_view_shared_by_surviving_op() {
        let (store, _dir) = make_store().await;
        let root = store.root_operation_id().clone();

        let view = make_view();
        let view_id = store.write_view(&view).await.unwrap();
        let op_a_id = store
            .write_operation(&make_operation(view_id.clone(), root))
            .await
            .unwrap();
        let op_b_id = store
            .write_operation(&make_operation(view_id.clone(), op_a_id.clone()))
            .await
            .unwrap();

        store
            .gc(&[op_a_id.clone()], SystemTime::now())
            .await
            .unwrap();

        store.read_operation(&op_a_id).await.unwrap();
        store.read_view(&view_id).await.unwrap();
        assert!(store.read_operation(&op_b_id).await.is_err());
    }

    #[tokio::test]
    async fn test_gc_deletes_long_unreachable_chain() {
        let (store, _dir) = make_store().await;
        let root = store.root_operation_id().clone();

        let (_, op1_id) = write_tagged_view_and_op(&store, root, "1").await;
        let (_, op2_id) = write_tagged_view_and_op(&store, op1_id.clone(), "2").await;
        let (_, op3_id) = write_tagged_view_and_op(&store, op2_id.clone(), "3").await;
        let (_, op4_id) = write_tagged_view_and_op(&store, op3_id.clone(), "4").await;

        store
            .gc(&[op1_id.clone()], SystemTime::now())
            .await
            .unwrap();

        store.read_operation(&op1_id).await.unwrap();
        assert!(store.read_operation(&op2_id).await.is_err());
        assert!(store.read_operation(&op3_id).await.is_err());
        assert!(store.read_operation(&op4_id).await.is_err());
    }
}
